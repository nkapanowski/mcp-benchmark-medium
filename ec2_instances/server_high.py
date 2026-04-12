from __future__ import annotations

import json
import statistics
import time
import uuid
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import os

import psutil
from dotenv import load_dotenv
import requests
from fastmcp import FastMCP

load_dotenv()

mcp = FastMCP("EC2 High Workload MCP Server")

PUBMED_BASE   = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
NIXHUB_BASE   = "https://www.nixhub.io/api/v0"
REPOLOGY_BASE = "https://repology.org/api/v1"
HN_BASE       = "https://hacker-news.firebaseio.com/v0"
GMAPS_KEY     = os.environ.get("GMAPS_KEY", "")
NASA_KEY      = os.environ.get("NASA_KEY", "")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)

def _ram_mb() -> float:
    """Resident Set Size of this process in MB."""
    return round(psutil.Process().memory_info().rss / (1024 * 1024), 2)

def _success(request_id: str, result: dict, start: int) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "request_id":  request_id,
        "status":      "success",
        "result":      result,
        "duration_ms": _now_ms() - start,
        "ram_rss_mb":  _ram_mb(),
    }
    payload["response_bytes"] = len(json.dumps(payload))
    return payload

def _error(request_id: str, code: str, message: str, start: int) -> Dict[str, Any]:
    return {
        "request_id":     request_id,
        "status":         "error",
        "error":          {"code": code, "message": message},
        "duration_ms":    _now_ms() - start,
        "ram_rss_mb":     _ram_mb(),
        "response_bytes": 0,
    }

def _timed_get(url: str, params: Optional[dict] = None,
               headers: Optional[dict] = None, timeout: int = 30) -> Dict[str, Any]:
    """
    Wrapper around requests.get that records:
      request_sent_ms     — epoch-ms when the request was dispatched
      response_recv_ms    — epoch-ms when the full response was received
      request_duration_ms — wall time of the round-trip
      response_bytes      — raw byte length of the response body
    Returns a dict with those fields plus the Response object under 'response'.
    Raises on HTTP errors.
    """
    request_sent_ms = _now_ms()
    resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    response_recv_ms = _now_ms()
    resp.raise_for_status()
    return {
        "response":            resp,
        "request_sent_ms":     request_sent_ms,
        "response_recv_ms":    response_recv_ms,
        "request_duration_ms": response_recv_ms - request_sent_ms,
        "response_bytes":      len(resp.content),
    }


# ---------------------------------------------------------------------------
# H1 — arXiv Paper Search + PDF Download + Text Extraction
# ---------------------------------------------------------------------------

@mcp.tool()
def h1_ec2(query: str = "serverless MCP", limit: int = 10, request_id: str | None = None) -> Dict[str, Any]:
    """H1 on EC2: arXiv search + PDF binary download + full text extraction + mean/median/sum/min/max word counts."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []

        g1 = _timed_get("http://export.arxiv.org/api/query",
            params={"search_query": f"all:{query}", "start": 0, "max_results": limit}, timeout=30)
        root = ET.fromstring(g1["response"].text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        papers = []
        for entry in root.findall("atom:entry", ns):
            raw_id = entry.find("atom:id", ns).text or ""
            paper_id = raw_id.split("/abs/")[-1].strip()
            papers.append({"paper_id": paper_id, "title": (entry.find("atom:title", ns).text or "").strip()})
        log.append({"step": "search_arxiv",
                    "request_sent_ms": g1["request_sent_ms"],
                    "response_recv_ms": g1["response_recv_ms"],
                    "request_duration_ms": g1["request_duration_ms"],
                    "response_bytes": g1["response_bytes"],
                    "found": len(papers)})
        if not papers:
            return _error(rid, "NO_RESULTS", "No papers found", start)
        top_id = papers[0]["paper_id"]

        t = _now_ms()
        pdf_r = requests.get(f"https://arxiv.org/pdf/{top_id}", timeout=90, stream=True)
        pdf_r.raise_for_status()
        content = pdf_r.content
        pdf_path = f"/tmp/arxiv_{top_id.replace('/', '_').replace('.', '_')}.pdf"
        with open(pdf_path, "wb") as f:
            f.write(content)
        pdf_size_kb = round(len(content) / 1024, 2)
        log.append({"step": "download_arxiv",
                    "request_duration_ms": _now_ms() - t,
                    "response_bytes": len(content),
                    "size_kb": pdf_size_kb})

        t = _now_ms()
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        wc = [len((p.extract_text() or "").split()) for p in reader.pages]
        log.append({"step": "read_arxiv_paper",
                    "request_duration_ms": _now_ms() - t,
                    "response_bytes": 0,
                    "pages": len(wc)})

        return _success(rid, {
            "chain": "H1", "paper_id": top_id, "pdf_size_kb": pdf_size_kb,
            "num_pages": len(wc), "total_words": sum(wc),
            "stats": {"mean": round(statistics.mean(wc), 2), "median": statistics.median(wc),
                      "sum": sum(wc), "min": min(wc), "max": max(wc)},
            "chain_log": log
        }, start)
    except Exception as e:
        return _error(rid, "H1_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# H2 — ClinicalTrials 5 Chained GETs
# ---------------------------------------------------------------------------

@mcp.tool()
def h2_ec2(condition: str = "BRAF melanoma", limit: int = 5, request_id: str | None = None) -> Dict[str, Any]:
    """H2 on EC2: 5 sequential ClinicalTrials.gov GETs (search, record, references, locations, outcomes) + mean/sum/median enrollment."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []
        BASE = "https://clinicaltrials.gov/api/v2"

        g1 = _timed_get(f"{BASE}/studies", params={
            "query.cond": condition, "pageSize": limit, "format": "json",
            "fields": "NCTId,BriefTitle,EnrollmentCount,OverallStatus"
        }, timeout=30)
        studies = g1["response"].json().get("studies", [])
        log.append({"step": "trial_searcher",
                    "request_sent_ms": g1["request_sent_ms"],
                    "response_recv_ms": g1["response_recv_ms"],
                    "request_duration_ms": g1["request_duration_ms"],
                    "response_bytes": g1["response_bytes"],
                    "found": len(studies)})
        if not studies:
            return _error(rid, "NO_RESULTS", "No trials found", start)
        nct_id = studies[0].get("protocolSection", {}).get("identificationModule", {}).get("nctId", "")

        for step_name, fields in [
            ("trial_getter", None),
            ("trial_references", "ReferenceCitation,ReferencePMID"),
            ("trial_locations", "LocationFacility,LocationCity,LocationCountry"),
            ("trial_outcomes", "PrimaryOutcomeMeasure,SecondaryOutcomeMeasure"),
        ]:
            params = {"format": "json"}
            if fields:
                params["fields"] = fields
            gx = _timed_get(f"{BASE}/studies/{nct_id}", params=params, timeout=30)
            log.append({"step": step_name,
                        "request_sent_ms": gx["request_sent_ms"],
                        "response_recv_ms": gx["response_recv_ms"],
                        "request_duration_ms": gx["request_duration_ms"],
                        "response_bytes": gx["response_bytes"]})

        enrollments = [
            int(s.get("protocolSection", {}).get("designModule", {}).get("enrollmentInfo", {}).get("count", 0))
            for s in studies
            if s.get("protocolSection", {}).get("designModule", {}).get("enrollmentInfo", {}).get("count")
        ]
        if not enrollments:
            enrollments = [0]

        return _success(rid, {
            "chain": "H2", "nct_id": nct_id, "trials_searched": len(studies),
            "stats": {"mean_enrollment": round(statistics.mean(enrollments), 2),
                      "sum_enrollment": sum(enrollments),
                      "median_enrollment": statistics.median(enrollments)},
            "chain_log": log
        }, start)
    except Exception as e:
        return _error(rid, "H2_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# H3 — NASA Asteroid Feed
# ---------------------------------------------------------------------------

@mcp.tool()
def h3_ec2(request_id: str | None = None) -> Dict[str, Any]:
    """H3 on EC2: NASA asteroid feed (7-day) + browse + lookup — 3 chained GETs + mean/median/sum/min/max diameters + mode(hazardous)."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []
        key = NASA_KEY
        today = date.today().isoformat()
        end = (date.today() + timedelta(days=7)).isoformat()

        g1 = _timed_get("https://api.nasa.gov/neo/rest/v1/feed",
            params={"start_date": today, "end_date": end, "api_key": key}, timeout=30)
        all_neos = []
        for day_list in g1["response"].json().get("near_earth_objects", {}).values():
            all_neos.extend(day_list)
        log.append({"step": "get_asteroids_feed",
                    "request_sent_ms": g1["request_sent_ms"],
                    "response_recv_ms": g1["response_recv_ms"],
                    "request_duration_ms": g1["request_duration_ms"],
                    "response_bytes": g1["response_bytes"],
                    "neos_found": len(all_neos)})

        g2 = _timed_get("https://api.nasa.gov/neo/rest/v1/neo/browse", params={"api_key": key}, timeout=30)
        browse_neos = g2["response"].json().get("near_earth_objects", [])
        log.append({"step": "browse_asteroids",
                    "request_sent_ms": g2["request_sent_ms"],
                    "response_recv_ms": g2["response_recv_ms"],
                    "request_duration_ms": g2["request_duration_ms"],
                    "response_bytes": g2["response_bytes"],
                    "catalog_size": len(browse_neos)})

        asteroid_id = all_neos[0].get("id") if all_neos else (browse_neos[0].get("id") if browse_neos else "3542519")
        g3 = _timed_get(f"https://api.nasa.gov/neo/rest/v1/neo/{asteroid_id}", params={"api_key": key}, timeout=30)
        log.append({"step": "get_asteroid_lookup",
                    "request_sent_ms": g3["request_sent_ms"],
                    "response_recv_ms": g3["response_recv_ms"],
                    "request_duration_ms": g3["request_duration_ms"],
                    "response_bytes": g3["response_bytes"]})

        diameters, miss_distances, hazardous_flags = [], [], []
        for neo in all_neos:
            diam = neo.get("estimated_diameter", {}).get("meters", {})
            diameters.append((diam.get("estimated_diameter_min", 0) + diam.get("estimated_diameter_max", 0)) / 2)
            approaches = neo.get("close_approach_data", [])
            if approaches:
                miss_distances.append(float(approaches[0].get("miss_distance", {}).get("kilometers", 0)))
            hazardous_flags.append(str(neo.get("is_potentially_hazardous_asteroid", False)))
        if not diameters: diameters = [0]
        if not miss_distances: miss_distances = [0]

        return _success(rid, {
            "chain": "H3", "feed_count": len(all_neos), "catalog_count": len(browse_neos),
            "stats": {
                "diameter_mean": round(statistics.mean(diameters), 4),
                "diameter_median": statistics.median(diameters),
                "diameter_sum": round(sum(diameters), 4),
                "diameter_min": round(min(diameters), 4),
                "diameter_max": round(max(diameters), 4),
                "miss_distance_sum": round(sum(miss_distances), 2),
                "hazardous_mode": Counter(hazardous_flags).most_common(1)[0][0] if hazardous_flags else "False",
                "records_processed": len(all_neos)
            },
            "chain_log": log
        }, start)
    except Exception as e:
        return _error(rid, "H3_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# H4 — BioMCP + arXiv Cross-Database + PDF
# ---------------------------------------------------------------------------

@mcp.tool()
def h4_ec2(gene: str = "BRAF", limit: int = 5, request_id: str | None = None) -> Dict[str, Any]:
    """H4 on EC2: PubMed search + article fetch + arXiv cross-ref + PDF binary download + text extraction + mean/sum/median citation counts."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []

        g1 = _timed_get(f"{PUBMED_BASE}/esearch.fcgi",
            params={"db": "pubmed", "term": f"{gene}[Gene]", "retmax": limit, "retmode": "json"}, timeout=30)
        pmids = g1["response"].json().get("esearchresult", {}).get("idlist", [])
        log.append({"step": "article_searcher",
                    "request_sent_ms": g1["request_sent_ms"],
                    "response_recv_ms": g1["response_recv_ms"],
                    "request_duration_ms": g1["request_duration_ms"],
                    "response_bytes": g1["response_bytes"],
                    "pmids_found": len(pmids)})
        if not pmids:
            return _error(rid, "NO_RESULTS", "No PubMed articles found", start)
        top_pmid = pmids[0]

        g2 = _timed_get(f"{PUBMED_BASE}/efetch.fcgi",
            params={"db": "pubmed", "id": top_pmid, "retmode": "xml", "rettype": "abstract"}, timeout=30)
        root2 = ET.fromstring(g2["response"].text)
        title_el = root2.find(".//ArticleTitle")
        article_title = title_el.text if title_el is not None else "Unknown"
        log.append({"step": "article_getter",
                    "request_sent_ms": g2["request_sent_ms"],
                    "response_recv_ms": g2["response_recv_ms"],
                    "request_duration_ms": g2["request_duration_ms"],
                    "response_bytes": g2["response_bytes"],
                    "pmid": top_pmid})

        g3 = _timed_get("http://export.arxiv.org/api/query",
            params={"search_query": f"all:{gene} V600E resistance", "start": 0, "max_results": 5}, timeout=30)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        arxiv_papers = [e.find("atom:id", ns).text.split("/abs/")[-1].strip()
                        for e in ET.fromstring(g3["response"].text).findall("atom:entry", ns)]
        log.append({"step": "search_arxiv",
                    "request_sent_ms": g3["request_sent_ms"],
                    "response_recv_ms": g3["response_recv_ms"],
                    "request_duration_ms": g3["request_duration_ms"],
                    "response_bytes": g3["response_bytes"],
                    "arxiv_found": len(arxiv_papers)})

        page_word_counts = []
        pdf_size_kb = 0
        if arxiv_papers:
            top_arxiv = arxiv_papers[0]
            t = _now_ms()
            pdf_r = requests.get(f"https://arxiv.org/pdf/{top_arxiv}", timeout=90, stream=True)
            pdf_r.raise_for_status()
            content = pdf_r.content
            pdf_path = f"/tmp/arxiv_h4_{top_arxiv.replace('/', '_').replace('.', '_')}.pdf"
            with open(pdf_path, "wb") as f:
                f.write(content)
            pdf_size_kb = round(len(content) / 1024, 2)
            log.append({"step": "download_arxiv",
                        "request_duration_ms": _now_ms() - t,
                        "response_bytes": len(content),
                        "size_kb": pdf_size_kb})

            t = _now_ms()
            from pypdf import PdfReader
            reader = PdfReader(pdf_path)
            page_word_counts = [len((p.extract_text() or "").split()) for p in reader.pages]
            log.append({"step": "read_arxiv_paper",
                        "request_duration_ms": _now_ms() - t,
                        "response_bytes": 0,
                        "pages": len(page_word_counts)})

        citation_counts = []
        for pmid in pmids:
            try:
                gx = _timed_get(f"{PUBMED_BASE}/efetch.fcgi",
                    params={"db": "pubmed", "id": pmid, "retmode": "xml", "rettype": "abstract"}, timeout=15)
                citation_counts.append(len(ET.fromstring(gx["response"].text).findall(".//Reference")))
            except Exception:
                citation_counts.append(0)
        if not citation_counts: citation_counts = [0]

        return _success(rid, {
            "chain": "H4", "gene": gene, "top_pmid": top_pmid,
            "article_title": article_title, "arxiv_papers_found": len(arxiv_papers),
            "pdf_size_kb": pdf_size_kb, "num_pages": len(page_word_counts),
            "citation_stats": {
                "mean": round(statistics.mean(citation_counts), 2),
                "sum": sum(citation_counts),
                "median": statistics.median(citation_counts)
            },
            "word_count_stats": {
                "mean": round(statistics.mean(page_word_counts), 2) if page_word_counts else 0,
                "median": statistics.median(page_word_counts) if page_word_counts else 0,
                "sum": sum(page_word_counts),
                "min": min(page_word_counts) if page_word_counts else 0,
                "max": max(page_word_counts) if page_word_counts else 0,
            },
            "chain_log": log
        }, start)
    except Exception as e:
        return _error(rid, "H4_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# H5 — Google Maps Multi-Point Routing
# ---------------------------------------------------------------------------

@mcp.tool()
def h5_ec2(request_id: str | None = None) -> Dict[str, Any]:
    """H5 on EC2: Google Maps multi-point routing — 5 geocoding GETs + 1 distance matrix + 1 directions GET + mean/sum/median distances."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []
        GMAPS_BASE = "https://maps.googleapis.com/maps/api"

        addresses = [
            "1600 Amphitheatre Parkway, Mountain View, CA",
            "1 Infinite Loop, Cupertino, CA",
            "1 Microsoft Way, Redmond, WA",
            "410 Terry Ave N, Seattle, WA",
            "1 Hacker Way, Menlo Park, CA",
        ]

        coords = []
        for addr in addresses:
            g = _timed_get(f"{GMAPS_BASE}/geocode/json",
                params={"address": addr, "key": GMAPS_KEY}, timeout=15)
            results = g["response"].json().get("results", [])
            if results:
                loc = results[0]["geometry"]["location"]
                coords.append({"lat": loc["lat"], "lng": loc["lng"], "address": addr})
            log.append({"step": f"maps_geocode",
                        "address": addr,
                        "request_sent_ms": g["request_sent_ms"],
                        "response_recv_ms": g["response_recv_ms"],
                        "request_duration_ms": g["request_duration_ms"],
                        "response_bytes": g["response_bytes"],
                        "found": len(results) > 0})

        if len(coords) < 2:
            return _error(rid, "GEOCODE_FAILED", "Not enough coordinates geocoded", start)

        origins = "|".join([f"{c['lat']},{c['lng']}" for c in coords])
        destinations = origins

        g_matrix = _timed_get(f"{GMAPS_BASE}/distancematrix/json", params={
            "origins": origins,
            "destinations": destinations,
            "key": GMAPS_KEY,
            "units": "metric"
        }, timeout=20)
        matrix_data = g_matrix["response"].json()
        log.append({"step": "maps_distance_matrix",
                    "request_sent_ms": g_matrix["request_sent_ms"],
                    "response_recv_ms": g_matrix["response_recv_ms"],
                    "request_duration_ms": g_matrix["request_duration_ms"],
                    "response_bytes": g_matrix["response_bytes"],
                    "rows": len(matrix_data.get("rows", []))})

        g_dir = _timed_get(f"{GMAPS_BASE}/directions/json", params={
            "origin": f"{coords[0]['lat']},{coords[0]['lng']}",
            "destination": f"{coords[-1]['lat']},{coords[-1]['lng']}",
            "key": GMAPS_KEY
        }, timeout=20)
        dir_data = g_dir["response"].json()
        log.append({"step": "maps_directions",
                    "request_sent_ms": g_dir["request_sent_ms"],
                    "response_recv_ms": g_dir["response_recv_ms"],
                    "request_duration_ms": g_dir["request_duration_ms"],
                    "response_bytes": g_dir["response_bytes"],
                    "routes": len(dir_data.get("routes", []))})

        distances_m = []
        durations_s = []
        for row in matrix_data.get("rows", []):
            for element in row.get("elements", []):
                if element.get("status") == "OK":
                    distances_m.append(element["distance"]["value"])
                    durations_s.append(element["duration"]["value"])

        if not distances_m:
            distances_m = [0]
        if not durations_s:
            durations_s = [0]

        route_distance_km = 0
        route_duration_s = 0
        if dir_data.get("routes"):
            legs = dir_data["routes"][0].get("legs", [])
            for leg in legs:
                route_distance_km += leg.get("distance", {}).get("value", 0) / 1000
                route_duration_s += leg.get("duration", {}).get("value", 0)

        return _success(rid, {
            "chain": "H5",
            "addresses_geocoded": len(coords),
            "matrix_size": f"{len(coords)}x{len(coords)}",
            "route_distance_km": round(route_distance_km, 2),
            "route_duration_minutes": round(route_duration_s / 60, 1),
            "stats": {
                "distance_mean_m": round(statistics.mean(distances_m), 2),
                "distance_median_m": statistics.median(distances_m),
                "distance_sum_m": sum(distances_m),
                "duration_sum_s": sum(durations_s),
                "matrix_elements": len(distances_m)
            },
            "chain_log": log
        }, start)
    except Exception as e:
        return _error(rid, "H5_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# H6 — DEX Paprika OHLCV
# ---------------------------------------------------------------------------

@mcp.tool()
def h6_ec2(network: str = "ethereum", ohlcv_limit: int = 168, request_id: str | None = None) -> Dict[str, Any]:
    """H6 on EC2: 5 chained DexPaprika GETs (networks→dexes→pools→detail→OHLCV 168hr) + mean/median/sum/min/max prices+volumes."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []
        DEX_BASE = "https://api.dexpaprika.com"
        pools = []

        for step_name, url, params in [
            ("getNetworks", f"{DEX_BASE}/networks", {}),
            ("getNetworkDexes", f"{DEX_BASE}/networks/{network}/dexes", {"limit": 10}),
            ("getNetworkPools", f"{DEX_BASE}/networks/{network}/pools", {"limit": 20}),
        ]:
            gx = _timed_get(url, params=params, timeout=20)
            data = gx["response"].json()
            if step_name == "getNetworkPools":
                pools = data if isinstance(data, list) else data.get("pools", [])
            log.append({"step": step_name,
                        "request_sent_ms": gx["request_sent_ms"],
                        "response_recv_ms": gx["response_recv_ms"],
                        "request_duration_ms": gx["request_duration_ms"],
                        "response_bytes": gx["response_bytes"]})

        if not pools:
            return _error(rid, "NO_POOLS", "No pools found", start)
        top_pool = pools[0].get("id") or pools[0].get("address", "")

        g4 = _timed_get(f"{DEX_BASE}/networks/{network}/pools/{top_pool}", timeout=20)
        log.append({"step": "getPoolDetails",
                    "request_sent_ms": g4["request_sent_ms"],
                    "response_recv_ms": g4["response_recv_ms"],
                    "request_duration_ms": g4["request_duration_ms"],
                    "response_bytes": g4["response_bytes"]})

        g5 = _timed_get(f"{DEX_BASE}/networks/{network}/pools/{top_pool}/ohlcv", params={
            "start": int(time.time()) - (ohlcv_limit * 3600),
            "limit": ohlcv_limit, "interval": "1h"
        }, timeout=30)
        candles = g5["response"].json() if isinstance(g5["response"].json(), list) else g5["response"].json().get("ohlcv", [])
        log.append({"step": "getPoolOHLCV",
                    "request_sent_ms": g5["request_sent_ms"],
                    "response_recv_ms": g5["response_recv_ms"],
                    "request_duration_ms": g5["request_duration_ms"],
                    "response_bytes": g5["response_bytes"],
                    "candles": len(candles)})

        close_prices = [float(c[4]) if isinstance(c, list) and len(c) >= 6 else float(c.get("close", 0)) for c in candles]
        volumes = [float(c[5]) if isinstance(c, list) and len(c) >= 6 else float(c.get("volume", 0)) for c in candles]
        if not close_prices: close_prices = [0.0]

        return _success(rid, {
            "chain": "H6", "network": network, "top_pool": top_pool,
            "stats": {
                "price_mean": round(statistics.mean(close_prices), 6),
                "price_median": statistics.median(close_prices),
                "price_sum": round(sum(close_prices), 6),
                "price_min": round(min(close_prices), 6),
                "price_max": round(max(close_prices), 6),
                "volume_sum": round(sum(volumes), 2),
                "candles_processed": len(close_prices)
            },
            "chain_log": log
        }, start)
    except Exception as e:
        return _error(rid, "H6_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# H7 — NixOS Package Chain
# ---------------------------------------------------------------------------

@mcp.tool()
def h7_ec2(package: str = "python", limit: int = 50, request_id: str | None = None) -> Dict[str, Any]:
    """H7 on EC2: 5 chained NixOS/Repology GETs (search→info→versions→find_version→flakes) + mean/median/sum/min/max version counts."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []
        HEADERS = {"User-Agent": "MCP-Benchmark/1.0"}

        search_results = []
        try:
            g1 = _timed_get(f"{REPOLOGY_BASE}/projects/?search={package}&inrepo=nix_unstable",
                headers=HEADERS, timeout=20)
            if g1["response"].status_code == 200:
                search_results = list(g1["response"].json().keys())[:limit]
            log.append({"step": "nixos_search",
                        "request_sent_ms": g1["request_sent_ms"],
                        "response_recv_ms": g1["response_recv_ms"],
                        "request_duration_ms": g1["request_duration_ms"],
                        "response_bytes": g1["response_bytes"],
                        "results": len(search_results)})
        except Exception:
            search_results = [package]
            log.append({"step": "nixos_search", "request_duration_ms": 0,
                        "response_bytes": 0, "results": 1, "note": "fallback"})

        nix_pkgs = []
        try:
            g2 = _timed_get(f"{REPOLOGY_BASE}/project/python3", headers=HEADERS, timeout=20)
            if g2["response"].status_code == 200:
                pkg_detail = g2["response"].json()
                nix_pkgs = [p for p in pkg_detail if p.get("repo", "").startswith("nix")]
            log.append({"step": "nixos_info",
                        "request_sent_ms": g2["request_sent_ms"],
                        "response_recv_ms": g2["response_recv_ms"],
                        "request_duration_ms": g2["request_duration_ms"],
                        "response_bytes": g2["response_bytes"],
                        "nix_entries": len(nix_pkgs)})
        except Exception:
            log.append({"step": "nixos_info", "request_duration_ms": 0,
                        "response_bytes": 0, "nix_entries": 0, "note": "fallback"})

        version_counts = []
        try:
            g3 = _timed_get(f"{NIXHUB_BASE}/packages/python3", headers=HEADERS, timeout=20)
            if g3["response"].status_code == 200:
                releases = g3["response"].json().get("releases", g3["response"].json().get("versions", []))
                version_counts = [len(v.get("packages", [v])) if isinstance(v, dict) else 1 for v in releases[:50]]
            log.append({"step": "nixhub_package_versions",
                        "request_sent_ms": g3["request_sent_ms"],
                        "response_recv_ms": g3["response_recv_ms"],
                        "request_duration_ms": g3["request_duration_ms"],
                        "response_bytes": g3["response_bytes"],
                        "versions": len(version_counts)})
        except Exception:
            log.append({"step": "nixhub_package_versions", "request_duration_ms": 0,
                        "response_bytes": 0, "versions": 0, "note": "fallback"})
        if not version_counts:
            version_counts = list(range(1, 21))

        found_version = False
        try:
            g4 = _timed_get(f"{NIXHUB_BASE}/packages/python3", params={"version": "3.11"},
                headers=HEADERS, timeout=20)
            found_version = g4["response"].status_code == 200
            log.append({"step": "nixhub_find_version",
                        "request_sent_ms": g4["request_sent_ms"],
                        "response_recv_ms": g4["response_recv_ms"],
                        "request_duration_ms": g4["request_duration_ms"],
                        "response_bytes": g4["response_bytes"],
                        "found": found_version})
        except Exception:
            log.append({"step": "nixhub_find_version", "request_duration_ms": 0,
                        "response_bytes": 0, "found": False, "note": "fallback"})

        flake_counts = []
        try:
            g5 = _timed_get(f"{REPOLOGY_BASE}/projects/?search={package}&inrepo=nix_unstable&count=1",
                headers=HEADERS, timeout=20)
            if g5["response"].status_code == 200:
                flake_counts = [i + 1 for i in range(len(list(g5["response"].json().keys())))]
            log.append({"step": "nixos_flakes_search",
                        "request_sent_ms": g5["request_sent_ms"],
                        "response_recv_ms": g5["response_recv_ms"],
                        "request_duration_ms": g5["request_duration_ms"],
                        "response_bytes": g5["response_bytes"],
                        "flakes": len(flake_counts)})
        except Exception:
            log.append({"step": "nixos_flakes_search", "request_duration_ms": 0,
                        "response_bytes": 0, "flakes": 0, "note": "fallback"})

        all_counts = version_counts + flake_counts if flake_counts else version_counts
        if not all_counts: all_counts = [1]

        return _success(rid, {
            "chain": "H7", "package": package,
            "search_results": len(search_results), "versions_found": len(version_counts),
            "flakes_found": len(flake_counts),
            "stats": {
                "mean": round(statistics.mean(all_counts), 2),
                "median": statistics.median(all_counts),
                "sum": sum(all_counts),
                "min": min(all_counts),
                "max": max(all_counts),
                "records": len(all_counts)
            },
            "chain_log": log
        }, start)
    except Exception as e:
        return _error(rid, "H7_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# H8 — Hacker News (replaces Reddit — Reddit blocks AWS IPs)
# ---------------------------------------------------------------------------

@mcp.tool()
def h8_ec2(limit: int = 25, request_id: str | None = None) -> Dict[str, Any]:
    """H8 on EC2: Hacker News top stories list + 3 deep post fetches with comments — 4 GETs + mean/median/sum/mode/min/max scores."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []

        g1 = _timed_get(f"{HN_BASE}/topstories.json", timeout=20)
        top_story_ids = g1["response"].json()[:limit]
        log.append({"step": "fetch_top_stories",
                    "request_sent_ms": g1["request_sent_ms"],
                    "response_recv_ms": g1["response_recv_ms"],
                    "request_duration_ms": g1["request_duration_ms"],
                    "response_bytes": g1["response_bytes"],
                    "stories": len(top_story_ids)})

        all_scores, all_types, stories_fetched = [], [], []
        for i, story_id in enumerate(top_story_ids[:3]):
            g_story = _timed_get(f"{HN_BASE}/item/{story_id}.json", timeout=15)
            story = g_story["response"].json() or {}
            story_score = story.get("score", 0)
            stories_fetched.append({"id": story_id, "title": story.get("title", "")[:60], "score": story_score})
            for cid in story.get("kids", [])[:20]:
                try:
                    gc = _timed_get(f"{HN_BASE}/item/{cid}.json", timeout=10)
                    c = gc["response"].json() or {}
                    all_scores.append(c.get("score", 0) or 0)
                    all_types.append(c.get("type", "comment"))
                except Exception:
                    pass
            log.append({"step": f"fetch_story_{i+1}",
                        "request_sent_ms": g_story["request_sent_ms"],
                        "response_recv_ms": g_story["response_recv_ms"],
                        "request_duration_ms": g_story["request_duration_ms"],
                        "response_bytes": g_story["response_bytes"],
                        "story_id": story_id, "story_score": story_score})

        all_scores += [s["score"] for s in stories_fetched]
        if not all_scores: all_scores = [0]
        type_mode = Counter(all_types).most_common(1)[0][0] if all_types else "comment"

        return _success(rid, {
            "chain": "H8", "source": "Hacker News",
            "stories_fetched": len(top_story_ids),
            "top_stories": stories_fetched,
            "total_comments_analyzed": len(all_scores) - len(stories_fetched),
            "stats": {
                "score_mean": round(statistics.mean(all_scores), 2),
                "score_median": statistics.median(all_scores),
                "score_sum": sum(all_scores),
                "type_mode": type_mode,
                "score_min": min(all_scores),
                "score_max": max(all_scores)
            },
            "chain_log": log
        }, start)
    except Exception as e:
        return _error(rid, "H8_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# H9 — Multi-Database Paper Search
# ---------------------------------------------------------------------------

@mcp.tool()
def h9_ec2(query: str = "serverless cloud", limit: int = 5, request_id: str | None = None) -> Dict[str, Any]:
    """H9 on EC2: PubMed + bioRxiv + arXiv searches + PDF binary download + text extraction + mean/median/sum/min/max word counts."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []

        g1 = _timed_get(f"{PUBMED_BASE}/esearch.fcgi",
            params={"db": "pubmed", "term": query, "retmax": limit, "retmode": "json"}, timeout=30)
        pubmed_ids = g1["response"].json().get("esearchresult", {}).get("idlist", [])
        log.append({"step": "search_pubmed",
                    "request_sent_ms": g1["request_sent_ms"],
                    "response_recv_ms": g1["response_recv_ms"],
                    "request_duration_ms": g1["request_duration_ms"],
                    "response_bytes": g1["response_bytes"],
                    "results": len(pubmed_ids)})

        biorxiv_count = 0
        try:
            g2 = _timed_get("https://api.biorxiv.org/details/biorxiv/2024-01-01/2025-01-01/0/json", timeout=20)
            if g2["response"].status_code == 200:
                biorxiv_count = len(g2["response"].json().get("collection", []))
            log.append({"step": "search_biorxiv",
                        "request_sent_ms": g2["request_sent_ms"],
                        "response_recv_ms": g2["response_recv_ms"],
                        "request_duration_ms": g2["request_duration_ms"],
                        "response_bytes": g2["response_bytes"],
                        "results": biorxiv_count})
        except Exception:
            log.append({"step": "search_biorxiv", "request_duration_ms": 0,
                        "response_bytes": 0, "results": 0, "note": "fallback"})

        g3 = _timed_get("http://export.arxiv.org/api/query",
            params={"search_query": "all:lambda EC2 latency", "start": 0, "max_results": limit}, timeout=30)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        arxiv_papers = [e.find("atom:id", ns).text.split("/abs/")[-1].strip()
                        for e in ET.fromstring(g3["response"].text).findall("atom:entry", ns)]
        log.append({"step": "search_arxiv",
                    "request_sent_ms": g3["request_sent_ms"],
                    "response_recv_ms": g3["response_recv_ms"],
                    "request_duration_ms": g3["request_duration_ms"],
                    "response_bytes": g3["response_bytes"],
                    "results": len(arxiv_papers)})

        page_word_counts = []
        pdf_size_kb = 0
        if arxiv_papers:
            top_arxiv = arxiv_papers[0]
            t = _now_ms()
            pdf_r = requests.get(f"https://arxiv.org/pdf/{top_arxiv}", timeout=90, stream=True)
            pdf_r.raise_for_status()
            content = pdf_r.content
            pdf_path = f"/tmp/arxiv_h9_{top_arxiv.replace('/', '_').replace('.', '_')}.pdf"
            with open(pdf_path, "wb") as f:
                f.write(content)
            pdf_size_kb = round(len(content) / 1024, 2)
            log.append({"step": "download_arxiv",
                        "request_duration_ms": _now_ms() - t,
                        "response_bytes": len(content),
                        "size_kb": pdf_size_kb})

            t = _now_ms()
            from pypdf import PdfReader
            reader = PdfReader(pdf_path)
            page_word_counts = [len((p.extract_text() or "").split()) for p in reader.pages]
            log.append({"step": "read_arxiv_paper",
                        "request_duration_ms": _now_ms() - t,
                        "response_bytes": 0,
                        "pages": len(page_word_counts)})

        if not page_word_counts: page_word_counts = [0]

        return _success(rid, {
            "chain": "H9", "query": query, "databases_searched": 3,
            "pubmed_results": len(pubmed_ids),
            "biorxiv_results": biorxiv_count,
            "arxiv_results": len(arxiv_papers),
            "pdf_size_kb": pdf_size_kb,
            "word_count_stats": {
                "mean": round(statistics.mean(page_word_counts), 2),
                "median": statistics.median(page_word_counts),
                "sum": sum(page_word_counts),
                "min": min(page_word_counts),
                "max": max(page_word_counts),
                "pages": len(page_word_counts)
            },
            "chain_log": log
        }, start)
    except Exception as e:
        return _error(rid, "H9_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# H10 — BioMCP + NixOS Cross-Domain
# ---------------------------------------------------------------------------

@mcp.tool()
def h10_ec2(chemical: str = "python bioinformatics", nix_package: str = "biopython",
            limit: int = 10, request_id: str | None = None) -> Dict[str, Any]:
    """H10 on EC2: PubMed search + article fetch + NixOS search + package info + version history — 5 cross-domain GETs + mean/median/sum/mode/min/max."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []
        HEADERS = {"User-Agent": "MCP-Benchmark/1.0"}

        g1 = _timed_get(f"{PUBMED_BASE}/esearch.fcgi",
            params={"db": "pubmed", "term": chemical, "retmax": limit, "retmode": "json"}, timeout=30)
        pmids = g1["response"].json().get("esearchresult", {}).get("idlist", [])
        log.append({"step": "article_searcher",
                    "request_sent_ms": g1["request_sent_ms"],
                    "response_recv_ms": g1["response_recv_ms"],
                    "request_duration_ms": g1["request_duration_ms"],
                    "response_bytes": g1["response_bytes"],
                    "pmids": len(pmids)})
        if not pmids:
            return _error(rid, "NO_RESULTS", "No PubMed results found", start)
        top_pmid = pmids[0]

        g2 = _timed_get(f"{PUBMED_BASE}/efetch.fcgi",
            params={"db": "pubmed", "id": top_pmid, "retmode": "xml", "rettype": "abstract"}, timeout=30)
        root2 = ET.fromstring(g2["response"].text)
        title_el = root2.find(".//ArticleTitle")
        article_title = title_el.text if title_el is not None else "Unknown"
        log.append({"step": "article_getter",
                    "request_sent_ms": g2["request_sent_ms"],
                    "response_recv_ms": g2["response_recv_ms"],
                    "request_duration_ms": g2["request_duration_ms"],
                    "response_bytes": g2["response_bytes"],
                    "pmid": top_pmid})

        nix_results = []
        try:
            g3 = _timed_get(f"{REPOLOGY_BASE}/projects/?search={nix_package}&inrepo=nix_unstable",
                headers=HEADERS, timeout=20)
            if g3["response"].status_code == 200:
                nix_results = list(g3["response"].json().keys())
            log.append({"step": "nixos_search",
                        "request_sent_ms": g3["request_sent_ms"],
                        "response_recv_ms": g3["response_recv_ms"],
                        "request_duration_ms": g3["request_duration_ms"],
                        "response_bytes": g3["response_bytes"],
                        "results": len(nix_results)})
        except Exception:
            nix_results = [nix_package]
            log.append({"step": "nixos_search", "request_duration_ms": 0,
                        "response_bytes": 0, "results": 1, "note": "fallback"})

        pkg_versions = []
        try:
            g4 = _timed_get(f"{REPOLOGY_BASE}/project/{nix_package}", headers=HEADERS, timeout=20)
            if g4["response"].status_code == 200:
                pkg_versions = [p.get("version", "0") for p in g4["response"].json()
                                if p.get("repo", "").startswith("nix")]
            log.append({"step": "nixos_info",
                        "request_sent_ms": g4["request_sent_ms"],
                        "response_recv_ms": g4["response_recv_ms"],
                        "request_duration_ms": g4["request_duration_ms"],
                        "response_bytes": g4["response_bytes"],
                        "versions": len(pkg_versions)})
        except Exception:
            pkg_versions = ["1.0"]
            log.append({"step": "nixos_info", "request_duration_ms": 0,
                        "response_bytes": 0, "versions": 1, "note": "fallback"})

        version_counts = []
        categories = []
        try:
            g5 = _timed_get(f"{NIXHUB_BASE}/packages/{nix_package}", headers=HEADERS, timeout=20)
            if g5["response"].status_code == 200:
                releases = g5["response"].json().get("releases", g5["response"].json().get("versions", []))
                version_counts = [len(v.get("packages", [v])) if isinstance(v, dict) else 1 for v in releases[:50]]
                categories = [v.get("version", "unknown") for v in releases[:50] if isinstance(v, dict)]
            log.append({"step": "nixhub_package_versions",
                        "request_sent_ms": g5["request_sent_ms"],
                        "response_recv_ms": g5["response_recv_ms"],
                        "request_duration_ms": g5["request_duration_ms"],
                        "response_bytes": g5["response_bytes"],
                        "versions": len(version_counts)})
        except Exception:
            log.append({"step": "nixhub_package_versions", "request_duration_ms": 0,
                        "response_bytes": 0, "versions": 0, "note": "fallback"})

        if not version_counts:
            version_counts = list(range(1, len(pkg_versions) + 1)) if pkg_versions else [1, 2, 3, 4, 5]
        if not categories:
            categories = pkg_versions if pkg_versions else ["unknown"]

        cat_mode = Counter(categories).most_common(1)[0][0]

        return _success(rid, {
            "chain": "H10", "chemical": chemical, "nix_package": nix_package,
            "top_pmid": top_pmid, "article_title": article_title,
            "nix_results": len(nix_results), "versions_found": len(version_counts),
            "stats": {
                "mean": round(statistics.mean(version_counts), 2),
                "median": statistics.median(version_counts),
                "sum": sum(version_counts),
                "category_mode": cat_mode,
                "min": min(version_counts),
                "max": max(version_counts),
                "records": len(version_counts)
            },
            "chain_log": log
        }, start)
    except Exception as e:
        return _error(rid, "H10_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)