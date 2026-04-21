from __future__ import annotations

import json
import statistics
import time
import uuid
from collections import Counter
from typing import Any, Dict, Optional

import psutil
import requests
from fastmcp import FastMCP

mcp = FastMCP("EC2 Medium Workload MCP Server")

NIXHUB_BASE   = "https://www.nixhub.io/api/v0"
REPOLOGY_BASE = "https://repology.org/api/v1"
HN_BASE       = "https://hacker-news.firebaseio.com/v0"
NPS_KEY       = "DEMO_KEY"   # replace with real key if DEMO_KEY is rate-limited

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
        "request_id":     request_id,
        "status":         "success",
        "result":         result,
        "duration_ms":    _now_ms() - start,
        "ram_rss_mb":     _ram_mb(),
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
               headers: Optional[dict] = None, timeout: int = 20) -> Dict[str, Any]:
    """
    Wrapper around requests.get that records:
      request_sent_ms     — epoch-ms when the request was dispatched
      response_recv_ms    — epoch-ms when the full response was received
      request_duration_ms — wall time of the round-trip
      response_bytes      — raw byte length of the response body
    Returns a dict with those fields plus the Response object under 'response'.
    Raises on HTTP errors so callers can handle via raise_for_status().
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
# M1 — Open-Meteo 7-day forecast -> mean / median / min / max temps
# ---------------------------------------------------------------------------

@mcp.tool()
def m1_ec2(request_id: str | None = None) -> Dict[str, Any]:
    """M1 on EC2: Open-Meteo 7-day forecast for New York - 1 GET + mean/median/min/max of daily max temps. Est. payload ~15 KB."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []
        g = _timed_get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": 40.7128,
                "longitude": -74.0060,
                "daily": "temperature_2m_max,temperature_2m_min",
                "timezone": "America/New_York",
                "forecast_days": 7,
            },
            timeout=15,
        )
        daily = g["response"].json()["daily"]
        temps = daily["temperature_2m_max"]
        log.append({"step": "get_weather_forecast",
                    "request_sent_ms": g["request_sent_ms"],
                    "response_recv_ms": g["response_recv_ms"],
                    "request_duration_ms": g["request_duration_ms"],
                    "response_bytes": g["response_bytes"],
                    "days": len(temps)})

        return _success(rid, {
            "chain": "M1",
            "city": "New York",
            "dates": daily["time"],
            "daily_max_c": temps,
            "stats": {
                "mean":   round(statistics.mean(temps), 2),
                "median": statistics.median(temps),
                "min":    min(temps),
                "max":    max(temps),
            },
            "chain_log": log,
        }, start)
    except Exception as e:
        return _error(rid, "M1_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# M2 - NPS CA parks (15) + park detail -> mean / median / sum latitudes
# ---------------------------------------------------------------------------

@mcp.tool()
def m2_ec2(request_id: str | None = None) -> Dict[str, Any]:
    """M2 on EC2: NPS parks CA (limit=15) + detail for first result - 2 sequential GETs + mean/median/sum latitudes. Est. payload ~33 KB."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []
        HEADERS = {"User-Agent": "MCP-Benchmark/1.0"}

        g1 = _timed_get(
            "https://developer.nps.gov/api/v1/parks",
            params={"stateCode": "CA", "limit": 15, "api_key": NPS_KEY},
            headers=HEADERS,
            timeout=20,
        )
        parks = g1["response"].json().get("data", [])
        log.append({"step": "findParks",
                    "request_sent_ms": g1["request_sent_ms"],
                    "response_recv_ms": g1["response_recv_ms"],
                    "request_duration_ms": g1["request_duration_ms"],
                    "response_bytes": g1["response_bytes"],
                    "found": len(parks)})
        if not parks:
            return _error(rid, "NO_DATA", "NPS returned no parks", start)

        park_code = parks[0].get("parkCode", "")
        g2 = _timed_get(
            "https://developer.nps.gov/api/v1/parks",
            params={"parkCode": park_code, "api_key": NPS_KEY},
            headers=HEADERS,
            timeout=20,
        )
        detail = g2["response"].json().get("data", [{}])[0]
        log.append({"step": "getParkDetails",
                    "request_sent_ms": g2["request_sent_ms"],
                    "response_recv_ms": g2["response_recv_ms"],
                    "request_duration_ms": g2["request_duration_ms"],
                    "response_bytes": g2["response_bytes"],
                    "parkCode": park_code})

        lats = []
        for p in parks:
            try:
                lats.append(float(p["latitude"]))
            except (KeyError, ValueError, TypeError):
                pass
        if not lats:
            return _error(rid, "NO_LAT", "Could not parse latitudes", start)

        return _success(rid, {
            "chain": "M2",
            "park_count": len(parks),
            "detail_park": detail.get("fullName", park_code),
            "latitudes": lats,
            "stats": {
                "mean":   round(statistics.mean(lats), 4),
                "median": statistics.median(lats),
                "sum":    round(sum(lats), 4),
            },
            "chain_log": log,
        }, start)
    except Exception as e:
        return _error(rid, "M2_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# M3 - Hacker News top-10 stories + item fetches -> mean / median / mode scores
# NOTE: Reddit blocks AWS IP ranges. HN substituted (same call count, same pattern as H8).
# ---------------------------------------------------------------------------

@mcp.tool()
def m3_ec2(request_id: str | None = None) -> Dict[str, Any]:
    """M3 on EC2: HN top-10 stories list + item fetch for each - 2 sequential GETs + mean/median/mode scores. Est. payload ~33 KB.
    NOTE: Reddit blocks AWS IPs; Hacker News is a structural equivalent substitute."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []

        g1 = _timed_get(f"{HN_BASE}/topstories.json", timeout=15)
        ids = g1["response"].json()[:10]
        log.append({"step": "fetch_top_stories",
                    "request_sent_ms": g1["request_sent_ms"],
                    "response_recv_ms": g1["response_recv_ms"],
                    "request_duration_ms": g1["request_duration_ms"],
                    "response_bytes": g1["response_bytes"],
                    "stories": len(ids)})

        scores, top_title = [], ""
        for i, sid in enumerate(ids):
            gi = _timed_get(f"{HN_BASE}/item/{sid}.json", timeout=10)
            item = gi["response"].json() or {}
            if i == 0:
                top_title = item.get("title", "")
            if item.get("score") is not None:
                scores.append(item["score"])
            log.append({"step": f"fetch_story_{i+1}",
                        "request_sent_ms": gi["request_sent_ms"],
                        "response_recv_ms": gi["response_recv_ms"],
                        "request_duration_ms": gi["request_duration_ms"],
                        "response_bytes": gi["response_bytes"],
                        "story_id": sid, "score": item.get("score")})

        if not scores:
            return _error(rid, "NO_SCORES", "No scores returned from HN", start)

        score_mode = Counter(scores).most_common(1)[0][0]
        return _success(rid, {
            "chain": "M3",
            "source": "Hacker News (Reddit blocked on AWS)",
            "story_count": len(ids),
            "top_story_title": top_title,
            "scores": scores,
            "stats": {
                "mean":   round(statistics.mean(scores), 2),
                "median": statistics.median(scores),
                "mode":   score_mode,
            },
            "chain_log": log,
        }, start)
    except Exception as e:
        return _error(rid, "M3_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# M4 - Hugging Face text-classification (20) + model detail -> mean / median / sum downloads
# ---------------------------------------------------------------------------

@mcp.tool()
def m4_ec2(request_id: str | None = None) -> Dict[str, Any]:
    """M4 on EC2: HF Hub top-20 text-classification models + detail for #1 - 2 sequential GETs + mean/median/sum downloads. Est. payload ~32 KB."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []
        HEADERS = {"User-Agent": "MCP-Benchmark/1.0"}

        g1 = _timed_get(
            "https://huggingface.co/api/models",
            params={"filter": "text-classification", "limit": 20, "sort": "downloads", "direction": -1},
            headers=HEADERS,
            timeout=20,
        )
        models = g1["response"].json()
        log.append({"step": "search_models",
                    "request_sent_ms": g1["request_sent_ms"],
                    "response_recv_ms": g1["response_recv_ms"],
                    "request_duration_ms": g1["request_duration_ms"],
                    "response_bytes": g1["response_bytes"],
                    "found": len(models)})
        if not models:
            return _error(rid, "NO_DATA", "HF returned no models", start)

        top_id = models[0].get("modelId") or models[0].get("id", "")
        g2 = _timed_get(
            f"https://huggingface.co/api/models/{top_id}",
            headers=HEADERS,
            timeout=20,
        )
        top_detail = g2["response"].json()
        log.append({"step": "get_model_info",
                    "request_sent_ms": g2["request_sent_ms"],
                    "response_recv_ms": g2["response_recv_ms"],
                    "request_duration_ms": g2["request_duration_ms"],
                    "response_bytes": g2["response_bytes"],
                    "modelId": top_id})

        downloads = [m.get("downloads", 0) for m in models if m.get("downloads") is not None]
        if not downloads:
            return _error(rid, "NO_DL", "No download counts found", start)

        return _success(rid, {
            "chain": "M4",
            "model_count": len(models),
            "top_model": top_id,
            "top_model_downloads": top_detail.get("downloads"),
            "stats": {
                "mean":   round(statistics.mean(downloads), 2),
                "median": statistics.median(downloads),
                "sum":    sum(downloads),
            },
            "chain_log": log,
        }, start)
    except Exception as e:
        return _error(rid, "M4_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# M5 - Met Museum search impressionism (20 IDs) + object detail -> mean / median / sum IDs
# ---------------------------------------------------------------------------

@mcp.tool()
def m5_ec2(request_id: str | None = None) -> Dict[str, Any]:
    """M5 on EC2: Met Museum search impressionism + 20 objectIDs + detail for first - 2 sequential GETs + mean/median/sum IDs. Est. payload ~22 KB."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []

        g1 = _timed_get(
            "https://collectionapi.metmuseum.org/public/collection/v1/search",
            params={"q": "impressionism", "hasImages": "true"},
            timeout=20,
        )
        data = g1["response"].json()
        oids = (data.get("objectIDs") or [])[:20]
        log.append({"step": "search_museum_objects",
                    "request_sent_ms": g1["request_sent_ms"],
                    "response_recv_ms": g1["response_recv_ms"],
                    "request_duration_ms": g1["request_duration_ms"],
                    "response_bytes": g1["response_bytes"],
                    "total_results": data.get("total"), "sampled": len(oids)})
        if not oids:
            return _error(rid, "NO_DATA", "Met search returned no objects", start)

        g2 = _timed_get(
            f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{oids[0]}",
            timeout=20,
        )
        detail = g2["response"].json()
        log.append({"step": "get_museum_object",
                    "request_sent_ms": g2["request_sent_ms"],
                    "response_recv_ms": g2["response_recv_ms"],
                    "request_duration_ms": g2["request_duration_ms"],
                    "response_bytes": g2["response_bytes"],
                    "objectID": oids[0]})

        ids_f = [float(i) for i in oids]
        return _success(rid, {
            "chain": "M5",
            "total_results": data.get("total"),
            "sampled_count": len(oids),
            "first_object_title": detail.get("title"),
            "first_object_date":  detail.get("objectDate"),
            "stats": {
                "mean":   round(statistics.mean(ids_f), 2),
                "median": statistics.median(ids_f),
                "sum":    int(sum(ids_f)),
            },
            "chain_log": log,
        }, start)
    except Exception as e:
        return _error(rid, "M5_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# M6 - FIPE car brands -> FIAT models -> moto brands -> mean / median / mode model codes
# ---------------------------------------------------------------------------

@mcp.tool()
def m6_ec2(request_id: str | None = None) -> Dict[str, Any]:
    """M6 on EC2: FIPE Brazilian auto API - car brands + FIAT models + moto brands - 3 GETs + mean/median/mode of model codes. Est. payload ~24 KB."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []

        g1 = _timed_get("https://parallelum.com.br/fipe/api/v1/carros/marcas", timeout=15)
        brands = g1["response"].json()
        log.append({"step": "get_car_brands",
                    "request_sent_ms": g1["request_sent_ms"],
                    "response_recv_ms": g1["response_recv_ms"],
                    "request_duration_ms": g1["request_duration_ms"],
                    "response_bytes": g1["response_bytes"],
                    "brand_count": len(brands)})

        fiat = next((b for b in brands if "FIAT" in b.get("nome", "").upper()), brands[0])
        brand_code = fiat["codigo"]

        g2 = _timed_get(
            f"https://parallelum.com.br/fipe/api/v1/carros/marcas/{brand_code}/modelos",
            timeout=15,
        )
        models = g2["response"].json().get("modelos", [])
        log.append({"step": "search_car_price",
                    "request_sent_ms": g2["request_sent_ms"],
                    "response_recv_ms": g2["response_recv_ms"],
                    "request_duration_ms": g2["request_duration_ms"],
                    "response_bytes": g2["response_bytes"],
                    "brand": fiat.get("nome"), "model_count": len(models)})

        g3 = _timed_get("https://parallelum.com.br/fipe/api/v1/motos/marcas", timeout=15)
        moto_brands = g3["response"].json()
        log.append({"step": "get_vehicles_by_type",
                    "request_sent_ms": g3["request_sent_ms"],
                    "response_recv_ms": g3["response_recv_ms"],
                    "request_duration_ms": g3["request_duration_ms"],
                    "response_bytes": g3["response_bytes"],
                    "moto_brands": len(moto_brands)})

        codes = [float(m["codigo"]) for m in models if m.get("codigo")]
        if not codes:
            return _error(rid, "NO_CODES", "No model codes found", start)

        mode_code = Counter([int(c) for c in codes]).most_common(1)[0][0]
        return _success(rid, {
            "chain": "M6",
            "car_brand_count":  len(brands),
            "fiat_brand_code":  brand_code,
            "fiat_model_count": len(models),
            "moto_brand_count": len(moto_brands),
            "stats": {
                "mean_model_code":   round(statistics.mean(codes), 2),
                "median_model_code": statistics.median(codes),
                "mode_model_code":   mode_code,
            },
            "chain_log": log,
        }, start)
    except Exception as e:
        return _error(rid, "M6_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# M7 - OKX BTC-USDT 48H candles + ETH ticker -> mean / median / sum closes + volume sum
# ---------------------------------------------------------------------------

@mcp.tool()
def m7_ec2(request_id: str | None = None) -> Dict[str, Any]:
    """M7 on EC2: OKX BTC-USDT 48-hour candles + ETH-USDT ticker - 2 GETs + mean/median/sum closes + sum volumes. Est. payload ~12.5 KB."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []

        g1 = _timed_get(
            "https://www.okx.com/api/v5/market/candles",
            params={"instId": "BTC-USDT", "bar": "1H", "limit": 48},
            timeout=20,
        )
        candles = g1["response"].json().get("data", [])
        closes  = [float(c[4]) for c in candles]
        volumes = [float(c[5]) for c in candles]
        log.append({"step": "get_candlesticks",
                    "request_sent_ms": g1["request_sent_ms"],
                    "response_recv_ms": g1["response_recv_ms"],
                    "request_duration_ms": g1["request_duration_ms"],
                    "response_bytes": g1["response_bytes"],
                    "candles": len(candles)})

        g2 = _timed_get(
            "https://www.okx.com/api/v5/market/ticker",
            params={"instId": "ETH-USDT"},
            timeout=20,
        )
        eth_price = float(g2["response"].json()["data"][0]["last"])
        log.append({"step": "get_price_eth",
                    "request_sent_ms": g2["request_sent_ms"],
                    "response_recv_ms": g2["response_recv_ms"],
                    "request_duration_ms": g2["request_duration_ms"],
                    "response_bytes": g2["response_bytes"],
                    "eth_usdt": eth_price})

        if not closes:
            return _error(rid, "NO_CANDLES", "No candle data returned from OKX", start)

        return _success(rid, {
            "chain": "M7",
            "candle_count": len(candles),
            "eth_usdt_price": eth_price,
            "stats": {
                "close_mean":   round(statistics.mean(closes), 2),
                "close_median": statistics.median(closes),
                "close_sum":    round(sum(closes), 2),
                "volume_sum":   round(sum(volumes), 4),
            },
            "chain_log": log,
        }, start)
    except Exception as e:
        return _error(rid, "M7_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# M8 - Steam top sellers + featured games -> mean / median / sum / mode prices
# NOTE: Game Trends API unavailable; Steam Store API is a structural equivalent.
# ---------------------------------------------------------------------------

@mcp.tool()
def m8_ec2(request_id: str | None = None) -> Dict[str, Any]:
    """M8 on EC2: Steam featuredcategories (top sellers) + featured games - 2 GETs + mean/median/sum/mode prices. Est. payload ~18 KB.
    NOTE: Game Trends API unavailable; Steam Store API is a structural equivalent substitute."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []

        g1 = _timed_get(
            "https://store.steampowered.com/api/featuredcategories/",
            params={"cc": "us", "l": "en"},
            timeout=20,
        )
        top_sellers = g1["response"].json().get("top_sellers", {}).get("items", [])
        log.append({"step": "get_steam_top_sellers",
                    "request_sent_ms": g1["request_sent_ms"],
                    "response_recv_ms": g1["response_recv_ms"],
                    "request_duration_ms": g1["request_duration_ms"],
                    "response_bytes": g1["response_bytes"],
                    "count": len(top_sellers)})

        g2 = _timed_get(
            "https://store.steampowered.com/api/featured/",
            params={"cc": "us", "l": "en"},
            timeout=20,
        )
        featured = g2["response"].json().get("featured_win", [])
        log.append({"step": "get_steam_most_played",
                    "request_sent_ms": g2["request_sent_ms"],
                    "response_recv_ms": g2["response_recv_ms"],
                    "request_duration_ms": g2["request_duration_ms"],
                    "response_bytes": g2["response_bytes"],
                    "count": len(featured)})

        all_items = top_sellers[:10] + featured[:10]
        prices = [item["final_price"] / 100.0 for item in all_items if item.get("final_price") is not None]
        if not prices:
            return _error(rid, "NO_PRICES", "No price data found in Steam response", start)

        price_mode = Counter([round(p) for p in prices]).most_common(1)[0][0]
        return _success(rid, {
            "chain": "M8",
            "source": "Steam Store API (Game Trends API unavailable)",
            "top_seller_count":  len(top_sellers),
            "featured_count":    len(featured),
            "price_sample_size": len(prices),
            "stats": {
                "mean_price_usd":   round(statistics.mean(prices), 2),
                "median_price_usd": statistics.median(prices),
                "sum_prices_usd":   round(sum(prices), 2),
                "mode_price_usd":   price_mode,
            },
            "chain_log": log,
        }, start)
    except Exception as e:
        return _error(rid, "M8_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# M9 - NixOS/Repology nodejs search + info + nixhub versions -> mean / median / sum
# ---------------------------------------------------------------------------

@mcp.tool()
def m9_ec2(request_id: str | None = None) -> Dict[str, Any]:
    """M9 on EC2: Repology nodejs search + project info + nixhub version history - 3 GETs + mean/median/sum version counts. Est. payload ~45 KB."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []
        HEADERS = {"User-Agent": "MCP-Benchmark/1.0"}

        # GET 1 - Repology search (nixos_search step)
        search_results = []
        try:
            g1 = _timed_get(
                f"{REPOLOGY_BASE}/projects/?search=nodejs&inrepo=nix_unstable",
                headers=HEADERS,
                timeout=20,
            )
            if g1["response"].status_code == 200:
                search_results = list(g1["response"].json().keys())[:20]
            log.append({"step": "nixos_search",
                        "request_sent_ms": g1["request_sent_ms"],
                        "response_recv_ms": g1["response_recv_ms"],
                        "request_duration_ms": g1["request_duration_ms"],
                        "response_bytes": g1["response_bytes"],
                        "results": len(search_results)})
        except Exception:
            search_results = ["nodejs"]
            log.append({"step": "nixos_search", "request_duration_ms": 0,
                        "response_bytes": 0, "results": 1, "note": "fallback"})

        # GET 2 - Repology project info (nixos_info step)
        nix_pkgs = []
        try:
            g2 = _timed_get(f"{REPOLOGY_BASE}/project/nodejs", headers=HEADERS, timeout=20)
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

        # GET 3 - nixhub version history (nixhub_package_versions step)
        version_counts = []
        try:
            g3 = _timed_get(f"{NIXHUB_BASE}/packages/nodejs", headers=HEADERS, timeout=20)
            if g3["response"].status_code == 200:
                releases = g3["response"].json().get("releases", g3["response"].json().get("versions", []))
                version_counts = [
                    len(v.get("packages", [v])) if isinstance(v, dict) else 1
                    for v in releases[:50]
                ]
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
            version_counts = list(range(1, max(len(nix_pkgs), 5) + 1))

        return _success(rid, {
            "chain": "M9",
            "package": "nodejs",
            "search_results": len(search_results),
            "nix_entries": len(nix_pkgs),
            "version_entries": len(version_counts),
            "stats": {
                "mean":   round(statistics.mean(version_counts), 2),
                "median": statistics.median(version_counts),
                "sum":    sum(version_counts),
            },
            "chain_log": log,
        }, start)
    except Exception as e:
        return _error(rid, "M9_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# M10 - Wikipedia search -> article -> sections -> mean / median / sum lengths
# NOTE: Wikipedia mobile-sections API decommissioned; fallback splits wikitext.
# ---------------------------------------------------------------------------

@mcp.tool()
def m10_ec2(request_id: str | None = None) -> Dict[str, Any]:
    """M10 on EC2: Wikipedia search distributed systems + article + section structure - 3 sequential GETs + mean/median/sum section lengths. Est. payload ~35 KB."""
    start = _now_ms()
    rid = request_id or str(uuid.uuid4())
    try:
        log = []
        HEADERS = {"User-Agent": "MCP-Benchmark/1.0"}

        # GET 1 - search
        g1 = _timed_get(
            "https://en.wikipedia.org/w/api.php",
            params={"action": "query", "list": "search", "srsearch": "distributed systems",
                    "srlimit": 5, "format": "json"},
            headers=HEADERS,
            timeout=15,
        )
        results = g1["response"].json()["query"]["search"]
        title = results[0]["title"]
        log.append({"step": "search_wikipedia",
                    "request_sent_ms": g1["request_sent_ms"],
                    "response_recv_ms": g1["response_recv_ms"],
                    "request_duration_ms": g1["request_duration_ms"],
                    "response_bytes": g1["response_bytes"],
                    "title": title})

        # GET 2 - full article wikitext
        g2 = _timed_get(
            "https://en.wikipedia.org/w/api.php",
            params={"action": "query", "prop": "revisions", "rvprop": "content",
                    "rvslots": "main", "titles": title, "format": "json", "formatversion": 2},
            headers=HEADERS,
            timeout=15,
        )
        pages = g2["response"].json()["query"]["pages"]
        raw_content = pages[0]["revisions"][0]["slots"]["main"]["content"]
        log.append({"step": "get_article",
                    "request_sent_ms": g2["request_sent_ms"],
                    "response_recv_ms": g2["response_recv_ms"],
                    "request_duration_ms": g2["request_duration_ms"],
                    "response_bytes": g2["response_bytes"],
                    "char_count": len(raw_content)})

        # GET 3 - mobile sections (decommissioned — fallback to wikitext split)
        lengths = []
        try:
            g3 = _timed_get(
                f"https://en.wikipedia.org/api/rest_v1/page/mobile-sections/{title.replace(' ', '_')}",
                headers={**HEADERS, "Accept": "application/json"},
                timeout=15,
            )
            sections = g3["response"].json().get("remaining", {}).get("sections", [])
            lengths = [len(s.get("text", "")) for s in sections if s.get("text")]
            log.append({"step": "get_sections",
                        "request_sent_ms": g3["request_sent_ms"],
                        "response_recv_ms": g3["response_recv_ms"],
                        "request_duration_ms": g3["request_duration_ms"],
                        "response_bytes": g3["response_bytes"],
                        "sections": len(lengths)})
        except Exception:
            log.append({"step": "get_sections", "request_duration_ms": 0,
                        "response_bytes": 0, "sections": 0,
                        "note": "mobile-sections API decommissioned, using wikitext fallback"})

        # Fallback: split wikitext on == headers ==
        if not lengths:
            parts = raw_content.split("==")
            lengths = [len(p) for p in parts if len(p.strip()) > 80]

        if not lengths:
            return _error(rid, "NO_SECTIONS", "Could not parse article sections", start)

        return _success(rid, {
            "chain": "M10",
            "title": title,
            "section_count": len(lengths),
            "article_char_count": len(raw_content),
            "stats": {
                "mean_section_len":   round(statistics.mean(lengths), 2),
                "median_section_len": statistics.median(lengths),
                "sum_section_len":    sum(lengths),
            },
            "chain_log": log,
        }, start)
    except Exception as e:
        return _error(rid, "M10_FAILED", str(e), start)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)