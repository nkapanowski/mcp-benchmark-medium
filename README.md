# EC2 High Workload MCP Server

**CSI 4150/5150: ML Operations — Serverless MCP Gateways for Tool-Using LLMs**
Oakland University · Spring 2026
Team: Landon Strong, Natalie Kapanowski, Kaeden Bryer

---

## Overview

This repository contains the EC2 heavy-tier MCP server implementation for the serverless MCP gateway benchmark project. The server exposes 10 heavy workload chain tools (H1–H10) via the Model Context Protocol over HTTPS, deployed on AWS EC2 behind CloudFront.

Each tool implements a self-contained multi-step chain of real external HTTP calls followed by compute-intensive statistical processing. All tools return per-step timing, RAM usage, and serialized payload size for benchmark comparison against the Lambda serverless implementation.

---

## Architecture

```
Claude / MCP Client
        |
        v
CloudFront (HTTPS + TLS termination)
https://d2plqe3qtelgql.cloudfront.net/mcp
        |
        v
EC2 Instance (Ubuntu, t3.micro)
        |
        v
Nginx (Docker) — origin auth gate (X-Origin-Auth header)
        |
        v
FastMCP Server (Docker) — server_high.py
port 8000
```

---

## Files

| File | Description |
|------|-------------|
| `server_high.py` | Main MCP server — 10 heavy workload tool chains (H1–H10) |
| `server.py` | EC2 baseline MCP server (add, hash, timestamp, word dictionary tools) |
| `Dockerfile` | Python 3.11 container for the MCP server |
| `docker-compose.yml` | Two-container setup: mcp (FastMCP) + edge (Nginx) |
| `nginx.conf` | Reverse proxy config with CloudFront origin auth gate |
| `requirements.txt` | Python dependencies |

---

## Tools

| Tool | Chain | External APIs | Key Stat |
|------|-------|--------------|----------|
| h1_ec2 | arXiv search + PDF download + text extraction | arXiv | mean/median/sum/min/max word counts |
| h2_ec2 | ClinicalTrials.gov 5 chained GETs | ClinicalTrials.gov | mean/sum/median enrollment |
| h3_ec2 | NASA asteroid feed + browse + lookup | NASA NEO API | diameter mean/median/sum/min/max |
| h4_ec2 | PubMed + arXiv cross-database + PDF | PubMed + arXiv | citation mean/sum/median |
| h5_ec2 | Google Maps 5 geocodes + distance matrix + directions | Google Maps API | distance mean/median/sum |
| h6_ec2 | DexPaprika 5 chained GETs + 168hr OHLCV | DexPaprika | price mean/median/sum/min/max |
| h7_ec2 | NixOS/Repology 5 chained GETs + version history | Repology + nixhub | version count mean/median/sum |
| h8_ec2 | Hacker News top stories + deep comment fetch | Hacker News | score mean/median/sum/mode/min/max |
| h9_ec2 | PubMed + bioRxiv + arXiv + PDF download | PubMed + bioRxiv + arXiv | word count mean/median/sum/min/max |
| h10_ec2 | PubMed + NixOS cross-domain chain | PubMed + Repology + nixhub | version mean/median/sum/mode/min/max |

---

## Response Schema

Every tool returns:

```json
{
  "request_id": "uuid",
  "status": "success",
  "result": {
    "chain": "H1",
    "stats": { ... },
    "chain_log": [
      {
        "step": "step_name",
        "request_sent_ms": 0,
        "response_recv_ms": 0,
        "request_duration_ms": 0,
        "response_bytes": 0
      }
    ]
  },
  "duration_ms": 0,
  "ram_rss_mb": 0,
  "response_bytes": 0
}
```

---

## API Keys

This server requires two API keys loaded from a `.env` file on EC2 (never committed to git):

```
GMAPS_KEY=your_google_maps_api_key
NASA_KEY=your_nasa_api_key
```

The `server_high.py` in this repo uses placeholder values. Replace with real keys in `.env` on EC2.

Required Google Maps APIs: Geocoding API, Distance Matrix API, Directions API, Maps JavaScript API.
NASA API key: free registration at https://api.nasa.gov

---

## Substitutions

The following substitutions were made from the original workload specification due to API constraints:

| Original | Substitute | Reason |
|----------|------------|--------|
| Reddit | Hacker News (H8) | Reddit blocks all AWS IP ranges |

---

## Deployment

### Prerequisites
- Docker + docker-compose on EC2
- AWS CloudFront distribution pointing to EC2 origin
- `.env` file with API keys on EC2

### Deploy

```bash
DOCKER_BUILDKIT=0 docker-compose down
DOCKER_BUILDKIT=0 docker-compose build
DOCKER_BUILDKIT=0 docker-compose up -d
```

### Test connectivity

```bash
source .env && curl -si -X POST http://localhost:8000/mcp \
  -H "Accept: application/json, text/event-stream" \
  -H "Content-Type: application/json" \
  -H "X-Origin-Auth: $ORIGIN_AUTH" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.1"}}}'
```

---

## CloudFront Endpoint

```
https://d2plqe3qtelgql.cloudfront.net/mcp
```

Add as a custom MCP connector in Claude.ai to use with the EC2 High Workload tools.

---

## Related Repositories

- [mcp-benchmark-medium](https://github.com/nkapanowski/mcp-benchmark-medium) — EC2 medium workload server (M1–M10)
- Lambda implementation — see teammate Jackson Beem's repo for the serverless comparison implementation