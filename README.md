# EC2 Medium Workload MCP Server
CSI 4150/5150: ML Operations — Serverless MCP Gateways for Tool-Using LLMs
Oakland University · Spring 2026
Team: Landon Strong, Natalie Kapanowski, Kaeden Bryer

# Overview
This repository contains the EC2 medium-tier MCP server implementation for the serverless MCP gateway benchmark project. The server exposes 10 medium workload chain tools (M1–M10) via the Model Context Protocol over HTTPS, deployed on AWS EC2 behind CloudFront.

Each tool implements a self-contained 1–3 step chain of real external HTTP calls followed by lightweight statistical processing. All tools return per-step timing, RAM usage, and serialized payload size for benchmark comparison against the Lambda serverless implementation.

## Architecture

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
FastMCP Server (Docker) — server_medium.py
port 8000
Files
File	Description
server_medium.py	Main MCP server — 10 medium workload tool chains (M1–M10)
server.py	EC2 baseline MCP server (add, hash, timestamp, word dictionary tools)
Dockerfile	Python 3.11 container for the MCP server
docker-compose.yml	Two-container setup: mcp (FastMCP) + edge (Nginx)
nginx.conf	Reverse proxy config with CloudFront origin auth gate
requirements.txt	Python dependencies
Tools
Tool	Chain	External APIs	Key Stat
m1_ec2	Open-Meteo 7-day NYC forecast	Open-Meteo	mean/median/min/max daily max temps
m2_ec2	NPS CA parks (15) + park detail	NPS	mean/median/sum latitudes
m3_ec2	Hacker News top-10 stories + item fetches	Hacker News	mean/median/mode scores
m4_ec2	Hugging Face top-20 text-classification + model detail	Hugging Face Hub	mean/median/sum downloads
m5_ec2	Met Museum impressionism search (20) + object detail	Met Museum	mean/median/sum IDs
m6_ec2	FIPE car brands + FIAT models + moto brands	FIPE (parallelum.com.br)	mean/median/mode model codes
m7_ec2	OKX BTC-USDT 48h candles + ETH-USDT ticker	OKX	mean/median/sum closes + volume sum
m8_ec2	Steam top sellers + featured games	Steam Store	mean/median/sum/mode prices
m9_ec2	NixOS/Repology nodejs search + info + nixhub versions	Repology + nixhub	mean/median/sum version counts
m10_ec2	Wikipedia search + article + section structure	Wikipedia	mean/median/sum section lengths
Response Schema
Every tool returns:


{
  "request_id": "uuid",
  "status": "success",
  "result": {
    "chain": "M1",
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
# API Keys
This server requires one API key loaded from a .env file on EC2 (never committed to git):


NPS_KEY=your_nps_api_key
The server_medium.py in this repo defaults to DEMO_KEY for NPS, which is rate-limited. Replace with a real key in .env on EC2.

NPS API key: free registration at https://www.nps.gov/subjects/developer/get-started.htm

All other endpoints (Open-Meteo, Hacker News, Hugging Face, Met Museum, FIPE, OKX, Steam, Repology, nixhub, Wikipedia) are public and require no authentication.

# Substitutions
The following substitutions were made from the original workload specification due to API constraints:

Original	Substitute	Reason
Reddit	Hacker News (M3)	Reddit blocks all AWS IP ranges
Game Trends API	Steam Store API (M8)	Game Trends API unavailable
Wikipedia mobile-sections (M10)	Wikitext header split fallback	mobile-sections REST endpoint decommissioned
Deployment
Prerequisites
Docker + docker-compose on EC2
AWS CloudFront distribution pointing to EC2 origin
.env file with API keys on EC2
# Deploy

DOCKER_BUILDKIT=0 docker-compose down
DOCKER_BUILDKIT=0 docker-compose build
DOCKER_BUILDKIT=0 docker-compose up -d
Test connectivity

source .env && curl -si -X POST http://localhost:8000/mcp \
  -H "Accept: application/json, text/event-stream" \
  -H "Content-Type: application/json" \
  -H "X-Origin-Auth: $ORIGIN_AUTH" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.1"}}}'
# CloudFront Endpoint

https://d2plqe3qtelgql.cloudfront.net/mcp
Add as a custom MCP connector in Claude.ai to use with the EC2 Medium Workload tools.

# Related Repositories
mcp-benchmark-high — EC2 high workload server (H1–H10)
Lambda implementation — see teammate Jackson Beem's repo for the serverless comparison implementation
Key changes from the high version:

Title/scope changed to "medium-tier" / M1–M10
Tool table replaced with M1–M10 entries (sourced from your server_medium.py docstrings)
API keys section reduced to just NPS_KEY (medium server doesn't use Google Maps or NASA)
Substitutions table now lists all three medium-tier swaps (HN, Steam, Wikipedia wikitext fallback)
File reference updated to s
