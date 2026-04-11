# mcp-benchmark-medium
Medium EC2 instance (t3.micro) running M1–M10 sequential MCP tool chains (2-3 external HTTP calls, 5–100KB payloads). Collects metrics needed for EC2 vs Lambda performance comparison. Handles real multi-step data dependencies between chained calls, supports LLM-driven dispatch, and returns structured JSON responses per tool
