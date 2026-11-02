# mcp-relay

Relays MCP servers running on your machine to a Databricks workspace, so Genie Code and other Databricks agentic features can use them as a remote MCP server.

## Getting Started

1. Install [uv](https://docs.astral.sh/uv/), Node.js (for `npx`-based MCPs) and the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/), then run `databricks auth login --profile <name>`.
2. `cp mcp_servers.example.json mcp_servers.json` and list your local MCP servers.
3. `cp .env.example .env` and set `DATABRICKS_PROFILE` (other settings are optional).
4. `uv run mcp-relay` deploys the app on first run and keeps the relay connected. Without a clone, run `uvx --from git+https://github.com/facha/mcp-relay mcp-relay` in the folder holding your `.env` and `mcp_servers.json`.
5. Add the printed `https://<app-url>/mcp` endpoint as an MCP server in your Databricks workspace.
