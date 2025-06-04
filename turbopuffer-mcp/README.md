# Turbopuffer MCP Server

A Model Context Protocol (MCP) server that brings Turbopuffer's powerful vector and full-text search capabilities to Claude Desktop.

## Quick Start

Choose region from https://turbopuffer.com/docs/regions

**Configure Claude Desktop:**

Add the following to your Claude Desktop configuration file (`~/Library/Application\ Support/Claude/claude_desktop_config.json` on macOS):

```json
{
   "mcpServers": {
      "turbopuffer": {
            "command": "uv",
            "args": [
               "--directory",
               "absolute/path/to/turbopuffer-mcp/",
               "run",
               "turbopuffer_mcp.py"
            ],
            "env": {
               "TURBOPUFFER_API_KEY": "your_token_here",
               "TURBOPUFFER_BASE_URL": "aws-eu-central-1.turbopuffer.com"
            }
      }
   }
}
```

**Restart Claude Desktop** to load the MCP server.
