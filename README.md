# Sqllite MCP Server

## Installation

### Prerequisites

```bash

$ python3 -m venv .venv --upgrade-deps 

$ source .venv/bin/activate

$ pip install "mcp[cli]" httpx

$ pip install  faker

```

## For Claude Desktop:

```json

    "sqlite-db": {
      "command": "/Users/clojure/Desktop/sqllite-mcp-server/.venv/bin/python",
      "args": [
        "/Users/clojure/Desktop/sqllite-mcp-server/sqllite_mcp_server.py"
      ]
    }

```
