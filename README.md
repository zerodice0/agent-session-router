# agent-session-router

`agent-session-router` routes messages to a named, currently connected AI agent
session and returns the result to the requesting session.

The project keeps each AgentBridge pair isolated. A small router is added above
those pairs so a coordinator can select a recipient by `agentId` instead of
sharing one pair between several agents.

## Status

This repository currently contains the local-first routing foundation:

- in-memory agent registry
- WebSocket registration and discovery
- targeted request delivery
- correlated replies with timeouts
- neutral, environment-independent examples

AgentBridge adapters and an MCP-facing coordinator tool are planned next. They
are intentionally not represented by placeholder implementations.

## Run locally

Requirements: Bun 1.3 or newer.

```bash
bun test
bun run start
```

With the router running, use another terminal for a real WebSocket round trip:

```bash
bun run smoke
```

The router listens on `127.0.0.1:8787` by default. Override it only in a trusted
environment:

```bash
ROUTER_HOST=127.0.0.1 ROUTER_PORT=8787 bun run start
```

Set `ROUTER_TOKEN` to require clients to provide a matching token when they
register. Message content and tokens are never written to router logs.

If the router uses a different port, pass the same endpoint to the smoke client:

```bash
ROUTER_URL=ws://127.0.0.1:18787/ws bun run smoke
```

## Protocol sketch

A worker first registers a globally unique identifier:

```json
{
  "type": "register",
  "protocolVersion": 1,
  "agent": {
    "agentId": "local:reviewer",
    "side": "claude"
  }
}
```

A coordinator can then send a request:

```json
{
  "type": "send",
  "requestId": "request-001",
  "to": "local:reviewer",
  "content": "Review the current change and summarize actionable findings."
}
```

See [docs/design.md](docs/design.md) for the architecture, security boundary,
and implementation phases.

## Privacy rule

Do not commit real hostnames, IP addresses, usernames, SSH aliases, credentials,
company project identifiers, or environment-specific paths. Keep deployment
configuration outside the repository and use neutral examples in documentation
and tests.
