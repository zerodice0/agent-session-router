# agent-session-router

`agent-session-router` routes messages between named, explicitly connected AI
agent sessions and returns each result to the requesting session.

Each system runs a provider connector for its local agent. The connector opens
an outbound WebSocket to the router, registers a neutral `agentId`, translates
inbound deliveries through a provider-specific session adapter, and exposes
router messaging tools back to the local agent. Agents never connect directly
to one another.

## Status

This repository currently contains the local-first routing and first provider
adapter foundation:

- in-memory agent registry
- WebSocket registration and discovery
- targeted request delivery
- correlated replies with timeouts
- a duplex provider-neutral gateway with correlated `listAgents` and `send`
- nested coordinator -> worker -> worker round-trip coverage
- a Codex App Server adapter with an injectable JSONL transport
- a Claude Agent SDK adapter with injectable query/startup boundaries
- a Claude Code Channel adapter for explicitly opted-in interactive sessions
- agent-scoped delegated router connections for outbound-only provider tools
- standard MCP `agent_list` and `agent_send` tools shared by Codex and Claude
- an in-memory mock session adapter with two-worker isolation tests
- a reviewed provider-native integration and central connection design
- neutral, environment-independent examples

AgentBridge was evaluated as a reference implementation but is not a runtime
dependency and will not be modified or forked for this project. The decision is
recorded in [docs/agentbridge-integration.md](docs/agentbridge-integration.md).
The Codex adapter and MCP tool bridge have completed a live two-Codex exchange:
agent discovery, Codex A -> `agent_send` -> Codex B -> A nested completion, and
parallel response isolation. The Claude managed-session adapter reuses the same
MCP tool contract and has completed fake-SDK lifecycle validation. The Claude
Channel adapter has completed in-memory MCP wire and loopback router validation;
manual consent and a live interactive Claude <-> Codex exchange remain external
integration gates.

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

The current implementation is local-first. A multi-system deployment must keep
the router behind authenticated TLS and use outbound gateway connections with
per-agent credentials before changing the loopback default. See
[docs/provider-integration.md](docs/provider-integration.md).

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

See [docs/design.md](docs/design.md) for the architecture and implementation
phases, [docs/provider-integration.md](docs/provider-integration.md) for the
provider and central-routing boundary,
[docs/codex-integration.md](docs/codex-integration.md) for the implemented Codex
adapter and live validation procedure,
[docs/claude-integration.md](docs/claude-integration.md) for the Claude Agent SDK
adapter and authenticated validation procedure,
[docs/claude-channel-integration.md](docs/claude-channel-integration.md) for the
interactive Claude Code Channel adapter and manual validation procedure, and
[docs/agentbridge-integration.md](docs/agentbridge-integration.md) for the
AgentBridge evaluation decision.

## Privacy rule

Do not commit real hostnames, IP addresses, usernames, SSH aliases, credentials,
company project identifiers, or environment-specific paths. Keep deployment
configuration outside the repository and use neutral examples in documentation
and tests.
