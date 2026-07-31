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
- application heartbeat and bounded reconnect without request replay
- router-derived `idle`/`busy` presence plus optional public activity metadata
- nested coordinator -> worker -> worker round-trip coverage
- a Codex App Server adapter with an injectable JSONL transport
- a prompt-capable Codex console sharing the gateway-owned App Server thread
- a stock Codex CLI MCP gateway with correlated send/wait/reply tools
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
the prompt-capable Codex console has completed a live Claude exchange on a
separate development machine. The stock Codex TUI MCP path retains one manual
Claude <-> Codex validation gate.

## Run locally

Requirements: Bun 1.3 or newer, Python 3, and an authenticated Codex CLI for
interactive provider runs.

```bash
bun test
bun run start
```

For the shortest interactive workflow, use the Python launcher from the
repository root:

```bash
python3 scripts/asr.py router
python3 scripts/asr.py codex-cli worker-a
```

Run each long-lived command in its own terminal. Short names such as `worker-a`
are normalized to neutral IDs such as `local:worker-a`; router URLs, working
directories, and defaults are supplied by the launcher.

To make `asr` available in future shells, print and review the generated shell
function once, then append it to the appropriate shell startup file:

```bash
python3 scripts/asr.py shell-init
python3 scripts/asr.py shell-init >> ~/.zshrc
```

Use `~/.bashrc` instead for Bash. Do not repeat the append command after the
function has been installed. A configured session can then be started with
`asr router` and `asr codex-cli worker-a`. Run `asr doctor` to check the local
commands without reading or printing credentials.

`codex-cli` starts the stock Codex TUI and injects one process-local MCP server;
it does not modify user-level Codex configuration. Inside Codex, ask it to call
`agent_list` or `agent_send`. To accept one inbound request, ask it to call
`agent_wait`, handle the returned message, and call `agent_reply` with the same
`requestId`. Passing Codex flags remains explicit:

```bash
asr codex-cli worker-a -- --search
```

The older `asr codex worker-a` command remains available when automatic router
delivery is more important than using the stock TUI. It opens the repository's
small prompt console in front of a gateway-owned App Server thread.

For the optional interactive Claude Channel, run `asr setup-claude` once from
this repository and then use `asr claude reviewer`. To use Claude Auto mode and
publish a non-sensitive work summary in `agent_list`, run:

```bash
asr claude reviewer --activity "reviewing tests" --auto
```

`--dangerously-load-development-channels` remains necessary for a custom
Channel during Claude's research preview. It bypasses the Channel plugin
allowlist for this explicitly selected local server; it is not
`bypassPermissions` and does not disable tool safety. `--auto` independently
selects Claude's permission mode when the account supports it.

With the router running, use another terminal for a real WebSocket round trip:

```bash
python3 scripts/asr.py smoke
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

Inside the older prompt-capable Codex console, ordinary input starts a Codex turn.
Use `/agents` to list peers, `/send local:worker-a message` for a direct router
check that does not start a local model turn, and `/quit` to exit. The existing
`bun run gateway:codex` command remains the non-interactive automation worker.

With an interactive provider connected, `asr smoke worker-a` sends one neutral
request to `local:worker-a` and reports only pass/fail; it intentionally does
not print or persist the provider response. This command consumes one provider
turn, unlike the router-only `asr smoke` command.

The current implementation is local-first but can keep the router on loopback
and expose its TCP port only inside a trusted tailnet. Gate the port with
Tailscale Grants and retain `ROUTER_TOKEN` as defense in depth. Public or
non-overlay deployment still requires authenticated TLS and per-agent
credentials. See [docs/provider-integration.md](docs/provider-integration.md).

## Protocol sketch

A worker first registers a globally unique identifier:

```json
{
  "type": "register",
  "protocolVersion": 1,
  "agent": {
    "agentId": "local:reviewer",
    "side": "claude",
    "activity": "reviewing tests"
  }
}
```

`activity` is optional, operator-supplied, limited to 160 printable characters,
and visible to every connected agent. `agent_list` also includes router-derived
`status: "idle" | "busy"`; clients cannot claim their own status.

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
