# agent-session-router

`agent-session-router` connects named Claude Code, Codex, and provider-neutral
agent sessions through one central WebSocket router. Each agent connects
outbound, selects a specific peer, and receives only the correlated reply.

The current implementation includes:

- targeted agent discovery, request/reply, timeout, and disconnect handling;
- heartbeat, reconnect, busy state, and response isolation;
- interactive Claude Code Channel and Codex CLI integrations;
- provider-neutral gateway and mock adapter tests;
- a local launcher with router profiles and unique agent IDs.

## Quick start

Requirements:

- Bun 1.3 or newer;
- Python 3;
- an authenticated Claude Code or Codex CLI for provider runs;
- optional `fzf` for the interactive selector;
- optional Tailscale for access-controlled remote router sharing.

```bash
bun install --frozen-lockfile
bun test
```

Install the `agent-session-router` executable in `~/.local/bin`:

```bash
python3 scripts/asr.py install
agent-session-router doctor
```

The installer creates a symbolic link to the repository launcher and never
replaces an unrelated file. `~/.local/bin` must be present in `PATH`.

Claude Code needs one repository-local Channel setup before its first run:

```bash
agent-session-router setup-claude
```

## Interactive launcher

Run `agent-session-router` without arguments:

```bash
agent-session-router
```

The launcher lets you:

1. start a local or shared router, Claude Code, or Codex;
2. select a saved router profile or add a router address;
3. enter a unique agent ID such as `reviewer` or `worker-a`;
4. publish an optional activity summary;
5. choose provider-specific options such as Claude Auto mode.

`fzf` is used when installed. Otherwise the launcher displays a numbered menu.
Short agent names are normalized automatically, for example `reviewer` becomes
`local:reviewer`. Two live sessions on the same router must use different IDs.

`Start router on this device` asks whether the router should remain local, use
Tailscale, or be exposed directly to the LAN. Tailscale is preferred when its
CLI is installed and connected. The shorter `agent-session-router router`
command opens the same fzf access selector when run in a terminal. In scripts
and other non-interactive environments it keeps the existing loopback default.
`Stop router on this device` verifies and terminates the local router and
disables its matching Tailscale Serve TCP forward.

## Router profiles

The built-in `local` profile points to `ws://127.0.0.1:8787/ws`. Selecting
`Add router address` accepts a host, `host:port`, `ws://` URL, or `wss://` URL.
A bare `host-a` value becomes `ws://host-a:8787/ws`.

Starting a shared router creates or updates a separate `this-device` profile.
The built-in `local` profile is never replaced, and shared startup preserves
the server's current default profile. Profiles remain local to each machine and
can be added later on a remote machine through its CLI or SSH.

Custom profiles are stored outside the repository at:

```text
~/.config/agent-session-router/config.json
```

Only router URLs and the last selected profile are stored. Authentication
tokens remain in `ROUTER_TOKEN` and are never written to the profile.

Profiles can also be managed explicitly:

```bash
agent-session-router profile add tailnet host-a:8787
agent-session-router profile list
agent-session-router profile use tailnet
```

For scripted provider runs, set `ROUTER_URL` directly:

```bash
ROUTER_URL=ws://host-a:8787/ws agent-session-router claude reviewer
ROUTER_URL=ws://host-a:8787/ws agent-session-router codex-cli worker-a
```

## Common commands

| Command | Purpose |
| --- | --- |
| `agent-session-router` | Open the interactive launcher |
| `agent-session-router router` | Choose local, Tailscale, or LAN access interactively |
| `agent-session-router router stop` | Stop the verified local router and Tailscale forward |
| `agent-session-router router --share` | Share through Tailscale when available |
| `agent-session-router router --share=lan` | Explicitly share on the current LAN |
| `agent-session-router claude reviewer` | Start Claude Code as `local:reviewer` |
| `agent-session-router codex-cli worker-a` | Start stock Codex CLI with router tools |
| `agent-session-router codex worker-a` | Start the prompt-capable Codex connector |
| `agent-session-router smoke` | Run a local router round trip |
| `agent-session-router test` | Run the automated test suite |

Claude Code receives router deliveries through its Channel and can use
`agent_list`, `agent_send`, and `agent_reply`. Stock Codex CLI exposes
`agent_list`, `agent_send`, `agent_wait`, and `agent_reply`; ask Codex to call
`agent_wait` when it should accept an inbound request.

## Network and security

The router binds to `127.0.0.1` by default. `router --share` checks for a working
Tailscale CLI first. When available, it configures a background Tailscale Serve
TCP forwarder to the loopback router, updates the `this-device` profile, and
prints a header before the router logs with the exact profile name, router
address, and command to run on another machine. It also reports whether the
same `ROUTER_TOKEN` is required without printing the token value. Review the
tailnet Grants for the exposed port.

Before starting another Bun server, the launcher checks the local `/healthz`.
It reuses an already-running agent-session-router and reports that status. If a
different service owns the port, startup stops before changing Tailscale Serve
or printing a usable profile.

If Tailscale is unavailable, automatic sharing stops instead of exposing the
LAN unexpectedly. LAN mode requires an explicit interactive confirmation or
`--share=lan`. It binds to all interfaces so both `local` and `this-device`
remain usable. It prints a warning because `ws://` traffic is not
transport-encrypted and any reachable peer can attempt a connection. Use it
only on a trusted LAN.

An LLM operating another machine over an existing SSH connection should run
the printed `agent-session-router profile add ... --force` command there. This
is safer than replacing the complete configuration file because it preserves
the remote machine's other profiles. SSH can provision the profile and token,
but it does not authenticate or encrypt the router connection itself.

If this workflow later becomes a skill, give it an explicit name such as
`agent-session-router-remote-setup` and require an explicit invocation. Generic
keywords such as `router`, `SSH`, `Codex`, or `Claude` should not trigger it.

Set the same `ROUTER_TOKEN` on the router and provider connector processes when
registration authentication is required. Do not place tokens, real hostnames,
IP addresses, usernames, or environment-specific paths in this repository.

The router currently routes message text in memory and does not persist a
conversation transcript.

## Documentation

- [Architecture and protocol](docs/design.md)
- [Provider integration boundary](docs/provider-integration.md)
- [Claude Code Channel integration](docs/claude-channel-integration.md)
- [Codex integration](docs/codex-integration.md)
