# Claude integration

ASR has two Claude paths:

- `asr claude` runs the installed Claude Code host with the Claude Channel MCP
  boundary.
- `asr gateway claude` owns a managed Claude bridge and uses the retained Node
  Claude SDK package.

Both paths accept an optional workspace and a credential whose claims match the
selected client. Router collaboration and task delivery require an explicit
workspace join. Pass `--workspace ROOM` to join at startup, or use
`workspace_join` through stock Claude's MCP tools after launch. Until joined,
the provider can run local work but receives no router transcript or task delivery:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/claude-code.json" \
  claude local:claude --workspace team-room
asr --profile local --credential "$HOME/.config/agent-session-router/claude-sdk.json" \
  gateway claude local:claude --workspace team-room
```

## Stock Claude Code and Channel

Prepare the local MCP registration once:

```sh
asr setup-claude
```

Then obtain the required development/organization channel opt-in and restart the
Claude host. The native host launches Claude with the Channel MCP registration;
it does not rely on a legacy Python process or a shared router environment token.

Stock Claude uses an explicit readiness boundary. A successful process launch is
not a workspace join: join with `--workspace team-room` at startup or
`workspace_join` after launch, using a credential that grants the room. The
Channel server must also report ready. Ordinary workspace chat does not wake a
Claude turn. Claude must inspect task state and explicitly call lifecycle tools.

Claude Code's provider resume option is explicit:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/claude-code.json" \
  claude local:claude --workspace team-room --resume SESSION_ID
```

Review the durable task checkpoint before resuming. If a prior attempt was
interrupted, do not reuse it until stop evidence is confirmed.

## Managed Claude gateway

The gateway owns the Claude child through the packaged Node bridge:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/claude-sdk.json" \
  gateway claude local:claude --workspace team-room
```

Node is required for this mode only. The release archive must contain
`share/agent-session-router/integrations/claude-sdk/bridge.js`, its manifest, and
its packaged Claude SDK dependencies. The native executable resolves this
installed asset layout; it does not search a source checkout.

The gateway filters provider environment variables, keeps router credentials out
of the child, and terminates the child when the host is interrupted. The current
gateway command has no implicit chat-resume behavior; start a new managed session
after reviewing the checkpoint and use an explicit task transition.

## Credentials and setup

Issue separate credentials for the stock and managed clients:

```sh
asr --profile local credential issue \
  --agent local:claude-code --side claude --client claude-code \
  --workspace team-room --output "$HOME/.config/agent-session-router/claude-code.json"
asr --profile local credential issue \
  --agent local:claude-sdk --side claude --client claude-sdk \
  --workspace team-room --output "$HOME/.config/agent-session-router/claude-sdk.json"
```

Credential files are private JSON. A `claude-code` credential is rejected for
`gateway claude`, and vice versa. Do not put bearer values in `ROUTER_TOKEN`,
`AGENT_ROUTER_TOKEN`, or provider prompts.

## Task lifecycle and handoff

The Claude MCP catalog exposes:

```text
workspace_join
task_list / task_get / task_history
task_request
task_begin
task_checkpoint
task_pause or task_complete
```

The executor calls `task_begin` for the assigned attempt, records summary,
next steps, artifacts, and risks in `task_checkpoint`, then explicitly pauses or
completes. A Claude response or readiness event does not begin or complete work.

`task interrupt` requests a stop but initially leaves stop evidence unknown.
Assignment to a replacement is allowed while evidence is unknown, but new
`task_request` and `task_begin` operations remain fenced. Managed hosts can
automatically confirm matching terminal/reap evidence. Read `task get` again;
only if evidence is still unknown and the operator has observed the real
execution stop should the operator call `task confirm-stopped` with the exact
interrupted attempt and freshly read task version. A replacement Claude session
reviews the checkpoint and uses an authorized identity; it never shares an
unconfirmed provider process.

## Optional external work tracking

GitHub and Linear access is configured by the router administrator per workspace
with private token files. Claude does not log into a personal backlog. Operators
explicitly choose any import, link, or publish operation and resolve an
unconfirmed external write before retrying.

## Trust and networking

Local Claude sessions use the built-in `local` loopback profile; do not add or
replace it. Remote operation uses WSS and scoped credentials, with `ASR_CA_FILE`
for a private CA when needed. Remote operator commands require `--credential FILE`
or `ASR_CREDENTIAL_FILE`.

`router --share=lan` requires nonloopback `ASR_BIND` and all of `ROUTER_TLS_CERT`,
`ROUTER_TLS_KEY`, and `ROUTER_PUBLIC_URL` (WSS, `/ws`, matching bind port).
Explicit Tailscale sharing requires loopback and no ASR TLS. With TLS configured,
auto sharing selects local or LAN by bind; without TLS it selects valid Tailscale
or local on loopback, never plaintext LAN. See the
[README transport examples](../README.md#profiles-remote-routers-and-tls).
Certificate verification is never disabled to make Claude connect.
