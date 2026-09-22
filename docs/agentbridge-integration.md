# AgentBridge integration

## Current status

Native v2 does not ship an AgentBridge adapter. The router's supported runtime is
the Rust `asr` executable with its native WebSocket, task, workspace, and MCP
boundaries. AgentBridge can remain an external system, but there is no built-in
command that attaches it, imports its sessions, or translates its environment
contract.

AgentBridge is not an invitation provider. The supported stock onboarding
providers are exactly `omp`, `claude-code`, and `codex-cli`; use an administrator's
[generated invitation](provider-integration.md#invitation-onboarding) for those
hosts, without preinstalling ASR or manually copying long-lived credentials.
That flow neither installs AgentBridge nor imports its sessions.

For separately provisioned native integrations, the lower-level commands remain:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/codex.json" \
  codex local:codex --workspace team-room
asr --profile local --credential "$HOME/.config/agent-session-router/claude.json" \
  claude local:claude --workspace team-room
```

These commands use an explicit workspace for router collaboration and delivery,
plus a scoped credential. Without a selected/joined workspace, a local provider
prompt receives no router transcript or task delivery. Stock Claude can call
`workspace_join({"name":"ROOM"})` through MCP after launch; interactive Codex
supports `/workspace join ROOM`. The managed SDK/App Server routes documented in
[Claude integration](claude-integration.md) and [Codex integration](codex-integration.md)
remain distinct compatibility modes, not additional invitation providers. These
commands do not read an AgentBridge personal backlog or consume the old
shared-token launcher environment.

## Upstream reference

The following links are preserved as external design references; they are not
claims that the corresponding adapter is included in this repository:

- [AgentBridge `v0.1.30` release](https://github.com/raysonmeng/agent-bridge/releases/tag/v0.1.30)
- [control protocol](https://github.com/raysonmeng/agent-bridge/blob/v0.1.30/src/control-protocol.ts#L76-L181)
- [Claude attach admission](https://github.com/raysonmeng/agent-bridge/blob/v0.1.30/src/daemon-identity.ts#L139-L170)
- [Codex turn injection](https://github.com/raysonmeng/agent-bridge/blob/v0.1.30/src/codex-adapter.ts#L505-L555)
- [final Codex message conversion](https://github.com/raysonmeng/agent-bridge/blob/v0.1.30/src/codex-adapter.ts#L2030-L2065)
- [Claude channel and reply integration](https://github.com/raysonmeng/agent-bridge/blob/v0.1.30/src/claude-adapter.ts#L234-L260)

Those references describe AgentBridge's own protocol and process assumptions.
They do not define ASR v2's identity, workspace, or task lifecycle.

## Mapping concepts to native v2

| AgentBridge concept | Native ASR v2 boundary |
| --- | --- |
| Attach/admission | A scoped credential plus explicit workspace membership (invitation startup binding or MCP `workspace_join({"name":"ROOM"})`). |
| Agent identity | Credential subject, side, client, and workspace grants. |
| Turn delivery | Durable task request to a joined-ready provider; stock Codex uses explicit MCP pull, while Channel uses a correlated notification. |
| Reply correlation | `request_id` and provider-specific reply tools; replies do not complete tasks. |
| Session handoff | `task interrupt`, confirmed stop evidence (automatic managed-host evidence or operator recovery), then a new request/attempt in a new provider session. |
| External work item | An administrator-configured GitHub or Linear target and an explicit import/link/publish command. |
| Assignment/execution | Assignee names responsibility; only the requested attempt's `task_begin` establishes its actual executor/session. Chat and replies are not task completion. |

No concept mapping automatically migrates an AgentBridge session. A migration
must have an administrator create/select the ASR workspace, obtain an invitation
for a supported stock host or separately issue the appropriate managed credential,
and review any prior checkpoint before beginning work. Invitation installation
records configuration (`configured` / `restart_required`), not active membership
or model execution; verify the new provider through its own actual MCP tools.

Assignment to a replacement is permitted while stop evidence is unknown, but a
new `task_request` or `task_begin` is fenced. Managed hosts can automatically
confirm exact terminal/reap evidence. Read `task get` again; only if evidence
remains unknown and the operator has observed the real execution stop should the
operator call `task confirm-stopped` with the exact interrupted attempt and
freshly read task version.

## Coexistence and trust

An external AgentBridge installation may coexist with the native router on its
own ports and credentials. Do not point it at ASR's native endpoint unless its
operator has implemented and reviewed the protocol v2 registration, workspace
grants, task fences, and TLS policy. ASR will reject an unknown or mismatched
client identity rather than treating a legacy token as authorization.

The built-in `local` profile is the loopback endpoint and cannot be added or
replaced. Remote coexistence requires WSS, scoped credentials, and a trusted CA;
remote operator commands require `--credential FILE` or `ASR_CREDENTIAL_FILE`.
LAN sharing requires nonloopback `ASR_BIND` and all of `ROUTER_TLS_CERT`,
`ROUTER_TLS_KEY`, and `ROUTER_PUBLIC_URL` (WSS, `/ws`, matching bind port).
Explicit Tailscale sharing requires loopback and no ASR TLS. Auto sharing selects
local or LAN by bind with TLS configured; otherwise it selects valid Tailscale or
local on loopback, never plaintext LAN. See the
[README transport examples](../README.md#profiles-remote-routers-and-tls).
No live AgentBridge account, provider account, or Tailscale network is required
for native tests.
