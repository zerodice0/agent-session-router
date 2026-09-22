# Codex integration

ASR supports three Codex modes. All are native Rust commands and accept an
optional workspace. Router collaboration and task delivery require an explicit
workspace join, not necessarily `--workspace ROOM` at startup: MCP clients can
call `workspace_join` after launch. `asr codex` may run a local prompt without a
workspace, but that prompt has no workspace transcript or router delivery:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/codex.json" \
  codex-cli local:codex --workspace team-room
asr --profile local --credential "$HOME/.config/agent-session-router/codex.json" \
  codex local:codex --workspace team-room
asr --profile local --credential "$HOME/.config/agent-session-router/codex.json" \
  gateway codex local:codex --workspace team-room
```

The first command runs the installed stock Codex CLI. The second starts an
interactive Codex App Server session owned by ASR. The third starts a managed
Codex App Server provider subprocess. The managed modes use the caller's working
directory and close the provider child when the ASR host is interrupted.

## Credentials and workspace

Issue a credential whose claims match the selected mode:

```sh
asr --profile local credential issue \
  --agent local:codex --side codex --client codex-cli \
  --workspace team-room --output "$HOME/.config/agent-session-router/codex.json"
```

Use `codex-app-server` instead of `codex-cli` when the credential is for
`codex` or `gateway codex`. A credential mismatch is rejected before a provider
child is launched. `--workspace team-room` joins that room at startup; without it,
delivery waits for an explicit join to an authorized workspace. Stock Codex can
call `workspace_join` through MCP, and interactive `asr codex` supports
`/workspace join ROOM`.

Codex CLI mode uses an explicit MCP pull boundary. Router task events do not
become unsolicited CLI turns, and an ordinary workspace message does not wake a
busy provider. The provider must inspect workspace and task tools, request work,
and explicitly begin its assigned attempt.

## Stock Codex CLI

`codex-cli` preserves the installed CLI's arguments. Put provider arguments after
`--`; ASR consumes only its own flags:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/codex.json" \
  codex-cli local:codex --workspace team-room -- --resume SESSION_ID
```

Use the provider's own resume semantics only after reviewing the durable task
checkpoint. A provider session is not reconstructed from chat history.

## Owned interactive Codex

`asr codex` runs an interactive App Server thread and connects it to terminal
input. Ordinary input is local provider work; router delivery remains fenced by
workspace join, readiness, and task state. Its child receives a filtered
environment and never receives router bearer values through legacy token
variables.

The process returns a terminal status on EOF or interruption. Process exit does
not complete a task; checkpoint and pause/complete through the task lifecycle,
or use the interrupted-attempt recovery described below.

## Managed Codex gateway

`asr gateway codex` starts the App Server child under native supervision:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/codex.json" \
  gateway codex local:codex --workspace team-room
```

The gateway uses the current working directory and passes only the App Server
arguments needed for the managed protocol. The managed child is terminated on
router interruption; the router does not assume a clean task completion from
process termination.

## Task lifecycle from Codex

The production MCP catalog exposes the native lifecycle:

```text
workspace_join
task_list / task_get / task_history
task_request
task_begin
task_checkpoint
task_pause or task_complete
```

`task_checkpoint` stores summary, next steps, artifacts, and risks. Expected task
versions and operation IDs fence concurrent updates. A response sent to another
agent is not a task completion.

An interrupted attempt initially has unknown stop evidence. Assignment to a
replacement is allowed, but new `task_request` and `task_begin` operations remain
fenced until stop confirmation. Managed hosts can automatically confirm exact
terminal/reap evidence. Read `task get` again; only if evidence is still unknown
and the operator has observed the actual execution stop should the operator use
`task confirm-stopped` with the exact interrupted attempt and freshly read task
version. The replacement uses a new Codex session and reviews the checkpoint.

## Router profiles and network trust

Profiles store only a router address. Built-in `local` already points to
`ws://127.0.0.1:8787/ws`; it cannot be added or replaced:

```sh
asr --profile local workspace list --json
```

Loopback is the default. Remote routers require WSS and a scoped Codex credential;
configure `ASR_CA_FILE` when a private CA is needed. Remote operator commands also
require `--credential FILE` or `ASR_CREDENTIAL_FILE`.

`router --share=lan` requires nonloopback `ASR_BIND` plus `ROUTER_TLS_CERT`,
`ROUTER_TLS_KEY`, and `ROUTER_PUBLIC_URL` (WSS, `/ws`, matching bind port).
Explicit Tailscale sharing requires loopback and no ASR TLS. Auto sharing selects
local or LAN by bind when TLS is configured; otherwise it selects valid Tailscale
or local on loopback, never plaintext LAN. See the
[README transport examples](../README.md#profiles-remote-routers-and-tls).
ASR does not disable certificate verification or place router bearer values in
Codex provider environment variables.
