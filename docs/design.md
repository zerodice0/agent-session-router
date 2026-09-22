# Native v2 design

Agent Session Router is a native Rust process with one durable protocol boundary.
The `asr` executable starts the router, runs operator and agent clients, owns
provider subprocesses in managed modes, and exposes production MCP over stdio.
There is no TypeScript or Python router runtime.

## Runtime boundaries

```text
asr router ── loopback/TLS WebSocket ── operator and provider clients
     │
     ├── SQLite state: credentials, workspaces, tasks, attempts, events
     ├── native MCP server: workspace/task/integration tools
     └── optional provider subprocess
           ├── stock Codex CLI or Claude Code (MCP boundary)
           ├── owned or managed Codex App Server
           ├── packaged Node Claude bridge
           └── external OMP host and linked package
```

The default router endpoint is `ws://127.0.0.1:8787/ws`. The `local` profile is
built in and cannot be added or replaced; profiles contain only a router address.
Address selection prefers explicit `--profile`, then `ROUTER_URL`, then the saved
default profile, then built-in `local`. `ROUTER_URL` is a supported non-secret
address override. Authentication uses an operator credential or an agent
credential whose grants include the requested workspace. Remote operator clients
require `--credential PATH` or `ASR_CREDENTIAL_FILE`; automatic operator credential
discovery is limited to the owned local router. Credential files are private JSON
files; no shared `ROUTER_TOKEN` environment contract exists.

The default data directory follows XDG conventions and can be overridden with
`ASR_DATA_DIR`; configuration can be overridden with `ASR_CONFIG_PATH`. The
router's local state is not a model prompt and is not an implicit chat transcript.

## Protocol and identities

Protocol version 2 validates every registration and request at the boundary.
Agent identity includes an agent ID, side (`claude`, `codex`, or `generic`), and
client (`omp`, `claude-code`, `claude-sdk`, `codex-cli`, `codex-app-server`, or
`generic`). The router rejects an identity mismatch, duplicate registration,
missing workspace grant, stale expected version, or invalid operation ID.

The primary agent may create a scoped delegate credential for a nested worker.
Delegation is a capability, not a shared secret: the delegate is tied to its
owner and cannot register as another agent or claim another workspace.

## Workspace and delivery model

Joining a workspace is explicit. `workspace join` or MCP `workspace_join`
establishes a cursor and emits the join event. Provider `--workspace ROOM` is an
initial join convenience, not the only route to membership. A provider cannot
receive delivery until its join and readiness handshake succeed; one that has
not joined a workspace does not start delivery.

The router delivers durable task events to the joined ready provider session. It
uses one in-flight delivery per provider, acknowledgement and cursor fences,
timeouts, disconnect handling, and stale-session protection. It does not replay a
chat transcript or inject task text into an unsolicited provider turn. Stock
Codex CLI uses an explicit MCP pull boundary; stock Claude Channel similarly
requires the provider to be joined and ready.

Workspace events are durable records for coordination. `workspace list`,
`workspace members`, `workspace history`, and `workspace watch` expose state
without copying private content into provider environment variables or logs.

## Task state machine

Tasks use optimistic versions and idempotent operation IDs:

```text
todo ── request/begin ──> in_progress ── checkpoint ──> in_progress
  │                            │  │
  │                            │  ├── pause ──> paused or blocked
  │                            │  └── complete ──> done
  │                            └── interrupt ──> stop evidence pending
  ├── cancel ──> cancelled
  └── reopen <────────────── paused/blocked/cancelled
```

An executor must call `task_begin` for the assigned attempt. It records progress
with `task_checkpoint`, and must explicitly call `task_pause` or `task_complete`.
Each checkpoint contains a summary, next steps, artifacts, and risks. Assignment
alone does not wake or start an executor. Readiness, a chat reply, or a provider
process that merely starts does not begin or complete a task.

An interrupt requests a provider stop and initially records `unknown` stop
evidence. Assignment may change while that evidence is unknown, but new task
requests and `task_begin` remain fenced; the old provider session must not be
reused for execution. Managed hosts can automatically confirm the stop from
terminal or child-reap evidence tied to the exact attempt and provider session.
Read the task again after interruption: if evidence is already `confirmed`, no
manual confirmation is needed. Only if it remains `unknown` and the operator
actually observed execution stop should `task confirm-stopped` record that
evidence, using the exact interrupted attempt UUID and a freshly read task version.
If automatic confirmation or another mutation wins the race, read and reassess
instead of retrying stale confirmation. Once confirmed, use the current version
for handoff; the next executor reviews the prior checkpoint before `task_begin`.

## MCP boundary

The production Rust MCP server uses the official MCP framing and client
interoperability path. Roles include `delegate`, `codex-cli`,
`claude-channel`, and `omp`. The catalog includes workspace operations, task
inspection and lifecycle operations, agent coordination, and explicitly selected
external integration operations.

MCP input is bounded and validated before dispatch. The server limits frame sizes
and concurrent calls, rejects unknown fields and identity spoofing, and returns
typed errors rather than embedding secrets in responses. Provider-facing
instructions treat task, peer, and external text as untrusted data.

## Optional external integrations

GitHub and Linear are optional server-private targets. Configuration associates a
workspace and provider target with a private token file and read or write access.
The router exposes only public metadata and explicit operations. `task import`,
`task link`, and `task publish` never read an agent's personal backlog or initiate
a provider login. External writes are serialized per target and use an operation
record with `running`, `applied`, `not_applied`, or `unconfirmed` resolution; an
operator must resolve an unconfirmed operation before retrying.

## Provider process policy

Stock modes preserve the provider's own process and MCP contract. Managed modes
own the child process, pass only an allowlisted environment, and terminate the
child on router shutdown or interruption. The Claude SDK bridge is a retained
Node package used only by `gateway claude`; the OMP and Claude Channel packages
are optional integration surfaces, not alternate router implementations.

Resume is always explicit. A resumed provider reviews durable task state and the
last checkpoint; a new provider session is never inferred from a chat message.

## Network and secret handling

Loopback is the safe default. `router start --share=tailscale` requires valid
Tailscale configuration, a loopback bind, and no ASR TLS settings. Tailscale Serve
forwards raw TCP and publishes `ws://<TAILSCALE_IP>:<PORT>/ws` over the encrypted
tailnet, not an HTTPS/WSS endpoint. With ASR TLS configured, `--share=auto`
selects local mode on loopback or LAN mode on a non-loopback bind. Without TLS,
auto mode requires loopback and selects valid Tailscale sharing when available,
otherwise local mode. There is no plaintext LAN fallback.

Direct `--share=lan` requires a non-loopback `ASR_BIND` and all of
`ROUTER_TLS_CERT`, `ROUTER_TLS_KEY`, and `ROUTER_PUBLIC_URL`. The public URL must
use WSS, the `/ws` path, and the same port as the bind address. Partial TLS
configuration and non-loopback binds without TLS fail closed. Remote clients use
private credentials and a trusted CA, with `ASR_CA_FILE` supplying private CA
trust when needed.

Router credentials, provider credentials, and integration tokens remain in
separate private files. The router stores hashes for authentication and does not
print bearer values. Child provider environments are filtered; integration
configuration errors fail closed. Content and secret values are not emitted to
diagnostic logs.

## Verification invariants

Native integration tests exercise router ownership, workspace grants, optimistic
task transitions, provider child cleanup, the production MCP server with an
official client, and TLS trust/reuse/fail-closed behavior. The implementation
plan's concurrency requirement is normative: two workers' responses remain
isolated under concurrency. Release CI repeats focused black-box scenarios
against the compiled current-platform `asr` binary without live accounts or
Tailscale.
