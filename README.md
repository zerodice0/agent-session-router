# Agent Session Router

Agent Session Router (ASR) is a native Rust v2 router for explicit agent workspaces,
durable tasks, provider sessions, and operator-controlled handoff. The `asr` binary
owns the router and the protocol boundary; provider hosts join a named workspace
before they can receive work.

## Breaking cutover

This checkout no longer ships the old TypeScript router/provider launcher, the
Python launcher, or the shared-token environment contract. Commands named
`agent-session-router`, `bun run ...`, and the old `scripts/asr.py` entry point are
not part of v2. Use the compiled `asr` binary and per-agent or operator credential
files instead.

The Rust binary is the only core runtime. The retained `integrations/omp` package
is an optional package loaded by an external OMP host. The retained
`integrations/claude-sdk` package is used only by the managed Claude gateway and
therefore requires Node when that mode is selected. Bun is only needed to package
the optional integration artifacts; it is not needed to run the router, workspace,
task, credential, or stock-provider commands.

## Build and install

Build the native executable with the repository's pinned Rust toolchain:

```sh
cargo build --locked --release
./target/release/asr --help
```

Release archives have this layout:

```text
bin/asr
share/agent-session-router/integrations/omp/
share/agent-session-router/integrations/claude-sdk/
```

Run `bin/asr install` from an archive to install the binary and its adjacent
integration assets into `$HOME/.local/bin` (or the configured destination). The
installer refuses conflicting or unsafe paths; it does not search a source
checkout for runtime assets.

On macOS, distribute a signed and notarized archive through the normal
organization policy. Do not bypass Gatekeeper or weaken system quarantine as an
installation step.

## Native v2 quickstart

The default local router binds to `ws://127.0.0.1:8787/ws`. Start it in the
background and use the built-in `local` profile to create a workspace:

```sh
asr router start --background
asr --profile local workspace create team-room
asr profile list
```

`local` is built in and cannot be added or replaced. A profile stores a router
address, not a secret. The operator credential for the owned local router is
managed by the router. Issue a scoped agent credential into a private file when a
provider host needs to run as a separate identity:

```sh
asr --profile local credential issue \
  --agent local:worker-a --side codex --client codex-app-server \
  --workspace team-room --output "$HOME/.config/agent-session-router/worker-a.json"
```

Credential files contain bearer material and are written with private permissions.
Keep them outside source control. Use `--credential PATH` before the subcommand
when starting a provider:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/worker-a.json" \
  codex local:worker-a --workspace team-room
```

Workspace membership is explicit. A human can join with `workspace join`, and an
MCP client can use `workspace_join`. Provider `--workspace ROOM` is an initial
join convenience, not the only way to join; delivery requires a successful join
and readiness handshake:

```sh
asr --profile local workspace join team-room
```

There is no automatic chat wake-up. Durable task events are delivered only to the
joined, ready provider session, and task events are not silently copied into a
model prompt.

### Self-owned task flow

The simplest workflow is self-owned: create a task in the room, assign it to the
running provider identity, request it, and let that provider own the attempt.
Task creation is JSON on stdin:

```sh
printf '%s\n' '{"title":"Review the release manifest","description":"Check native archive paths and report risks."}' |
  asr --profile local task create team-room --stdin --json
asr --profile local task list team-room --json
asr --profile local task assign team-room TASK_ID \
  --agent local:worker-a --expected-version VERSION --json
asr --profile local task request team-room TASK_ID \
  --expected-version ASSIGNED_VERSION --json
```

`TASK_ID`, `VERSION`, and `ASSIGNED_VERSION` above are values returned by the
previous command. Creation intentionally has no implicit assignee; the explicit
assign step is what makes this flow self-owned. Assignment alone does not wake or
start an executor. Every mutation uses an expected version and, where applicable,
an operation ID so a retry cannot apply a stale or duplicate mutation.

The assigned provider uses the production MCP tools
`workspace_join`, `task_begin`, `task_checkpoint`, `task_pause`, and
`task_complete`. A checkpoint records a summary, next steps, artifacts, and risks.
Pause is a durable state transition; completion is explicit and does not happen
merely because an agent replied. Inspect state and history from the operator CLI:

```sh
asr --profile local task show team-room TASK_ID --json
asr --profile local task history team-room TASK_ID --json
```

### Stop-unconfirmed handoff

Interrupting an active attempt requests a stop but does not prove that execution
has ended. Until stop evidence is confirmed, treat the attempt as
`stop-unconfirmed`; new task requests and `task_begin` remain fenced, and the old
provider session must not be reused for execution. Assignment can change while
stop evidence is unknown, but assigning alone does not wake or start an executor:

```sh
printf '%s\n' 'Stop before handing this task to worker-b.' |
  asr --profile local task interrupt team-room TASK_ID \
    --expected-version VERSION --stdin --json
```

Managed hosts can automatically confirm the stop from terminal or child-reap
evidence tied to the exact attempt and provider session. Read the task again
after the interrupt rather than reusing its returned version:

```sh
asr --profile local task show team-room TASK_ID --json
```

If stop evidence is already `confirmed`, skip manual confirmation. Only if it is
still `unknown` and the operator has actually observed execution stop, confirm
the exact interrupted attempt UUID using the freshly read task version:

```sh
printf '%s\n' 'Provider exit observed by the operator.' |
  asr --profile local task confirm-stopped team-room TASK_ID \
    --attempt ATTEMPT_UUID --expected-version CURRENT_VERSION --stdin --json
```

If confirmation races with automatic evidence or another mutation, read the task
again and reassess; do not blindly retry the old version or confirm an already
confirmed attempt. Once stop evidence is confirmed, read the current version and
hand off to a joined, ready worker:

```sh
asr --profile local task show team-room TASK_ID --json
asr --profile local task assign team-room TASK_ID \
  --agent local:worker-b --expected-version CURRENT_VERSION --json
asr --profile local task request team-room TASK_ID \
  --expected-version ASSIGNED_VERSION --json
```

Use `CURRENT_VERSION` from the latest `task show` and `ASSIGNED_VERSION` from the
assign response. The new executor must review the prior checkpoint before calling
`task_begin`.

## Provider modes

Provider commands accept an optional workspace. `--workspace ROOM` requests an
initial join; supported MCP clients can also join explicitly with `workspace_join`.
Router collaboration and task delivery require actual workspace membership and
readiness, not merely the launch flag. A native Codex command may run a local
prompt without a workspace, but that prompt has no workspace transcript or router
delivery. Choose stock provider behavior when the provider owns its normal CLI,
or a managed gateway when ASR owns the provider subprocess:

| Mode | Command | Runtime behavior |
| --- | --- | --- |
| Stock Codex CLI | `asr codex-cli AGENT --workspace ROOM` | Uses the installed Codex CLI and an explicit MCP pull boundary; it does not inject unsolicited turns. |
| Owned Codex app server | `asr codex AGENT --workspace ROOM` | ASR owns an interactive Codex App Server session and terminal input. |
| Managed Codex | `asr gateway codex AGENT --workspace ROOM` | ASR launches and supervises Codex App Server. |
| Stock Claude Code | `asr claude AGENT --workspace ROOM` | Uses Claude Channel MCP after `setup-claude` and organization/development opt-in. |
| Managed Claude | `asr gateway claude AGENT --workspace ROOM` | Uses the packaged Node Claude bridge; Node is required for this mode only. |
| External OMP | `asr omp AGENT --workspace ROOM` | Uses a linked, enabled OMP integration package; restart OMP after setup. |

Provider sessions do not resume from an implicit chat transcript. Use the
provider's explicit resume option where supported and review the durable task
checkpoint first. `asr claude` accepts `--resume`; managed provider supervision
also tears down its child process on interruption.

## Optional private GitHub and Linear integrations

External integrations are server-private configuration, not a personal backlog
connector and not an agent login flow. The router reads
`$ASR_DATA_DIR/integrations.json` by default, or the path in
`ASR_INTEGRATIONS_FILE`. A configuration has version `1` and absolute private
token-file paths:

```json
{
  "version": 1,
  "connections": [
    {
      "provider": "github",
      "workspace": "team-room",
      "repository": "org/repo",
      "access": "read",
      "tokenFile": "/private/path/github-token"
    },
    {
      "provider": "linear",
      "workspace": "team-room",
      "teamId": "TEAM_UUID",
      "projectId": "PROJECT_UUID",
      "access": "write",
      "tokenFile": "/private/path/linear-token"
    }
  ]
}
```

When an administrator has configured and reloaded a target, operators can check
it and explicitly import, link, or publish a task:

```sh
asr --profile local integration check team-room github --json
asr --profile local task import team-room github EXTERNAL_ID --json
asr --profile local task link team-room TASK_ID github EXTERNAL_ID \
  --expected-version VERSION --json
asr --profile local task publish team-room TASK_ID github \
  --kind issue --expected-version VERSION --json
```

No command reads a user's personal backlog, asks an agent to log in, or publishes
without an explicit workspace, provider, and target.

## Profiles, remote routers, and TLS

Use a profile for a configured remote router and a private operator credential
provided by its administrator:

```sh
asr profile add tailnet wss://router.example/ws
asr --profile tailnet --credential "$HOME/.config/agent-session-router/remote-operator.json" \
  workspace list --json
```

Remote operator commands require `--credential PATH` or `ASR_CREDENTIAL_FILE`;
profiles do not carry credentials. Router address selection is explicit
`--profile` first, then `ROUTER_URL`, then the saved default profile, then built-in
`local`. `ROUTER_URL` remains a supported non-secret address override, not the old
shared-token authentication contract.

`router start --share=tailscale` requires valid Tailscale configuration, a
loopback bind, and no ASR TLS settings. Tailscale Serve forwards the router port
over raw TCP and publishes `ws://<TAILSCALE_IP>:<PORT>/ws`; the tailnet encrypts
transport, but ASR does not terminate HTTPS/WSS in this mode. With ASR TLS
configured, `--share=auto` selects local mode for a loopback
bind or LAN mode for a non-loopback bind. Without ASR TLS, auto mode requires
loopback and selects valid Tailscale sharing when available, otherwise local
mode. It never falls back to plaintext LAN.

Direct `--share=lan` requires a non-loopback `ASR_BIND` and all three TLS settings:
`ROUTER_TLS_CERT`, `ROUTER_TLS_KEY`, and `ROUTER_PUBLIC_URL`. The public URL must
use `wss://`, the `/ws` path, and the same port as the bind address. For example,
on a router host with a certificate for `router.example`, a private key readable
only by its owner, and a trusted CA file:

```sh
ASR_BIND=0.0.0.0:8787 \
ROUTER_TLS_CERT=/private/path/router-cert.pem \
ROUTER_TLS_KEY=/private/path/router-key.pem \
ROUTER_PUBLIC_URL=wss://router.example:8787/ws \
ASR_CA_FILE=/private/path/ca.pem \
  asr router start --background --share=lan
```

On the client, provision the operator credential and CA file securely, then use
the matching endpoint:

```sh
asr profile add lan wss://router.example:8787/ws
ASR_CA_FILE=/private/path/ca.pem \
  asr --profile lan --credential "$HOME/.config/agent-session-router/remote-operator.json" \
    workspace list --json
```

`ASR_CA_FILE` supplies trust for a private CA; it can be omitted when the
certificate is already trusted by the client. Keep provider and integration
secrets in their private files, and use scoped per-agent credentials for providers.

The router enforces workspace grants, agent identity, single-flight delivery,
timeouts, disconnect fences, and durable acknowledgements. It does not log task
content or bearer values as a substitute for access control.

## Development and verification

Core changes use Rust:

```sh
cargo test --locked --all-targets
cargo build --locked --release
```

Optional integration package checks are separate:

```sh
bun run typecheck:integrations
bun run test:integrations
bun run build:integrations
```

The release workflow also runs focused native black-box tests against the compiled
current-platform executable, including router/task state, fake provider
subprocesses, the production MCP server with an official client, and the TLS
child-process boundary. It uses no live provider account or Tailscale dependency.

## Design notes

- [Design](docs/design.md)
- [Provider integration](docs/provider-integration.md)
- [Codex integration](docs/codex-integration.md)
- [Claude integration](docs/claude-integration.md)
- [Claude Channel integration](docs/claude-channel-integration.md)
- [AgentBridge integration](docs/agentbridge-integration.md)
