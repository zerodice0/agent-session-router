# Provider integration

Provider integration is a native v2 host boundary. The `asr` executable selects a
router profile and credential and starts one provider mode. A workspace is
optional for a local provider prompt. Router collaboration and task delivery
require explicit workspace membership: pass `--workspace ROOM` at startup or
call `workspace_join` through the provider's MCP tools after launch. There is no
shared provider token, TypeScript `GatewayClient`, or Python launcher.

## Common contract

A provider invocation has:

- a named agent identity whose credential claims the selected side and client and,
  when collaboration is requested, grants the selected workspace;
- a router profile containing only the `ws://` or `wss://` address;
- an explicit workspace join, either via `--workspace ROOM` or `workspace_join`;
- a readiness and join handshake before task events can be delivered.

For example:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/worker.json" \
  codex-cli local:worker --workspace team-room
```

Without `--workspace`, router delivery remains disabled until the provider
explicitly joins a workspace and is ready. A joined-ready session receives durable
task events; ordinary chat does not wake a provider and task events are not
silently inserted into an arbitrary model turn.

Provider identity is checked against the credential. A credential for
`codex-cli` cannot be reused as `claude-code`, and a worker cannot register as
another agent or claim a workspace it was not granted.

## Stock versus managed modes

| Mode | ASR owns the child? | Delivery boundary | Resume/setup |
| --- | --- | --- | --- |
| `codex-cli` | No | Explicit MCP pull tools | Use the installed Codex CLI; pass its own arguments after `--`. |
| `claude` | No | Claude Channel MCP | Run `setup-claude` once, obtain organization/development opt-in, then restart the Claude host. |
| `omp` | No | Linked OMP integration package | Run `setup-omp`, enable the package in OMP, then restart OMP. |
| `codex` | Yes, interactive App Server | Owned App Server plus terminal | ASR controls the session and closes its child on interruption. |
| `gateway codex` | Yes | Managed Codex App Server | Requires the installed Codex executable; review a checkpoint before a fresh session. |
| `gateway claude` | Yes | Managed Claude bridge | Requires the packaged bridge and Node; review a checkpoint before a fresh session. |

Managed modes filter the child environment and never pass router bearer values
or integration tokens as an ambient provider secret. Provider shutdown is tied to
the native host's cancellation path.

## Setup commands

Claude Channel setup installs the local MCP registration used by `asr claude`:

```sh
asr setup-claude
```

The setup command is local configuration only. Claude still needs the permitted
development/organization channel opt-in described in
[Claude Channel integration](claude-channel-integration.md), and Claude must be
restarted after setup or an opt-in change.

OMP setup checks the host's existing plugin state and links the packaged
integration only when the expected package is absent:

```sh
asr setup-omp
```

Enable the linked `@agent-session-router/omp-integration` package in OMP and
restart OMP. A restart is required because the external host owns plugin loading;
reloading the router alone does not reload OMP's extension.

## Profiles and credentials

Profiles are non-secret addresses. The built-in `local` profile already points to
`ws://127.0.0.1:8787/ws` and cannot be added or replaced:

```sh
asr profile list
asr --profile local workspace list --json
```

URL selection is explicit `--profile`, then nonempty `ROUTER_URL`, then the saved
default profile, then built-in `local`. Remote operator commands require
`--credential FILE` or `ASR_CREDENTIAL_FILE`; the automatic owned-local operator
credential is not a remote authentication mechanism. See the
[README transport examples](../README.md#profiles-remote-routers-and-tls) for
remote profile and credential setup.

Issue credentials with the exact side and client that the provider will run:

```sh
asr --profile local credential issue \
  --agent local:claude-worker --side claude --client claude-code \
  --workspace team-room --output "$HOME/.config/agent-session-router/claude.json"
```

Credential files are private JSON files. Keep them in a private directory and
pass them with `--credential`; do not put bearer values in `ROUTER_TOKEN`,
`AGENT_ROUTER_TOKEN`, shell history, or provider prompts.

## Workspace and task delivery

An operator can join a room directly:

```sh
asr --profile local workspace join team-room
```

The provider joins the same room using `--workspace team-room` at startup or
`workspace_join` after launch; the operator's join does not join the provider.
Once joined, agents inspect `workspace_history`, `task_list`, and `task_get`, then
explicitly request and begin assigned work. The lifecycle tools are:

```text
task_begin → task_checkpoint* → task_pause
task_begin → task_checkpoint* → task_complete
```

`task_checkpoint` records summary, next steps, artifacts, and risks. A provider
reply is not a task transition.

When an operator interrupts work, the attempt initially has unknown stop evidence.
Assignment to a replacement is allowed while stop evidence is unknown, but a new
`task_request` or `task_begin` is fenced until the old execution is confirmed
stopped. Managed hosts can automatically confirm matching terminal/reap evidence.
Read `task get` again before recovery; only if evidence is still unknown and the
operator has observed the real execution stop should the operator call
`task confirm-stopped`, using the exact interrupted attempt and freshly read task
version. The new executor reviews the checkpoint and uses its own provider
session; it never resumes an unconfirmed old process.

## Explicit resume and host cleanup

Resume belongs to the provider mode, not to the router's chat history. Stock
Claude accepts `--resume`; stock OMP and Codex arguments can be passed after
`--` where their host supports resume. Managed Codex and Claude supervise their
provider child and close it on router interruption. If the child exits, the task
still requires a checkpoint or completion mutation.

The native host rejects a missing, revoked, expired, mismatched, or
workspace-ineligible credential before launching a provider. This fail-closed
ordering prevents a provider subprocess from starting with an unusable router
identity.

## Optional external work tracking

GitHub and Linear targets are configured by the router administrator in a
server-private integration file. Agents do not log into a personal account or
read a personal backlog. An operator can explicitly check a configured target,
import an external ID, link it to a task, or publish a selected task/report:

```sh
asr --profile local integration check team-room linear --json
asr --profile local task import team-room linear EXTERNAL_ID --json
asr --profile local task link team-room TASK_ID linear EXTERNAL_ID \
  --expected-version VERSION --json
```

External operations have durable status and require operator resolution when an
outcome is unconfirmed. Never retry an unconfirmed write until
`external-resolve` records whether the provider applied it.

## Network and trust

Local operation uses loopback. Remote operation uses WSS, scoped credentials, and
a trusted CA (configured with `ASR_CA_FILE` when needed). `router --share=lan`
requires a nonloopback `ASR_BIND` and all of `ROUTER_TLS_CERT`, `ROUTER_TLS_KEY`,
and `ROUTER_PUBLIC_URL`; the public URL must use `wss://`, path `/ws`, and the bind
port. There is no plaintext LAN fallback.

Explicit `--share=tailscale` requires a valid Tailscale setup, loopback bind, and
no ASR TLS configuration. With TLS configured, `--share=auto` selects local or LAN
according to the bind address; without TLS it requires loopback and selects valid
Tailscale or local only. See the
[README transport examples](../README.md#profiles-remote-routers-and-tls).
The provider bridge and router never disable certificate verification to make a
connection succeed.
