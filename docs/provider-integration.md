# Provider integration

ASR supports copy-and-paste onboarding for three stock providers: `omp`,
`claude-code`, and `codex-cli`. An administrator supplies a generated invitation;
the recipient can install ASR and the current provider's integration without a
preinstalled ASR executable or manually copying a long-lived credential.

The native Rust host boundary also retains explicit-credential stock launchers
and managed Claude SDK/Codex App Server modes. These are distinct from invitation
providers; there is no shared provider token, TypeScript `GatewayClient`, Python
launcher, or built-in AgentBridge adapter.

## Invitation onboarding

On the server, an administrator with access to the owned router runs:

```sh
asr onboarding prompt --workspace team-room --name office --provider omp
```

Use `--create-workspace` only when the administrator intends to create a missing
room. Choose `--provider claude-code`, `--provider codex-cli`, or omit the provider
restriction to let the recipient choose one of the three supported hosts. Each
invitation enrolls one device and one provider identity, not all installed hosts.
Another device or provider needs a fresh invitation. Workspace creation and
invitation issuance remain server-administrator operations.

Paste the entire generated prompt into the intended provider conversation.
Confirm trust in its sender and obtain shell permission through the host's normal
approval flow. The prompt requires `ASR_PROVIDER` to identify the host running
that conversation; it does not infer the host from executables on PATH. This
variable contains only a provider name, never a token. Execute the generated
stdin-fed shell block as supplied, with tracing disabled. It downloads and checks
the native binary's size and SHA-256, then passes the invitation JSON on stdin to
`onboarding install --provider PROVIDER --stdin`. Do not put invitation JSON in
arguments, environment variables, URLs, or diagnostic output.

The server must already have a bootstrap bundle in `$ASR_DATA_DIR/bootstrap`
(or `ASR_BOOTSTRAP_DIR`). Native bundles contain the canonical provider assets
and Cargo-derived package versions. Supported target IDs are:

- `aarch64-apple-darwin`
- `x86_64-apple-darwin`
- `aarch64-unknown-linux-gnu`
- `x86_64-unknown-linux-gnu`

Only targets actually supplied by that server can install. There is no assumed
public package download or automatic source build. The client needs POSIX `sh`,
`uname`, `curl`, `mktemp`, `chmod`, `wc`, `rm`, and `sha256sum` or `shasum`, plus
the current provider's CLI:

| Provider | Required host features |
| --- | --- |
| `claude-code` | `claude`: user-scoped local plugin marketplaces, plugin install/list and marketplace list with JSON inspection, stdio MCP add/get. |
| `codex-cli` | `codex`: stdio MCP add and MCP get/list with JSON inspection, skill discovery. |
| `omp` | `omp`: user-scoped plugin link/enable, plugin list with JSON inspection, packaged skill discovery. |

Missing tools or unsupported host versions require the user's normal installation
or upgrade procedure. ASR does not use `sudo`, install system packages, bypass
approval, alter provider login, or silently enable a previously disabled plugin.
Provider authentication/model configuration is separate from ASR installation.

### Private state, ownership, and resume

The invitation is a **10-minute, one-use secret**. Someone who obtains the prompt
before redemption can race the intended recipient. Share it only through a
trusted channel; do not publish it in issues or logs. It contains no long-lived
credential, administrator token, or private key. The client generates its own
scoped credential, and the server stores its hash. Neither provider prompts nor
workspace messages should contain credential/profile/journal contents.

The installer uses
`$HOME/.local/share/agent-session-router/versions/<manifestSha256>/bin/asr` and
absolute paths in host definitions. It does not replace an existing PATH binary
or edit shell startup files. A profile binds each enrolled provider to its own
credential file and workspace, while the native host registration selects one
explicit profile per provider. That fixed MCP/plugin name is not a per-project
multi-profile switcher; installing another invitation must not overwrite it.
Bindings for other providers are preserved. A conflicting profile/server or
existing binding is rejected, not silently redirected.

The private journal under the ASR configuration directory records
`onboarding/<inviteId>/<provider>/state.json`; credentials remain in its private
`credential.json`. Installation ownership is recorded in
`onboarding/installations/<provider>.json`. Every managed file/host definition
must match the recorded file digest or exact command, absolute executable,
profile, provider scope, and expected registration. A familiar server/plugin
name, identical-looking user definition, or successful host health check is not
ownership. Unknown skill, plugin, marketplace, MCP, or OMP link definitions cause
`provider_configuration_conflict`; ASR will not delete or overwrite them.

If interrupted, use the installer's exact absolute-ASR resume command. With that
installed executable available as `asr`, the command forms are:

```sh
asr onboarding resume INVITE_ID --provider omp
asr --profile office onboarding status --provider omp --json
```

Resume reuses the same journal, enrollment, and pending credential. It may adopt
an exact step whose recorded intent was committed before interruption; it does
not create a second identity or claim unknown configuration. Preserve the journal
and resolve reported conflicts explicitly before resuming. On the server,
`asr onboarding revoke INVITE_ID` revokes the invitation; an already issued
credential must be revoked separately through the credential commands.

### Configured is not active

The durable stages are `prepared`, `enrolled`, and `configured`. Reports expose
`profile`, `provider`, `serverId`, `workspace`, `route`, `stage`, `activation`,
and `nextAction`; status also reports transport reachability. `configured` with
`activation: "restart_required"` means installation is recorded, not that the
current conversation loaded MCP. Earlier stages report `not_checked`.
Status inspects private installation records and probes the endpoint without
opening a temporary provider connection. It does not attest to current host
registry state, membership, Channel activation, or model execution.

Follow `nextAction`, then use the actual provider session's MCP
`workspace_list` and `workspace_members` to confirm its own identity in the
invited workspace. An operator CLI join or another provider's connection is not
that proof. Normal Codex and OMP startup use their installed bindings; Claude's
Channel startup additionally needs the approved launcher and policy described
in [Claude Channel integration](claude-channel-integration.md).

## Common contract

A provider connection has:

- an agent identity whose credential claims the selected side/client and grants
  the selected workspace;
- a router profile, with non-secret route/trust metadata and private credential
  file references for invitation bindings;
- an explicit workspace selection from the invitation binding,
  `--workspace ROOM`, or `workspace_join({"name":"ROOM"})`;
- a readiness and join handshake before task events can be delivered.

For example:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/worker.json" \
  codex-cli local:worker --workspace team-room
```

This example uses the lower-level explicit-credential path. Without a selected
startup workspace (including one from an invitation binding), router delivery
remains disabled until the provider explicitly joins and is ready. A joined-ready
session receives durable task events; ordinary chat does not wake a model, start
a task, or silently become an arbitrary model turn.

Provider identity is checked against the credential. A credential for
`codex-cli` cannot be reused as `claude-code`, and a worker cannot register as
another agent or claim a workspace it was not granted.

## Stock versus managed modes

| Mode | ASR owns the child? | Delivery boundary | Resume/setup |
| --- | --- | --- | --- |
| `codex-cli` | No | Explicit MCP pull tools | Invitation provider `codex-cli`; start a new `codex` session. The wrapper forwards host arguments after `--`. |
| `claude` | No | Claude Channel MCP | Invitation provider `claude-code`; use the installer's restart command and required policy/Channel opt-in. |
| `omp` | No | Linked OMP integration package | Invitation provider `omp`; restart the normal `omp` process after installation. |
| `codex` | Yes, interactive App Server | Owned App Server plus terminal | ASR controls the session and closes its child on interruption. |
| `gateway codex` | Yes | Managed Codex App Server | Requires the installed Codex executable; review a checkpoint before a fresh session. |
| `gateway claude` | Yes | Managed Claude bridge | Requires the packaged bridge and Node; review a checkpoint before a fresh session. |

Managed modes filter the child environment and never pass router bearer values
or integration tokens as an ambient provider secret. Provider shutdown is tied to
the native host's cancellation path.

## Lower-level setup commands

Invitation installation already configures the selected host. Do not run these
commands as a second registration step for an onboarded provider. They remain
available for manually provisioned installations with explicit credentials.

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

## Stock OMP startup and workspace commands

The invitation installer links the packaged
`@agent-session-router/omp-integration` using `omp plugin link PACKAGE_PATH
--scope user`. Its packaged skill comes from `omp/skills/asr/SKILL.md`. The
private `hosts/omp.json` beside the ASR config contains only `version`,
`executable`, `profile`, and `workspace`; it holds no credential value. OMP reads
it from the `ASR_CONFIG_PATH` parent, otherwise
`$XDG_CONFIG_HOME/agent-session-router`, otherwise
`$HOME/.config/agent-session-router`. An explicit `ASR_EXECUTABLE` keeps the
lower-level wrapper configuration path instead.

After installation, exit and restart with `omp`; a command reload is not a
plugin-registry restart. Normal interactive startup connects the native MCP
child and joins the configured workspace once, after connection. The native
profile binding supplies the credential; no inline credential is required.
If the user disabled an existing integration, ASR reports `plugin_disabled`
rather than overriding that choice. Enable it only by an explicit user decision:
`omp plugin enable @agent-session-router/omp-integration --scope user`, then
resume and restart as instructed.

The extension provides equivalent commands:

```text
/asr workspace list
/asr workspace find team
/asr workspace join team-room
/asr workspace members
/asr workspace history
/asr workspace post Hello team
/asr workspace status
/asr workspace leave
```

`/workspace list|find QUERY|join NAME|members|history|post TEXT|status|leave` is
the shorter alias. The packaged skill is also available as
`/skill:asr workspace ...` when skill commands are enabled. These use the same
provider connection, not a shell operator connection. List/find traverse all
accessible pages with a limit of 100; find filters names using ASCII
case-insensitive substring matching. Members shows connected agents, and
history shows a bounded page rather than implying the complete transcript.
Use the MCP history cursor for explicitly requested additional pages.

Joining uses the native tool schema `workspace_join({"name":"team-room"})`.
A connected but unjoined session can join. To switch rooms, explicitly leave
and wait for confirmed success before joining the next room. Pending/running
work or unconfirmed stop evidence may block the transition; never force a
disconnect to bypass it. An unconfirmed leave preserves uncertain membership
and does not authorize joining elsewhere. Profile changes require a new provider
connection, not a live hot-swap.

## Profiles and credentials

Manually added profiles store a non-secret router address; invitation profiles
also store server identity, verified route/CA-file metadata, and provider
bindings, never bearer values. Built-in `local` already points to
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

For a manual or managed installation, issue credentials with the exact side and
client that the provider will run. Invitation recipients do not do this step:

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

The provider joins using its invitation startup binding, `--workspace team-room`,
or `workspace_join({"name":"team-room"})`; the operator's join does not join the
provider. Workspace list/find must honor grant-filtered, bounded pages. Inspect
members and bounded history through the provider MCP connection, then
`task_list` and `task_get` before explicitly requesting work. The lifecycle tools
are:

```text
task_begin → task_checkpoint* → task_pause
task_begin → task_checkpoint* → task_complete
```

`task_checkpoint` records summary, next steps, artifacts, and risks. A provider
reply is not a task transition.

An assignee is the intended responsible agent, not proof of an executing attempt.
Assignment does not request execution. A task request is distinct from
`task_begin`, which identifies the actual executor/session; neither a request
acceptance, readiness, chat post, nor provider reply completes the task.

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

Invitation route candidates must already lead to the same router. Administrators
can provide repeated `--endpoint KIND=URL` (`tailnet`, `lan`, `public`, or
local-only `local`) and `--ca-file PATH` to `onboarding prompt`. These flags do
not create listeners, tunnels, firewall rules, certificates, or a public service.
The client checks supplied candidates in Tailnet → LAN → public order; an
unreachable candidate can fall through, but TLS, server-identity, or digest
mismatch stops installation. Loopback invitations work only on that machine and
cannot be mixed with remote routes.

Tailnet onboarding uses the supported raw Tailnet IPv4 route and verifies the
peer through Tailscale; it does not infer trust from a `100.x` address or add
MagicDNS support. LAN/public candidates require WSS with hostname/CA verification.
The trusted prompt may carry a public CA certificate, never its private key.
Aliases/proxies must forward the fixed onboarding paths and `/ws` to the same
server; arbitrary path prefixes and redirect-based bootstrap are unsupported.
Routes are checked again when a new provider MCP connection starts, not used to
hot-swap an active task. Reachability depends on the operator's actual network,
ACL, proxy, and TLS configuration; public or cross-device LAN access is not
guaranteed. These bootstrap endpoints are not a web dashboard or HTTP admin API.
