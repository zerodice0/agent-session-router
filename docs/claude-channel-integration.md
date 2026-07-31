# Claude Code Channel provider adapter

## Status and boundary

The repository contains an optional two-way Claude Code Channel adapter for an
interactive session that explicitly loads it. It does not invoke `claude -p` or
the Agent SDK. The user starts Claude Code and approves the development Channel;
Claude Code then spawns the local stdio MCP process.

This avoids Agent SDK credit usage for the connector path, but the interactive
Claude turn still consumes the usage allowance of the active Claude Code
authentication method.

Automated MCP wire and loopback router tests are complete. The remaining live
gate is manual because starting an interactive Claude turn consumes provider
usage and displays local consent dialogs.

Official sources checked on 2026-07-31:

- [Push events into a running session with Channels](https://code.claude.com/docs/en/channels)
- [Channels reference](https://code.claude.com/docs/en/channels-reference)
- [Claude Code MCP configuration](https://code.claude.com/docs/en/mcp)
- [Claude Code authentication](https://code.claude.com/docs/en/iam)
- [Claude Code permission modes](https://code.claude.com/docs/en/permission-modes)

Channels are a research preview. Custom Channels require explicit development
opt-in, and organization policy may disable them. This adapter remains optional
and does not change the managed Agent SDK gateway.

## Components and startup

- `ClaudeChannelAdapter` owns one pending router request and accepts one
  correlated reply.
- `claude-channel-mcp` declares `claude/channel`, emits Channel notifications,
  and exposes `agent_reply`, `agent_list`, and `agent_send`.
- `claude-channel` connects the MCP lifecycle to one primary `GatewayClient`.
- `bun run channel:claude` is the stdio entrypoint spawned by Claude Code.

The Python launcher reduces the one-time setup and later startup commands:

```bash
python3 scripts/asr.py setup-claude
python3 scripts/asr.py claude reviewer
python3 scripts/asr.py claude reviewer --activity "reviewing tests" --auto
```

The first command registers the repository-local MCP entry. The second starts
Claude with the neutral `local:reviewer` identity and the explicit development
Channel opt-in. The third additionally publishes a non-sensitive activity and
requests Auto permission mode when the account supports it.

The launcher's `--dangerously-load-development-channels` is not
`--dangerously-skip-permissions` and does not select `bypassPermissions`. It is
currently required because custom Channels are outside Claude's research-
preview plugin allowlist. It can bypass a configured Channel allowlist for this
explicit server, so it must be used only with the trusted repository-local MCP
entry. Auto mode is independent and does not make the development flag
unnecessary. Removing the flag before packaging and approving this Channel as
a plugin prevents the Channel from registering.

The Channel process connects to the router only after Claude Code completes the
MCP initialization handshake. Closing the MCP session disconnects the gateway
and removes the agent registration. MCP initialization does not prove that
Channel policy accepted notifications: Claude Code can silently discard a
notification when the Channel is not enabled. Only `agent_reply` is a delivery
and completion acknowledgement.

## Request and response flow

```text
coordinator       router       Channel MCP       interactive Claude
    | send(id)       |              |                    |
    |--------------->| deliver(id)  |                    |
    |                |------------->| channel event(id)  |
    |                |              |------------------->|
    |                |              | agent_reply(id)    |
    |                |              |<-------------------|
    |                | reply(id)    |                    |
    |                |<-------------|                    |
    | result(id)     |              |                    |
    |<---------------|              |                    |
```

Claude may also call `agent_list` and `agent_send` through the same MCP process.
Those calls use the primary gateway connection, preserve their own opaque
request IDs, and can run while one inbound Channel request is pending. The
router derives `from` from the registered socket rather than model input.
List results include router-derived `idle`/`busy` status and an optional public
activity supplied when each connector starts.

## Channel notification schema

```json
{
  "method": "notifications/claude/channel",
  "params": {
    "content": "Review the current change.",
    "meta": {
      "request_id": "request-001",
      "from": "local:coordinator",
      "timeout_ms": "60000"
    }
  }
}
```

Channel metadata values are strings and keys use only letters, digits, and
underscores as required by Claude Code. `request_id` is the router correlation
identifier. The Channel adapter does not introduce another provider session ID.

The reply tool input is:

```json
{
  "request_id": "request-001",
  "text": "The review is complete."
}
```

A reply is accepted only when `request_id` matches the single current request,
the text is non-empty, and no reply was previously accepted. Tool results never
echo reply text.

## Busy, timeout, and disconnect

| Condition | Result/action |
| --- | --- |
| another inbound request is pending | `session_busy`; no second notification |
| notification write fails | `provider_disconnected` |
| no correlated reply before deadline | `request_timeout` |
| mismatched or late reply | tool error `request_not_found` |
| empty reply | tool error `invalid_reply` |
| MCP closes during a request | gateway disconnect and `target_disconnected` |
| adapter is already closed | `provider_not_ready` |

Claude Code does not acknowledge processing a notification. It can queue
Channel events while busy, but this adapter deliberately emits at most one
pending event and rejects another router delivery. There is no replay after a
timeout or disconnect.

## Authentication and trust boundary

Claude authentication and usage accounting belong to the interactive Claude
Code process. Before testing, use `/status` inside Claude Code to confirm the
intended subscription authentication. An `ANTHROPIC_API_KEY` in the process
environment can take precedence and cause API billing; do not print or record
its value.

For the current local test, keep the router tokenless and bound to
`127.0.0.1`. Do not put credentials, real paths, hostnames, or machine-specific
configuration in `.mcp.json` or this repository. The Channel is an inbound
prompt boundary: load only this trusted local MCP entry and do not use the
development allowlist bypass for untrusted servers.

The central router sees routed message plaintext. Provider credentials and
Claude session metadata never enter router messages, logs, or test fixtures.

## Manual interactive validation

Use a disposable local workspace and neutral identifiers.

1. Install the locked dependencies and run the automated suite:

   ```bash
   bun install --frozen-lockfile
   bun test
   ```

2. Start the tokenless loopback router in one terminal:

   ```bash
   bun run start
   ```

3. From the repository root, add the Channel only to the current local Claude
   project configuration:

   ```bash
   claude mcp add --transport stdio --scope local agent-session-router-channel -- bun run channel:claude
   ```

4. Start an interactive session with the explicit research-preview opt-in and
   approve only this local development Channel. Add `--permission-mode auto`
   independently when the account supports Auto mode:

   ```bash
   claude --dangerously-load-development-channels server:agent-session-router-channel
   ```

5. Use `/mcp` to verify that `agent_reply`, `agent_list`, and `agent_send` are
   available. The Channel registers as `local:claude-channel` only after this
   MCP initialization.
6. Start a mock or provider gateway under a neutral worker ID. Ask Claude to
   call `agent_list`, then `agent_send` to that exact worker. Verify one
   correlated result returns.
7. From another connected coordinator, send a request to
   `local:claude-channel`. Verify the event appears in the open session and
   Claude calls `agent_reply` with the same `request_id`.
8. Send another request before replying to the first and verify
   `session_busy`. Then verify a short deadline produces `request_timeout` and
   a late `agent_reply` is rejected.
9. Close the Claude session during one pending request and verify the requester
   receives `target_disconnected` without replay.
10. Remove the local MCP entry after the test if it is no longer needed:

    ```bash
    claude mcp remove agent-session-router-channel
    ```

Do not capture prompts, responses, account output, tokens, or local configuration
paths in test logs or repository files.
