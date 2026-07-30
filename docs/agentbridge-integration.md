# AgentBridge integration decision

## Status

Decision: not selected as a required runtime integration.

AgentBridge was reviewed as a reference implementation while defining the
session boundary for this project. `agent-session-router` will not patch, fork,
vendor, or require AgentBridge. A future optional adapter may be considered only
if AgentBridge publishes a stable external session request/result contract that
can be consumed without modifying its source.

## Why it was not selected

The review used the official `v0.1.30` release (`55120e8`) available on
2026-07-30. That stable control protocol did not provide the combination needed
by this router:

- a gateway connection role separate from the single attached Claude frontend;
- a request identifier correlated through the final Codex response;
- a control request that pushes work into Claude and receives a correlated
  Claude reply;
- an external API that preserves AgentBridge's existing busy, approval, and
  disconnect ownership.

Using the existing Claude attach role for a sidecar would contend with the live
frontend. Connecting another client directly to a Codex app-server would bypass
AgentBridge's control ownership and would not turn the AgentBridge control
socket into a supported request/result API.

Primary source references:

- [AgentBridge `v0.1.30` release](https://github.com/raysonmeng/agent-bridge/releases/tag/v0.1.30)
- [control protocol](https://github.com/raysonmeng/agent-bridge/blob/v0.1.30/src/control-protocol.ts#L76-L181)
- [Claude attach admission](https://github.com/raysonmeng/agent-bridge/blob/v0.1.30/src/daemon-identity.ts#L139-L170)
- [Codex turn injection](https://github.com/raysonmeng/agent-bridge/blob/v0.1.30/src/codex-adapter.ts#L505-L555)
- [final Codex message conversion](https://github.com/raysonmeng/agent-bridge/blob/v0.1.30/src/codex-adapter.ts#L2030-L2065)
- [Claude channel and reply integration](https://github.com/raysonmeng/agent-bridge/blob/v0.1.30/src/claude-adapter.ts#L234-L260)

## What remains useful

The review produced provider-independent constraints that remain part of this
project:

- one explicit `agentId` maps to one active inbound session gateway;
- caller `requestId` is preserved to the final result;
- accepted delivery is not final completion;
- busy defaults to rejection rather than implicit queue, steer, or interrupt;
- timeout and disconnect remove pending state and never trigger automatic
  replay;
- provider credentials and session metadata stay on the agent system;
- prompts, responses, credentials, and runtime paths are not logged.

The current router, `GatewayClient`, `SessionAdapter`, and mock isolation tests
implement or validate these provider-neutral rules. None imports AgentBridge
code or reads AgentBridge runtime state.

## Selected direction

Provider-native adapters sit below the gateway boundary:

- Codex: the official App Server lifecycle (`initialize`, `thread/start` or
  `thread/resume`, `turn/start`, item events, and `turn/completed`);
- Claude managed session: the official Agent SDK or CLI streaming session;
- Claude live session, optional: an explicitly enabled Claude Code Channel with
  a correlated reply tool.

The exact connection, trust, and agent-to-agent message flow is documented in
[provider-integration.md](provider-integration.md).

## Reconsideration criteria

AgentBridge can be reconsidered as an optional provider adapter if all of the
following are true in a future stable release:

1. A non-exclusive authenticated gateway role is public and versioned.
2. The API returns final results correlated by a caller request ID.
3. Claude and Codex target readiness, busy, timeout, and disconnect semantics
   are defined.
4. The integration can be implemented without modifying or depending on
   internal pair files beyond documented read-only configuration.
5. A development-machine test proves that using it does not displace or corrupt
   an existing interactive session.

Until then, AgentBridge remains a source reference rather than a delivery
dependency or roadmap blocker.
