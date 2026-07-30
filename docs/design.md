# Agent Session Router design

## 1. Purpose

AgentBridge provides a useful one-to-one connection between one Claude session
and one Codex thread. It does not provide recipient selection across several
independent pairs.

Agent Session Router adds that missing selection layer while preserving pair
isolation. A coordinator sends a message to an `agentId`; the router resolves the
currently connected session, forwards the request, and returns the correlated
result.

## 2. Goals

- Keep one active agent session per registered `agentId`.
- Route a request to one explicit recipient.
- Correlate the eventual result through a caller-provided `requestId`.
- Report offline, duplicate, unauthorized, and timeout states clearly.
- Bind to loopback by default for safe local integration testing.
- Avoid storing message content or environment-specific connection details.

## 3. Non-goals for the first version

- Group chat or broadcast delivery.
- Durable offline queues.
- Automatic task scheduling or load balancing.
- Shared state between router processes.
- Public internet exposure.
- Replacing AgentBridge's Claude or Codex integration.

These features should only be added after a concrete test demonstrates that the
local request/reply flow is insufficient.

## 4. Architecture

```text
Coordinator session
        |
        | agent_send(agentId, prompt)
        v
+-----------------------+
| Agent Session Router  |
| - registry            |
| - pending requests    |
+-----------------------+
      |             |
      v             v
Gateway A         Gateway B
      |             |
Agent pair A      Agent pair B
```

### 4.1 Router

The router owns two in-memory maps:

- `agentId -> active WebSocket`
- `requestId -> requester, recipient, timeout`

It never chooses a recipient implicitly. Every `send` message names exactly one
`agentId`.

### 4.2 Gateway

A future AgentBridge gateway will translate between the router protocol and one
local AgentBridge pair. The gateway registers one neutral identifier such as
`local:reviewer` and keeps environment-specific pair metadata in local runtime
configuration, not in this repository.

The gateway is the only component allowed to read a pair's local credentials.
Those credentials must never be forwarded to the router.

### 4.3 Coordinator adapter

A future MCP adapter will expose a minimal tool surface:

```text
agent_list()
agent_send(target, prompt, timeoutMs?)
agent_status(requestId)
```

The first adapter should wrap the router protocol directly. It should not add a
second task model, database, or message broker.

## 5. Protocol

Protocol messages are JSON objects with a string `type`. Registration includes a
numeric `protocolVersion`, currently `1`.

### 5.1 Client to router

- `register`: claim one `agentId` for the current connection.
- `list`: request the active agent list.
- `send`: deliver content to one active recipient.
- `reply`: complete one request previously delivered to this connection.
- `ping`: verify that the router is responsive.

### 5.2 Router to client

- `registered`: registration succeeded.
- `agents`: active agent descriptors.
- `accepted`: the request was delivered to the recipient socket.
- `deliver`: a recipient should process the request.
- `result`: successful or failed request completion.
- `error`: protocol or routing failure.
- `pong`: response to `ping`.

### 5.3 Request lifecycle

```text
requester            router                recipient
    | send              |                       |
    |------------------>|                       |
    |                   | deliver               |
    |                   |---------------------->|
    | accepted          |                       |
    |<------------------|                       |
    |                   | reply                 |
    |                   |<----------------------|
    | result            |                       |
    |<------------------|                       |
```

The router rejects duplicate live `requestId` values. A request is removed when
it receives one valid reply, times out, or either relevant socket disconnects.

## 6. Identity and addressing

An `agentId` is a routing identifier, not a hostname or process address. It must
match this pattern:

```text
^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$
```

Recommended neutral structure:

```text
<scope>:<role>
```

Examples:

- `local:coordinator`
- `local:reviewer`
- `lab:tester`

Duplicate live registrations are rejected. This avoids silent replacement and
makes session ownership visible.

## 7. Busy and failure behavior

- Unknown recipient: return `target_offline` immediately.
- Duplicate `agentId`: return `agent_conflict`.
- Duplicate `requestId`: return `request_conflict`.
- Recipient disconnect: return `target_disconnected` to the requester.
- Requester disconnect: cancel the pending request without retrying.
- Timeout: return `request_timeout` and discard a later reply.

The first version does not queue or retry. This keeps delivery semantics clear
while the actual AgentBridge integration is being validated.

## 8. Security and privacy

- Default listener: `127.0.0.1` only.
- Optional shared token for local integration tests.
- Never log prompts, responses, tokens, credentials, or local pair metadata.
- Never place real infrastructure identifiers in source, tests, or examples.
- Reject a reply unless it comes from the connection that received the request.
- Use TLS and per-gateway credentials before enabling any non-loopback listener.

A later multi-host mode should use outbound authenticated gateway connections.
The router should not directly access a remote AgentBridge control port.

## 9. Implementation phases

### Phase 1: local routing foundation

- WebSocket router
- in-memory registry
- targeted send/reply flow
- unit tests

### Phase 2: AgentBridge gateway

- one gateway per isolated pair
- router-to-session delivery
- session-to-router reply correlation
- busy and completion translation

### Phase 3: coordinator MCP adapter

- `agent_list`
- `agent_send`
- `agent_status` only if asynchronous polling proves necessary

### Phase 4: optional multi-host transport

- authenticated outbound gateway connections
- TLS
- reconnect behavior
- deployment configuration stored outside the repository

## 10. Validation plan

The first end-to-end test should run entirely on one development machine:

1. Start the router on loopback.
2. Register a coordinator and two mock workers.
3. Send requests to each worker by `agentId`.
4. Verify replies return only to the matching requester and `requestId`.
5. Verify duplicate registration, offline target, timeout, and disconnect paths.
6. Replace one mock worker with an AgentBridge gateway.

Multi-host deployment is not required to validate the routing model.

