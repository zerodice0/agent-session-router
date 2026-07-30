import {
  normalizeTimeoutMs,
  parseClientMessage,
  type AgentDescriptor,
  type RouterErrorCode,
  type ServerMessage,
} from "./protocol";
import { AgentRegistry, sendMessage, type AgentConnection } from "./registry";

interface SocketData {
  connectedAt: number;
}

interface PendingRequest {
  requestId: string;
  requester: AgentConnection;
  requesterId: string;
  recipient: AgentConnection;
  recipientId: string;
  timer: ReturnType<typeof setTimeout>;
}

export interface RouterOptions {
  hostname?: string;
  port?: number;
  /** null explicitly disables an inherited ROUTER_TOKEN for in-process tests. */
  token?: string | null;
  logEvents?: boolean;
}

function parsePort(value: string | undefined): number {
  if (value === undefined) return 8787;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 65_535) {
    throw new Error("ROUTER_PORT must be an integer between 1 and 65535");
  }
  return parsed;
}

export function startRouter(options: RouterOptions = {}) {
  const registry = new AgentRegistry();
  const pendingRequests = new Map<string, PendingRequest>();
  const hostname = options.hostname?.trim() || "127.0.0.1";
  const port = options.port ?? parsePort(process.env.ROUTER_PORT);
  const expectedToken = options.token === null ? undefined : options.token ?? process.env.ROUTER_TOKEN;
  const logEvents = options.logEvents ?? false;

  function sendError(
    connection: AgentConnection,
    code: RouterErrorCode,
    message: string,
    requestId?: string,
  ): void {
    sendMessage(connection, { type: "error", code, message, requestId });
  }

  function requireRegistered(connection: AgentConnection, requestId?: string): AgentDescriptor | null {
    const registered = registry.getByConnection(connection);
    if (registered) return registered.descriptor;
    sendError(connection, "not_registered", "Register before using the router", requestId);
    return null;
  }

  function finishRequest(requestId: string, message: ServerMessage): void {
    const pending = pendingRequests.get(requestId);
    if (!pending) return;
    clearTimeout(pending.timer);
    pendingRequests.delete(requestId);
    sendMessage(pending.requester, message);
  }

  function handleDisconnect(connection: AgentConnection): void {
    const disconnected = registry.unregister(connection);

    for (const [requestId, pending] of pendingRequests) {
      if (pending.requester === connection) {
        clearTimeout(pending.timer);
        pendingRequests.delete(requestId);
        continue;
      }

      if (pending.recipient === connection) {
        finishRequest(requestId, {
          type: "error",
          requestId,
          code: "target_disconnected",
          message: `Target disconnected: ${pending.recipientId}`,
        });
      }
    }

    if (logEvents && disconnected) {
      console.info(`agent disconnected: ${disconnected.descriptor.agentId}`);
    }
  }

  function handleMessage(connection: AgentConnection, raw: string): void {
    const message = parseClientMessage(raw);
    if (!message) {
      sendError(connection, "invalid_message", "Message does not match protocol version 1");
      return;
    }

    if (message.type === "register") {
      if (registry.getByConnection(connection)) {
        sendError(connection, "agent_conflict", "This connection is already registered");
        return;
      }
      if (expectedToken !== undefined && message.token !== expectedToken) {
        sendError(connection, "unauthorized", "Registration token is invalid");
        return;
      }
      if (!registry.register(message.agent, connection)) {
        sendError(connection, "agent_conflict", `Agent is already connected: ${message.agent.agentId}`);
        return;
      }
      sendMessage(connection, { type: "registered", agent: message.agent });
      if (logEvents) console.info(`agent registered: ${message.agent.agentId}`);
      return;
    }

    const sender = requireRegistered(connection, "requestId" in message ? message.requestId : undefined);
    if (!sender) return;

    switch (message.type) {
      case "list":
        sendMessage(connection, { type: "agents", requestId: message.requestId, agents: registry.list() });
        return;
      case "ping":
        sendMessage(connection, { type: "pong", requestId: message.requestId });
        return;
      case "send": {
        if (pendingRequests.has(message.requestId)) {
          sendError(connection, "request_conflict", "Request ID is already active", message.requestId);
          return;
        }
        const target = registry.get(message.to);
        if (!target) {
          sendError(connection, "target_offline", `Target is not connected: ${message.to}`, message.requestId);
          return;
        }
        const timeoutMs = normalizeTimeoutMs(message.timeoutMs);
        const timer = setTimeout(() => {
          finishRequest(message.requestId, {
            type: "error",
            requestId: message.requestId,
            code: "request_timeout",
            message: `Request timed out after ${timeoutMs}ms`,
          });
        }, timeoutMs);
        pendingRequests.set(message.requestId, {
          requestId: message.requestId,
          requester: connection,
          requesterId: sender.agentId,
          recipient: target.connection,
          recipientId: target.descriptor.agentId,
          timer,
        });
        sendMessage(target.connection, {
          type: "deliver",
          requestId: message.requestId,
          from: sender.agentId,
          content: message.content,
          timeoutMs,
        });
        sendMessage(connection, { type: "accepted", requestId: message.requestId, to: message.to });
        return;
      }
      case "reply": {
        const pending = pendingRequests.get(message.requestId);
        if (!pending) {
          sendError(connection, "request_not_found", "Request is no longer active", message.requestId);
          return;
        }
        if (pending.recipient !== connection) {
          sendError(connection, "reply_forbidden", "Only the request recipient can reply", message.requestId);
          return;
        }
        finishRequest(message.requestId, {
          type: "result",
          requestId: message.requestId,
          from: sender.agentId,
          ok: message.ok,
          content: message.content,
          error: message.error,
        });
      }
    }
  }

  return Bun.serve<SocketData>({
    hostname,
    port,
    fetch(request, server) {
      const url = new URL(request.url);
      if (url.pathname === "/healthz") {
        return Response.json({ status: "ok", connectedAgents: registry.list().length });
      }
      if (url.pathname === "/ws" && server.upgrade(request, { data: { connectedAt: Date.now() } })) {
        return undefined;
      }
      return new Response("Not found", { status: 404 });
    },
    websocket: {
      message(socket, message) {
        const raw = typeof message === "string" ? message : new TextDecoder().decode(message);
        handleMessage(socket, raw);
      },
      close(socket) {
        handleDisconnect(socket);
      },
    },
  });
}

if (import.meta.main) {
  const server = startRouter({
    hostname: process.env.ROUTER_HOST?.trim() || "127.0.0.1",
    logEvents: true,
  });
  console.info(`agent-session-router listening on ws://${server.hostname}:${server.port}/ws`);
}
