export const PROTOCOL_VERSION = 1 as const;
export const DEFAULT_REQUEST_TIMEOUT_MS = 60_000;
export const MAX_REQUEST_TIMEOUT_MS = 10 * 60_000;

export type AgentSide = "claude" | "codex" | "generic";

export interface AgentDescriptor {
  agentId: string;
  side: AgentSide;
}

export type ClientMessage =
  | {
      type: "register";
      protocolVersion: typeof PROTOCOL_VERSION;
      agent: AgentDescriptor;
      token?: string;
    }
  | { type: "list"; requestId: string }
  | {
      type: "send";
      requestId: string;
      to: string;
      content: string;
      timeoutMs?: number;
    }
  | {
      type: "reply";
      requestId: string;
      ok: boolean;
      content?: string;
      error?: string;
    }
  | { type: "ping"; requestId: string };

export type RouterErrorCode =
  | "invalid_message"
  | "unauthorized"
  | "not_registered"
  | "agent_conflict"
  | "target_offline"
  | "request_conflict"
  | "request_not_found"
  | "reply_forbidden"
  | "target_disconnected"
  | "request_timeout";

export type ServerMessage =
  | { type: "registered"; agent: AgentDescriptor }
  | { type: "agents"; requestId: string; agents: AgentDescriptor[] }
  | { type: "accepted"; requestId: string; to: string }
  | { type: "deliver"; requestId: string; from: string; content: string }
  | {
      type: "result";
      requestId: string;
      from: string;
      ok: boolean;
      content?: string;
      error?: string;
    }
  | { type: "error"; requestId?: string; code: RouterErrorCode; message: string }
  | { type: "pong"; requestId: string };

const AGENT_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const REQUEST_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;

export function isAgentId(value: unknown): value is string {
  return typeof value === "string" && AGENT_ID_PATTERN.test(value);
}

export function isRequestId(value: unknown): value is string {
  return typeof value === "string" && REQUEST_ID_PATTERN.test(value);
}

export function normalizeTimeoutMs(value: unknown): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return DEFAULT_REQUEST_TIMEOUT_MS;
  }

  return Math.max(1, Math.min(Math.floor(value), MAX_REQUEST_TIMEOUT_MS));
}

export function parseClientMessage(raw: string): ClientMessage | null {
  let value: unknown;

  try {
    value = JSON.parse(raw);
  } catch {
    return null;
  }

  if (!value || typeof value !== "object") return null;
  const message = value as Record<string, unknown>;

  switch (message.type) {
    case "register": {
      if (message.protocolVersion !== PROTOCOL_VERSION) return null;
      if (!message.agent || typeof message.agent !== "object") return null;
      const agent = message.agent as Record<string, unknown>;
      if (!isAgentId(agent.agentId)) return null;
      if (agent.side !== "claude" && agent.side !== "codex" && agent.side !== "generic") {
        return null;
      }
      if (message.token !== undefined && typeof message.token !== "string") return null;
      return message as ClientMessage;
    }
    case "list":
    case "ping":
      return isRequestId(message.requestId) ? (message as ClientMessage) : null;
    case "send":
      if (!isRequestId(message.requestId) || !isAgentId(message.to)) return null;
      if (typeof message.content !== "string" || message.content.length === 0) return null;
      if (message.timeoutMs !== undefined && typeof message.timeoutMs !== "number") return null;
      return message as ClientMessage;
    case "reply":
      if (!isRequestId(message.requestId) || typeof message.ok !== "boolean") return null;
      if (message.content !== undefined && typeof message.content !== "string") return null;
      if (message.error !== undefined && typeof message.error !== "string") return null;
      return message as ClientMessage;
    default:
      return null;
  }
}

