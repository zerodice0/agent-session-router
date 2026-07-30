export const PROTOCOL_VERSION = 1 as const;
export const DEFAULT_REQUEST_TIMEOUT_MS = 60_000;
export const MAX_REQUEST_TIMEOUT_MS = 10 * 60_000;

export type AgentSide = "claude" | "codex" | "generic";
export type RegistrationRole = "agent" | "delegate";

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
      delegationToken?: string;
    }
  | {
      type: "register_delegate";
      protocolVersion: typeof PROTOCOL_VERSION;
      agentId: string;
      delegationToken: string;
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
  | { type: "registered"; agent: AgentDescriptor; role?: RegistrationRole }
  | { type: "agents"; requestId: string; agents: AgentDescriptor[] }
  | { type: "accepted"; requestId: string; to: string }
  | {
      type: "deliver";
      requestId: string;
      from: string;
      content: string;
      timeoutMs?: number;
    }
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
const DELEGATION_TOKEN_PATTERN = /^[A-Za-z0-9_-]{32,256}$/;

export function isAgentId(value: unknown): value is string {
  return typeof value === "string" && AGENT_ID_PATTERN.test(value);
}

export function isRequestId(value: unknown): value is string {
  return typeof value === "string" && REQUEST_ID_PATTERN.test(value);
}

export function isDelegationToken(value: unknown): value is string {
  return typeof value === "string" && DELEGATION_TOKEN_PATTERN.test(value);
}

export function isAgentDescriptor(value: unknown): value is AgentDescriptor {
  if (!value || typeof value !== "object") return false;
  const descriptor = value as Record<string, unknown>;
  return (
    isAgentId(descriptor.agentId) &&
    (descriptor.side === "claude" ||
      descriptor.side === "codex" ||
      descriptor.side === "generic")
  );
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
      if (message.delegationToken !== undefined && !isDelegationToken(message.delegationToken)) {
        return null;
      }
      return message as ClientMessage;
    }
    case "register_delegate":
      if (message.protocolVersion !== PROTOCOL_VERSION) return null;
      if (!isAgentId(message.agentId) || !isDelegationToken(message.delegationToken)) return null;
      return message as ClientMessage;
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

export function parseServerMessage(raw: unknown): ServerMessage | null {
  if (typeof raw !== "string") return null;

  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return null;
  }

  if (!value || typeof value !== "object") return null;
  const message = value as Record<string, unknown>;

  switch (message.type) {
    case "registered":
      return isAgentDescriptor(message.agent) &&
        (message.role === undefined || message.role === "agent" || message.role === "delegate")
        ? (message as unknown as ServerMessage)
        : null;
    case "agents":
      return isRequestId(message.requestId) &&
        Array.isArray(message.agents) &&
        message.agents.every(isAgentDescriptor)
        ? (message as unknown as ServerMessage)
        : null;
    case "accepted":
      return isRequestId(message.requestId) && isAgentId(message.to)
        ? (message as unknown as ServerMessage)
        : null;
    case "deliver":
      if (!isRequestId(message.requestId) || !isAgentId(message.from)) return null;
      if (typeof message.content !== "string") return null;
      if (message.timeoutMs !== undefined && typeof message.timeoutMs !== "number") return null;
      return message as unknown as ServerMessage;
    case "result":
      if (!isRequestId(message.requestId) || !isAgentId(message.from)) return null;
      if (typeof message.ok !== "boolean") return null;
      if (message.content !== undefined && typeof message.content !== "string") return null;
      if (message.error !== undefined && typeof message.error !== "string") return null;
      return message as unknown as ServerMessage;
    case "error":
      if (message.requestId !== undefined && !isRequestId(message.requestId)) return null;
      if (!isRouterErrorCode(message.code) || typeof message.message !== "string") return null;
      return message as unknown as ServerMessage;
    case "pong":
      return isRequestId(message.requestId) ? (message as unknown as ServerMessage) : null;
    default:
      return null;
  }
}

function isRouterErrorCode(value: unknown): value is RouterErrorCode {
  return (
    value === "invalid_message" ||
    value === "unauthorized" ||
    value === "not_registered" ||
    value === "agent_conflict" ||
    value === "target_offline" ||
    value === "request_conflict" ||
    value === "request_not_found" ||
    value === "reply_forbidden" ||
    value === "target_disconnected" ||
    value === "request_timeout"
  );
}
