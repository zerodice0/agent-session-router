import type { AgentDescriptor } from "./protocol";

export interface AgentSendOptions {
  requestId?: string;
  timeoutMs?: number;
}

export type AgentSendResult =
  | { requestId: string; from: string; ok: true; content: string }
  | { requestId: string; from?: string; ok: false; error: string };

/**
 * Provider-neutral outbound messaging boundary available to coordinator and
 * worker agents. Implementations must keep every response correlated with the
 * originating requestId.
 */
export interface AgentMessenger {
  listAgents(requestId?: string): Promise<AgentDescriptor[]>;
  send(target: string, content: string, options?: AgentSendOptions): Promise<AgentSendResult>;
}
