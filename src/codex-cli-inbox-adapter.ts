import type { SessionAdapter, SessionRequest, SessionResult } from "./session-adapter";

export const DEFAULT_CODEX_CLI_WAIT_MS = 30_000;
export const MAX_CODEX_CLI_WAIT_MS = 60_000;

export interface CodexCliInboxMessage {
  requestId: string;
  from: string;
  content: string;
  timeoutMs: number;
}

export type CodexCliWaitResult =
  | { ok: true; message: CodexCliInboxMessage }
  | {
      ok: false;
      error: "provider_not_ready" | "wait_busy" | "reply_pending" | "wait_timeout";
    };

export type CodexCliReplyResult =
  | { ok: true }
  | {
      ok: false;
      error: "provider_not_ready" | "request_not_found" | "request_not_claimed" | "invalid_reply";
    };

interface PendingRequest {
  request: SessionRequest;
  expiresAt: number;
  claimed: boolean;
  resolve: (result: SessionResult) => void;
  timer: ReturnType<typeof setTimeout>;
}

interface PendingWait {
  resolve: (result: CodexCliWaitResult) => void;
  timer: ReturnType<typeof setTimeout>;
}

/**
 * Pull-based inbox for a stock Codex CLI MCP server. MCP cannot inject an
 * unsolicited user turn, so one router delivery remains pending until Codex
 * claims it with agent_wait and completes it with agent_reply.
 */
export class CodexCliInboxAdapter implements SessionAdapter {
  #pending: PendingRequest | null = null;
  #waiter: PendingWait | null = null;
  #closed = false;

  handle(request: SessionRequest): Promise<SessionResult> {
    if (this.#closed) return Promise.resolve({ ok: false, error: "provider_not_ready" });
    if (this.#pending) return Promise.resolve({ ok: false, error: "session_busy" });

    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.#settle(request.requestId, { ok: false, error: "request_timeout" });
      }, Math.max(1, request.timeoutMs));
      this.#pending = {
        request,
        expiresAt: Date.now() + Math.max(1, request.timeoutMs),
        claimed: false,
        resolve,
        timer,
      };
      this.#releaseWaiter();
    });
  }

  wait(waitMs = DEFAULT_CODEX_CLI_WAIT_MS): Promise<CodexCliWaitResult> {
    if (this.#closed) return Promise.resolve({ ok: false, error: "provider_not_ready" });
    if (this.#pending?.claimed) return Promise.resolve({ ok: false, error: "reply_pending" });
    if (this.#pending) return Promise.resolve(this.#claimPending(this.#pending));
    if (this.#waiter) return Promise.resolve({ ok: false, error: "wait_busy" });

    const boundedWaitMs = Math.max(1, Math.min(Math.floor(waitMs), MAX_CODEX_CLI_WAIT_MS));
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        if (this.#waiter?.timer !== timer) return;
        this.#waiter = null;
        resolve({ ok: false, error: "wait_timeout" });
      }, boundedWaitMs);
      this.#waiter = { resolve, timer };
    });
  }

  reply(requestId: string, content: string): CodexCliReplyResult {
    if (this.#closed) return { ok: false, error: "provider_not_ready" };
    if (typeof content !== "string" || content.length === 0) {
      return { ok: false, error: "invalid_reply" };
    }
    const pending = this.#pending;
    if (!pending || pending.request.requestId !== requestId) {
      return { ok: false, error: "request_not_found" };
    }
    if (!pending.claimed) return { ok: false, error: "request_not_claimed" };

    this.#settle(requestId, { ok: true, content });
    return { ok: true };
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;

    const waiter = this.#waiter;
    this.#waiter = null;
    if (waiter) {
      clearTimeout(waiter.timer);
      waiter.resolve({ ok: false, error: "provider_not_ready" });
    }

    const requestId = this.#pending?.request.requestId;
    if (requestId) this.#settle(requestId, { ok: false, error: "provider_disconnected" });
  }

  #releaseWaiter(): void {
    const pending = this.#pending;
    const waiter = this.#waiter;
    if (!pending || pending.claimed || !waiter) return;
    this.#waiter = null;
    clearTimeout(waiter.timer);
    waiter.resolve(this.#claimPending(pending));
  }

  #claimPending(pending: PendingRequest): CodexCliWaitResult {
    pending.claimed = true;
    return {
      ok: true,
      message: {
        requestId: pending.request.requestId,
        from: pending.request.from,
        content: pending.request.content,
        timeoutMs: Math.max(1, pending.expiresAt - Date.now()),
      },
    };
  }

  #settle(requestId: string, result: SessionResult): void {
    const pending = this.#pending;
    if (!pending || pending.request.requestId !== requestId) return;
    this.#pending = null;
    clearTimeout(pending.timer);
    pending.resolve(result);
  }
}
