import type { SessionAdapter, SessionRequest, SessionResult } from "./session-adapter";

export const CLAUDE_CHANNEL_NOTIFICATION_METHOD = "notifications/claude/channel";

export interface ClaudeChannelEvent {
  content: string;
  meta: {
    request_id: string;
    from: string;
    timeout_ms: string;
  };
}

export type ClaudeChannelNotifier = (event: ClaudeChannelEvent) => Promise<void>;

export type ClaudeChannelReplyResult =
  | { ok: true }
  | { ok: false; error: "provider_not_ready" | "request_not_found" | "invalid_reply" };

interface PendingChannelRequest {
  requestId: string;
  resolve: (result: SessionResult) => void;
  timer: ReturnType<typeof setTimeout>;
}

/**
 * Maps one router delivery to one Claude Code Channel notification. A
 * notification write is not an acknowledgement: only a correlated reply tool
 * call completes the request.
 */
export class ClaudeChannelAdapter implements SessionAdapter {
  #pending: PendingChannelRequest | null = null;
  #closed = false;

  constructor(private readonly notify: ClaudeChannelNotifier) {}

  handle(request: SessionRequest): Promise<SessionResult> {
    if (this.#closed) return Promise.resolve({ ok: false, error: "provider_not_ready" });
    if (this.#pending) return Promise.resolve({ ok: false, error: "session_busy" });

    return new Promise<SessionResult>((resolve) => {
      const timer = setTimeout(() => {
        this.#settle(request.requestId, { ok: false, error: "request_timeout" });
      }, Math.max(1, request.timeoutMs));

      this.#pending = { requestId: request.requestId, resolve, timer };
      void Promise.resolve()
        .then(() =>
          this.notify({
            content: request.content,
            meta: {
              request_id: request.requestId,
              from: request.from,
              timeout_ms: String(request.timeoutMs),
            },
          }),
        )
        .catch(() => {
          this.#settle(request.requestId, { ok: false, error: "provider_disconnected" });
        });
    });
  }

  reply(requestId: string, content: string): ClaudeChannelReplyResult {
    if (this.#closed) return { ok: false, error: "provider_not_ready" };
    if (typeof content !== "string" || content.length === 0) {
      return { ok: false, error: "invalid_reply" };
    }
    if (this.#pending?.requestId !== requestId) {
      return { ok: false, error: "request_not_found" };
    }

    this.#settle(requestId, { ok: true, content });
    return { ok: true };
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    const requestId = this.#pending?.requestId;
    if (requestId) {
      this.#settle(requestId, { ok: false, error: "provider_disconnected" });
    }
  }

  #settle(requestId: string, result: SessionResult): void {
    const pending = this.#pending;
    if (!pending || pending.requestId !== requestId) return;
    this.#pending = null;
    clearTimeout(pending.timer);
    pending.resolve(result);
  }
}
