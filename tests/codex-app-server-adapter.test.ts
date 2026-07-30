import { describe, expect, test } from "bun:test";
import { CodexAppServerAdapter } from "../src/codex-app-server-adapter";
import type {
  CodexAppServerTransport,
  CodexCloseListener,
  CodexMessageListener,
} from "../src/codex-app-server-transport";

type JsonObject = Record<string, unknown>;

class FakeCodexTransport implements CodexAppServerTransport {
  readonly sent: JsonObject[] = [];
  readonly #messageListeners = new Set<CodexMessageListener>();
  readonly #closeListeners = new Set<CodexCloseListener>();
  #closed = false;
  turnStartHandler: ((message: JsonObject) => void) | null = null;

  constructor(readonly threadId = "thread-owned") {}

  send(value: unknown): void {
    if (this.#closed) throw new Error("Fake transport is closed");
    if (!isJsonObject(value)) throw new TypeError("Expected JSON object");
    this.sent.push(value);

    if (typeof value.id !== "number" || typeof value.method !== "string") return;
    if (value.method === "initialize") {
      this.emit({ id: value.id, result: { userAgent: "test" } });
    } else if (value.method === "thread/start" || value.method === "thread/resume") {
      this.emit({ id: value.id, result: { thread: { id: this.threadId } } });
    } else if (value.method === "turn/start") {
      this.turnStartHandler?.(value);
    } else if (value.method === "turn/interrupt") {
      this.emit({ id: value.id, result: {} });
    }
  }

  onMessage(listener: CodexMessageListener): () => void {
    this.#messageListeners.add(listener);
    return () => this.#messageListeners.delete(listener);
  }

  onClose(listener: CodexCloseListener): () => void {
    this.#closeListeners.add(listener);
    return () => this.#closeListeners.delete(listener);
  }

  emit(message: JsonObject): void {
    for (const listener of [...this.#messageListeners]) listener(message);
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const listener of [...this.#closeListeners]) listener();
  }
}

function isJsonObject(value: unknown): value is JsonObject {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function responseId(message: JsonObject): number {
  if (typeof message.id !== "number") throw new Error("Missing request id");
  return message.id;
}

describe("CodexAppServerAdapter", () => {
  test("initializes a gateway-owned thread and returns only the correlated final answer", async () => {
    const transport = new FakeCodexTransport();
    const adapter = await CodexAppServerAdapter.connect({ transport });
    transport.turnStartHandler = (message) => {
      const id = responseId(message);
      transport.emit({
        id,
        result: { turn: { id: "turn-one", status: "inProgress", items: [] } },
      });
      transport.emit({
        method: "item/completed",
        params: {
          threadId: transport.threadId,
          turnId: "other-turn",
          completedAtMs: 1,
          item: { type: "agentMessage", id: "other-item", text: "wrong" },
        },
      });
      transport.emit({
        method: "item/commandExecution/requestApproval",
        id: "approval-one",
        params: {
          threadId: transport.threadId,
          turnId: "turn-one",
          itemId: "command-one",
        },
      });
      transport.emit({
        method: "item/completed",
        params: {
          threadId: transport.threadId,
          turnId: "turn-one",
          completedAtMs: 2,
          item: {
            type: "agentMessage",
            id: "commentary-one",
            text: "progress",
            phase: "commentary",
          },
        },
      });
      transport.emit({
        method: "item/completed",
        params: {
          threadId: transport.threadId,
          turnId: "turn-one",
          completedAtMs: 3,
          item: {
            type: "agentMessage",
            id: "answer-one",
            text: "final-result",
            phase: "final_answer",
          },
        },
      });
      transport.emit({
        method: "turn/completed",
        params: {
          threadId: transport.threadId,
          turn: { id: "turn-one", status: "completed", items: [] },
        },
      });
    };

    try {
      expect(adapter.threadId).toBe("thread-owned");
      expect(transport.sent.slice(0, 3).map(({ method }) => method)).toEqual([
        "initialize",
        "initialized",
        "thread/start",
      ]);

      const result = await adapter.handle({
        requestId: "router-request-one",
        from: "local:coordinator",
        content: "perform-task",
        timeoutMs: 1_000,
      });
      expect(result).toEqual({ ok: true, content: "final-result" });
      expect(transport.sent).toContainEqual({
        id: "approval-one",
        result: { decision: "decline" },
      });
      expect(transport.sent).toContainEqual({
        method: "turn/start",
        id: expect.any(Number),
        params: {
          threadId: "thread-owned",
          input: [{ type: "text", text: "perform-task" }],
        },
      });
    } finally {
      adapter.close();
    }
  });

  test("rejects concurrent turns as busy and supports a selected resume thread", async () => {
    const transport = new FakeCodexTransport("thread-resumed");
    const adapter = await CodexAppServerAdapter.connect({
      transport,
      thread: { mode: "resume", threadId: "thread-resumed" },
    });
    transport.turnStartHandler = (message) => {
      transport.emit({
        id: responseId(message),
        result: { turn: { id: "turn-busy", status: "inProgress", items: [] } },
      });
    };

    try {
      expect(transport.sent).toContainEqual({
        method: "thread/resume",
        id: expect.any(Number),
        params: { threadId: "thread-resumed" },
      });
      const first = adapter.handle({
        requestId: "busy-first",
        from: "local:coordinator",
        content: "first",
        timeoutMs: 1_000,
      });
      expect(
        await adapter.handle({
          requestId: "busy-second",
          from: "local:coordinator",
          content: "second",
          timeoutMs: 1_000,
        }),
      ).toEqual({ ok: false, error: "session_busy" });

      transport.emit({
        method: "item/completed",
        params: {
          threadId: transport.threadId,
          turnId: "turn-busy",
          completedAtMs: 1,
          item: { type: "agentMessage", id: "answer-busy", text: "first-result" },
        },
      });
      transport.emit({
        method: "turn/completed",
        params: {
          threadId: transport.threadId,
          turn: { id: "turn-busy", status: "completed", items: [] },
        },
      });
      expect(await first).toEqual({ ok: true, content: "first-result" });
    } finally {
      adapter.close();
    }
  });

  test("interrupts a timed-out turn and fails an active turn on disconnect", async () => {
    const timeoutTransport = new FakeCodexTransport();
    const timeoutAdapter = await CodexAppServerAdapter.connect({ transport: timeoutTransport });
    timeoutTransport.turnStartHandler = (message) => {
      timeoutTransport.emit({
        id: responseId(message),
        result: { turn: { id: "turn-timeout", status: "inProgress", items: [] } },
      });
    };

    try {
      expect(
        await timeoutAdapter.handle({
          requestId: "timeout-request",
          from: "local:coordinator",
          content: "wait",
          timeoutMs: 20,
        }),
      ).toEqual({ ok: false, error: "request_timeout" });
      expect(timeoutTransport.sent).toContainEqual({
        method: "turn/interrupt",
        id: expect.any(Number),
        params: { threadId: "thread-owned", turnId: "turn-timeout" },
      });
    } finally {
      timeoutAdapter.close();
    }

    const disconnectTransport = new FakeCodexTransport();
    const disconnectAdapter = await CodexAppServerAdapter.connect({
      transport: disconnectTransport,
    });
    disconnectTransport.turnStartHandler = (message) => {
      disconnectTransport.emit({
        id: responseId(message),
        result: { turn: { id: "turn-disconnect", status: "inProgress", items: [] } },
      });
    };

    const pending = disconnectAdapter.handle({
      requestId: "disconnect-request",
      from: "local:coordinator",
      content: "wait",
      timeoutMs: 1_000,
    });
    disconnectTransport.close();
    expect(await pending).toEqual({ ok: false, error: "provider_disconnected" });
    disconnectAdapter.close();
  });

  test("maps failed and interrupted terminal turn states", async () => {
    const transport = new FakeCodexTransport();
    const adapter = await CodexAppServerAdapter.connect({ transport });
    let turnNumber = 0;
    transport.turnStartHandler = (message) => {
      turnNumber += 1;
      const turnId = `turn-terminal-${turnNumber}`;
      const status = turnNumber === 1 ? "failed" : "interrupted";
      transport.emit({
        id: responseId(message),
        result: { turn: { id: turnId, status: "inProgress", items: [] } },
      });
      transport.emit({
        method: "turn/completed",
        params: {
          threadId: transport.threadId,
          turn: { id: turnId, status, items: [] },
        },
      });
    };

    try {
      expect(
        await adapter.handle({
          requestId: "failed-request",
          from: "local:coordinator",
          content: "first",
          timeoutMs: 1_000,
        }),
      ).toEqual({ ok: false, error: "codex_turn_failed" });
      expect(
        await adapter.handle({
          requestId: "interrupted-request",
          from: "local:coordinator",
          content: "second",
          timeoutMs: 1_000,
        }),
      ).toEqual({ ok: false, error: "codex_turn_interrupted" });
    } finally {
      adapter.close();
    }
  });
});
