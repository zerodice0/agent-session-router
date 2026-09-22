import { describe, expect, test } from "bun:test";
import type { Options, SDKMessage } from "@anthropic-ai/claude-agent-sdk";
import {
  BridgeServer,
  ClaudeBridgeSession,
  FrameDecoder,
  MAX_FRAME_BYTES,
  encodeFrame,
  type ClaudeQueryHandle,
  type ClaudeSdkRuntime,
  type ClaudeWarmHandle,
} from "./bridge";

const SESSION_A = "11111111-1111-4111-8111-111111111111";
const SESSION_B = "22222222-2222-4222-8222-222222222222";

type QueryFactory = (options: Options) => ClaudeQueryHandle;

class FakeRuntime implements ClaudeSdkRuntime {
  readonly startupCalls: Array<{ options: Options; initializeTimeoutMs: number }> = [];
  readonly queryCalls: Array<{ prompt: string; options: Options }> = [];
  readonly warmPrompts: string[] = [];

  constructor(
    private readonly warmFactory: QueryFactory,
    private readonly factories: QueryFactory[] = [],
  ) {}

  async startup(params: {
    options: Options;
    initializeTimeoutMs: number;
  }): Promise<ClaudeWarmHandle> {
    this.startupCalls.push(params);
    return {
      query: (prompt) => {
        if (typeof prompt !== "string") throw new Error("unexpected streaming prompt");
        this.warmPrompts.push(prompt);
        return this.warmFactory(params.options);
      },
      close() {},
    };
  }

  query(params: { prompt: string; options: Options }): ClaudeQueryHandle {
    this.queryCalls.push(params);
    const factory = this.factories.shift();
    if (!factory) throw new Error("unexpected query");
    return factory(params.options);
  }
}

function messages(...values: SDKMessage[]): ClaudeQueryHandle {
  return {
    async *[Symbol.asyncIterator]() {
      for (const value of values) yield value;
    },
    close() {},
  };
}

function resultMessage(
  subtype:
    | "success"
    | "error_during_execution"
    | "error_max_turns"
    | "error_max_budget_usd"
    | "error_max_structured_output_retries",
  sessionId: string,
  result = "",
): SDKMessage {
  return {
    type: "result",
    subtype,
    session_id: sessionId,
    is_error: subtype !== "success",
    ...(subtype === "success" ? { result } : {}),
  } as unknown as SDKMessage;
}

function waiting(options: Options): ClaudeQueryHandle {
  return {
    async *[Symbol.asyncIterator]() {
      await new Promise<void>((_resolve, reject) => {
        options.abortController?.signal.addEventListener(
          "abort",
          () => reject(new Error("aborted")),
          { once: true },
        );
      });
    },
    close() {},
  };
}

function initialize(id: number, resumeSessionId?: string) {
  return {
    v: 1 as const,
    id,
    method: "initialize" as const,
    params: {
      ...(resumeSessionId ? { resumeSessionId } : {}),
      cwd: "/tmp/project",
      executablePath: "/opt/claude",
      initializeTimeoutMs: 10_000,
      maxTurns: 8,
      mcp: {
        command: "/opt/asr/bin/asr",
        args: ["mcp", "delegate", "--context-file", "/tmp/private/context.json"],
        env: {
          PATH: "/neutral/bin",
          ROUTER_TOKEN: "must-not-forward",
          GITHUB_TOKEN: "must-not-forward",
        },
      },
    },
  };
}

describe("Claude SDK framed bridge", () => {
  test("prewarms, returns terminal content, and resumes only a captured UUID session", async () => {
    const runtime = new FakeRuntime(
      () => messages(resultMessage("success", SESSION_A, "first")),
      [() => messages(resultMessage("success", SESSION_B, "second"))],
    );
    const server = new BridgeServer(
      new ClaudeBridgeSession(runtime, {
        PATH: "/neutral/bin",
        ANTHROPIC_API_KEY: "provider-key",
        ROUTER_TOKEN: "central-secret",
        AGENT_ROUTER_DELEGATION_TOKEN: "central-secret",
        ASR_CA_FILE: "/tmp/central-ca.pem",
        LINEAR_API_KEY: "central-secret",
      }),
    );

    expect(await server.dispatch(initialize(1))).toMatchObject({
      ok: true,
      result: { state: "ready" },
    });
    const options = runtime.startupCalls[0]?.options;
    expect(options?.tools).toEqual([]);
    expect(options?.strictMcpConfig).toBe(true);
    expect(options?.permissionMode).toBe("dontAsk");
    expect(options?.env?.ANTHROPIC_API_KEY).toBe("provider-key");
    expect(options?.env?.ROUTER_TOKEN).toBeUndefined();
    expect(options?.env?.AGENT_ROUTER_DELEGATION_TOKEN).toBeUndefined();
    expect(options?.env?.LINEAR_API_KEY).toBeUndefined();
    expect(options?.env?.ASR_CA_FILE).toBeUndefined();
    expect(options?.mcpServers?.agent_session_router).toMatchObject({
      type: "stdio",
      command: "/opt/asr/bin/asr",
      alwaysLoad: true,
      timeout: 605_000,
      env: { PATH: "/neutral/bin" },
    });

    expect(
      await server.dispatch({
        v: 1,
        id: 2,
        method: "turn",
        params: { requestId: "request-1", prompt: "first", timeoutMs: 2_000 },
      }),
    ).toEqual({
      v: 1,
      id: 2,
      ok: true,
      result: { content: "first", sessionId: SESSION_A },
    });
    expect(runtime.warmPrompts).toEqual(["first"]);

    expect(
      await server.dispatch({
        v: 1,
        id: 3,
        method: "turn",
        params: { requestId: "request-2", prompt: "second", timeoutMs: 2_000 },
      }),
    ).toMatchObject({ ok: true, result: { content: "second", sessionId: SESSION_B } });
    expect(runtime.queryCalls[0]?.options.resume).toBe(SESSION_A);
  });

  test("maps stable SDK failures and cancels an active iterator without exposing details", async () => {
    const errorRuntime = new FakeRuntime(() =>
      messages(resultMessage("error_max_budget_usd", SESSION_A)),
    );
    const errorServer = new BridgeServer(new ClaudeBridgeSession(errorRuntime));
    await errorServer.dispatch(initialize(1));
    expect(
      await errorServer.dispatch({
        v: 1,
        id: 2,
        method: "turn",
        params: { requestId: "budget", prompt: "work", timeoutMs: 1_000 },
      }),
    ).toEqual({
      v: 1,
      id: 2,
      ok: false,
      error: { code: "claude_max_budget", sessionId: SESSION_A },
    });

    const cancelRuntime = new FakeRuntime(waiting);
    const cancelServer = new BridgeServer(new ClaudeBridgeSession(cancelRuntime));
    await cancelServer.dispatch(initialize(1));
    const active = cancelServer.dispatch({
      v: 1,
      id: 2,
      method: "turn",
      params: { requestId: "active", prompt: "wait", timeoutMs: 10_000 },
    });
    await Promise.resolve();
    expect(
      await cancelServer.dispatch({
        v: 1,
        id: 3,
        method: "cancel",
        params: { requestId: "active", reason: "task_interrupted" },
      }),
    ).toEqual({ v: 1, id: 3, ok: true, result: { cancelled: true } });
    expect(await active).toMatchObject({
      ok: false,
      error: { code: "task_interrupted" },
    });
  });

  test("times out before the router deadline and rejects duplicate correlation ids", async () => {
    const runtime = new FakeRuntime(waiting);
    const server = new BridgeServer(new ClaudeBridgeSession(runtime));
    await server.dispatch(initialize(1));
    expect(
      await server.dispatch({
        v: 1,
        id: 2,
        method: "turn",
        params: { requestId: "slow", prompt: "wait", timeoutMs: 20 },
      }),
    ).toMatchObject({ ok: false, error: { code: "request_timeout" } });
    expect(await server.dispatch(initialize(1))).toEqual({
      v: 1,
      id: 1,
      ok: false,
      error: { code: "bridge_protocol_error" },
    });
  });

  test("decodes fragmented and coalesced frames and fails closed on invalid lengths", () => {
    const first = encodeFrame({ v: 1, id: 1, method: "shutdown", params: {} });
    const second = encodeFrame({ v: 1, id: 2, method: "shutdown", params: {} });
    const joined = Buffer.concat([first, second]);
    const decoder = new FrameDecoder();
    expect(decoder.push(joined.subarray(0, 3))).toEqual([]);
    expect(decoder.push(joined.subarray(3))).toEqual([
      { v: 1, id: 1, method: "shutdown", params: {} },
      { v: 1, id: 2, method: "shutdown", params: {} },
    ]);
    decoder.finish();

    const oversized = Buffer.alloc(4);
    oversized.writeUInt32BE(MAX_FRAME_BYTES + 1, 0);
    expect(() => new FrameDecoder().push(oversized)).toThrow("bridge_protocol_error");
  });
});
