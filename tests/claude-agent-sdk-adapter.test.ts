import { describe, expect, test } from "bun:test";
import type { Options, SDKMessage } from "@anthropic-ai/claude-agent-sdk";
import {
  ClaudeAgentSdkAdapter,
  type ClaudeQueryHandle,
  type ClaudeSdkRuntime,
  type ClaudeWarmQuery,
} from "../src/claude-agent-sdk-adapter";
import {
  CLAUDE_AGENT_LIST_TOOL,
  CLAUDE_AGENT_SEND_TOOL,
  createClaudeAgentSdkConfiguration,
} from "../src/claude-agent-sdk-config";

const SESSION_A = "11111111-1111-4111-8111-111111111111";
const SESSION_B = "22222222-2222-4222-8222-222222222222";

type QueryFactory = (options: Options) => ClaudeQueryHandle;

class FakeClaudeRuntime implements ClaudeSdkRuntime {
  readonly startupCalls: Array<{ options: Options; initializeTimeoutMs: number }> = [];
  readonly queryCalls: Array<{ prompt: string; options: Options }> = [];
  readonly warmPrompts: string[] = [];
  warmClosed = false;

  constructor(
    private readonly warmFactory: QueryFactory,
    private readonly queryFactories: QueryFactory[] = [],
  ) {}

  async startup(params: {
    options: Options;
    initializeTimeoutMs: number;
  }): Promise<ClaudeWarmQuery> {
    this.startupCalls.push(params);
    return {
      query: (prompt) => {
        this.warmPrompts.push(prompt);
        return this.warmFactory(params.options);
      },
      close: () => {
        this.warmClosed = true;
      },
    };
  }

  query(params: { prompt: string; options: Options }): ClaudeQueryHandle {
    this.queryCalls.push(params);
    const factory = this.queryFactories.shift();
    if (!factory) throw new Error("Unexpected Claude query");
    return factory(params.options);
  }
}

function messages(...values: SDKMessage[]): ClaudeQueryHandle {
  let closed = false;
  return {
    async *[Symbol.asyncIterator]() {
      for (const value of values) yield value;
    },
    close() {
      closed = true;
    },
    get closed() {
      return closed;
    },
  } as ClaudeQueryHandle;
}

function resultMessage(
  subtype: "success" | "error_during_execution" | "error_max_turns",
  sessionId: string,
  result = "",
): SDKMessage {
  return {
    type: "result",
    subtype,
    is_error: subtype !== "success",
    session_id: sessionId,
    ...(subtype === "success" ? { result } : {}),
  } as unknown as SDKMessage;
}

function waitingQuery(options: Options): ClaudeQueryHandle {
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

describe("Claude Agent SDK configuration", () => {
  test("exposes only the two delegated MCP tools and strips central credentials", () => {
    const configuration = createClaudeAgentSdkConfiguration({
      routerUrl: "ws://127.0.0.1:18787/ws",
      agentId: "local:worker-a",
      delegationToken: "d".repeat(64),
      processEnvironment: {
        PATH: "/neutral/bin",
        ANTHROPIC_API_KEY: "provider-secret",
        ROUTER_TOKEN: "central-secret",
        ROUTER_URL: "ws://127.0.0.1:18787/ws",
        GATEWAY_AGENT_ID: "local:worker-a",
        GATEWAY_AGENT_ACTIVITY: "reviewing tests",
      },
    });

    const options = configuration.options;
    expect(options.tools).toEqual([]);
    expect(options.allowedTools).toEqual([CLAUDE_AGENT_LIST_TOOL, CLAUDE_AGENT_SEND_TOOL]);
    expect(options.permissionMode).toBe("dontAsk");
    expect(options.settingSources).toEqual([]);
    expect(options.strictMcpConfig).toBe(true);
    expect(options.skills).toEqual([]);
    expect(options.plugins).toEqual([]);
    expect(options.env?.ANTHROPIC_API_KEY).toBe("provider-secret");
    expect(options.env?.ROUTER_TOKEN).toBeUndefined();
    expect(options.env?.ROUTER_URL).toBeUndefined();
    expect(options.env?.GATEWAY_AGENT_ID).toBeUndefined();
    expect(options.env?.GATEWAY_AGENT_ACTIVITY).toBeUndefined();

    expect(options.mcpServers?.agent_session_router).toMatchObject({
      type: "sdk",
      name: "agent_session_router",
    });
    expect(options.env?.AGENT_ROUTER_DELEGATION_TOKEN).toBeUndefined();
    configuration.close();
  });
});

describe("ClaudeAgentSdkAdapter", () => {
  test("prewarms, returns one terminal result, and resumes the captured session", async () => {
    const runtime = new FakeClaudeRuntime(
      () => messages(resultMessage("success", SESSION_A, "first-result")),
      [() => messages(resultMessage("success", SESSION_B, "second-result"))],
    );
    const adapter = await ClaudeAgentSdkAdapter.connect({
      baseOptions: { tools: [] },
      runtime,
      initializeTimeoutMs: 321,
    });

    expect(runtime.startupCalls).toHaveLength(1);
    expect(runtime.startupCalls[0]?.initializeTimeoutMs).toBe(321);
    expect(
      await adapter.handle({
        requestId: "request-a",
        from: "local:coordinator",
        content: "first",
        timeoutMs: 1_000,
      }),
    ).toEqual({ ok: true, content: "first-result" });
    expect(adapter.sessionId).toBe(SESSION_A);
    expect(runtime.warmPrompts).toEqual(["first"]);

    expect(
      await adapter.handle({
        requestId: "request-b",
        from: "local:coordinator",
        content: "second",
        timeoutMs: 1_000,
      }),
    ).toEqual({ ok: true, content: "second-result" });
    expect(runtime.queryCalls[0]?.options.resume).toBe(SESSION_A);
    expect(adapter.sessionId).toBe(SESSION_B);
    adapter.close();
  });

  test("rejects a concurrent turn and aborts a timed-out query", async () => {
    const runtime = new FakeClaudeRuntime(waitingQuery);
    const adapter = await ClaudeAgentSdkAdapter.connect({
      baseOptions: { tools: [] },
      runtime,
    });
    const first = adapter.handle({
      requestId: "request-active",
      from: "local:coordinator",
      content: "wait",
      timeoutMs: 20,
    });

    expect(
      await adapter.handle({
        requestId: "request-busy",
        from: "local:coordinator",
        content: "second",
        timeoutMs: 100,
      }),
    ).toEqual({ ok: false, error: "session_busy" });
    expect(await first).toEqual({ ok: false, error: "request_timeout" });
    expect(runtime.startupCalls[0]?.options.abortController?.signal.aborted).toBe(true);
    adapter.close();
  });

  test("maps SDK terminal errors without exposing provider error text", async () => {
    const runtime = new FakeClaudeRuntime(() =>
      messages(resultMessage("error_max_turns", SESSION_A)),
    );
    const adapter = await ClaudeAgentSdkAdapter.connect({
      baseOptions: { tools: [] },
      runtime,
      resumeSessionId: SESSION_A,
    });

    expect(
      await adapter.handle({
        requestId: "request-error",
        from: "local:coordinator",
        content: "work",
        timeoutMs: 1_000,
      }),
    ).toEqual({ ok: false, error: "claude_max_turns" });
    expect(runtime.startupCalls[0]?.options.resume).toBe(SESSION_A);
    adapter.close();
  });

  test("maps thrown SDK failures to a generic error", async () => {
    const runtime = new FakeClaudeRuntime(() => ({
      async *[Symbol.asyncIterator]() {
        throw new Error("provider detail must not escape");
      },
      close() {},
    }));
    const adapter = await ClaudeAgentSdkAdapter.connect({
      baseOptions: { tools: [] },
      runtime,
    });

    expect(
      await adapter.handle({
        requestId: "request-sdk-error",
        from: "local:coordinator",
        content: "work",
        timeoutMs: 1_000,
      }),
    ).toEqual({ ok: false, error: "claude_sdk_error" });
    adapter.close();
  });

  test("fails an active query on close and reports a missing terminal result", async () => {
    const closeRuntime = new FakeClaudeRuntime(waitingQuery);
    const activeAdapter = await ClaudeAgentSdkAdapter.connect({
      baseOptions: { tools: [] },
      runtime: closeRuntime,
    });
    const active = activeAdapter.handle({
      requestId: "request-close",
      from: "local:coordinator",
      content: "wait",
      timeoutMs: 1_000,
    });
    activeAdapter.close();
    expect(await active).toEqual({ ok: false, error: "provider_disconnected" });

    const emptyRuntime = new FakeClaudeRuntime(() => messages());
    const emptyAdapter = await ClaudeAgentSdkAdapter.connect({
      baseOptions: { tools: [] },
      runtime: emptyRuntime,
    });
    expect(
      await emptyAdapter.handle({
        requestId: "request-empty",
        from: "local:coordinator",
        content: "work",
        timeoutMs: 1_000,
      }),
    ).toEqual({ ok: false, error: "claude_no_result" });
    emptyAdapter.close();
  });
});
