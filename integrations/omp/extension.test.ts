import { describe, expect, test } from "bun:test";
import { z } from "zod";
import {
  createOmpExtension,
  type ChildSelection,
  type OmpContext,
  type OmpHost,
  type OmpMcpConnection,
  type ToolCallOptions,
} from "./extension";

type Handler = (event: unknown, context: OmpContext) => void | Promise<void>;
type Tool = Parameters<OmpHost["registerTool"]>[0];
type Command = Parameters<OmpHost["registerCommand"]>[1];

class FakeConnection implements OmpMcpConnection {
  readonly calls: Array<{ name: string; args: Record<string, unknown>; options: ToolCallOptions }> = [];
  readonly notifications: Array<{ method: string; params: Record<string, unknown> }> = [];
  closed = false;
  nextResult: unknown = { content: [{ type: "text", text: "ok" }] };

  async callTool(
    name: string,
    args: Record<string, unknown>,
    options: ToolCallOptions,
  ): Promise<unknown> {
    this.calls.push({ name, args, options });
    return this.nextResult;
  }

  async notify(method: string, params: Record<string, unknown>): Promise<void> {
    this.notifications.push({ method, params });
  }

  async close(): Promise<void> {
    this.closed = true;
  }
}

class FakeHost implements OmpHost {
  readonly zod = z;
  readonly tools: Record<string, Tool> = {};
  readonly commands: Record<string, Command> = {};
  readonly handlers: Record<string, Handler[]> = {};
  readonly messages: Array<{
    content: string;
    options: { attribution: "agent"; deliverAs: "followUp" };
  }> = [];

  registerTool(tool: Tool): void {
    this.tools[tool.name] = tool;
  }

  registerCommand(name: string, command: Command): void {
    this.commands[name] = command;
  }

  on(event: string, handler: Handler): void {
    (this.handlers[event] ??= []).push(handler);
  }

  sendUserMessage(
    content: string,
    options: { attribution: "agent"; deliverAs: "followUp" },
  ): void {
    this.messages.push({ content, options });
  }

  async emit(event: string, payload: unknown, context: OmpContext): Promise<void> {
    for (const handler of this.handlers[event] ?? []) await handler(payload, context);
  }
}

function createContext(): OmpContext & { aborts: number; idle: boolean } {
  return {
    cwd: "/tmp/project",
    hasUI: true,
    idle: true,
    aborts: 0,
    isIdle() {
      return this.idle;
    },
    abort() {
      this.aborts += 1;
    },
    ui: { notify() {} },
  };
}

describe("OMP retained extension", () => {
  test("forwards tools with bounded SDK options and owns terminal work lifecycle", async () => {
    const host = new FakeHost();
    const connection = new FakeConnection();
    let selection: ChildSelection | undefined;
    let receive: ((notification: unknown) => Promise<void>) | undefined;
    createOmpExtension({
      env: {
        ASR_EXECUTABLE: "/opt/asr/bin/asr",
        ASR_CREDENTIAL_FILE: "/tmp/agent.json",
      },
      async connect(value, onNotification) {
        selection = value;
        receive = onNotification as (notification: unknown) => Promise<void>;
        return connection;
      },
    })(host);
    const context = createContext();

    await host.emit("session_start", {}, context);
    expect(selection).toEqual({
      executable: "/opt/asr/bin/asr",
      credentialFile: "/tmp/agent.json",
      profile: undefined,
      workspace: undefined,
      cwd: "/tmp/project",
    });
    expect(connection.notifications.at(-1)).toEqual({
      method: "notifications/agent_session_router/host_state",
      params: { v: 1, ready: true },
    });

    await receive?.({
      method: "notifications/agent_session_router/work",
      params: {
        v: 1,
        workspace: "red",
        requestId: "request-1",
        from: "coordinator",
        content: "review this",
        timeoutMs: 4_000,
        task: { id: 7, expectedVersion: 2 },
      },
    });
    expect(host.messages).toHaveLength(1);
    expect(host.messages[0]?.options).toEqual({
      attribution: "agent",
      deliverAs: "followUp",
    });

    connection.nextResult = {
      structuredContent: {
        task: {
          id: 7,
          currentAttempt: { id: "attempt-1", sessionId: "session-1" },
        },
      },
    };
    await host.tools.task_begin?.execute(
      "tool-1",
      { taskId: 7, timeoutMs: 12_000 },
      undefined,
      undefined,
      context,
    );
    expect(connection.calls.at(-1)?.options).toMatchObject({
      timeout: 65_000,
      maxTotalTimeout: 65_000,
      resetTimeoutOnProgress: false,
    });

    await host.tools.agent_send?.execute(
      "tool-2",
      { targetId: "worker", timeoutMs: 12_000 },
      undefined,
      undefined,
      context,
    );
    expect(connection.calls.at(-1)?.options.timeout).toBe(17_000);

    await host.emit("agent_start", {}, context);
    const beforeNonterminal = connection.notifications.length;
    await host.emit("agent_end", { isTerminal: false }, context);
    expect(connection.notifications).toHaveLength(beforeNonterminal);
    await host.emit("agent_end", { isTerminal: true }, context);
    expect(connection.notifications.slice(-3).map((entry) => entry.method)).toEqual([
      "notifications/agent_session_router/task_terminal",
      "notifications/agent_session_router/work_finished",
      "notifications/agent_session_router/host_state",
    ]);
  });

  test("rejects overlapping work, aborts only an exact cancellation, and closes on session change", async () => {
    const host = new FakeHost();
    const connection = new FakeConnection();
    let receive: ((notification: unknown) => Promise<void>) | undefined;
    createOmpExtension({
      env: { ASR_EXECUTABLE: "/opt/asr/bin/asr" },
      async connect(_selection, onNotification) {
        receive = onNotification as (notification: unknown) => Promise<void>;
        return connection;
      },
    })(host);
    const context = createContext();
    await host.emit("session_start", {}, context);

    const work = {
      method: "notifications/agent_session_router/work",
      params: {
        v: 1,
        workspace: "red",
        requestId: "request-1",
        from: "coordinator",
        content: "first",
        timeoutMs: 1_000,
      },
    };
    await receive?.(work);
    await receive?.({ ...work, params: { ...work.params, requestId: "request-2" } });
    expect(connection.notifications.at(-1)).toEqual({
      method: "notifications/agent_session_router/work_finished",
      params: { v: 1, requestId: "request-2", error: "session_busy" },
    });

    await receive?.({
      method: "notifications/agent_session_router/cancel",
      params: { v: 1, workspace: "red", requestId: "other" },
    });
    expect(context.aborts).toBe(0);
    await receive?.({
      method: "notifications/agent_session_router/cancel",
      params: { v: 1, workspace: "red", requestId: "request-1" },
    });
    expect(context.aborts).toBe(1);

    await host.emit("session_switch", {}, context);
    expect(connection.closed).toBe(true);
  });
});
