import { describe, expect, test } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createServer } from "node:net";
import type { AgentMessenger, AgentSendOptions, AgentSendResult } from "../src/agent-messenger";
import { CodexCliInboxAdapter } from "../src/codex-cli-inbox-adapter";
import { createCodexCliMcpServer } from "../src/codex-cli-mcp";
import { GatewayClient } from "../src/gateway-client";
import { MockSessionAdapter } from "../src/mock-session-adapter";
import type { AgentDescriptor } from "../src/protocol";
import { startRouter } from "../src/router";

class RecordingMessenger implements AgentMessenger {
  readonly sends: Array<{ target: string; content: string; options?: AgentSendOptions }> = [];

  constructor(
    readonly agents: AgentDescriptor[],
    readonly result: AgentSendResult = {
      requestId: "outbound-request",
      from: "local:reviewer",
      ok: true,
      content: "review-result",
    },
  ) {}

  async listAgents(): Promise<AgentDescriptor[]> {
    return this.agents;
  }

  async send(
    target: string,
    content: string,
    options?: AgentSendOptions,
  ): Promise<AgentSendResult> {
    this.sends.push({ target, content, options });
    return this.result;
  }
}

async function connectMcp(server: ReturnType<typeof createCodexCliMcpServer>) {
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  const client = new Client({ name: "codex-cli-test-client", version: "0.1.0" });
  await Promise.all([server.connect(serverTransport), client.connect(clientTransport)]);
  return client;
}

async function availablePort(): Promise<number> {
  const probe = createServer();
  await new Promise<void>((resolve, reject) => {
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => resolve());
  });
  const address = probe.address();
  if (!address || typeof address === "string") throw new Error("Unable to allocate a test port");
  await new Promise<void>((resolve, reject) => {
    probe.close((error) => (error ? reject(error) : resolve()));
  });
  return address.port;
}

describe("CodexCliInboxAdapter", () => {
  test("long-polls, claims one correlated request, and accepts one reply", async () => {
    const adapter = new CodexCliInboxAdapter();
    const waiting = adapter.wait(1_000);
    const pending = adapter.handle({
      requestId: "inbound-request",
      from: "local:reviewer",
      content: "review-task",
      timeoutMs: 1_000,
    });

    expect(await waiting).toMatchObject({
      ok: true,
      message: {
        requestId: "inbound-request",
        from: "local:reviewer",
        content: "review-task",
      },
    });
    expect(await adapter.wait(5)).toEqual({ ok: false, error: "reply_pending" });
    expect(adapter.reply("wrong-request", "wrong-result")).toEqual({
      ok: false,
      error: "request_not_found",
    });
    expect(adapter.reply("inbound-request", "review-result")).toEqual({ ok: true });
    expect(await pending).toEqual({ ok: true, content: "review-result" });
    expect(adapter.reply("inbound-request", "late-result")).toEqual({
      ok: false,
      error: "request_not_found",
    });
    adapter.close();
  });

  test("keeps one queued request busy until it is claimed and replied", async () => {
    const adapter = new CodexCliInboxAdapter();
    const first = adapter.handle({
      requestId: "first-request",
      from: "local:coordinator",
      content: "first-task",
      timeoutMs: 1_000,
    });
    expect(
      await adapter.handle({
        requestId: "second-request",
        from: "local:coordinator",
        content: "second-task",
        timeoutMs: 1_000,
      }),
    ).toEqual({ ok: false, error: "session_busy" });
    expect(adapter.reply("first-request", "too-early")).toEqual({
      ok: false,
      error: "request_not_claimed",
    });
    expect(await adapter.wait(10)).toMatchObject({
      ok: true,
      message: { requestId: "first-request", content: "first-task" },
    });
    expect(adapter.reply("first-request", "first-result")).toEqual({ ok: true });
    expect(await first).toEqual({ ok: true, content: "first-result" });
    adapter.close();
  });

  test("expires requests and releases an active wait on close", async () => {
    const adapter = new CodexCliInboxAdapter();
    expect(
      await adapter.handle({
        requestId: "timeout-request",
        from: "local:coordinator",
        content: "timeout-task",
        timeoutMs: 5,
      }),
    ).toEqual({ ok: false, error: "request_timeout" });
    expect(adapter.reply("timeout-request", "late-result")).toEqual({
      ok: false,
      error: "request_not_found",
    });

    const waiting = adapter.wait(1_000);
    adapter.close();
    expect(await waiting).toEqual({ ok: false, error: "provider_not_ready" });
    expect(await adapter.wait(1)).toEqual({ ok: false, error: "provider_not_ready" });
  });
});

describe("Codex CLI MCP", () => {
  test("exposes list/send/wait/reply inside a stock Codex client", async () => {
    const adapter = new CodexCliInboxAdapter();
    const messenger = new RecordingMessenger([
      { agentId: "local:worker-a", side: "codex" },
      { agentId: "local:reviewer", side: "claude" },
    ]);
    const server = createCodexCliMcpServer({
      agentId: "local:worker-a",
      adapter,
      messenger,
    });
    const client = await connectMcp(server);

    try {
      expect((await client.listTools()).tools.map(({ name }) => name).sort()).toEqual([
        "agent_list",
        "agent_reply",
        "agent_send",
        "agent_wait",
      ]);
      expect(await client.callTool({ name: "agent_list", arguments: {} })).toMatchObject({
        structuredContent: {
          agents: [{ agentId: "local:reviewer", side: "claude" }],
        },
      });
      expect(
        await client.callTool({
          name: "agent_send",
          arguments: {
            target: "local:reviewer",
            message: "review-task",
            timeoutMs: 2_000,
          },
        }),
      ).toMatchObject({
        content: [{ type: "text", text: "review-result" }],
        structuredContent: {
          requestId: "outbound-request",
          from: "local:reviewer",
          ok: true,
        },
      });

      const pending = adapter.handle({
        requestId: "mcp-inbound",
        from: "local:reviewer",
        content: "mcp-task",
        timeoutMs: 1_000,
      });
      expect(
        await client.callTool({ name: "agent_wait", arguments: { waitMs: 100 } }),
      ).toMatchObject({
        structuredContent: {
          ok: true,
          requestId: "mcp-inbound",
          from: "local:reviewer",
          message: "mcp-task",
        },
      });
      expect(
        await client.callTool({
          name: "agent_reply",
          arguments: { requestId: "mcp-inbound", text: "mcp-result" },
        }),
      ).toMatchObject({ structuredContent: { ok: true } });
      expect(await pending).toEqual({ ok: true, content: "mcp-result" });
      expect(
        await client.callTool({ name: "agent_wait", arguments: { waitMs: 5 } }),
      ).toMatchObject({ structuredContent: { ok: false, error: "wait_timeout" } });
      expect(messenger.sends).toEqual([
        {
          target: "local:reviewer",
          content: "review-task",
          options: { timeoutMs: 2_000 },
        },
      ]);
    } finally {
      adapter.close();
      await client.close();
      await server.close();
    }
  });

  test("round-trips one pulled request through the real router", async () => {
    const port = await availablePort();
    const router = startRouter({ hostname: "127.0.0.1", port, token: null });
    const routerUrl = `ws://127.0.0.1:${router.port}/ws`;
    const adapter = new CodexCliInboxAdapter();
    const worker = new GatewayClient({
      routerUrl,
      agent: { agentId: "local:worker-a", side: "codex" },
      adapter,
    });
    const coordinator = new GatewayClient({
      routerUrl,
      agent: { agentId: "local:coordinator", side: "generic" },
      adapter: new MockSessionAdapter(async () => ({ ok: false, error: "unexpected_delivery" })),
    });

    try {
      await Promise.all([worker.connect(), coordinator.connect()]);
      const firstResult = coordinator.send("local:worker-a", "first-task", { timeoutMs: 1_000 });
      const inbound = await adapter.wait(1_000);
      expect(inbound).toMatchObject({
        ok: true,
        message: { from: "local:coordinator", content: "first-task" },
      });
      if (!inbound.ok) throw new Error("Expected one routed request");
      const busyResult = coordinator.send("local:worker-a", "second-task", { timeoutMs: 1_000 });
      expect(await busyResult).toMatchObject({ ok: false, error: "session_busy" });
      expect(adapter.reply(inbound.message.requestId, "first-result")).toEqual({ ok: true });
      expect(await firstResult).toMatchObject({
        ok: true,
        from: "local:worker-a",
        content: "first-result",
      });
    } finally {
      adapter.close();
      worker.disconnect();
      coordinator.disconnect();
      router.stop(true);
    }
  });
});
