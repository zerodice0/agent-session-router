import { describe, expect, test } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createServer } from "node:net";
import { z } from "zod";
import type { AgentMessenger, AgentSendOptions, AgentSendResult } from "../src/agent-messenger";
import {
  CLAUDE_CHANNEL_NOTIFICATION_METHOD,
  ClaudeChannelAdapter,
  type ClaudeChannelEvent,
} from "../src/claude-channel-adapter";
import {
  CLAUDE_CHANNEL_SERVER_NAME,
  createClaudeChannelMcpServer,
} from "../src/claude-channel-mcp";
import { GatewayClient } from "../src/gateway-client";
import { MockSessionAdapter } from "../src/mock-session-adapter";
import type { AgentDescriptor } from "../src/protocol";
import { startRouter } from "../src/router";

const ChannelNotificationSchema = z.object({
  method: z.literal(CLAUDE_CHANNEL_NOTIFICATION_METHOD),
  params: z.object({
    content: z.string(),
    meta: z.object({
      request_id: z.string(),
      from: z.string(),
      timeout_ms: z.string(),
    }),
  }),
});

class RecordingMessenger implements AgentMessenger {
  readonly sends: Array<{ target: string; content: string; options?: AgentSendOptions }> = [];

  constructor(
    readonly agents: AgentDescriptor[],
    readonly result: AgentSendResult = {
      requestId: "nested-request",
      from: "local:worker-a",
      ok: true,
      content: "nested-result",
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

class ChannelInbox {
  readonly events: ClaudeChannelEvent[] = [];
  readonly #waiters = new Map<string, (event: ClaudeChannelEvent) => void>();

  push(event: ClaudeChannelEvent): void {
    this.events.push(event);
    const resolve = this.#waiters.get(event.meta.request_id);
    if (!resolve) return;
    this.#waiters.delete(event.meta.request_id);
    resolve(event);
  }

  waitFor(requestId: string): Promise<ClaudeChannelEvent> {
    const found = this.events.find(({ meta }) => meta.request_id === requestId);
    if (found) return Promise.resolve(found);
    return new Promise((resolve) => this.#waiters.set(requestId, resolve));
  }
}

async function availablePort(): Promise<number> {
  const probe = createServer();
  await new Promise<void>((resolve, reject) => {
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => resolve());
  });
  const address = probe.address();
  if (!address || typeof address === "string") throw new Error("Unable to allocate a test port");
  const port = address.port;
  await new Promise<void>((resolve, reject) => {
    probe.close((error) => (error ? reject(error) : resolve()));
  });
  return port;
}

async function connectMcp(
  runtime: ReturnType<typeof createClaudeChannelMcpServer>,
  inbox: ChannelInbox,
) {
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  const client = new Client({ name: "channel-test-client", version: "0.1.0" });
  client.setNotificationHandler(ChannelNotificationSchema, ({ params }) => {
    inbox.push(params);
  });
  await Promise.all([runtime.server.connect(serverTransport), client.connect(clientTransport)]);
  return client;
}

describe("ClaudeChannelAdapter", () => {
  test("correlates one notification and reply without treating the write as an acknowledgement", async () => {
    const events: ClaudeChannelEvent[] = [];
    const adapter = new ClaudeChannelAdapter(async (event) => {
      events.push(event);
    });

    const first = adapter.handle({
      requestId: "channel-request",
      from: "local:coordinator",
      content: "review-task",
      timeoutMs: 1_000,
    });
    await Bun.sleep(0);

    expect(events).toEqual([
      {
        content: "review-task",
        meta: {
          request_id: "channel-request",
          from: "local:coordinator",
          timeout_ms: "1000",
        },
      },
    ]);
    expect(
      await adapter.handle({
        requestId: "busy-request",
        from: "local:coordinator",
        content: "second-task",
        timeoutMs: 1_000,
      }),
    ).toEqual({ ok: false, error: "session_busy" });
    expect(adapter.reply("wrong-request", "wrong-result")).toEqual({
      ok: false,
      error: "request_not_found",
    });
    expect(adapter.reply("channel-request", "review-result")).toEqual({ ok: true });
    expect(await first).toEqual({ ok: true, content: "review-result" });
    expect(adapter.reply("channel-request", "late-result")).toEqual({
      ok: false,
      error: "request_not_found",
    });
  });

  test("times out, maps notification failure, and closes an active request", async () => {
    const timeoutAdapter = new ClaudeChannelAdapter(async () => {});
    expect(
      await timeoutAdapter.handle({
        requestId: "timeout-request",
        from: "local:coordinator",
        content: "timeout-task",
        timeoutMs: 5,
      }),
    ).toEqual({ ok: false, error: "request_timeout" });
    expect(timeoutAdapter.reply("timeout-request", "late-result")).toEqual({
      ok: false,
      error: "request_not_found",
    });

    const failedAdapter = new ClaudeChannelAdapter(async () => {
      throw new Error("private provider error");
    });
    expect(
      await failedAdapter.handle({
        requestId: "failed-request",
        from: "local:coordinator",
        content: "failed-task",
        timeoutMs: 1_000,
      }),
    ).toEqual({ ok: false, error: "provider_disconnected" });

    const closedAdapter = new ClaudeChannelAdapter(async () => {});
    const active = closedAdapter.handle({
      requestId: "closed-request",
      from: "local:coordinator",
      content: "closed-task",
      timeoutMs: 1_000,
    });
    closedAdapter.close();
    expect(await active).toEqual({ ok: false, error: "provider_disconnected" });
    expect(
      await closedAdapter.handle({
        requestId: "after-close",
        from: "local:coordinator",
        content: "unused",
        timeoutMs: 1_000,
      }),
    ).toEqual({ ok: false, error: "provider_not_ready" });
  });
});

describe("Claude Channel MCP", () => {
  test("declares the channel, emits notifications, and exposes reply/list/send tools", async () => {
    const inbox = new ChannelInbox();
    const messenger = new RecordingMessenger([
      { agentId: "local:claude-channel", side: "claude" },
      { agentId: "local:worker-a", side: "codex" },
    ]);
    let notify = async (_event: ClaudeChannelEvent) => {};
    const adapter = new ClaudeChannelAdapter((event) => notify(event));
    const runtime = createClaudeChannelMcpServer({
      agentId: "local:claude-channel",
      adapter,
      messenger,
    });
    notify = runtime.notify;
    const client = await connectMcp(runtime, inbox);

    try {
      expect(client.getServerCapabilities()?.experimental).toEqual({ "claude/channel": {} });
      expect((await client.listTools()).tools.map(({ name }) => name).sort()).toEqual([
        "agent_list",
        "agent_reply",
        "agent_send",
      ]);

      const pending = adapter.handle({
        requestId: "mcp-channel-request",
        from: "local:coordinator",
        content: "mcp-task",
        timeoutMs: 1_000,
      });
      expect(await inbox.waitFor("mcp-channel-request")).toMatchObject({
        content: "mcp-task",
        meta: { request_id: "mcp-channel-request", from: "local:coordinator" },
      });

      expect(
        await client.callTool({
          name: "agent_reply",
          arguments: { request_id: "mcp-channel-request", text: "mcp-result" },
        }),
      ).toMatchObject({ structuredContent: { ok: true } });
      expect(await pending).toEqual({ ok: true, content: "mcp-result" });
      expect(
        await client.callTool({
          name: "agent_reply",
          arguments: { request_id: "mcp-channel-request", text: "late-result" },
        }),
      ).toMatchObject({
        isError: true,
        structuredContent: { ok: false, error: "request_not_found" },
      });

      expect(await client.callTool({ name: "agent_list", arguments: {} })).toMatchObject({
        structuredContent: {
          agents: [{ agentId: "local:worker-a", side: "codex" }],
        },
      });
      expect(
        await client.callTool({
          name: "agent_send",
          arguments: {
            target: "local:worker-a",
            message: "nested-task",
            timeoutMs: 2_000,
          },
        }),
      ).toMatchObject({
        content: [{ type: "text", text: "nested-result" }],
        structuredContent: { requestId: "nested-request", from: "local:worker-a", ok: true },
      });
      expect(messenger.sends).toEqual([
        {
          target: "local:worker-a",
          content: "nested-task",
          options: { timeoutMs: 2_000 },
        },
      ]);
    } finally {
      adapter.close();
      await client.close();
      await runtime.server.close();
    }
  });

  test("round-trips inbound replies and isolates parallel outbound worker responses", async () => {
    const port = await availablePort();
    const router = startRouter({ hostname: "127.0.0.1", port, token: null });
    const routerUrl = `ws://127.0.0.1:${router.port}/ws`;
    const inbox = new ChannelInbox();
    let notify = async (_event: ClaudeChannelEvent) => {};
    const channelAdapter = new ClaudeChannelAdapter((event) => notify(event));
    const channelGateway = new GatewayClient({
      routerUrl,
      agent: { agentId: "local:claude-channel", side: "claude" },
      adapter: channelAdapter,
    });
    const runtime = createClaudeChannelMcpServer({
      agentId: "local:claude-channel",
      adapter: channelAdapter,
      messenger: channelGateway,
    });
    notify = runtime.notify;
    let markChannelReady!: () => void;
    const channelReady = new Promise<void>((resolve) => {
      markChannelReady = resolve;
    });
    runtime.server.server.oninitialized = () => {
      void channelGateway.connect().then(markChannelReady);
    };
    runtime.server.server.onclose = () => {
      channelGateway.disconnect();
      channelAdapter.close();
    };
    const client = await connectMcp(runtime, inbox);
    const coordinator = new GatewayClient({
      routerUrl,
      agent: { agentId: "local:coordinator", side: "generic" },
      adapter: new MockSessionAdapter(async () => ({ ok: false, error: "unexpected_delivery" })),
    });
    const workerA = new GatewayClient({
      routerUrl,
      agent: { agentId: "local:worker-a", side: "codex" },
      adapter: new MockSessionAdapter(async (request) => ({
        ok: true,
        content: `worker-a:${request.content}`,
      })),
    });
    const workerB = new GatewayClient({
      routerUrl,
      agent: { agentId: "local:worker-b", side: "codex" },
      adapter: new MockSessionAdapter(async (request) => ({
        ok: true,
        content: `worker-b:${request.content}`,
      })),
    });

    try {
      await Promise.all([
        coordinator.connect(),
        workerA.connect(),
        workerB.connect(),
      ]);
      await channelReady;

      const inbound = coordinator.send("local:claude-channel", "channel-task", {
        requestId: "channel-inbound",
        timeoutMs: 1_000,
      });
      expect(await inbox.waitFor("channel-inbound")).toMatchObject({
        content: "channel-task",
        meta: { request_id: "channel-inbound", from: "local:coordinator" },
      });
      await client.callTool({
        name: "agent_reply",
        arguments: { request_id: "channel-inbound", text: "channel-result" },
      });
      expect(await inbound).toEqual({
        requestId: "channel-inbound",
        from: "local:claude-channel",
        ok: true,
        content: "channel-result",
      });

      const [resultA, resultB] = await Promise.all([
        client.callTool({
          name: "agent_send",
          arguments: { target: "local:worker-a", message: "task-a" },
        }),
        client.callTool({
          name: "agent_send",
          arguments: { target: "local:worker-b", message: "task-b" },
        }),
      ]);
      expect(resultA).toMatchObject({
        content: [{ type: "text", text: "worker-a:task-a" }],
        structuredContent: { from: "local:worker-a", ok: true },
      });
      expect(resultB).toMatchObject({
        content: [{ type: "text", text: "worker-b:task-b" }],
        structuredContent: { from: "local:worker-b", ok: true },
      });

      const first = coordinator.send("local:claude-channel", "first-task", {
        requestId: "channel-first",
        timeoutMs: 1_000,
      });
      await inbox.waitFor("channel-first");
      expect(
        await coordinator.send("local:claude-channel", "second-task", {
          requestId: "channel-second",
          timeoutMs: 1_000,
        }),
      ).toEqual({
        requestId: "channel-second",
        from: "local:claude-channel",
        ok: false,
        error: "session_busy",
      });
      await client.callTool({
        name: "agent_reply",
        arguments: { request_id: "channel-first", text: "first-result" },
      });
      expect(await first).toMatchObject({ ok: true, content: "first-result" });
      expect(inbox.events.filter(({ meta }) => meta.request_id === "channel-second")).toHaveLength(0);

      await client.close();
      expect(channelGateway.connected).toBe(false);
    } finally {
      coordinator.disconnect();
      channelGateway.disconnect();
      workerA.disconnect();
      workerB.disconnect();
      channelAdapter.close();
      if (client.transport) await client.close();
      await runtime.server.close();
      router.stop(true);
    }
  });
});
