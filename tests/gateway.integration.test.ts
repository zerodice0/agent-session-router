import { describe, expect, test } from "bun:test";
import { createServer } from "node:net";
import { DelegatedMessengerClient } from "../src/delegated-messenger-client";
import { GatewayClient } from "../src/gateway-client";
import { MockSessionAdapter } from "../src/mock-session-adapter";
import { PROTOCOL_VERSION, type AgentSide, type ClientMessage, type ServerMessage } from "../src/protocol";
import { startRouter } from "../src/router";

const WAIT_TIMEOUT_MS = 2_000;

class RouterTestClient {
  readonly received: ServerMessage[] = [];
  readonly #inbox: ServerMessage[] = [];
  readonly #waiters: Array<{
    predicate: (message: ServerMessage) => boolean;
    resolve: (message: ServerMessage) => void;
    reject: (error: Error) => void;
    timer: ReturnType<typeof setTimeout>;
  }> = [];

  private constructor(readonly socket: WebSocket) {
    socket.addEventListener("message", (event) => {
      if (typeof event.data !== "string") return;
      const message = JSON.parse(event.data) as ServerMessage;
      this.received.push(message);
      const waiterIndex = this.#waiters.findIndex(({ predicate }) => predicate(message));
      if (waiterIndex === -1) {
        this.#inbox.push(message);
        return;
      }
      const [waiter] = this.#waiters.splice(waiterIndex, 1);
      if (!waiter) return;
      clearTimeout(waiter.timer);
      waiter.resolve(message);
    });

    socket.addEventListener("close", () => {
      for (const waiter of this.#waiters.splice(0)) {
        clearTimeout(waiter.timer);
        waiter.reject(new Error("Router test connection closed"));
      }
    });
  }

  static async register(url: string, agentId: string, side: AgentSide = "generic") {
    const socket = new WebSocket(url);
    await new Promise<void>((resolve, reject) => {
      socket.addEventListener("open", () => resolve(), { once: true });
      socket.addEventListener("error", () => reject(new Error("Unable to open router test connection")), {
        once: true,
      });
    });
    const client = new RouterTestClient(socket);
    const registered = client.waitFor(
      (message) => message.type === "registered" && message.agent.agentId === agentId,
    );
    client.send({
      type: "register",
      protocolVersion: PROTOCOL_VERSION,
      agent: { agentId, side },
    });
    await registered;
    return client;
  }

  send(message: ClientMessage): void {
    this.socket.send(JSON.stringify(message));
  }

  waitFor(predicate: (message: ServerMessage) => boolean): Promise<ServerMessage> {
    const inboxIndex = this.#inbox.findIndex(predicate);
    if (inboxIndex !== -1) {
      const [message] = this.#inbox.splice(inboxIndex, 1);
      return Promise.resolve(message!);
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const index = this.#waiters.findIndex((waiter) => waiter.timer === timer);
        if (index !== -1) this.#waiters.splice(index, 1);
        reject(new Error("Timed out waiting for router test message"));
      }, WAIT_TIMEOUT_MS);
      this.#waiters.push({ predicate, resolve, reject, timer });
    });
  }

  close(): void {
    this.socket.close();
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

async function startTestRouter() {
  const port = await availablePort();
  const server = startRouter({ hostname: "127.0.0.1", port, token: null });
  return { server, url: `ws://127.0.0.1:${server.port}/ws` };
}

function deferredResult() {
  let resolve!: (value: { ok: true; content: string }) => void;
  const promise = new Promise<{ ok: true; content: string }>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("GatewayClient with router", () => {
  test("selects one of two workers and keeps responses correlated", async () => {
    const { server, url } = await startTestRouter();
    const coordinator = await RouterTestClient.register(url, "local:coordinator");
    const workerAAdapter = new MockSessionAdapter(async (request) => ({
      ok: true,
      content: `worker-a:${request.content}`,
    }));
    const workerBAdapter = new MockSessionAdapter(async (request) => ({
      ok: true,
      content: `worker-b:${request.content}`,
    }));
    const workerA = new GatewayClient({
      routerUrl: url,
      agent: { agentId: "local:worker-a", side: "generic" },
      adapter: workerAAdapter,
    });
    const workerB = new GatewayClient({
      routerUrl: url,
      agent: { agentId: "local:worker-b", side: "generic" },
      adapter: workerBAdapter,
    });

    try {
      await Promise.all([workerA.connect(), workerB.connect()]);
      const resultA = coordinator.waitFor(
        (message) => message.type === "result" && message.requestId === "request-a",
      );
      const resultB = coordinator.waitFor(
        (message) => message.type === "result" && message.requestId === "request-b",
      );

      coordinator.send({
        type: "send",
        requestId: "request-a",
        to: "local:worker-a",
        content: "task-a",
      });
      coordinator.send({
        type: "send",
        requestId: "request-b",
        to: "local:worker-b",
        content: "task-b",
      });

      expect(await resultA).toMatchObject({
        type: "result",
        requestId: "request-a",
        from: "local:worker-a",
        ok: true,
        content: "worker-a:task-a",
      });
      expect(await resultB).toMatchObject({
        type: "result",
        requestId: "request-b",
        from: "local:worker-b",
        ok: true,
        content: "worker-b:task-b",
      });
      expect(workerAAdapter.requests.map(({ requestId }) => requestId)).toEqual(["request-a"]);
      expect(workerBAdapter.requests.map(({ requestId }) => requestId)).toEqual(["request-b"]);
      expect(workerAAdapter.requests[0]?.timeoutMs).toBe(60_000);
    } finally {
      workerA.disconnect();
      workerB.disconnect();
      coordinator.close();
      server.stop(true);
    }
  });

  test("returns session_busy without mixing a second request into the active adapter", async () => {
    const { server, url } = await startTestRouter();
    const coordinator = await RouterTestClient.register(url, "local:coordinator");
    const first = deferredResult();
    const adapter = new MockSessionAdapter((request) => {
      if (request.requestId === "busy-first") return first.promise;
      return { ok: true, content: "unexpected" };
    });
    const worker = new GatewayClient({
      routerUrl: url,
      agent: { agentId: "local:worker-a", side: "generic" },
      adapter,
    });

    try {
      await worker.connect();
      coordinator.send({
        type: "send",
        requestId: "busy-first",
        to: "local:worker-a",
        content: "first-task",
      });
      await coordinator.waitFor(
        (message) => message.type === "accepted" && message.requestId === "busy-first",
      );

      const busyResult = coordinator.waitFor(
        (message) => message.type === "result" && message.requestId === "busy-second",
      );
      coordinator.send({
        type: "send",
        requestId: "busy-second",
        to: "local:worker-a",
        content: "second-task",
      });
      expect(await busyResult).toMatchObject({
        type: "result",
        requestId: "busy-second",
        from: "local:worker-a",
        ok: false,
        error: "session_busy",
      });
      expect(adapter.requests.map(({ requestId }) => requestId)).toEqual(["busy-first"]);

      const firstResult = coordinator.waitFor(
        (message) => message.type === "result" && message.requestId === "busy-first",
      );
      first.resolve({ ok: true, content: "first-result" });
      expect(await firstResult).toMatchObject({ content: "first-result", ok: true });
    } finally {
      worker.disconnect();
      coordinator.close();
      server.stop(true);
    }
  });

  test("covers offline, duplicate agent/request IDs, timeout, and disconnect", async () => {
    const { server, url } = await startTestRouter();
    const coordinator = await RouterTestClient.register(url, "local:coordinator");
    const duplicate = deferredResult();
    const late = deferredResult();
    const adapter = new MockSessionAdapter((request) => {
      if (request.requestId === "duplicate-request") return duplicate.promise;
      if (request.requestId === "timeout-request") return late.promise;
      return { ok: true, content: "result" };
    });
    const worker = new GatewayClient({
      routerUrl: url,
      agent: { agentId: "local:worker-a", side: "generic" },
      adapter,
    });

    try {
      await worker.connect();

      const duplicateAgent = new GatewayClient({
        routerUrl: url,
        agent: { agentId: "local:worker-a", side: "generic" },
        adapter: new MockSessionAdapter(async () => ({ ok: true, content: "unused" })),
      });
      await expect(duplicateAgent.connect()).rejects.toThrow("agent_conflict");
      duplicateAgent.disconnect();

      const offline = coordinator.waitFor(
        (message) => message.type === "error" && message.requestId === "offline-request",
      );
      coordinator.send({
        type: "send",
        requestId: "offline-request",
        to: "local:offline",
        content: "offline-task",
      });
      expect(await offline).toMatchObject({ type: "error", code: "target_offline" });

      coordinator.send({
        type: "send",
        requestId: "duplicate-request",
        to: "local:worker-a",
        content: "first-copy",
      });
      await coordinator.waitFor(
        (message) => message.type === "accepted" && message.requestId === "duplicate-request",
      );
      const requestConflict = coordinator.waitFor(
        (message) =>
          message.type === "error" &&
          message.requestId === "duplicate-request" &&
          message.code === "request_conflict",
      );
      coordinator.send({
        type: "send",
        requestId: "duplicate-request",
        to: "local:worker-a",
        content: "second-copy",
      });
      expect(await requestConflict).toMatchObject({ code: "request_conflict" });
      const duplicateResult = coordinator.waitFor(
        (message) => message.type === "result" && message.requestId === "duplicate-request",
      );
      duplicate.resolve({ ok: true, content: "first-copy-result" });
      expect(await duplicateResult).toMatchObject({ ok: true, content: "first-copy-result" });

      const timeout = coordinator.waitFor(
        (message) => message.type === "error" && message.requestId === "timeout-request",
      );
      coordinator.send({
        type: "send",
        requestId: "timeout-request",
        to: "local:worker-a",
        content: "timeout-task",
        timeoutMs: 25,
      });
      expect(await timeout).toMatchObject({ type: "error", code: "request_timeout" });
      late.resolve({ ok: true, content: "late-result" });

      const disconnectWorker = await RouterTestClient.register(url, "local:worker-b");
      const disconnected = coordinator.waitFor(
        (message) => message.type === "error" && message.requestId === "disconnect-request",
      );
      const delivered = disconnectWorker.waitFor(
        (message) => message.type === "deliver" && message.requestId === "disconnect-request",
      );
      coordinator.send({
        type: "send",
        requestId: "disconnect-request",
        to: "local:worker-b",
        content: "disconnect-task",
      });
      await delivered;
      disconnectWorker.close();
      expect(await disconnected).toMatchObject({ type: "error", code: "target_disconnected" });
    } finally {
      worker.disconnect();
      coordinator.close();
      server.stop(true);
    }
  });

  test("supports duplex list/send calls and isolates two worker responses", async () => {
    const { server, url } = await startTestRouter();
    const coordinator = new GatewayClient({
      routerUrl: url,
      agent: { agentId: "local:coordinator", side: "generic" },
      adapter: new MockSessionAdapter(async () => ({ ok: false, error: "unexpected_delivery" })),
    });
    const workerA = new GatewayClient({
      routerUrl: url,
      agent: { agentId: "local:worker-a", side: "generic" },
      adapter: new MockSessionAdapter(async (request) => ({
        ok: true,
        content: `worker-a:${request.content}`,
      })),
    });
    const workerB = new GatewayClient({
      routerUrl: url,
      agent: { agentId: "local:worker-b", side: "generic" },
      adapter: new MockSessionAdapter(async (request) => ({
        ok: true,
        content: `worker-b:${request.content}`,
      })),
    });

    try {
      await Promise.all([coordinator.connect(), workerA.connect(), workerB.connect()]);
      const agents = await coordinator.listAgents("list-workers");
      expect(agents.map(({ agentId }) => agentId).sort()).toEqual([
        "local:coordinator",
        "local:worker-a",
        "local:worker-b",
      ]);

      const [resultA, resultB] = await Promise.all([
        coordinator.send("local:worker-a", "task-a", { requestId: "outbound-a" }),
        coordinator.send("local:worker-b", "task-b", { requestId: "outbound-b" }),
      ]);
      expect(resultA).toEqual({
        requestId: "outbound-a",
        from: "local:worker-a",
        ok: true,
        content: "worker-a:task-a",
      });
      expect(resultB).toEqual({
        requestId: "outbound-b",
        from: "local:worker-b",
        ok: true,
        content: "worker-b:task-b",
      });
    } finally {
      coordinator.disconnect();
      workerA.disconnect();
      workerB.disconnect();
      server.stop(true);
    }
  });

  test("returns a nested worker result through the original coordinator request", async () => {
    const { server, url } = await startTestRouter();
    const coordinator = new GatewayClient({
      routerUrl: url,
      agent: { agentId: "local:coordinator", side: "generic" },
      adapter: new MockSessionAdapter(async () => ({ ok: false, error: "unexpected_delivery" })),
    });
    let workerA!: GatewayClient;
    const workerAAdapter = new MockSessionAdapter(async (request) => {
      await new Promise((resolve) => setTimeout(resolve, 25));
      const child = await workerA.send("local:worker-b", `nested:${request.content}`, {
        requestId: "nested-child",
        timeoutMs: 5_000,
      });
      return child.ok
        ? { ok: true, content: `worker-a:${child.content}` }
        : { ok: false, error: child.error };
    });
    workerA = new GatewayClient({
      routerUrl: url,
      agent: { agentId: "local:worker-a", side: "generic" },
      adapter: workerAAdapter,
    });
    const workerBAdapter = new MockSessionAdapter(async (request) => ({
      ok: true,
      content: `worker-b:${request.content}`,
    }));
    const workerB = new GatewayClient({
      routerUrl: url,
      agent: { agentId: "local:worker-b", side: "generic" },
      adapter: workerBAdapter,
    });

    try {
      await Promise.all([coordinator.connect(), workerA.connect(), workerB.connect()]);
      const result = await coordinator.send("local:worker-a", "parent-task", {
        requestId: "nested-parent",
        timeoutMs: 1_000,
      });

      expect(result).toEqual({
        requestId: "nested-parent",
        from: "local:worker-a",
        ok: true,
        content: "worker-a:worker-b:nested:parent-task",
      });
      expect(workerAAdapter.requests.map(({ requestId }) => requestId)).toEqual(["nested-parent"]);
      expect(workerBAdapter.requests).toEqual([
        expect.objectContaining({
          requestId: "nested-child",
          from: "local:worker-a",
          content: "nested:parent-task",
        }),
      ]);
      expect(workerBAdapter.requests[0]!.timeoutMs).toBeLessThan(1_000);
    } finally {
      coordinator.disconnect();
      workerA.disconnect();
      workerB.disconnect();
      server.stop(true);
    }
  });

  test("lets an authorized delegate send as its owning agent and caps the nested timeout", async () => {
    const { server, url } = await startTestRouter();
    const delegationToken = "d".repeat(64);
    const coordinator = new GatewayClient({
      routerUrl: url,
      agent: { agentId: "local:coordinator", side: "generic" },
      adapter: new MockSessionAdapter(async () => ({ ok: false, error: "unexpected_delivery" })),
    });
    const delegate = new DelegatedMessengerClient({
      routerUrl: url,
      agentId: "local:worker-a",
      delegationToken,
    });
    const workerBAdapter = new MockSessionAdapter(async (request) => ({
      ok: true,
      content: `worker-b:${request.content}`,
    }));
    const workerB = new GatewayClient({
      routerUrl: url,
      agent: { agentId: "local:worker-b", side: "generic" },
      adapter: workerBAdapter,
    });
    const workerA = new GatewayClient({
      routerUrl: url,
      agent: { agentId: "local:worker-a", side: "generic" },
      delegationToken,
      adapter: new MockSessionAdapter(async (request) => {
        if (!delegate.connected) await delegate.connect();
        const agents = await delegate.listAgents("delegate-list");
        expect(agents.map(({ agentId }) => agentId).sort()).toEqual([
          "local:coordinator",
          "local:worker-a",
          "local:worker-b",
        ]);
        const child = await delegate.send("local:worker-b", `delegated:${request.content}`, {
          requestId: "delegate-child",
          timeoutMs: 5_000,
        });
        return child.ok
          ? { ok: true, content: `worker-a:${child.content}` }
          : { ok: false, error: child.error };
      }),
    });

    try {
      await Promise.all([coordinator.connect(), workerA.connect(), workerB.connect()]);
      const rejected = new DelegatedMessengerClient({
        routerUrl: url,
        agentId: "local:worker-a",
        delegationToken: "x".repeat(64),
      });
      await expect(rejected.connect()).rejects.toThrow("unauthorized");
      rejected.disconnect();

      const result = await coordinator.send("local:worker-a", "parent-task", {
        requestId: "delegate-parent",
        timeoutMs: 1_000,
      });

      expect(result).toEqual({
        requestId: "delegate-parent",
        from: "local:worker-a",
        ok: true,
        content: "worker-a:worker-b:delegated:parent-task",
      });
      expect(workerBAdapter.requests).toEqual([
        expect.objectContaining({
          requestId: "delegate-child",
          from: "local:worker-a",
          content: "delegated:parent-task",
        }),
      ]);
      expect(workerBAdapter.requests[0]!.timeoutMs).toBeLessThan(1_000);

      workerA.disconnect();
      await Bun.sleep(10);
      expect(delegate.connected).toBe(false);
    } finally {
      delegate.disconnect();
      coordinator.disconnect();
      workerA.disconnect();
      workerB.disconnect();
      server.stop(true);
    }
  });
});
