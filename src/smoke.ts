import { PROTOCOL_VERSION, type ServerMessage } from "./protocol";

const routerUrl = process.env.ROUTER_URL?.trim() || "ws://127.0.0.1:8787/ws";
const token = process.env.ROUTER_TOKEN;
const smokeTimeoutMs = 3_000;

function waitForMessage(
  socket: WebSocket,
  predicate: (message: ServerMessage) => boolean,
): Promise<ServerMessage> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("Timed out waiting for router message"));
    }, smokeTimeoutMs);

    const onMessage = (event: MessageEvent) => {
      let message: ServerMessage;
      try {
        message = JSON.parse(String(event.data)) as ServerMessage;
      } catch {
        return;
      }
      if (!predicate(message)) return;
      cleanup();
      resolve(message);
    };

    const onClose = () => {
      cleanup();
      reject(new Error("Router connection closed before the expected message"));
    };

    function cleanup() {
      clearTimeout(timer);
      socket.removeEventListener("message", onMessage);
      socket.removeEventListener("close", onClose);
    }

    socket.addEventListener("message", onMessage);
    socket.addEventListener("close", onClose);
  });
}

function openSocket(): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(routerUrl);
    socket.addEventListener("open", () => resolve(socket), { once: true });
    socket.addEventListener("error", () => reject(new Error(`Unable to connect to ${routerUrl}`)), {
      once: true,
    });
  });
}

async function register(agentId: string): Promise<WebSocket> {
  const socket = await openSocket();
  const registered = waitForMessage(socket, (message) => message.type === "registered");
  socket.send(
    JSON.stringify({
      type: "register",
      protocolVersion: PROTOCOL_VERSION,
      agent: { agentId, side: "generic" },
      ...(token === undefined ? {} : { token }),
    }),
  );
  await registered;
  return socket;
}

const requestId = `smoke-${crypto.randomUUID()}`;
const coordinator = await register("smoke:coordinator");
const worker = await register("smoke:worker");

try {
  const accepted = waitForMessage(
    coordinator,
    (message) => message.type === "accepted" && message.requestId === requestId,
  );
  const delivered = waitForMessage(
    worker,
    (message) => message.type === "deliver" && message.requestId === requestId,
  );

  coordinator.send(
    JSON.stringify({
      type: "send",
      requestId,
      to: "smoke:worker",
      content: "smoke request",
    }),
  );

  await accepted;
  const delivery = await delivered;
  if (delivery.type !== "deliver" || delivery.from !== "smoke:coordinator") {
    throw new Error("Delivery did not preserve the requester identity");
  }

  const result = waitForMessage(
    coordinator,
    (message) => message.type === "result" && message.requestId === requestId,
  );
  worker.send(
    JSON.stringify({
      type: "reply",
      requestId,
      ok: true,
      content: "smoke response",
    }),
  );

  const reply = await result;
  if (reply.type !== "result" || reply.from !== "smoke:worker" || reply.content !== "smoke response") {
    throw new Error("Reply did not preserve routing metadata");
  }

  console.info("smoke test passed: coordinator -> worker -> coordinator");
} finally {
  coordinator.close();
  worker.close();
}

