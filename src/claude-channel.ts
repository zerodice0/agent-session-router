import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { ClaudeChannelAdapter, type ClaudeChannelNotifier } from "./claude-channel-adapter";
import { createClaudeChannelMcpServer } from "./claude-channel-mcp";
import { GatewayClient } from "./gateway-client";
import { isAgentId } from "./protocol";

const routerUrl = process.env.ROUTER_URL ?? "ws://127.0.0.1:8787/ws";
const agentId = process.env.GATEWAY_AGENT_ID ?? "local:claude-channel";

if (!isAgentId(agentId)) throw new Error("Invalid GATEWAY_AGENT_ID");

let notify: ClaudeChannelNotifier = async () => {
  throw new Error("Claude Channel is not initialized");
};
const adapter = new ClaudeChannelAdapter((event) => notify(event));
const gateway = new GatewayClient({
  routerUrl,
  agent: { agentId, side: "claude" },
  adapter,
  token: process.env.ROUTER_TOKEN,
});
const channel = createClaudeChannelMcpServer({ agentId, adapter, messenger: gateway });
notify = channel.notify;

let closing = false;
async function close(): Promise<void> {
  if (closing) return;
  closing = true;
  gateway.disconnect();
  adapter.close();
}

channel.server.server.oninitialized = () => {
  void gateway.connect().catch(() => {
    console.error("Claude Channel router connection failed");
    void channel.server.close();
  });
};
channel.server.server.onclose = () => {
  void close();
};
channel.server.server.onerror = () => {};

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => {
    void close().finally(() => channel.server.close());
  });
}

try {
  await channel.server.connect(new StdioServerTransport());
} catch {
  console.error("Claude Channel failed");
  await close();
  process.exitCode = 1;
}
