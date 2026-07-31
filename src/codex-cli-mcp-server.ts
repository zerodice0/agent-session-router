import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import type { AgentMessenger, AgentSendOptions } from "./agent-messenger";
import { CodexCliInboxAdapter } from "./codex-cli-inbox-adapter";
import { createCodexCliMcpServer } from "./codex-cli-mcp";
import { GatewayClient } from "./gateway-client";
import { runtimeAgent } from "./runtime-agent";

const routerUrl = process.env.ROUTER_URL ?? "ws://127.0.0.1:8787/ws";
const agent = runtimeAgent("local:codex-cli", "codex");
const agentId = agent.agentId;

const adapter = new CodexCliInboxAdapter();
const gateway = new GatewayClient({
  routerUrl,
  agent,
  adapter,
  token: process.env.ROUTER_TOKEN,
});
let connection: Promise<void> | null = null;
const ensureConnected = () => (connection ??= gateway.connect());
const messenger: AgentMessenger = {
  async listAgents(requestId?: string) {
    await ensureConnected();
    return gateway.listAgents(requestId);
  },
  async send(target: string, content: string, options?: AgentSendOptions) {
    await ensureConnected();
    return gateway.send(target, content, options);
  },
};
const server = createCodexCliMcpServer({ agentId, adapter, messenger });

let closing = false;
async function close(): Promise<void> {
  if (closing) return;
  closing = true;
  gateway.disconnect();
  adapter.close();
}

server.server.oninitialized = () => {
  void ensureConnected().catch(() => {
    console.error("Codex CLI router connection failed");
    void server.close();
  });
};
server.server.onclose = () => {
  void close();
};
server.server.onerror = () => {};

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => {
    void close().finally(() => server.close());
  });
}

try {
  await server.connect(new StdioServerTransport());
} catch {
  console.error("Codex CLI MCP failed");
  await close();
  process.exitCode = 1;
}
