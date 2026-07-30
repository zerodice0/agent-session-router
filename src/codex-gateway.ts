import { CodexAppServerAdapter } from "./codex-app-server-adapter";
import { CodexStdioTransport } from "./codex-app-server-transport";
import { GatewayClient } from "./gateway-client";
import { isAgentId } from "./protocol";

const routerUrl = process.env.ROUTER_URL ?? "ws://127.0.0.1:8787/ws";
const agentId = process.env.GATEWAY_AGENT_ID ?? "local:codex";
const providerCwd = process.env.CODEX_CWD;
const resumeThreadId = process.env.CODEX_THREAD_ID;

if (!isAgentId(agentId)) throw new Error("Invalid GATEWAY_AGENT_ID");

const transport = CodexStdioTransport.spawn({
  ...(providerCwd === undefined ? {} : { cwd: providerCwd }),
});
let adapter: CodexAppServerAdapter | null = null;
let gateway: GatewayClient | null = null;

try {
  adapter = await CodexAppServerAdapter.connect({
    transport,
    thread:
      resumeThreadId === undefined
        ? { mode: "start" }
        : { mode: "resume", threadId: resumeThreadId },
  });
  gateway = new GatewayClient({
    routerUrl,
    agent: { agentId, side: "codex" },
    adapter,
    token: process.env.ROUTER_TOKEN,
  });
  await gateway.connect();
  await waitForShutdown();
} catch {
  console.error("Codex gateway failed");
  process.exitCode = 1;
} finally {
  gateway?.disconnect();
  adapter?.close();
  transport.close();
}

function waitForShutdown(): Promise<void> {
  return new Promise((resolve) => {
    const finish = () => resolve();
    process.once("SIGINT", finish);
    process.once("SIGTERM", finish);
  });
}
