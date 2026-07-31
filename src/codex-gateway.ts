import { CodexAppServerAdapter } from "./codex-app-server-adapter";
import { CodexStdioTransport } from "./codex-app-server-transport";
import { GatewayClient } from "./gateway-client";
import {
  createAgentToolsEnvironment,
  createCodexAppServerCommand,
  createDelegationToken,
} from "./agent-tools-config";
import { runtimeAgent } from "./runtime-agent";

const routerUrl = process.env.ROUTER_URL ?? "ws://127.0.0.1:8787/ws";
const agent = runtimeAgent("local:codex", "codex");
const agentId = agent.agentId;
const providerCwd = process.env.CODEX_CWD;
const resumeThreadId = process.env.CODEX_THREAD_ID;

const delegationToken = createDelegationToken();

const transport = CodexStdioTransport.spawn({
  command: createCodexAppServerCommand(),
  ...(providerCwd === undefined ? {} : { cwd: providerCwd }),
  env: createAgentToolsEnvironment({
    routerUrl,
    agentId,
    delegationToken,
  }),
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
    agent,
    adapter,
    token: process.env.ROUTER_TOKEN,
    delegationToken,
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
