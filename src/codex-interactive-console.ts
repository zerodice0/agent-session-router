import type { AgentMessenger } from "./agent-messenger";
import { GatewayRequestError } from "./gateway-client";
import { MAX_REQUEST_TIMEOUT_MS } from "./protocol";
import type { SessionAdapter } from "./session-adapter";

const HELP = [
  "Commands:",
  "  /agents                 List connected agents",
  "  /send <agent> <message> Send directly through the router",
  "  /help                   Show this help",
  "  /quit                   Exit",
  "Any other input is sent to the local Codex thread.",
].join("\n");

export interface CodexInteractiveConsoleOptions {
  agentId: string;
  adapter: SessionAdapter;
  messenger: AgentMessenger;
  write: (text: string) => void;
  turnTimeoutMs?: number;
  requestIdFactory?: () => string;
}

/**
 * Minimal prompt surface for a gateway-owned Codex thread. It deliberately
 * writes only to the attached terminal and has no transcript or log sink.
 */
export class CodexInteractiveConsole {
  readonly #agentId: string;
  readonly #adapter: SessionAdapter;
  readonly #messenger: AgentMessenger;
  readonly #write: (text: string) => void;
  readonly #turnTimeoutMs: number;
  readonly #requestIdFactory: () => string;

  constructor(options: CodexInteractiveConsoleOptions) {
    this.#agentId = options.agentId;
    this.#adapter = options.adapter;
    this.#messenger = options.messenger;
    this.#write = options.write;
    this.#turnTimeoutMs = Math.max(
      1,
      Math.min(Math.floor(options.turnTimeoutMs ?? MAX_REQUEST_TIMEOUT_MS), MAX_REQUEST_TIMEOUT_MS),
    );
    this.#requestIdFactory =
      options.requestIdFactory ?? (() => `interactive-${crypto.randomUUID()}`);
  }

  async execute(line: string): Promise<"continue" | "exit"> {
    const input = line.trim();
    if (input.length === 0) return "continue";
    if (input === "/quit" || input === "/exit") return "exit";
    if (input === "/help") {
      this.#write(`${HELP}\n`);
      return "continue";
    }
    if (input === "/agents") {
      await this.#listAgents();
      return "continue";
    }
    if (input === "/send" || input.startsWith("/send ")) {
      await this.#send(input);
      return "continue";
    }

    const result = await this.#adapter.handle({
      requestId: this.#requestIdFactory(),
      from: this.#agentId,
      content: input,
      timeoutMs: this.#turnTimeoutMs,
    });
    this.#write(result.ok ? `${result.content}\n` : `Codex request failed: ${result.error}\n`);
    return "continue";
  }

  async #listAgents(): Promise<void> {
    try {
      const agents = (await this.#messenger.listAgents()).filter(
        ({ agentId }) => agentId !== this.#agentId,
      );
      if (agents.length === 0) {
        this.#write("No other agents are connected.\n");
        return;
      }
      for (const agent of agents) {
        const status = agent.status ?? "unknown";
        const activity = agent.activity === undefined ? "" : ` - ${agent.activity}`;
        this.#write(`${agent.agentId} (${agent.side}, ${status})${activity}\n`);
      }
    } catch (error) {
      this.#write(`Unable to list agents: ${readGatewayError(error)}\n`);
    }
  }

  async #send(input: string): Promise<void> {
    const match = /^\/send\s+(\S+)\s+([\s\S]+)$/.exec(input);
    if (!match) {
      this.#write("Usage: /send <agent> <message>\n");
      return;
    }

    const [, target, message] = match;
    try {
      const result = await this.#messenger.send(target, message);
      this.#write(
        result.ok ? `${result.content}\n` : `Agent request failed: ${result.error}\n`,
      );
    } catch (error) {
      this.#write(`Unable to send request: ${readGatewayError(error)}\n`);
    }
  }
}

function readGatewayError(error: unknown): string {
  if (error instanceof GatewayRequestError) return error.code;
  return "gateway_error";
}
