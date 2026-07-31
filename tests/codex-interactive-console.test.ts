import { describe, expect, test } from "bun:test";
import type { AgentMessenger, AgentSendOptions, AgentSendResult } from "../src/agent-messenger";
import { CodexInteractiveConsole } from "../src/codex-interactive-console";
import type { AgentDescriptor } from "../src/protocol";
import type { SessionAdapter, SessionRequest, SessionResult } from "../src/session-adapter";

class RecordingAdapter implements SessionAdapter {
  readonly requests: SessionRequest[] = [];

  constructor(readonly result: SessionResult = { ok: true, content: "local-result" }) {}

  async handle(request: SessionRequest): Promise<SessionResult> {
    this.requests.push(request);
    return this.result;
  }
}

class RecordingMessenger implements AgentMessenger {
  readonly sends: Array<{ target: string; content: string; options?: AgentSendOptions }> = [];

  constructor(
    readonly agents: AgentDescriptor[],
    readonly result: AgentSendResult = {
      requestId: "routed-one",
      from: "local:worker-a",
      ok: true,
      content: "routed-result",
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

function setup(
  adapter = new RecordingAdapter(),
  messenger = new RecordingMessenger([
    { agentId: "local:codex", side: "codex" },
    {
      agentId: "local:worker-a",
      side: "codex",
      status: "busy",
      activity: "reviewing tests",
    },
  ]),
) {
  const output: string[] = [];
  const interactive = new CodexInteractiveConsole({
    agentId: "local:codex",
    adapter,
    messenger,
    write: (text) => output.push(text),
    requestIdFactory: () => "interactive-one",
  });
  return { adapter, messenger, output, interactive };
}

describe("CodexInteractiveConsole", () => {
  test("sends ordinary terminal input to the gateway-owned Codex thread", async () => {
    const { adapter, output, interactive } = setup();

    expect(await interactive.execute("review-task")).toBe("continue");
    expect(adapter.requests).toEqual([
      {
        requestId: "interactive-one",
        from: "local:codex",
        content: "review-task",
        timeoutMs: 600_000,
      },
    ]);
    expect(output).toEqual(["local-result\n"]);
  });

  test("lists peers and provides a direct correlated send command", async () => {
    const { messenger, output, interactive } = setup();

    await interactive.execute("/agents");
    await interactive.execute("/send local:worker-a routed-task");

    expect(output).toEqual([
      "local:worker-a (codex, busy) - reviewing tests\n",
      "routed-result\n",
    ]);
    expect(messenger.sends).toEqual([
      { target: "local:worker-a", content: "routed-task", options: undefined },
    ]);
  });

  test("keeps command errors content-free and exits explicitly", async () => {
    const adapter = new RecordingAdapter({ ok: false, error: "session_busy" });
    const messenger = new RecordingMessenger([], {
      requestId: "offline-one",
      ok: false,
      error: "target_offline",
    });
    const { output, interactive } = setup(adapter, messenger);

    await interactive.execute("local-task");
    await interactive.execute("/send local:worker-a routed-task");
    await interactive.execute("/send");

    expect(output).toEqual([
      "Codex request failed: session_busy\n",
      "Agent request failed: target_offline\n",
      "Usage: /send <agent> <message>\n",
    ]);
    expect(await interactive.execute("/quit")).toBe("exit");
  });
});
