import { describe, expect, test } from "bun:test";

function run(...args: string[]) {
  return Bun.spawnSync({
    cmd: ["python3", "scripts/asr.py", ...args],
    stdout: "pipe",
    stderr: "pipe",
  });
}

describe("asr launcher", () => {
  test("normalizes a short Codex name and keeps loopback defaults", () => {
    const result = run("--dry-run", "codex", "worker-a");
    expect(result.exitCode).toBe(0);
    expect(JSON.parse(result.stdout.toString())).toEqual({
      command: ["bun", "run", "codex:interactive"],
      routerUrl: "ws://127.0.0.1:8787/ws",
      agentId: "local:worker-a",
    });
  });

  test("starts stock Codex with only MCP environment names in process arguments", () => {
    const result = run(
      "--dry-run",
      "codex-cli",
      "worker-a",
      "--activity",
      "reviewing tests",
      "--",
      "--search",
    );
    expect(result.exitCode).toBe(0);
    const output = JSON.parse(result.stdout.toString());
    const command = output.command as string[];
    const joined = command.join(" ");

    expect(command[0]).toBe("codex");
    expect(command).toContain("-C");
    expect(command.at(-1)).toBe("--search");
    expect(joined).toContain("mcp_servers.agent_session_router_cli.command");
    expect(joined).toContain("ROUTER_URL");
    expect(joined).toContain("GATEWAY_AGENT_ID");
    expect(joined).toContain("GATEWAY_AGENT_ACTIVITY");
    expect(joined).toContain("ROUTER_TOKEN");
    expect(joined).toContain('enabled_tools=["agent_list","agent_send","agent_wait","agent_reply"]');
    expect(joined).not.toContain("ws://127.0.0.1:8787/ws");
    expect(joined).not.toContain("local:worker-a");
    expect(joined).not.toContain("reviewing tests");
    expect(output).toMatchObject({
      routerUrl: "ws://127.0.0.1:8787/ws",
      agentId: "local:worker-a",
    });
  });

  test("uses short commands for router, Claude, and tests", () => {
    expect(JSON.parse(run("--dry-run", "router").stdout.toString()).command).toEqual([
      "bun",
      "run",
      "start",
    ]);
    expect(JSON.parse(run("--dry-run", "claude", "reviewer").stdout.toString())).toMatchObject({
      command: [
        "claude",
        "--dangerously-load-development-channels",
        "server:agent-session-router-channel",
      ],
      agentId: "local:reviewer",
    });
    expect(
      JSON.parse(
        run(
          "--dry-run",
          "claude",
          "reviewer",
          "--activity",
          "reviewing tests",
          "--auto",
        ).stdout.toString(),
      ),
    ).toMatchObject({
      command: [
        "claude",
        "--permission-mode",
        "auto",
        "--dangerously-load-development-channels",
        "server:agent-session-router-channel",
      ],
      agentId: "local:reviewer",
    });
    expect(JSON.parse(run("--dry-run", "test").stdout.toString()).command).toEqual([
      "bun",
      "test",
    ]);
    expect(JSON.parse(run("--dry-run", "smoke").stdout.toString()).command).toEqual([
      "bun",
      "run",
      "smoke",
    ]);
    expect(JSON.parse(run("--dry-run", "smoke", "worker-a").stdout.toString())).toMatchObject({
      command: ["bun", "run", "smoke:provider"],
    });
  });

  test("rejects invalid agent names before launching a provider", () => {
    const result = run("--dry-run", "codex", "invalid name");
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr.toString()).toContain("agent name must use");

    const codexCli = run("--dry-run", "codex-cli", "invalid name");
    expect(codexCli.exitCode).not.toBe(0);

    const activity = run("--dry-run", "claude", "reviewer", "--activity", "unsafe\nvalue");
    expect(activity.exitCode).not.toBe(0);
  });
});
