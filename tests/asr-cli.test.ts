import { describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

function runWithEnvironment(environment: Record<string, string>, ...args: string[]) {
  return Bun.spawnSync({
    cmd: ["python3", "scripts/asr.py", ...args],
    env: { ...process.env, ...environment },
    stdout: "pipe",
    stderr: "pipe",
  });
}

function run(...args: string[]) {
  return runWithEnvironment({}, ...args);
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

  test("stores normalized router profiles outside the repository with private permissions", () => {
    const directory = mkdtempSync(join(tmpdir(), "asr-profile-test-"));
    const configPath = join(directory, "config.json");
    const environment = { ASR_CONFIG_PATH: configPath };

    try {
      const added = runWithEnvironment(environment, "profile", "add", "tailnet", "host-a:9876");
      expect(added.exitCode).toBe(0);
      expect(added.stdout.toString()).toContain("Saved router profile: tailnet");

      const config = JSON.parse(readFileSync(configPath, "utf8"));
      expect(config).toEqual({
        defaultProfile: "tailnet",
        profiles: { tailnet: { routerUrl: "ws://host-a:9876/ws" } },
        version: 1,
      });
      expect(statSync(configPath).mode & 0o777).toBe(0o600);

      const listed = runWithEnvironment(environment, "profile", "list");
      expect(listed.exitCode).toBe(0);
      expect(listed.stdout.toString()).toContain("* tailnet\tws://host-a:9876/ws");
      expect(listed.stdout.toString()).toContain("local\tws://127.0.0.1:8787/ws");

      const selected = runWithEnvironment(environment, "profile", "use", "local");
      expect(selected.exitCode).toBe(0);
      expect(JSON.parse(readFileSync(configPath, "utf8")).defaultProfile).toBe("local");
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test("rejects unsafe router profile values and accidental replacement", () => {
    const directory = mkdtempSync(join(tmpdir(), "asr-profile-test-"));
    const environment = { ASR_CONFIG_PATH: join(directory, "config.json") };

    try {
      expect(
        runWithEnvironment(environment, "profile", "add", "tailnet", "http://host-a:8787").exitCode,
      ).not.toBe(0);
      expect(
        runWithEnvironment(environment, "profile", "add", "tailnet", "ws://user@host-a:8787/ws")
          .exitCode,
      ).not.toBe(0);
      expect(
        runWithEnvironment(environment, "profile", "add", "local", "host-a:8787").exitCode,
      ).not.toBe(0);

      expect(
        runWithEnvironment(environment, "profile", "add", "tailnet", "host-a:8787").exitCode,
      ).toBe(0);
      const duplicate = runWithEnvironment(
        environment,
        "profile",
        "add",
        "tailnet",
        "host-b:8787",
      );
      expect(duplicate.exitCode).not.toBe(0);
      expect(duplicate.stderr.toString()).toContain("already exists");
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test("keeps profile dry runs side-effect free", () => {
    const directory = mkdtempSync(join(tmpdir(), "asr-profile-test-"));
    const configPath = join(directory, "config.json");

    try {
      const result = runWithEnvironment(
        { ASR_CONFIG_PATH: configPath },
        "--dry-run",
        "profile",
        "add",
        "tailnet",
        "host-a:8787",
      );
      expect(result.exitCode).toBe(0);
      expect(JSON.parse(result.stdout.toString())).toEqual({
        profile: "tailnet",
        routerUrl: "ws://host-a:8787/ws",
      });
      expect(existsSync(configPath)).toBeFalse();
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test("requires a terminal when asr is launched without a command", () => {
    const result = run();
    expect(result.exitCode).toBe(2);
    expect(result.stderr.toString()).toContain("Interactive asr requires a terminal");
  });
});
