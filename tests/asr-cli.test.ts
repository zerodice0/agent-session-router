import { describe, expect, test } from "bun:test";
import {
  existsSync,
  lstatSync,
  mkdtempSync,
  mkdirSync,
  readFileSync,
  realpathSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
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

async function startPortOwner(body: string) {
  const child = Bun.spawn({
    cmd: [
      "bun",
      "-e",
      `const server = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        fetch() { return new Response(${JSON.stringify(body)}); },
      });
      console.log(server.port);
      await new Promise(() => {});`,
    ],
    stdout: "pipe",
    stderr: "pipe",
  });
  const reader = child.stdout.getReader();
  let output = "";
  while (!output.includes("\n")) {
    const chunk = await reader.read();
    if (chunk.done) throw new Error("test port owner exited before reporting its port");
    output += new TextDecoder().decode(chunk.value);
  }
  reader.releaseLock();
  return { child, port: Number(output.trim()) };
}

describe("agent-session-router launcher", () => {
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

  test("shares on the LAN and updates a separate this-device profile", () => {
    const directory = mkdtempSync(join(tmpdir(), "agent-session-router-share-test-"));
    const binDirectory = join(directory, "bin");
    const configPath = join(directory, "config.json");
    mkdirSync(binDirectory);
    writeFileSync(join(binDirectory, "bun"), "#!/bin/sh\nexit 0\n", { mode: 0o755 });
    const environment = {
      ASR_CONFIG_PATH: configPath,
      PATH: `${binDirectory}:${process.env.PATH}`,
      ROUTER_HOST: "192.0.2.44",
      ROUTER_PORT: "9876",
      ROUTER_TOKEN: "test-token",
    };

    try {
      const dryRun = runWithEnvironment(environment, "--dry-run", "router", "--share=lan");
      expect(dryRun.exitCode).toBe(0);
      expect(JSON.parse(dryRun.stdout.toString())).toEqual({
        command: ["bun", "run", "start"],
        routerUrl: "ws://192.0.2.44:9876/ws",
        routerHost: "0.0.0.0",
        profile: "this-device",
      });
      expect(dryRun.stderr.toString()).toContain("direct LAN sharing");
      expect(existsSync(configPath)).toBeFalse();

      const started = runWithEnvironment(environment, "router", "--share=lan");
      expect(started.exitCode).toBe(0);
      expect(started.stdout.toString()).toContain("Router server");
      expect(started.stdout.toString()).toContain("Access: LAN");
      expect(started.stdout.toString()).toContain("Profile name: this-device");
      expect(started.stdout.toString()).toContain(
        "Router address: ws://192.0.2.44:9876/ws",
      );
      expect(started.stdout.toString()).toContain(
        "agent-session-router profile add this-device ws://192.0.2.44:9876/ws --force",
      );
      expect(started.stdout.toString()).toContain("ROUTER_TOKEN on the other device (value hidden)");
      expect(started.stdout.toString()).not.toContain("test-token");
      expect(JSON.parse(readFileSync(configPath, "utf8"))).toEqual({
        defaultProfile: "local",
        profiles: { "this-device": { routerUrl: "ws://192.0.2.44:9876/ws" } },
        version: 1,
      });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test("prefers an available Tailscale TCP forwarder for automatic sharing", () => {
    const directory = mkdtempSync(join(tmpdir(), "agent-session-router-share-test-"));
    const binDirectory = join(directory, "bin");
    const configPath = join(directory, "config.json");
    const tailscale = join(binDirectory, "tailscale");
    mkdirSync(binDirectory);
    writeFileSync(join(binDirectory, "bun"), "#!/bin/sh\nexit 0\n", { mode: 0o755 });
    writeFileSync(
      tailscale,
      '#!/bin/sh\nif [ "$1" = "ip" ]; then echo 100.64.0.10; exit 0; fi\n' +
        'if [ "$1" = "serve" ]; then exit 0; fi\nexit 1\n',
      { mode: 0o755 },
    );
    const environment = {
      ASR_CONFIG_PATH: configPath,
      PATH: `${binDirectory}:${process.env.PATH}`,
      ROUTER_PORT: "9877",
      ROUTER_TOKEN: "test-token",
    };

    try {
      const dryRun = runWithEnvironment(environment, "--dry-run", "router", "--share");
      expect(dryRun.exitCode).toBe(0);
      expect(JSON.parse(dryRun.stdout.toString())).toEqual({
        command: ["bun", "run", "start"],
        routerUrl: "ws://100.64.0.10:9877/ws",
        routerHost: "127.0.0.1",
        profile: "this-device",
        setupCommand: [
          tailscale,
          "serve",
          "--bg",
          "--tcp=9877",
          "tcp://127.0.0.1:9877",
        ],
      });
      expect(existsSync(configPath)).toBeFalse();

      const started = runWithEnvironment(environment, "router", "--share");
      expect(started.exitCode).toBe(0);
      expect(started.stdout.toString()).toContain("Access: Tailscale");
      expect(started.stdout.toString()).toContain(
        "Router address: ws://100.64.0.10:9877/ws",
      );
      expect(JSON.parse(readFileSync(configPath, "utf8"))).toMatchObject({
        defaultProfile: "local",
        profiles: { "this-device": { routerUrl: "ws://100.64.0.10:9877/ws" } },
      });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test("reuses an existing router instead of starting a second Bun server", async () => {
    const owner = await startPortOwner('{"status":"ok","connectedAgents":0}');
    const directory = mkdtempSync(join(tmpdir(), "agent-session-router-reuse-test-"));
    const binDirectory = join(directory, "bin");
    const configPath = join(directory, "config.json");
    const tailscale = join(binDirectory, "tailscale");
    mkdirSync(binDirectory);
    writeFileSync(join(binDirectory, "bun"), "#!/bin/sh\necho unexpected Bun start\nexit 99\n", {
      mode: 0o755,
    });
    writeFileSync(
      tailscale,
      '#!/bin/sh\nif [ "$1" = "ip" ]; then echo 100.64.0.10; exit 0; fi\n' +
        'if [ "$1" = "serve" ]; then exit 0; fi\nexit 1\n',
      { mode: 0o755 },
    );

    try {
      const result = runWithEnvironment(
        {
          ASR_CONFIG_PATH: configPath,
          PATH: `${binDirectory}:${process.env.PATH}`,
          ROUTER_PORT: String(owner.port),
        },
        "router",
        "--share",
      );
      expect(result.exitCode).toBe(0);
      expect(result.stdout.toString()).toContain(
        "Status: already running; reused existing router",
      );
      expect(result.stdout.toString()).not.toContain("unexpected Bun start");
      expect(result.stderr.toString()).not.toContain("EADDRINUSE");
      expect(JSON.parse(readFileSync(configPath, "utf8"))).toMatchObject({
        profiles: {
          "this-device": { routerUrl: `ws://100.64.0.10:${owner.port}/ws` },
        },
      });
    } finally {
      owner.child.kill();
      await owner.child.exited;
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test("rejects a port owned by another service before sharing", async () => {
    const owner = await startPortOwner('{"service":"other"}');
    const directory = mkdtempSync(join(tmpdir(), "agent-session-router-conflict-test-"));
    const configPath = join(directory, "config.json");

    try {
      const result = runWithEnvironment(
        {
          ASR_CONFIG_PATH: configPath,
          ROUTER_HOST: "192.0.2.44",
          ROUTER_PORT: String(owner.port),
        },
        "router",
        "--share=lan",
      );
      expect(result.exitCode).toBe(2);
      expect(result.stderr.toString()).toContain(
        `port ${owner.port} is already in use by another process`,
      );
      expect(result.stdout.toString()).not.toContain("Router server");
      expect(existsSync(configPath)).toBeFalse();
    } finally {
      owner.child.kill();
      await owner.child.exited;
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test("stops a verified router and disables its Tailscale forward", async () => {
    const owner = await startPortOwner('{"status":"ok","connectedAgents":0}');
    const directory = mkdtempSync(join(tmpdir(), "agent-session-router-stop-test-"));
    const binDirectory = join(directory, "bin");
    mkdirSync(binDirectory);
    writeFileSync(join(binDirectory, "tailscale"), "#!/bin/sh\nexit 0\n", { mode: 0o755 });

    try {
      const result = runWithEnvironment(
        {
          PATH: `${binDirectory}:${process.env.PATH}`,
          ROUTER_PORT: String(owner.port),
        },
        "router",
        "stop",
      );
      expect(result.exitCode).toBe(0);
      expect(result.stdout.toString()).toContain(`Stopped router on port ${owner.port}`);
      expect(result.stdout.toString()).toContain(
        `Disabled Tailscale Serve for port ${owner.port}`,
      );
      await owner.child.exited;
    } finally {
      if (owner.child.exitCode === null) owner.child.kill();
      await owner.child.exited;
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test("does not stop another service that owns the router port", async () => {
    const owner = await startPortOwner('{"service":"other"}');

    try {
      const result = runWithEnvironment(
        { ROUTER_PORT: String(owner.port) },
        "router",
        "stop",
      );
      expect(result.exitCode).toBe(2);
      expect(result.stderr.toString()).toContain("is owned by another process");
      expect(owner.child.exitCode).toBeNull();
    } finally {
      owner.child.kill();
      await owner.child.exited;
    }
  });

  test("does not fall back to LAN exposure when Tailscale is unavailable", () => {
    const directory = mkdtempSync(join(tmpdir(), "agent-session-router-share-test-"));
    const binDirectory = join(directory, "bin");
    const configPath = join(directory, "config.json");
    mkdirSync(binDirectory);
    writeFileSync(join(binDirectory, "tailscale"), "#!/bin/sh\nexit 1\n", { mode: 0o755 });

    try {
      const result = runWithEnvironment(
        {
          ASR_CONFIG_PATH: configPath,
          PATH: `${binDirectory}:${process.env.PATH}`,
        },
        "--dry-run",
        "router",
        "--share",
      );
      expect(result.exitCode).toBe(2);
      expect(result.stderr.toString()).toContain(
        "use --share=lan to explicitly accept LAN exposure",
      );
      expect(existsSync(configPath)).toBeFalse();
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
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

  test("installs one idempotent executable link without replacing other files", () => {
    const directory = mkdtempSync(join(tmpdir(), "agent-session-router-install-test-"));
    const target = join(directory, "agent-session-router");

    try {
      const installed = run("install", "--bin-dir", directory);
      expect(installed.exitCode).toBe(0);
      expect(installed.stdout.toString()).toContain("Installed agent-session-router");
      expect(lstatSync(target).isSymbolicLink()).toBeTrue();
      expect(realpathSync(target)).toBe(realpathSync("scripts/asr.py"));

      const repeated = run("install", "--bin-dir", directory);
      expect(repeated.exitCode).toBe(0);
      expect(repeated.stdout.toString()).toContain("Already installed");

      rmSync(target);
      writeFileSync(target, "keep me", "utf8");
      const conflict = run("install", "--bin-dir", directory);
      expect(conflict.exitCode).toBe(2);
      expect(conflict.stderr.toString()).toContain("installation target already exists");
      expect(readFileSync(target, "utf8")).toBe("keep me");
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test("keeps install dry runs side-effect free", () => {
    const directory = mkdtempSync(join(tmpdir(), "agent-session-router-install-test-"));

    try {
      const result = run("--dry-run", "install", "--bin-dir", directory);
      expect(result.exitCode).toBe(0);
      expect(JSON.parse(result.stdout.toString())).toMatchObject({
        command: "agent-session-router",
        target: join(realpathSync(directory), "agent-session-router"),
      });
      expect(existsSync(join(directory, "agent-session-router"))).toBeFalse();
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test("requires a terminal when agent-session-router is launched without a command", () => {
    const result = run();
    expect(result.exitCode).toBe(2);
    expect(result.stderr.toString()).toContain(
      "Interactive agent-session-router requires a terminal",
    );
  });
});
