import { copyFile, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";

const root = resolve(import.meta.dir, "..");
const output = join(root, "dist", "integrations");
const ompOutput = join(output, "omp");
const claudeOutput = join(output, "claude-sdk");

await rm(output, { recursive: true, force: true });
await mkdir(ompOutput, { recursive: true });
await mkdir(claudeOutput, { recursive: true });

const ompBuild = await Bun.build({
  entrypoints: [join(root, "integrations", "omp", "extension.ts")],
  outdir: ompOutput,
  naming: "index.js",
  format: "esm",
  target: "bun",
  minify: false,
  sourcemap: "none",
});
if (!ompBuild.success) throw new Error("OMP integration build failed");

const claudeBuild = await Bun.build({
  entrypoints: [join(root, "integrations", "claude-sdk", "bridge.ts")],
  outdir: claudeOutput,
  naming: "bridge.js",
  format: "esm",
  target: "node",
  external: ["@anthropic-ai/claude-agent-sdk"],
  minify: false,
  sourcemap: "none",
});
if (!claudeBuild.success) throw new Error("Claude SDK bridge build failed");

await writeJson(join(ompOutput, "package.json"), {
  name: "@agent-session-router/omp-integration",
  version: "0.1.0",
  private: true,
  type: "module",
  omp: { extensions: ["./index.js"] },
});

const rootPackageText = await readFile(join(root, "package.json"), "utf8");
const rootPackage = JSON.parse(rootPackageText) as {
  dependencies: Record<string, string>;
};
const claudeDependencies = Object.fromEntries(
  [
    "@anthropic-ai/claude-agent-sdk",
    "@anthropic-ai/sdk",
    "@modelcontextprotocol/sdk",
    "zod",
  ].map((name) => {
    const version = rootPackage.dependencies[name];
    if (!version) throw new Error(`Missing production dependency: ${name}`);
    return [name, version];
  }),
);
await writeFile(join(claudeOutput, "package.json"), rootPackageText, "utf8");
await copyFile(join(root, "bun.lock"), join(claudeOutput, "bun.lock"));

const install = Bun.spawn(
  [process.execPath, "install", "--production", "--frozen-lockfile", "--ignore-scripts"],
  {
    cwd: claudeOutput,
    stdin: "ignore",
    stdout: "inherit",
    stderr: "inherit",
    env: minimalBuildEnvironment(process.env),
  },
);
if ((await install.exited) !== 0) throw new Error("Claude SDK production dependency staging failed");
await rm(join(claudeOutput, "node_modules", ".bin"), { recursive: true, force: true });

await writeJson(join(claudeOutput, "package.json"), {
  name: "@agent-session-router/claude-sdk-integration",
  version: "0.1.0",
  private: true,
  type: "module",
  engines: { node: ">=22" },
  dependencies: claudeDependencies,
});
await writeJson(join(claudeOutput, "manifest.json"), {
  version: 1,
  entrypoint: "bridge.js",
  protocol: "length-prefixed-json-v1",
  maximumFrameBytes: 1_048_576,
  node: ">=22",
});
await rm(join(claudeOutput, "bun.lock"), { force: true });

async function writeJson(path: string, value: unknown): Promise<void> {
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

function minimalBuildEnvironment(
  source: Record<string, string | undefined>,
): Record<string, string> {
  const environment: Record<string, string> = {};
  for (const key of ["HOME", "PATH", "TMPDIR", "TMP", "TEMP", "XDG_CACHE_HOME"] as const) {
    const value = source[key];
    if (value !== undefined) environment[key] = value;
  }
  return environment;
}
