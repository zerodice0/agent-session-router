import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { lstat, open, readFile, readdir, rename, unlink } from "node:fs/promises";
import { join, resolve } from "node:path";

const targets = [
  "aarch64-apple-darwin",
  "x86_64-apple-darwin",
  "aarch64-unknown-linux-gnu",
  "x86_64-unknown-linux-gnu",
] as const;
const manifestName = "bootstrap-manifest.json";
const maxArchiveBytes = 512 * 1024 * 1024;

type Digest = { sha256: string; bytes: number };
type BootstrapArtifact = {
  target: string;
  binaryFile: string;
  binarySha256: string;
  archiveFile: string;
  archiveSha256: string;
  binaryBytes: number;
  archiveBytes: number;
};

class ManifestError extends Error {}

function fail(message: string): never {
  throw new ManifestError(message);
}

async function regularFile(path: string, label: string, maxBytes: number): Promise<number> {
  const stat = await lstat(path);
  if (!stat.isFile() || stat.isSymbolicLink()) {
    fail(`${label}: provide a regular file, not a symlink or directory.`);
  }
  if (stat.size < 1 || stat.size > maxBytes) {
    fail(`${label}: file is empty or exceeds the supported size limit.`);
  }
  return stat.size;
}

async function digestFile(path: string, label: string): Promise<Digest> {
  const expectedBytes = await regularFile(path, label, maxArchiveBytes);
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const stat = await file.stat();
    if (!stat.isFile() || stat.size !== expectedBytes) fail(`${label}: input changed; restage the bundle.`);
    const hash = createHash("sha256");
    let bytes = 0;
    for await (const chunk of file.createReadStream({ autoClose: false })) {
      bytes += chunk.length;
      if (bytes > expectedBytes) fail(`${label}: input changed; restage the bundle.`);
      hash.update(chunk);
    }
    if (bytes !== expectedBytes) fail(`${label}: input changed; restage the bundle.`);
    return { sha256: hash.digest("hex"), bytes };
  } finally {
    await file.close();
  }
}

async function verifyChecksum(dir: string, filename: string, digest: Digest): Promise<void> {
  const path = join(dir, `${filename}.sha256`);
  await regularFile(path, `${filename}.sha256`, 1024);
  const text = await readFile(path, "utf8");
  // Accept the standard sha256sum/shasum text and binary markers, exactly one record.
  const match = /^([a-fA-F0-9]{64}) [ *]([^\r\n]+)\r?\n?$/.exec(text);
  if (!match || match[2] !== filename) {
    fail(`${filename}.sha256: regenerate one SHA256 record naming exactly ${filename}.`);
  }
  if (match[1].toLowerCase() !== digest.sha256) {
    fail(`${filename}: checksum mismatch; rebuild or recopy the original artifact.`);
  }
}

async function tarOutput(
  args: string[],
  maxBytes: number,
  consume: (chunk: Uint8Array) => void,
): Promise<number> {
  let child;
  try {
    child = Bun.spawn(["tar", ...args], {
      stdin: "ignore",
      stdout: "pipe",
      stderr: "ignore",
      // Do not let TAR_OPTIONS or compression environment variables alter validation.
      env: { PATH: process.env.PATH ?? "/usr/bin:/bin", LANG: "C", LC_ALL: "C" },
    });
  } catch {
    fail("archive verification: install a supported tar executable and retry.");
  }
  let bytes = 0;
  try {
    const reader = child.stdout.getReader();
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        bytes += value.byteLength;
        if (bytes > maxBytes) fail("archive verification: bin/asr is duplicated, oversized, or does not match the raw binary.");
        consume(value);
      }
    } finally {
      reader.releaseLock();
    }
    if ((await child.exited) !== 0) {
      fail("archive verification: provide a valid gzip tar archive containing exactly one regular bin/asr file.");
    }
  } catch (error) {
    child.kill();
    await child.exited;
    throw error;
  }
  return bytes;
}

async function verifyArchivedBinary(path: string, binary: Digest): Promise<void> {
  // Listing only the fixed member keeps output bounded and detects duplicate members
  // and links without extracting any paths from the untrusted archive.
  const chunks: Buffer[] = [];
  await tarOutput(["-tvzf", path, "bin/asr"], 4096, (chunk) => chunks.push(Buffer.from(chunk)));
  const lines = Buffer.concat(chunks).toString("utf8").trimEnd().split("\n");
  if (lines.length !== 1 || !lines[0].startsWith("-") || !lines[0].endsWith(" bin/asr")) {
    fail("archive verification: bin/asr must occur exactly once as a regular file.");
  }
  const hash = createHash("sha256");
  const bytes = await tarOutput(["-xOzf", path, "bin/asr"], binary.bytes, (chunk) => hash.update(chunk));
  if (bytes !== binary.bytes || hash.digest("hex") !== binary.sha256) {
    fail("archive verification: bin/asr differs from the raw binary; stage both from the same build.");
  }
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  if (args.length !== 2 || args[0] !== "--dir" || !args[1] || args[1].startsWith("--")) {
    fail("usage: bun scripts/bootstrap-manifest.ts --dir DIRECTORY");
  }
  const dir = resolve(args[1]);
  const dirStat = await lstat(dir);
  if (!dirStat.isDirectory() || dirStat.isSymbolicLink()) fail("bundle directory: provide a real directory, not a symlink.");
  const names = new Set(await readdir(dir));
  const allowed = new Set<string>([manifestName]);
  for (const target of targets) {
    allowed.add(`asr-${target}`);
    allowed.add(`asr-${target}.sha256`);
    allowed.add(`agent-session-router-${target}.tar.gz`);
    allowed.add(`agent-session-router-${target}.tar.gz.sha256`);
  }
  if ([...names].some((name) => !allowed.has(name))) {
    fail("bundle directory: unsupported target or unexpected filename; include only fixed asr-TARGET binaries, agent-session-router-TARGET.tar.gz archives, and their .sha256 files.");
  }

  const cargo = Bun.TOML.parse(await readFile(resolve(import.meta.dir, "..", "Cargo.toml"), "utf8")) as {
    package?: { version?: unknown };
  };
  const asrVersion = cargo.package?.version;
  if (typeof asrVersion !== "string" || !asrVersion) fail("package version: Cargo.toml must define package.version as a nonempty string.");

  const artifacts: BootstrapArtifact[] = [];
  for (const target of targets) {
    const binaryFile = `asr-${target}`;
    const archiveFile = `agent-session-router-${target}.tar.gz`;
    const required = [binaryFile, archiveFile, `${archiveFile}.sha256`];
    if (!required.some((name) => names.has(name)) && !names.has(`${binaryFile}.sha256`)) continue;
    if (!required.every((name) => names.has(name))) {
      fail(`${target}: stage the raw binary, archive, and archive .sha256 together.`);
    }
    const binary = await digestFile(join(dir, binaryFile), binaryFile);
    const archive = await digestFile(join(dir, archiveFile), archiveFile);
    await verifyChecksum(dir, archiveFile, archive);
    if (names.has(`${binaryFile}.sha256`)) await verifyChecksum(dir, binaryFile, binary);
    await verifyArchivedBinary(join(dir, archiveFile), binary);
    artifacts.push({
      target,
      binaryFile,
      binarySha256: binary.sha256,
      archiveFile,
      archiveSha256: archive.sha256,
      binaryBytes: binary.bytes,
      archiveBytes: archive.bytes,
    });
  }
  if (artifacts.length === 0) fail("bundle directory: stage at least one supported target before generating a manifest.");

  const destination = join(dir, manifestName);
  if (names.has(manifestName)) await regularFile(destination, manifestName, 128 * 1024);
  const temporary = join(dir, `.bootstrap-manifest-${crypto.randomUUID()}.tmp`);
  const file = await open(temporary, "wx", 0o600);
  try {
    await file.writeFile(`${JSON.stringify({ version: 1, asrVersion, artifacts }, null, 2)}\n`, "utf8");
    await file.sync();
    await file.close();
    await rename(temporary, destination);
  } catch (error) {
    await file.close().catch(() => {});
    await unlink(temporary).catch(() => {});
    throw error;
  }
  console.log(`Created ${manifestName} for ${artifacts.length} target(s).`);
}

try {
  await main();
} catch (error) {
  // Filesystem and child-process errors can contain user paths; never print them.
  console.error(`bootstrap manifest: ${error instanceof ManifestError ? error.message : "unable to read or write bundle inputs; check directory permissions and restage complete artifacts."}`);
  process.exitCode = 1;
}
