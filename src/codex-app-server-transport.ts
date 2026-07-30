const DEFAULT_MAX_LINE_CHARS = 1_048_576;

export type CodexMessageListener = (message: unknown) => void;
export type CodexCloseListener = () => void;

/** Newline-delimited JSON-RPC transport used by the Codex App Server adapter. */
export interface CodexAppServerTransport {
  send(message: unknown): void | Promise<void>;
  onMessage(listener: CodexMessageListener): () => void;
  onClose(listener: CodexCloseListener): () => void;
  close(): void;
}

export interface CodexStdioTransportOptions {
  command?: readonly string[];
  cwd?: string;
  env?: Record<string, string | undefined>;
  maxLineChars?: number;
}

interface StdioProcess {
  stdin: {
    write(data: string): number;
    flush(): number | Promise<number>;
    end(): void;
  };
  stdout: ReadableStream<Uint8Array>;
  exited: Promise<number>;
  kill(): void;
}

/**
 * Owns one `codex app-server --listen stdio://` child process. Stderr is
 * discarded deliberately so prompts, responses, credentials, and local paths
 * cannot be copied into gateway logs.
 */
export class CodexStdioTransport implements CodexAppServerTransport {
  readonly #process: StdioProcess;
  readonly #maxLineChars: number;
  readonly #messageListeners = new Set<CodexMessageListener>();
  readonly #closeListeners = new Set<CodexCloseListener>();
  #closed = false;

  private constructor(process: StdioProcess, maxLineChars: number) {
    this.#process = process;
    this.#maxLineChars = maxLineChars;
    void this.#readMessages();
    void process.exited.then(() => this.#finishClose());
  }

  static spawn(options: CodexStdioTransportOptions = {}): CodexStdioTransport {
    const command = [...(options.command ?? ["codex", "app-server", "--listen", "stdio://"])];
    if (command.length === 0) throw new TypeError("Codex command must not be empty");

    const childProcess = Bun.spawn({
      cmd: command,
      stdin: "pipe",
      stdout: "pipe",
      stderr: "ignore",
      ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
      ...(options.env === undefined
        ? {}
        : { env: { ...process.env, ...options.env } }),
    }) as unknown as StdioProcess;

    return new CodexStdioTransport(
      childProcess,
      Math.max(1, Math.floor(options.maxLineChars ?? DEFAULT_MAX_LINE_CHARS)),
    );
  }

  async send(message: unknown): Promise<void> {
    if (this.#closed) throw new Error("Codex transport is closed");
    const line = JSON.stringify(message);
    if (typeof line !== "string") throw new TypeError("Codex protocol message must be JSON");
    if (line.length > this.#maxLineChars) throw new Error("Codex protocol message is too large");
    this.#process.stdin.write(`${line}\n`);
    await this.#process.stdin.flush();
  }

  onMessage(listener: CodexMessageListener): () => void {
    this.#messageListeners.add(listener);
    return () => this.#messageListeners.delete(listener);
  }

  onClose(listener: CodexCloseListener): () => void {
    if (this.#closed) {
      queueMicrotask(listener);
      return () => {};
    }
    this.#closeListeners.add(listener);
    return () => this.#closeListeners.delete(listener);
  }

  close(): void {
    if (this.#closed) return;
    this.#process.stdin.end();
    this.#process.kill();
    this.#finishClose();
  }

  async #readMessages(): Promise<void> {
    const reader = this.#process.stdout.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    try {
      while (!this.#closed) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        if (buffer.length > this.#maxLineChars && !buffer.includes("\n")) {
          throw new Error("Codex protocol message is too large");
        }

        let newlineIndex = buffer.indexOf("\n");
        while (newlineIndex !== -1) {
          const line = buffer.slice(0, newlineIndex).trimEnd();
          buffer = buffer.slice(newlineIndex + 1);
          if (line.length > 0) this.#emitParsedLine(line);
          newlineIndex = buffer.indexOf("\n");
        }
      }

      buffer += decoder.decode();
      const finalLine = buffer.trim();
      if (finalLine.length > 0) this.#emitParsedLine(finalLine);
    } catch {
      this.#process.kill();
    } finally {
      reader.releaseLock();
      this.#finishClose();
    }
  }

  #emitParsedLine(line: string): void {
    if (line.length > this.#maxLineChars) throw new Error("Codex protocol message is too large");
    const message = JSON.parse(line) as unknown;
    for (const listener of [...this.#messageListeners]) listener(message);
  }

  #finishClose(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const listener of [...this.#closeListeners]) listener();
    this.#messageListeners.clear();
    this.#closeListeners.clear();
  }
}
