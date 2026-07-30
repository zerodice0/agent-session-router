import type { AgentDescriptor, ServerMessage } from "./protocol";

export interface AgentConnection {
  send(message: string): number | void;
}

export interface RegisteredAgent {
  descriptor: AgentDescriptor;
  connection: AgentConnection;
}

export class AgentRegistry {
  readonly #agents = new Map<string, RegisteredAgent>();

  register(descriptor: AgentDescriptor, connection: AgentConnection): boolean {
    if (this.#agents.has(descriptor.agentId)) return false;
    this.#agents.set(descriptor.agentId, { descriptor, connection });
    return true;
  }

  unregister(connection: AgentConnection): RegisteredAgent | null {
    for (const [agentId, registered] of this.#agents) {
      if (registered.connection === connection) {
        this.#agents.delete(agentId);
        return registered;
      }
    }

    return null;
  }

  get(agentId: string): RegisteredAgent | undefined {
    return this.#agents.get(agentId);
  }

  getByConnection(connection: AgentConnection): RegisteredAgent | undefined {
    for (const registered of this.#agents.values()) {
      if (registered.connection === connection) return registered;
    }

    return undefined;
  }

  list(): AgentDescriptor[] {
    return [...this.#agents.values()]
      .map(({ descriptor }) => descriptor)
      .sort((left, right) => left.agentId.localeCompare(right.agentId));
  }
}

export function sendMessage(connection: AgentConnection, message: ServerMessage): void {
  connection.send(JSON.stringify(message));
}

