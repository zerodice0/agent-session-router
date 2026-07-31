import type { AgentDescriptor, AgentStatus, RegistrationRole, ServerMessage } from "./protocol";

export interface AgentConnection {
  send(message: string): number | void;
  close?(code?: number, reason?: string): void;
}

export interface RegisteredAgent {
  descriptor: AgentDescriptor;
  connection: AgentConnection;
  delegationToken?: string;
}

export interface RegisteredConnection {
  descriptor: AgentDescriptor;
  connection: AgentConnection;
  role: RegistrationRole;
}

export interface UnregisteredConnection extends RegisteredConnection {
  delegates: AgentConnection[];
}

export class AgentRegistry {
  readonly #agents = new Map<string, RegisteredAgent>();
  readonly #connections = new Map<AgentConnection, RegisteredConnection>();
  readonly #delegates = new Map<string, Set<AgentConnection>>();

  register(
    descriptor: AgentDescriptor,
    connection: AgentConnection,
    delegationToken?: string,
  ): boolean {
    if (this.#agents.has(descriptor.agentId) || this.#connections.has(connection)) return false;
    const presence: AgentDescriptor = { ...descriptor, status: "idle" };
    const registered = { descriptor: presence, connection, delegationToken };
    this.#agents.set(descriptor.agentId, registered);
    this.#connections.set(connection, { descriptor: presence, connection, role: "agent" });
    return true;
  }

  registerDelegate(agentId: string, delegationToken: string, connection: AgentConnection): boolean {
    if (this.#connections.has(connection)) return false;
    const registered = this.#agents.get(agentId);
    if (!registered?.delegationToken || registered.delegationToken !== delegationToken) return false;

    this.#connections.set(connection, {
      descriptor: registered.descriptor,
      connection,
      role: "delegate",
    });
    const delegates = this.#delegates.get(agentId) ?? new Set<AgentConnection>();
    delegates.add(connection);
    this.#delegates.set(agentId, delegates);
    return true;
  }

  unregister(connection: AgentConnection): UnregisteredConnection | null {
    const registered = this.#connections.get(connection);
    if (!registered) return null;
    this.#connections.delete(connection);

    if (registered.role === "delegate") {
      const delegates = this.#delegates.get(registered.descriptor.agentId);
      delegates?.delete(connection);
      if (delegates?.size === 0) this.#delegates.delete(registered.descriptor.agentId);
      return { ...registered, delegates: [] };
    }

    this.#agents.delete(registered.descriptor.agentId);
    const delegates = [...(this.#delegates.get(registered.descriptor.agentId) ?? [])];
    this.#delegates.delete(registered.descriptor.agentId);
    for (const delegate of delegates) this.#connections.delete(delegate);
    return { ...registered, delegates };
  }

  get(agentId: string): RegisteredAgent | undefined {
    return this.#agents.get(agentId);
  }

  getByConnection(connection: AgentConnection): RegisteredConnection | undefined {
    return this.#connections.get(connection);
  }

  setStatus(agentId: string, status: AgentStatus): void {
    const registered = this.#agents.get(agentId);
    if (registered) registered.descriptor.status = status;
  }

  list(): AgentDescriptor[] {
    return [...this.#agents.values()]
      .map(({ descriptor }) => ({ ...descriptor }))
      .sort((left, right) => left.agentId.localeCompare(right.agentId));
  }
}

export function sendMessage(connection: AgentConnection, message: ServerMessage): void {
  connection.send(JSON.stringify(message));
}
