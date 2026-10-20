import { API_BASE_URL } from "@/lib/api/client";

type Handler = (payload: unknown) => void;

const MAX_BACKOFF_MS = 30_000;
const BASE_BACKOFF_MS = 1_000;

/**
 * Heartbeat tuning. Defaults are intentionally cheap on mobile/battery:
 * a ping every 25s with a 10s pong deadline. The dev mock server can
 * disable heartbeats entirely (see `heartbeatEnabled`) when testing
 * purely offline flows.
 */
export const HEARTBEAT_INTERVAL_MS = 25_000;
export const HEARTBEAT_PONG_DEADLINE_MS = 10_000;

/** Snapshot of heartbeat state for health/observability consumers. */
export interface HeartbeatState {
  lastPongAt: number | null;
  consecutiveMisses: number;
  healthy: boolean;
}

/**
 * A small reconnecting WebSocket wrapper exposing a subscribe-by-topic API.
 * The backend contract this assumes (documented, not yet implemented
 * upstream): connect to `${wsUrl}`, receive JSON messages shaped
 * `{ topic: string, payload: unknown }`, one topic per push-based update
 * type (e.g. "oracle_update"). Callers must treat this connection as
 * best-effort — always keep an independent REST fetch as the source of
 * truth for initial state.
 *
 * An application-level heartbeat sends `{ type: "ping" }` frames on an
 * interval and expects `{ type: "pong" }` back within a deadline. Missed
 * deadlines mark the connection unhealthy and close it so the existing
 * reconnect logic takes over, detecting half-open sockets.
 */
class RealtimeClient {
  private socket: WebSocket | null = null;
  private handlers = new Map<string, Set<Handler>>();
  private attempt = 0;
  private closedByUser = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private pongDeadlineTimer: ReturnType<typeof setTimeout> | null = null;
  private lastPongAt: number | null = null;
  private consecutiveMisses = 0;
  private healthy = true;

  connect(): void {
    if (typeof WebSocket === "undefined" || this.socket) return;
    this.closedByUser = false;
    this.open();
  }

  private open(): void {
    const wsUrl = API_BASE_URL.replace(/^http/, "ws").replace(/\/api\/v1$/, "/ws");
    try {
      this.socket = new WebSocket(wsUrl);
    } catch {
      this.scheduleReconnect();
      return;
    }

    this.socket.addEventListener("open", () => {
      this.attempt = 0;
      this.healthy = true;
      this.consecutiveMisses = 0;
      this.startHeartbeat();
    });
    this.socket.addEventListener("message", (event) => {
      try {
        const data = JSON.parse(event.data as string) as {
          topic?: string;
          payload?: unknown;
          type?: string;
        };
        if (data.type === "pong") {
          this.handlePong();
          return;
        }
        if (data.type === "ping") {
          this.send({ type: "pong" });
          return;
        }
        if (typeof data.topic === "string") {
          this.handlers.get(data.topic)?.forEach((handler) => handler(data.payload));
        }
      } catch {
        // Malformed message — ignore, don't crash the connection.
      }
    });
    this.socket.addEventListener("close", () => {
      this.stopHeartbeat();
      this.socket = null;
      if (!this.closedByUser) this.scheduleReconnect();
    });
    this.socket.addEventListener("error", () => {
      this.socket?.close();
    });
  }

  private send(message: unknown): void {
    if (this.socket?.readyState !== WebSocket.OPEN) return;
    try {
      this.socket.send(JSON.stringify(message));
    } catch {
      // Send failures surface via the close/error handlers.
    }
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeatTimer = setInterval(() => {
      this.send({ type: "ping" });
      if (this.pongDeadlineTimer) clearTimeout(this.pongDeadlineTimer);
      this.pongDeadlineTimer = setTimeout(() => {
        this.pongDeadlineTimer = null;
        this.handleMissedPong();
      }, HEARTBEAT_PONG_DEADLINE_MS);
    }, HEARTBEAT_INTERVAL_MS);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    if (this.pongDeadlineTimer) clearTimeout(this.pongDeadlineTimer);
    this.heartbeatTimer = null;
    this.pongDeadlineTimer = null;
  }

  private handlePong(): void {
    this.lastPongAt = Date.now();
    this.consecutiveMisses = 0;
    this.healthy = true;
    if (this.pongDeadlineTimer) clearTimeout(this.pongDeadlineTimer);
    this.pongDeadlineTimer = null;
  }

  private handleMissedPong(): void {
    this.consecutiveMisses += 1;
    this.healthy = false;
    // Close the half-open socket; the close handler triggers reconnect.
    this.socket?.close();
  }

  getHeartbeatState(): HeartbeatState {
    return {
      lastPongAt: this.lastPongAt,
      consecutiveMisses: this.consecutiveMisses,
      healthy: this.healthy,
    };
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;
    const delay = Math.min(BASE_BACKOFF_MS * 2 ** this.attempt, MAX_BACKOFF_MS);
    this.attempt += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (!this.closedByUser) this.open();
    }, delay);
  }

  subscribe(topic: string, handler: Handler): () => void {
    if (!this.handlers.has(topic)) this.handlers.set(topic, new Set());
    this.handlers.get(topic)!.add(handler);
    this.connect();

    return () => {
      this.handlers.get(topic)?.delete(handler);
    };
  }

  disconnect(): void {
    this.closedByUser = true;
    this.stopHeartbeat();
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.socket?.close();
    this.socket = null;
  }
}

export const realtimeClient = new RealtimeClient();
