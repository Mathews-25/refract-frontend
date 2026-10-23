import type { RealtimeClient } from './client';
import {
  DEFAULT_REALTIME_LIMITS,
  InboundRateLimiter,
  RealtimeLimits,
  decideAdmission,
} from './limits';

/**
 * Per-topic subscription registry with reference counting and lazy teardown.
 *
 * The registry is the single source of truth for which logical topics are
 * active. Consumers call `subscribe(topic, handler)` and receive an
 * unsubscribe function; the underlying transport subscription is opened on the
 * first consumer and torn down only when the last consumer unsubscribes.
 *
 * Teardown ordering on route change: React unmounts the outgoing tree before
 * mounting the incoming one, so a topic shared across routes briefly drops to
 * zero refs. To avoid connection thrash we coalesce teardown within a tick —
 * if a new subscriber arrives before the microtask flush, the pending teardown
 * is cancelled and the existing transport subscription is reused.
 */

export type TopicHandler<T = unknown> = (event: T) => void;

export interface RegistryEntry {
  topic: string;
  count: number;
}

export interface RegistryChangeEvent {
  topic: string;
  count: number;
  active: boolean;
}

type ChangeListener = (event: RegistryChangeEvent) => void;

interface TopicState {
  handlers: Set<TopicHandler>;
  unsubscribeTransport: (() => void) | null;
  pendingTeardown: boolean;
}

/**
 * Minimal transport surface the registry depends on. Kept structural so tests
 * can inject a fake transport without importing the real client.
 */
export interface RegistryTransport {
  subscribe(topic: string, onEvent: (event: unknown) => void): () => void;
}

export interface RealtimeRegistryOptions {
  limits?: RealtimeLimits;
  /** Observability sink for rate-limit breaches and rejections. */
  report?: (event: RealtimeObservabilityEvent) => void;
}

export interface RealtimeObservabilityEvent {
  type: 'subscription-rejected' | 'rate-limit-breach';
  topic?: string;
  reason?: string;
  breaches?: number;
}

export class SubscriptionRegistry {
  private readonly transport: RegistryTransport;
  private readonly topics = new Map<string, TopicState>();
  private readonly listeners = new Set<ChangeListener>();
  private readonly limits: RealtimeLimits;
  private readonly report?: (event: RealtimeObservabilityEvent) => void;
  private readonly rateLimiter: InboundRateLimiter;

  constructor(transport: RegistryTransport, options: RealtimeRegistryOptions = {}) {
    this.transport = transport;
    this.limits = options.limits ?? DEFAULT_REALTIME_LIMITS;
    this.report = options.report;
    this.rateLimiter = new InboundRateLimiter(this.limits);
  }

  /**
   * Subscribe a handler to a topic. Returns an idempotent unsubscribe function.
   * The first subscriber opens the transport subscription; the last
   * unsubscribe schedules a coalesced teardown.
   */
  subscribe<T = unknown>(topic: string, handler: TopicHandler<T>): () => void {
    let state = this.topics.get(topic);
    if (!state) {
      const decision = decideAdmission([...this.topics.keys()], topic, this.limits);
      if (!decision.admitted) {
        this.report?.({ type: 'subscription-rejected', topic, reason: decision.reason });
        // eslint-disable-next-line no-console
        console.warn(`[realtime] subscription refused for "${topic}": ${decision.reason}`);
        return () => {};
      }
      if (decision.evict) {
        this.topics.delete(decision.evict);
      }
      state = { handlers: new Set(), unsubscribeTransport: null, pendingTeardown: false };
      this.topics.set(topic, state);
    }

    // Cancel any pending teardown — a new consumer arrived within the tick.
    state.pendingTeardown = false;

    const wrapped = handler as TopicHandler;
    state.handlers.add(wrapped);

    if (!state.unsubscribeTransport) {
      state.unsubscribeTransport = this.transport.subscribe(topic, (event) => {
        if (this.rateLimiter.record()) {
          this.report?.({ type: 'rate-limit-breach', topic, breaches: this.rateLimiter.getBreachCount() });
          return;
        }
        const current = this.topics.get(topic);
        if (!current) return;
        for (const h of current.handlers) h(event);
      });
    }

    this.emit({ topic, count: state.handlers.size, active: true });

    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.release(topic, wrapped);
    };
  }

  private release(topic: string, handler: TopicHandler): void {
    const state = this.topics.get(topic);
    if (!state) return;
    state.handlers.delete(handler);

    if (state.handlers.size > 0) {
      this.emit({ topic, count: state.handlers.size, active: true });
      return;
    }

    // Last consumer gone: coalesce teardown to the next microtask so a
    // StrictMode remount or rapid route swap reuses the subscription.
    state.pendingTeardown = true;
    this.emit({ topic, count: 0, active: false });
    queueMicrotask(() => {
      const current = this.topics.get(topic);
      if (!current || !current.pendingTeardown) return;
      current.pendingTeardown = false;
      current.unsubscribeTransport?.();
      current.unsubscribeTransport = null;
      this.topics.delete(topic);
    });
  }

  /** Re-establish all active topics after a reconnect. */
  resubscribeAll(): void {
    for (const [topic, state] of this.topics) {
      if (state.handlers.size === 0) continue;
      state.unsubscribeTransport?.();
      state.unsubscribeTransport = this.transport.subscribe(topic, (event) => {
        if (this.rateLimiter.record()) {
          this.report?.({ type: 'rate-limit-breach', topic, breaches: this.rateLimiter.getBreachCount() });
          return;
        }
        const current = this.topics.get(topic);
        if (!current) return;
        for (const h of current.handlers) h(event);
      });
    }
  }

  /** Inspectable snapshot of active topics and their consumer counts. */
  getActiveTopics(): RegistryEntry[] {
    const entries: RegistryEntry[] = [];
    for (const [topic, state] of this.topics) {
      if (state.handlers.size > 0) entries.push({ topic, count: state.handlers.size });
    }
    return entries;
  }

  /** Observe registry-level changes (for the dev inspector / health modules). */
  onChange(listener: ChangeListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  getRateBreachCount(): number {
    return this.rateLimiter.getBreachCount();
  }

  private emit(event: RegistryChangeEvent): void {
    for (const listener of this.listeners) listener(event);
  }
}

let defaultRegistry: SubscriptionRegistry | null = null;

/** Lazily bind the shared registry to the typed realtime client. */
export function getRegistry(client: RealtimeClient): SubscriptionRegistry {
  if (!defaultRegistry) defaultRegistry = new SubscriptionRegistry(client);
  return defaultRegistry;
}
