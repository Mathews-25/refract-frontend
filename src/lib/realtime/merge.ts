/**
 * Realtime merge utilities.
 *
 * Event classes are either *replaceable* (only the newest value matters, e.g.
 * oracle readings) or *append* (every event must be observed, e.g. logs).
 * Replaceable classes are coalesced within a bounded window so a busy stream
 * renders at most once per window while always flushing the latest value.
 */

export type MergeStrategy = 'replace' | 'append';

/**
 * Event classes that are inherently replaceable. Only the newest value of a
 * replaceable class is meaningful, so high-frequency bursts can be coalesced.
 */
export const REPLACEABLE_EVENT_CLASSES: ReadonlySet<string> = new Set([
  'oracle',
  'oracle-reading',
  'oracleReading',
  'price',
  'ticker',
]);

export function isReplaceableEventClass(eventClass: string): boolean {
  return REPLACEABLE_EVENT_CLASSES.has(eventClass);
}

export function mergeStrategyFor(eventClass: string): MergeStrategy {
  return isReplaceableEventClass(eventClass) ? 'replace' : 'append';
}

/**
 * Merge a batch of events for a single topic. Replaceable classes keep only the
 * latest event; append classes keep every event in arrival order.
 */
export function mergeEvents<T>(
  eventClass: string,
  events: readonly T[],
): T[] {
  if (events.length === 0) return [];
  if (mergeStrategyFor(eventClass) === 'replace') {
    return [events[events.length - 1]];
  }
  return [...events];
}

/**
 * Coalesces replaceable updates on a frame/timer budget.
 *
 * - `push` buffers the latest value per key.
 * - `flush` drains the buffer and invokes the sink once with the latest value
 *   for each key (trailing-edge flush, so the final value is never dropped).
 * - `schedule` requests a flush on the next animation frame (or a short timer
 *   fallback) unless coalescing is disabled.
 *
 * Coalescing is disabled under reduced motion and in automated tests, where
 * flushing must be deterministic.
 */
export interface CoalescerOptions {
  /** Called once per flush with the latest buffered value per key. */
  onFlush: (updates: Map<string, unknown>) => void;
  /** Override the coalescing window in ms. Defaults to one frame. */
  windowMs?: number;
  /** Force-disable coalescing (e.g. reduced motion or tests). */
  disabled?: boolean;
}

const DEFAULT_WINDOW_MS = 16;

export function prefersReducedMotion(): boolean {
  if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
    return false;
  }
  return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

export function isTestEnvironment(): boolean {
  return (
    typeof process !== 'undefined' &&
    (process.env?.NODE_ENV === 'test' || process.env?.VITEST === 'true')
  );
}

export function shouldCoalesce(disabled?: boolean): boolean {
  if (disabled) return false;
  if (isTestEnvironment()) return false;
  if (prefersReducedMotion()) return false;
  return true;
}

export interface Coalescer {
  push: (key: string, value: unknown) => void;
  flush: () => void;
  schedule: () => void;
  dispose: () => void;
}

export function createCoalescer(options: CoalescerOptions): Coalescer {
  const { onFlush, windowMs = DEFAULT_WINDOW_MS } = options;
  const buffer = new Map<string, unknown>();
  let handle: ReturnType<typeof setTimeout> | null = null;
  let frame: number | null = null;
  let disposed = false;

  const clearScheduled = () => {
    if (frame !== null && typeof cancelAnimationFrame === 'function') {
      cancelAnimationFrame(frame);
    }
    frame = null;
    if (handle !== null) {
      clearTimeout(handle);
    }
    handle = null;
  };

  const flush = () => {
    clearScheduled();
    if (buffer.size === 0) return;
    const updates = new Map(buffer);
    buffer.clear();
    onFlush(updates);
  };

  const schedule = () => {
    if (disposed || handle !== null || frame !== null) return;
    if (typeof requestAnimationFrame === 'function') {
      frame = requestAnimationFrame(() => {
        frame = null;
        flush();
      });
    } else {
      handle = setTimeout(() => {
        handle = null;
        flush();
      }, windowMs);
    }
  };

  const push = (key: string, value: unknown) => {
    if (disposed) return;
    buffer.set(key, value);
    if (shouldCoalesce(options.disabled)) {
      schedule();
    } else {
      // Deterministic path: flush immediately so tests and reduced-motion
      // users observe every update without a frame delay.
      flush();
    }
  };

  const dispose = () => {
    disposed = true;
    clearScheduled();
    buffer.clear();
  };

  return { push, flush, schedule, dispose };
}
