import { useEffect, useRef, useState } from 'react';
import { mergeRealtimeEvent, type RealtimeEvent, type RealtimeState } from '../lib/realtime/merge';

export interface UseRealtimeTopicOptions<T> {
  /**
   * Whether updates for this topic are replaceable (only the latest value matters).
   * Replaceable topics are coalesced to at most one render per animation frame.
   */
  replaceable?: boolean;
  /**
   * Optional selector to derive the rendered value from the merged state.
   * Defaults to the merged state itself.
   */
  select?: (state: RealtimeState<T>) => RealtimeState<T>;
}

const isTestEnv = (): boolean => {
  if (typeof process !== 'undefined' && process.env && process.env.NODE_ENV === 'test') {
    return true;
  }
  return typeof (globalThis as { __REALTIME_DISABLE_COALESCE__?: boolean }).__REALTIME_DISABLE_COALESCE__ === 'boolean'
    ? Boolean((globalThis as { __REALTIME_DISABLE_COALESCE__?: boolean }).__REALTIME_DISABLE_COALESCE__)
    : false;
};

const prefersReducedMotion = (): boolean => {
  if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
    return false;
  }
  try {
    return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  } catch {
    return false;
  }
};

const scheduleFrame = (cb: () => void): (() => void) => {
  if (typeof window !== 'undefined' && typeof window.requestAnimationFrame === 'function') {
    const id = window.requestAnimationFrame(cb);
    return () => window.cancelAnimationFrame(id);
  }
  const id = setTimeout(cb, 16);
  return () => clearTimeout(id);
};

/**
 * Subscribe to a realtime topic and expose the merged state.
 *
 * Replaceable topics (e.g. oracle readings) are buffered and flushed at most
 * once per animation frame so high-frequency streams cannot cause render
 * storms. A trailing-edge flush guarantees the latest value is always rendered.
 * Coalescing is disabled under reduced motion and in automated tests so
 * flushing is deterministic.
 */
export function useRealtimeTopic<T>(
  topic: string,
  initial: RealtimeState<T>,
  options: UseRealtimeTopicOptions<T> = {},
): RealtimeState<T> {
  const { replaceable = false, select } = options;
  const [state, setState] = useState<RealtimeState<T>>(initial);

  const stateRef = useRef<RealtimeState<T>>(initial);
  const pendingRef = useRef<RealtimeState<T> | null>(null);
  const cancelRef = useRef<(() => void) | null>(null);
  const selectRef = useRef(select);
  selectRef.current = select;

  const coalesce = replaceable && !isTestEnv() && !prefersReducedMotion();
  const coalesceRef = useRef(coalesce);
  coalesceRef.current = coalesce;

  const flush = (): void => {
    cancelRef.current = null;
    const next = pendingRef.current;
    if (next === null) {
      return;
    }
    pendingRef.current = null;
    stateRef.current = next;
    setState(next);
  };

  const apply = (next: RealtimeState<T>): void => {
    if (!coalesceRef.current) {
      stateRef.current = next;
      setState(next);
      return;
    }
    pendingRef.current = next;
    if (cancelRef.current === null) {
      cancelRef.current = scheduleFrame(flush);
    }
  };

  useEffect(() => {
    const handler = (event: RealtimeEvent<T>): void => {
      const merged = mergeRealtimeEvent(stateRef.current, event);
      const selected = selectRef.current ? selectRef.current(merged) : merged;
      apply(selected);
    };

    const unsubscribe = subscribeToTopic(topic, handler);

    return () => {
      unsubscribe();
      if (cancelRef.current !== null) {
        cancelRef.current();
        cancelRef.current = null;
      }
      // Trailing-edge flush on unmount so the latest value is never stranded.
      flush();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [topic]);

  return state;
}

/**
 * Deterministic flush hook for tests: forces any buffered replaceable update
 * to render immediately.
 */
export function useRealtimeFlush(): () => void {
  return () => {
    if (typeof (globalThis as { __REALTIME_FLUSH__?: () => void }).__REALTIME_FLUSH__ === 'function') {
      (globalThis as { __REALTIME_FLUSH__?: () => void }).__REALTIME_FLUSH__!();
    }
  };
}

// Placeholder subscription seam; the real transport wires this up elsewhere.
declare function subscribeToTopic<T>(
  topic: string,
  handler: (event: RealtimeEvent<T>) => void,
): () => void;
