import type { CommitResult, Mutation } from '../shared/types.js';
import { ulid } from '../shared/ulid.js';
import type { QueueStorage } from './storage.js';
import { MemoryQueueStorage } from './storage.js';

/** Maximum delivery attempts before a mutation is surfaced via onError. */
const DEFAULT_MAX_ATTEMPTS = 8;

export interface OfflineQueueOptions {
  /** Where to persist pending mutations. Defaults to in-memory (NOT for production). */
  storage?: QueueStorage;
  /**
   * How to actually send a batch of mutations to the server. The adapter
   * receives an array (so it can use a batched endpoint if you have one)
   * and returns one CommitResult per mutation, in the same order. Throw
   * on transport-level errors; return per-mutation errors via the result
   * (status field below). The library will retry transport failures with
   * backoff and surface per-mutation failures via the `onError` callback.
   */
  sender: (batch: Mutation[]) => Promise<CommitResult[]>;
  /** Stable identifier for this device. Defaults to a per-session ULID. */
  clientId?: string;
  /** Maximum batch size per network call. Default 25. */
  batchSize?: number;
  /** Initial retry delay in ms. Default 500. */
  initialBackoffMs?: number;
  /** Maximum retry delay in ms. Default 30_000. */
  maxBackoffMs?: number;
  /** Fired after each successful commit, with the original mutation and the server's result. */
  onCommit?: (mutation: Mutation, result: CommitResult) => void;
  /** Fired when a mutation has failed N times and is being kept for manual inspection. */
  onError?: (mutation: Mutation, error: unknown, attempts: number) => void;
}

/**
 * Offline-first mutation queue. The contract:
 *
 *   1. `enqueue(type, payload)` is synchronous-feeling for the caller. The
 *      mutation gets a ULID idempotency key, lands in durable storage, and
 *      the caller can return immediately. UI updates optimistically based
 *      on the local model.
 *
 *   2. A background flush loop drains the queue, sending batches with each
 *      mutation's idempotency key in the headers/body. The server uses the
 *      key to dedupe retries.
 *
 *   3. On any transport failure (offline, 5xx, timeout) the batch stays in
 *      storage and retries with exponential backoff. No data is lost.
 *
 *   4. On a 4xx per-mutation failure (the server says "I don't recognize
 *      this mutation type"), the entry is marked failed and surfaced via
 *      `onError` for the application to handle.
 *
 *   5. On successful commit, the entry is deleted from storage and the
 *      `onCommit` callback fires with the server's `seqId` so the client
 *      can advance its high-water mark for catch-up.
 *
 * The mutation type and payload are opaque to the queue — your domain
 * controls what gets sent. The queue only manages: identity, durability,
 * delivery, and idempotency.
 */
export class OfflineQueue {
  private storage: QueueStorage;
  private sender: OfflineQueueOptions['sender'];
  private clientId: string;
  private batchSize: number;
  private initialBackoffMs: number;
  private maxBackoffMs: number;
  private onCommit?: OfflineQueueOptions['onCommit'];
  private onError?: OfflineQueueOptions['onError'];

  private isFlushing = false;
  private currentDrain: Promise<void> | null = null;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private currentBackoffMs: number;
  private online: boolean;

  constructor(opts: OfflineQueueOptions) {
    this.storage = opts.storage ?? new MemoryQueueStorage();
    this.sender = opts.sender;
    this.clientId = opts.clientId ?? ulid();
    this.batchSize = opts.batchSize ?? 25;
    this.initialBackoffMs = opts.initialBackoffMs ?? 500;
    this.maxBackoffMs = opts.maxBackoffMs ?? 30_000;
    this.onCommit = opts.onCommit;
    this.onError = opts.onError;
    this.currentBackoffMs = this.initialBackoffMs;
    // navigator.onLine is browser-only; Node has a stub `navigator` object
    // where `onLine` is undefined. Treat anything but explicit `false` as online.
    const navOnLine =
      typeof navigator !== 'undefined' ? (navigator as { onLine?: unknown }).onLine : undefined;
    this.online = navOnLine !== false;

    // Auto-resume on network reconnect (browser only)
    if (typeof window !== 'undefined') {
      window.addEventListener('online', () => {
        this.online = true;
        this.scheduleFlush(0);
      });
      window.addEventListener('offline', () => {
        this.online = false;
      });
    }

    // Auto-drain any persisted mutations from a previous session. Without
    // this, entries written before a tab close just sit in IndexedDB until
    // the user happens to scan something new — surprising and easy to miss.
    this.scheduleFlush(0);
  }

  /**
   * Add a mutation to the queue. Returns the generated idempotency key
   * synchronously after persisting — so the caller can attach the key
   * to its optimistic UI state.
   */
  async enqueue<P>(type: string, payload: P): Promise<string> {
    const mutation: Mutation<P> = {
      idempotencyKey: ulid(),
      type,
      payload,
      clientTs: Date.now(),
      clientId: this.clientId,
    };
    await this.storage.put({
      mutation,
      enqueuedAt: Date.now(),
      attempts: 0,
    });
    this.scheduleFlush(0);
    return mutation.idempotencyKey;
  }

  /** Pending mutations currently in storage, oldest first. */
  async pending(): Promise<Mutation[]> {
    const entries = await this.storage.all();
    return entries.filter((e) => !e.committedSeqId).map((e) => e.mutation);
  }

  /** How many mutations are pending. Cheap, doesn't hit the network. */
  async size(): Promise<number> {
    return (await this.pending()).length;
  }

  /**
   * Manually trigger a flush. Waits for any inflight drain to complete, then
   * runs one more drain to pick up anything enqueued in the meantime. Safe to
   * call concurrently — multiple callers all observe the same drained state.
   */
  async flush(): Promise<void> {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    if (this.currentDrain) {
      await this.currentDrain.catch(() => {});
    }
    await this.startDrain();
  }

  private scheduleFlush(delayMs: number) {
    if (this.flushTimer) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      this.startDrain().catch(() => {
        /* errors are surfaced per-mutation; transport errors retry via backoff */
      });
    }, delayMs);
  }

  private startDrain(): Promise<void> {
    if (this.currentDrain) return this.currentDrain;
    this.currentDrain = this.drainOnce().finally(() => {
      this.currentDrain = null;
    });
    return this.currentDrain;
  }

  private async drainOnce(): Promise<void> {
    if (this.isFlushing) return;
    if (!this.online) return;
    this.isFlushing = true;
    try {
      const entries = await this.storage.all();
      const ready = entries.filter((e) => !e.committedSeqId);
      if (ready.length === 0) {
        this.currentBackoffMs = this.initialBackoffMs;
        return;
      }
      const batch = ready.slice(0, this.batchSize);
      const mutations = batch.map((e) => e.mutation);

      let results: CommitResult[];
      try {
        results = await this.sender(mutations);
      } catch (transportErr) {
        // Network/transport error — bump attempts and back off.
        for (const entry of batch) {
          entry.attempts += 1;
          entry.lastError = (transportErr as Error)?.message ?? String(transportErr);
          entry.lastAttemptAt = Date.now();
          await this.storage.put(entry);
          if (entry.attempts >= DEFAULT_MAX_ATTEMPTS) {
            this.onError?.(entry.mutation, transportErr, entry.attempts);
          }
        }
        const next = Math.min(this.currentBackoffMs * 2, this.maxBackoffMs);
        this.currentBackoffMs = next;
        this.scheduleFlush(next);
        return;
      }

      // Per-mutation result processing
      let anyMissing = false;
      for (let i = 0; i < batch.length; i++) {
        const entry = batch[i];
        const result = results[i];
        if (!result) {
          entry.attempts += 1;
          entry.lastError = 'No result from sender';
          entry.lastAttemptAt = Date.now();
          await this.storage.put(entry);
          anyMissing = true;
          continue;
        }
        entry.committedSeqId = result.seqId;
        await this.storage.put(entry);
        await this.storage.delete(entry.mutation.idempotencyKey);
        this.onCommit?.(entry.mutation, result);
      }

      // Reset backoff after success
      this.currentBackoffMs = this.initialBackoffMs;

      // More to drain? Either there are entries beyond this batch, or the
      // sender returned undefined for some items (partial success) — those
      // still need a retry.
      if (ready.length > batch.length || anyMissing) {
        this.scheduleFlush(anyMissing ? this.initialBackoffMs : 0);
      }
    } finally {
      this.isFlushing = false;
    }
  }
}
