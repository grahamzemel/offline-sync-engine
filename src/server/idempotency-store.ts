import type { CommitResult } from '../shared/types.js';

/**
 * Persistent dedup record. When a mutation commits, the result is stored
 * under its idempotency key; any retry of the same key returns this cached
 * payload instead of re-executing the handler.
 */
export interface IdempotencyRecord<R = unknown> {
  idempotencyKey: string;
  result: CommitResult<R>;
  /** Wall-clock ms. The store may evict records older than `ttlMs`. */
  createdAt: number;
}

export interface IdempotencyBackend {
  /** Atomically reserve a key. Returns null if reserved fresh, the existing record if already committed. */
  reserveOrGet<R>(idempotencyKey: string): Promise<IdempotencyRecord<R> | null>;
  /** Persist the result for a key. Called after the handler succeeds. */
  commit<R>(record: IdempotencyRecord<R>): Promise<void>;
  /** Drop a reservation that never got committed (handler threw). */
  release(idempotencyKey: string): Promise<void>;
}

/**
 * In-memory backend. Single-process only — use the Redis or Firestore
 * adapter you write for your own infra in production. The interface is
 * intentionally small so adapters are 20-30 lines.
 */
export class MemoryIdempotencyBackend implements IdempotencyBackend {
  private records = new Map<string, IdempotencyRecord<any>>();
  private reservations = new Set<string>();

  async reserveOrGet<R>(key: string): Promise<IdempotencyRecord<R> | null> {
    const existing = this.records.get(key);
    if (existing) return existing as IdempotencyRecord<R>;
    if (this.reservations.has(key)) {
      // Another concurrent request is already processing — caller should
      // wait & retry, but for the in-memory backend we just treat as "no
      // record yet" and accept that the duplicate handler may run.
      // Production backends (Redis SETNX, Firestore txn) prevent this.
      return null;
    }
    this.reservations.add(key);
    return null;
  }
  async commit<R>(record: IdempotencyRecord<R>): Promise<void> {
    this.records.set(record.idempotencyKey, record);
    this.reservations.delete(record.idempotencyKey);
  }
  async release(key: string): Promise<void> {
    this.reservations.delete(key);
  }

  /** Test helper: drop everything older than `ttlMs`. */
  evict(ttlMs: number, now = Date.now()): number {
    let dropped = 0;
    for (const [key, rec] of this.records.entries()) {
      if (now - rec.createdAt > ttlMs) {
        this.records.delete(key);
        dropped++;
      }
    }
    return dropped;
  }
}

/**
 * High-level idempotency store. Wraps a backend to provide the
 * standard "check, run, commit" flow.
 */
export class IdempotencyStore {
  constructor(private backend: IdempotencyBackend = new MemoryIdempotencyBackend()) {}

  /**
   * Process a mutation idempotently. If the key has been seen before, the
   * cached `CommitResult` is returned without running `handler` again.
   * Otherwise the handler runs and its result is cached for future retries.
   *
   * The `handler` should perform the actual mutation (DB write, charge,
   * etc.) and return the result that should be cached. If the handler
   * throws, the reservation is released so subsequent retries can run it
   * fresh.
   */
  async process<R>(
    idempotencyKey: string,
    handler: () => Promise<R>,
    assignSeqId: () => Promise<number>
  ): Promise<CommitResult<R>> {
    const existing = await this.backend.reserveOrGet<R>(idempotencyKey);
    if (existing) {
      return { ...existing.result, duplicate: true };
    }
    try {
      const result = await handler();
      const seqId = await assignSeqId();
      const commit: CommitResult<R> = {
        idempotencyKey,
        seqId,
        duplicate: false,
        result,
        serverTs: Date.now(),
      };
      await this.backend.commit({
        idempotencyKey,
        result: commit,
        createdAt: Date.now(),
      });
      return commit;
    } catch (err) {
      await this.backend.release(idempotencyKey);
      throw err;
    }
  }
}
