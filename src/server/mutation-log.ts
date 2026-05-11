import type { CommitResult } from '../shared/types.js';

/**
 * Append-only log of every committed mutation, indexed by monotonic seqId.
 * Powers two things:
 *
 *   1. seqId assignment for new commits.
 *   2. Catch-up replay: a client reconnects and says "give me seq > N",
 *      and the log returns everything that happened during the outage so
 *      no events are lost.
 *
 * The interface is pluggable so you can back it with Postgres, Redis
 * Streams, Firestore — whatever. The in-memory implementation is for
 * single-process apps and tests.
 */
export interface MutationLogBackend {
  /** Append a commit and return the assigned seqId. */
  append<R>(commit: Omit<CommitResult<R>, 'seqId'>): Promise<number>;
  /** Fetch every commit with seqId > sinceSeqId, in order, up to `limit`. */
  since<R>(sinceSeqId: number, limit?: number): Promise<CommitResult<R>[]>;
  /** The highest seqId assigned so far. */
  head(): Promise<number>;
}

export class MemoryMutationLogBackend implements MutationLogBackend {
  private entries: CommitResult<any>[] = [];

  async append<R>(commit: Omit<CommitResult<R>, 'seqId'>): Promise<number> {
    const seqId = this.entries.length + 1;
    this.entries.push({ ...commit, seqId } as CommitResult<R>);
    return seqId;
  }
  async since<R>(sinceSeqId: number, limit = 1000): Promise<CommitResult<R>[]> {
    return this.entries.slice(sinceSeqId, sinceSeqId + limit) as CommitResult<R>[];
  }
  async head(): Promise<number> {
    return this.entries.length;
  }
}

/**
 * High-level wrapper around a MutationLogBackend. The reason for the
 * extra class instead of using the backend directly: the wrapper is
 * where we'd add cross-cutting concerns later (metrics, tracing,
 * compaction). For now it's a thin pass-through.
 */
export class MutationLog {
  constructor(public backend: MutationLogBackend = new MemoryMutationLogBackend()) {}

  appendAndAssign<R>(commit: Omit<CommitResult<R>, 'seqId'>): Promise<number> {
    return this.backend.append(commit);
  }
  since<R>(sinceSeqId: number, limit?: number): Promise<CommitResult<R>[]> {
    return this.backend.since(sinceSeqId, limit);
  }
  head(): Promise<number> {
    return this.backend.head();
  }
}
