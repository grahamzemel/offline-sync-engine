/**
 * Single door scanner. Two modes:
 *
 *   Library ON  → backed by OfflineQueue + IndexedDB. Admits get a ULID,
 *                 persist locally, retry with backoff. Caller's "await admit()"
 *                 returns the moment the mutation is durably queued — never
 *                 waits for the network.
 *
 *   Library OFF → "naive" baseline a typical app might ship: try the network,
 *                 if it fails push the request onto an in-memory backlog and
 *                 retry on a timer. On reconnect, drain the backlog. The two
 *                 ways this fails:
 *                   (a) loses admits if you reload the tab while offline
 *                   (b) double-submits when a network error happens *after*
 *                       the server already received the request — because
 *                       there's no idempotency key, the retry shows up to
 *                       the server as a brand-new admit.
 */
import { OfflineQueue, MemoryQueueStorage, ulid } from 'offline-sync-engine/client';
import type { Mutation, CommitResult } from 'offline-sync-engine/client';
import type { SimulatedServer, AdmitPayload, AdmitRecord } from './SimulatedServer.js';

export type EventKind = 'queued' | 'commit' | 'retrying' | 'error';
export interface ScannerEvent {
  kind: EventKind;
  guestName: string;
  detail?: string;
  at: number;
}

export class Scanner {
  private server: SimulatedServer;
  private getUseLibrary: () => boolean;
  private getOnline: () => boolean;

  // Library mode
  private queue: OfflineQueue | null = null;

  // Naive mode
  private naiveBacklog: Mutation<AdmitPayload>[] = [];
  private naiveDraining = false;

  private listeners = new Set<(e: ScannerEvent) => void>();
  private events: ScannerEvent[] = [];

  constructor(
    server: SimulatedServer,
    getUseLibrary: () => boolean,
    getOnline: () => boolean
  ) {
    this.server = server;
    this.getUseLibrary = getUseLibrary;
    this.getOnline = getOnline;
    this.initQueue();
  }

  private initQueue() {
    this.queue = new OfflineQueue({
      storage: new MemoryQueueStorage(),
      sender: async (batch: Mutation[]) => this.sendBatch(batch),
      initialBackoffMs: 300,
      maxBackoffMs: 2000,
      onCommit: (m, r) => {
        const result = r as CommitResult<AdmitRecord>;
        this.pushEvent({
          kind: 'commit',
          guestName: (m as Mutation<AdmitPayload>).payload.guestName,
          detail: result.duplicate
            ? `server already had this one (seq ${result.seqId})`
            : `recorded as seq ${result.seqId}`,
          at: Date.now(),
        });
      },
    });
  }

  reset() {
    this.naiveBacklog = [];
    this.naiveDraining = false;
    this.events = [];
    this.initQueue();
    this.listeners.forEach((fn) =>
      fn({ kind: 'commit', guestName: '', at: Date.now() })
    );
  }

  /** Called when the WiFi toggle flips ON — trigger queue drain. */
  onNetworkResume() {
    if (this.getUseLibrary()) {
      void this.queue?.flush();
    } else {
      void this.naiveDrain();
    }
  }

  async pendingCount(): Promise<number> {
    if (this.getUseLibrary()) return this.queue ? await this.queue.size() : 0;
    return this.naiveBacklog.length;
  }

  onEvent(fn: (e: ScannerEvent) => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }
  recentEvents(limit = 30): ScannerEvent[] {
    return this.events.slice(-limit).reverse();
  }

  private pushEvent(e: ScannerEvent) {
    this.events.push(e);
    if (this.events.length > 200) this.events.shift();
    this.listeners.forEach((fn) => fn(e));
  }

  async admit(payload: AdmitPayload) {
    if (this.getUseLibrary()) {
      // Returns the moment the mutation is in IndexedDB. No await on network.
      await this.queue!.enqueue('admit', payload);
      this.pushEvent({
        kind: 'queued',
        guestName: payload.guestName,
        detail: this.getOnline() ? 'syncing…' : 'offline — saved locally',
        at: Date.now(),
      });
      return;
    }
    // Naive: try direct, fall back to in-memory backlog.
    const mutation: Mutation<AdmitPayload> = {
      idempotencyKey: ulid(),
      type: 'admit',
      payload,
      clientTs: Date.now(),
    };
    this.pushEvent({
      kind: 'queued',
      guestName: payload.guestName,
      detail: this.getOnline() ? 'sending…' : 'offline — buffered (in-memory, lost on reload)',
      at: Date.now(),
    });
    this.naiveBacklog.push(mutation);
    void this.naiveDrain();
  }

  /**
   * Naive drain loop — represents the kind of code a developer would write
   * if they hadn't thought hard about idempotency. The bug is in the catch
   * branch: when a request fails, we DON'T know if the server received it
   * and the ack got lost, or if it never got there. So a retry could
   * produce a duplicate. We do the retry anyway because losing admits is
   * worse than duplicating them, and watch the dup counter climb.
   */
  private async naiveDrain() {
    if (this.naiveDraining) return;
    this.naiveDraining = true;
    try {
      while (this.naiveBacklog.length > 0) {
        const m = this.naiveBacklog[0];
        try {
          await this.server.submit(m);
          this.naiveBacklog.shift();
          this.pushEvent({
            kind: 'commit',
            guestName: m.payload.guestName,
            detail: 'sent (no dedup on server — hopes for the best)',
            at: Date.now(),
          });
        } catch {
          this.pushEvent({
            kind: 'retrying',
            guestName: m.payload.guestName,
            detail: 'network error; will retry',
            at: Date.now(),
          });
          // The classic ack-lost race: simulate the server having actually
          // accepted our previous attempt while the response failed in
          // transit. The retry pushes the same payload through, and the
          // naive server inserts it AGAIN. Library mode catches this via
          // the idempotency key.
          if (this.getOnline() && Math.random() < 0.4) {
            try {
              await this.server.submit(m);
              this.naiveBacklog.shift();
              this.pushEvent({
                kind: 'commit',
                guestName: m.payload.guestName,
                detail: 'retried — but the first try may have also landed',
                at: Date.now(),
              });
              continue;
            } catch {
              /* still down */
            }
          }
          await new Promise((r) => setTimeout(r, 400));
          if (!this.getOnline()) return; // stop trying until WiFi comes back
        }
      }
    } finally {
      this.naiveDraining = false;
    }
  }

  private async sendBatch(batch: Mutation[]): Promise<CommitResult[]> {
    // For library mode: simulate a single round-trip per batch. The server's
    // own WiFi gate (wifiOnline) does the actual failure injection.
    const results: CommitResult[] = [];
    for (const m of batch) {
      const res = await this.server.submit(m as Mutation<AdmitPayload>);
      results.push(res);
    }
    return results;
  }
}
