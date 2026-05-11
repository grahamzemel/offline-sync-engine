/**
 * In-browser stand-in for what would be your Express/Fastify backend.
 *
 * Holds the truth: which guests have been admitted, in what order. Exposes
 * a single `submit(mutation)` that the scanner calls. The WiFi switch in
 * the UI gates whether `submit` actually delivers — when WiFi is OFF, we
 * throw an error to simulate the request never reaching the server.
 *
 * In library mode it uses IdempotencyStore to dedupe retries. In naive
 * mode it just inserts whatever it gets, so retries produce duplicate
 * admits — exactly the failure mode the library fixes.
 */
import {
  IdempotencyStore,
  MemoryIdempotencyBackend,
  MutationLog,
} from 'offline-sync-engine/server';
import type { CommitResult, Mutation } from 'offline-sync-engine/server';

export interface AdmitPayload {
  guestId: string;
  guestName: string;
}

export interface AdmitRecord {
  seqId: number;
  guestId: string;
  guestName: string;
  serverTs: number;
}

export class SimulatedServer {
  wifiOnline = true;
  useLibrary = false;

  private records: AdmitRecord[] = [];
  private duplicateAdmits = 0;
  private store = new IdempotencyStore(new MemoryIdempotencyBackend());
  private log = new MutationLog();

  private listeners = new Set<() => void>();
  onChange(fn: () => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }
  private emit() {
    this.listeners.forEach((fn) => fn());
  }

  reset() {
    this.records = [];
    this.duplicateAdmits = 0;
    this.store = new IdempotencyStore(new MemoryIdempotencyBackend());
    this.log = new MutationLog();
    this.emit();
  }

  totalRecorded(): number {
    return this.records.length;
  }
  duplicateCount(): number {
    return this.duplicateAdmits;
  }
  ledger(): AdmitRecord[] {
    return [...this.records];
  }

  async submit(mutation: Mutation<AdmitPayload>): Promise<CommitResult<AdmitRecord>> {
    // Simulated WiFi outage: drop the request entirely.
    if (!this.wifiOnline) {
      // Tiny delay so the UI can show "trying..." briefly.
      await new Promise((r) => setTimeout(r, 50));
      throw new Error('network');
    }
    // Realistic round-trip latency.
    await new Promise((r) => setTimeout(r, 80 + Math.random() * 60));

    if (this.useLibrary) {
      return this.store.process<AdmitRecord>(
        mutation.idempotencyKey,
        async () => {
          // The library guarantees this handler runs at most once per
          // idempotency key. We can safely insert without worrying about
          // duplicate-from-retry — only domain-level dups (same guest from
          // a fresh scan event) reach us here.
          const isGuestDup = this.records.some(
            (r) => r.guestId === mutation.payload.guestId
          );
          const seqId = await this.log.appendAndAssign({
            idempotencyKey: mutation.idempotencyKey,
            duplicate: false,
            result: null as unknown as AdmitRecord,
            serverTs: Date.now(),
          });
          const rec: AdmitRecord = {
            seqId,
            guestId: mutation.payload.guestId,
            guestName: mutation.payload.guestName,
            serverTs: Date.now(),
          };
          if (isGuestDup) this.duplicateAdmits++;
          this.records.push(rec);
          this.emit();
          return rec;
        },
        async () => 0 // seqId already assigned inside handler
      );
    }

    // Naive mode: blindly insert.
    const isGuestDup = this.records.some(
      (r) => r.guestId === mutation.payload.guestId
    );
    const seqId = this.records.length + 1;
    const rec: AdmitRecord = {
      seqId,
      guestId: mutation.payload.guestId,
      guestName: mutation.payload.guestName,
      serverTs: Date.now(),
    };
    if (isGuestDup) this.duplicateAdmits++;
    this.records.push(rec);
    this.emit();
    return {
      idempotencyKey: mutation.idempotencyKey,
      seqId,
      duplicate: false,
      result: rec,
      serverTs: Date.now(),
    };
  }
}
