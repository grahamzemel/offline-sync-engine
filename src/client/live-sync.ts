import type { SyncEvent } from '../shared/types.js';

export interface LiveSyncOptions<R = unknown> {
  /** URL of the SSE endpoint that broadcasts sync events. */
  url: string;
  /**
   * Endpoint that returns all events with seqId > since. Called automatically
   * on reconnect so the client doesn't miss anything that happened while it
   * was offline.
   */
  catchupUrl: (sinceSeqId: number) => string;
  /** Most-recent server seqId this client has applied. Updated as events arrive. */
  initialSeqId?: number;
  /** Fired for pending (optimistic) announcements from other clients. */
  onPending?: (event: Extract<SyncEvent<R>, { kind: 'pending' }>) => void;
  /** Fired for confirmed commits — both from other clients and the server's own writes. */
  onCommit?: (event: Extract<SyncEvent<R>, { kind: 'commit' }>) => void;
  /** Called on reconnect with the number of events caught up. */
  onReconnect?: (caughtUpEvents: number, newHeadSeqId: number) => void;
  /** Fetch implementation, defaults to globalThis.fetch. */
  fetcher?: typeof fetch;
}

/**
 * Client for the realtime broadcast channel. The job:
 *
 *   1. Subscribe to the SSE stream.
 *   2. Apply incoming events as they arrive, advancing the local seqId.
 *   3. When the connection drops and EventSource auto-reconnects, fetch
 *      every event since the last seen seqId from `catchupUrl` and replay
 *      them before resuming live updates. This eliminates the "scanner B
 *      missed an admit while their WiFi blipped" failure.
 *
 * Use this alongside the OfflineQueue: queue handles your *outgoing*
 * mutations, LiveSync handles *incoming* events from peers.
 */
export class LiveSync<R = unknown> {
  private url: string;
  private catchupUrl: (sinceSeqId: number) => string;
  private onPending?: LiveSyncOptions<R>['onPending'];
  private onCommit?: LiveSyncOptions<R>['onCommit'];
  private onReconnect?: LiveSyncOptions<R>['onReconnect'];
  private fetcher: typeof fetch;

  private lastSeqId: number;
  private source: EventSource | null = null;
  private wasOnline = false;
  private closed = false;

  constructor(opts: LiveSyncOptions<R>) {
    this.url = opts.url;
    this.catchupUrl = opts.catchupUrl;
    this.onPending = opts.onPending;
    this.onCommit = opts.onCommit;
    this.onReconnect = opts.onReconnect;
    this.lastSeqId = opts.initialSeqId ?? 0;
    this.fetcher = opts.fetcher ?? globalThis.fetch.bind(globalThis);
  }

  /** Open the SSE connection. Idempotent. */
  start(): void {
    if (this.source || this.closed) return;
    if (typeof EventSource === 'undefined') {
      throw new Error(
        'EventSource is not available in this environment. Polyfill or provide a custom transport.'
      );
    }
    this.source = new EventSource(this.url);
    this.source.onopen = () => {
      // First open vs reconnect — reconnect requires catchup.
      if (this.wasOnline) {
        void this.catchUp();
      }
      this.wasOnline = true;
    };
    this.source.onmessage = (e) => {
      try {
        const event = JSON.parse(e.data) as SyncEvent<R>;
        this.apply(event);
      } catch {
        /* ignore malformed payloads */
      }
    };
    this.source.onerror = () => {
      // EventSource auto-reconnects; we just need to remember we lost it.
      this.wasOnline = false;
    };
  }

  /** Close and stop reconnecting. */
  close(): void {
    this.closed = true;
    this.source?.close();
    this.source = null;
  }

  /** Most-recent committed seqId this client has applied. */
  getSeqId(): number {
    return this.lastSeqId;
  }

  private apply(event: SyncEvent<R>): void {
    if (event.kind === 'pending') {
      this.onPending?.(event);
      return;
    }
    // commit
    if (event.seqId <= this.lastSeqId) {
      // Already applied (catchup overlapped live stream). Idempotent: skip.
      return;
    }
    this.lastSeqId = event.seqId;
    this.onCommit?.(event);
  }

  private async catchUp(): Promise<void> {
    const since = this.lastSeqId;
    try {
      const res = await this.fetcher(this.catchupUrl(since));
      if (!res.ok) return;
      const body = (await res.json()) as { events: SyncEvent<R>[]; headSeqId: number };
      const events = Array.isArray(body.events) ? body.events : [];
      let applied = 0;
      for (const event of events) {
        if (event.kind === 'commit' && event.seqId > this.lastSeqId) {
          this.apply(event);
          applied++;
        }
      }
      this.onReconnect?.(applied, body.headSeqId ?? this.lastSeqId);
    } catch {
      /* swallow — next reconnect will retry catchup */
    }
  }
}
