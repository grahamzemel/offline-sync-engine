import type { SyncEvent } from '../shared/types.js';

/**
 * Anything that looks enough like a Node http ServerResponse for SSE.
 * Keeping the shape minimal so this works with Express, Fastify, raw http,
 * and edge runtimes that expose a `.write` stream.
 */
export interface SseWriter {
  write(chunk: string): boolean;
  end?(): void;
  setHeader?(name: string, value: string): void;
  flushHeaders?(): void;
  statusCode?: number;
  on?(event: 'close', handler: () => void): void;
}

interface Channel {
  topic: string;
  res: SseWriter;
}

/**
 * Broadcaster — manages SSE connections per topic and fans out SyncEvents
 * to subscribers. Topics are arbitrary strings; you decide whether to
 * broadcast per-user, per-room, per-event, etc.
 *
 * The two interesting methods:
 *
 *   - `announce(topic, event)`: send a 'pending' event so other clients
 *     can update optimistically *before* the commit. This is what lets
 *     scanner B know "scanner A is about to admit Sarah" before A's POST
 *     finishes.
 *
 *   - `commit(topic, event)`: send a 'commit' event after the mutation
 *     has been persisted with its seqId. Clients use the seqId to advance
 *     their watermark.
 */
export class Broadcaster {
  private channels = new Map<string, Set<Channel>>();

  /**
   * Attach a connection to a topic. Sends the SSE preamble, writes a
   * keep-alive comment every 25s, and removes the channel on close.
   *
   * Use this from your HTTP route handler:
   *
   *   app.get('/events/:topic', (req, res) => {
   *     broadcaster.subscribe(req.params.topic, res);
   *   });
   */
  subscribe(topic: string, res: SseWriter): void {
    res.statusCode = 200;
    res.setHeader?.('Content-Type', 'text/event-stream');
    res.setHeader?.('Cache-Control', 'no-cache, no-transform');
    res.setHeader?.('Connection', 'keep-alive');
    res.setHeader?.('X-Accel-Buffering', 'no'); // Disable nginx proxy buffering
    res.flushHeaders?.();
    res.write(': connected\n\n');

    const channel: Channel = { topic, res };
    const set = this.channels.get(topic) ?? new Set();
    set.add(channel);
    this.channels.set(topic, set);

    const heartbeat = setInterval(() => {
      try {
        res.write(': keepalive\n\n');
      } catch {
        clearInterval(heartbeat);
      }
    }, 25_000);

    res.on?.('close', () => {
      clearInterval(heartbeat);
      set.delete(channel);
      if (set.size === 0) this.channels.delete(topic);
    });
  }

  /** Send any event to subscribers of the given topic. */
  publish<R>(topic: string, event: SyncEvent<R>): void {
    const set = this.channels.get(topic);
    if (!set) return;
    const payload = `data: ${JSON.stringify(event)}\n\n`;
    for (const ch of set) {
      try {
        ch.res.write(payload);
      } catch {
        set.delete(ch);
      }
    }
  }

  /** Convenience: broadcast a `pending` announcement. */
  announce(
    topic: string,
    idempotencyKey: string,
    type: string,
    clientId?: string
  ): void {
    this.publish(topic, {
      kind: 'pending',
      idempotencyKey,
      type,
      clientId,
      announcedAt: Date.now(),
    });
  }

  /** Convenience: broadcast a `commit` event. */
  commit<R>(topic: string, commit: import('../shared/types.js').CommitResult<R>): void {
    this.publish(topic, { kind: 'commit', ...commit });
  }

  /** Active topic count — for /metrics or admin dashboards. */
  topicCount(): number {
    return this.channels.size;
  }
  /** Active connection count across all topics. */
  connectionCount(): number {
    let total = 0;
    for (const set of this.channels.values()) total += set.size;
    return total;
  }
}
