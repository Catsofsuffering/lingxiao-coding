/**
 * AgentActivityBuffer — in-memory ring buffer for agent:activity events.
 *
 * Mirrors GitActivityBuffer but records generic agent-visible work: file writes,
 * structured patches, shell commands, git actions and other write/execute tools.
 * The frontend fetches /api/v1/agent/activity/:sessionId on session switch so the
 * activity page does not depend on catching live SSE only.
 */

import type { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import type { EventEmitter } from '../core/EventEmitter.js';

export interface AgentActivityRecord {
  id?: string;
  sessionId: string;
  agentId: string;
  agentName: string;
  taskId?: string;
  toolName: string;
  toolCategory?: string;
  toolTier?: string;
  action?: string;
  success: boolean;
  timestamp: number;
  summary?: string;
  target?: string;
  files?: string[];
  command?: string;
  error?: string;
}

const MAX_EVENTS_PER_SESSION = 500;
const MAX_AGE_MS = 24 * 60 * 60 * 1000; // 24h

export class AgentActivityBuffer {
  /** sessionId → events array (most recent last) */
  private store = new Map<string, AgentActivityRecord[]>();
  private unsub: (() => void) | null = null;

  start(emitter: EventEmitter): void {
    if (this.unsub) return;
    this.unsub = emitter.subscribe('agent:activity', (data) => {
      this.addEvent(data as AgentActivityRecord);
    });
  }

  stop(): void {
    if (this.unsub) { this.unsub(); this.unsub = null; }
  }

  private addEvent(rec: AgentActivityRecord): void {
    const sid = rec.sessionId;
    if (!sid) return;
    const arr = this.store.get(sid) ?? [];
    arr.push({ ...rec });
    if (arr.length > MAX_EVENTS_PER_SESSION) {
      arr.splice(0, arr.length - MAX_EVENTS_PER_SESSION);
    }
    const cutoff = Date.now() - MAX_AGE_MS;
    while (arr.length > 0 && arr[0].timestamp < cutoff) {
      arr.shift();
    }
    this.store.set(sid, arr);
  }

  getEvents(sessionId: string): AgentActivityRecord[] {
    return [...(this.store.get(sessionId) ?? [])].map((rec, i) => ({
      ...rec,
      id: rec.id ?? `${rec.sessionId}-${rec.timestamp}-${rec.agentId}-${rec.toolName}-${i}`,
    }));
  }

  registerRoutes(
    fastify: FastifyInstance,
    requireAuth: (req: FastifyRequest, reply: FastifyReply) => boolean,
  ): void {
    // GET /api/v1/agent/activity/:sessionId — fetch buffered agent activity events
    fastify.get('/api/v1/agent/activity/:sessionId', async (request, reply) => {
      if (!requireAuth(request, reply)) return;
      const { sessionId } = request.params as { sessionId: string };
      if (!sessionId) {
        reply.status(400).send({ error: 'sessionId is required' });
        return;
      }
      return { data: this.getEvents(sessionId) };
    });
  }
}
