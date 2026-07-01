import { create } from 'zustand';

export interface AgentActivityEvent {
  id: string;
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

interface AgentActivityState {
  events: AgentActivityEvent[];
  addEvent: (event: AgentActivityEvent) => void;
  clear: () => void;
  /** Replace all events (used when loading from REST API) */
  setEvents: (events: AgentActivityEvent[]) => void;
  /** Remove events older than maxAgeMs to prevent unbounded growth */
  prune: (maxAgeMs?: number) => void;
}

const MAX_EVENTS = 500;
const DEFAULT_MAX_AGE_MS = 24 * 60 * 60 * 1000; // 24h

export const useAgentActivityStore = create<AgentActivityState>((set) => ({
  events: [],
  addEvent: (event) =>
    set((state) => {
      const events = [...state.events, event];
      if (events.length > MAX_EVENTS) {
        events.splice(0, events.length - MAX_EVENTS);
      }
      return { events };
    }),
  clear: () => set({ events: [] }),
  setEvents: (events) => set({ events }),
  prune: (maxAgeMs = DEFAULT_MAX_AGE_MS) =>
    set((state) => {
      const cutoff = Date.now() - maxAgeMs;
      return { events: state.events.filter((e) => e.timestamp >= cutoff) };
    }),
}));
