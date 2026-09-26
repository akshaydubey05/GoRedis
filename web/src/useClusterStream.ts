import { useEffect, useState } from "react";

export type Role = "follower" | "candidate" | "leader" | "down";

export type NodeView = {
  id: number;
  alive: boolean;
  role: string;
  term: number;
  commitIndex: number;
  logLen: number;
};

export type ClusterState = {
  nodes: NodeView[];
  leaderId: number;
};

export const API = (window as any).API_URL || "http://localhost:8080";

export function useClusterStream() {
  const [state, setState] = useState<ClusterState>({ nodes: [], leaderId: -1 });
  const [online, setOnline] = useState(false);
  const [prevRoles, setPrevRoles] = useState<Record<number, string>>({});
  const [events, setEvents] = useState<string[]>([]);

  useEffect(() => {
    const es = new EventSource(`${API}/api/events`);
    es.onopen = () => setOnline(true);
    es.onerror = () => setOnline(false);
    es.addEventListener("state", (e) => {
      const next: ClusterState = JSON.parse((e as MessageEvent).data);
      setPrevRoles((prevMap) => {
        const changes: string[] = [];
        next.nodes.forEach((n) => {
          const key = n.id;
          const label = n.alive ? n.role : "down";
          if (prevMap[key] && prevMap[key] !== label) {
            const t = new Date().toLocaleTimeString();
            changes.push(`${t}  N${n.id}: ${prevMap[key]} -> ${label}`);
          }
        });
        if (changes.length) {
          setEvents((old) => [...changes.reverse(), ...old].slice(0, 60));
        }
        const nextMap: Record<number, string> = {};
        next.nodes.forEach((n) => (nextMap[n.id] = n.alive ? n.role : "down"));
        return nextMap;
      });
      setState(next);
    });
    return () => es.close();
  }, []);

  return { state, online, events, pushEvent: (msg: string) => setEvents((e) => [msg, ...e].slice(0, 60)) };
}