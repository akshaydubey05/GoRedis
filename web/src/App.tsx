import { useState } from "react";
import { useClusterStream, API } from "./useClusterStream";
import { NodeCard } from "./NodeCard";
import { Terminal } from "./Terminal";
import { GitBranch, RefreshCw, Split, Zap } from "lucide-react";

async function post(path: string) {
  try {
    await fetch(`${API}${path}`, { method: "POST" });
  } catch {}
}

export default function App() {
  const { state, online, events, pushEvent } = useClusterStream();
  const [showTour, setShowTour] = useState(true);

  return (
    <div className="min-h-screen relative z-10 px-4 py-6 md:px-8">
      <div className="max-w-5xl mx-auto">

        {/* Header */}
        <div className="brutal-box p-4 mb-6 flex flex-wrap items-center justify-between gap-3 bg-white">
          <div>
            <h1 className="text-3xl font-bold tracking-tight uppercase">
              GoRedis Cluster
            </h1>

            <p className="mono text-xs mt-0.5">
              live raft consensus // built from scratch in go
            </p>
          </div>

          <div className="flex items-center gap-3">
            <span
              className={`w-3 h-3 border-2 border-black ${
                online ? "bg-[#4B7F52]" : "bg-[#B23A2E]"
              }`}
            />

            <span className="mono text-xs uppercase">
              {online ? "live" : "reconnecting"}
            </span>

            {state.leaderId >= 0 && (
              <span className="mono text-xs uppercase border-2 border-black px-2 py-0.5 bg-[#E8A33D]">
                Leader: N{state.leaderId}
              </span>
            )}

            <a
              href="https://github.com/akshaydubey05/GoRedis"
              target="_blank"
              rel="noreferrer"
              className="brutal-btn bg-black text-white px-2.5 py-1.5 flex items-center gap-1.5 text-xs"
            >
              <GitBranch size={14} />
              Source
            </a>
          </div>
        </div>

        {/* Intro / explainer section */}
        <div className="brutal-box p-5 mb-6 bg-white">
          <div className="flex items-start gap-3 mb-3">
            <span className="text-3xl leading-none">👋</span>

            <div>
              <h2 className="text-lg font-bold uppercase mb-1">
                What is this?
              </h2>

              <p className="text-sm leading-relaxed">
                This is a distributed, fault-tolerant key-value database — like
                Redis, but spread across multiple servers that agree on every
                write using the <b>Raft consensus algorithm</b>, implemented
                entirely from scratch in Go. If one server crashes, the others
                automatically elect a new leader and keep serving requests with{" "}
                <b>zero data loss</b>. This is the same core idea behind systems
                like etcd (which powers Kubernetes), CockroachDB, and Consul.
              </p>
            </div>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 mt-4">
            {/* Problem */}
            <div className="border-2 border-black p-3 bg-[#F2ECDC]">
              <div className="mono text-[10px] uppercase tracking-widest mb-1 text-black/60">
                The problem
              </div>

              <p className="text-xs leading-snug">
                A single database server is a single point of failure. If it
                crashes, your app goes down and you might lose data.
              </p>
            </div>

            {/* Fix */}
            <div className="border-2 border-black p-3 bg-[#F2ECDC]">
              <div className="mono text-[10px] uppercase tracking-widest mb-1 text-black/60">
                The fix
              </div>

              <p className="text-xs leading-snug">
                Run 3 copies. They vote on a "leader" that handles writes and
                copies them to the others. A majority must agree before any
                write is confirmed.
              </p>
            </div>

            {/* Below */}
            <div className="border-2 border-black p-3 bg-[#F2ECDC]">
              <div className="mono text-[10px] uppercase tracking-widest mb-1 text-black/60">
                Below
              </div>

              <p className="text-xs leading-snug">
                3 live nodes below. Kill the leader and watch the cluster heal
                itself in real time — no data lost.
              </p>
            </div>
          </div>
        </div>

        {/* Tour */}
        {showTour && (
          <div className="brutal-box-sm p-4 mb-6 bg-[#E8A33D] text-sm">
            <div className="flex items-start justify-between gap-3 mb-2">
              <p className="font-bold uppercase mono text-xs">
                Try it yourself — 3 steps
              </p>

              <button
                onClick={() => setShowTour(false)}
                className="mono text-xs underline shrink-0"
              >
                dismiss
              </button>
            </div>

            <ol className="space-y-1.5 list-none">
              <li className="flex gap-2">
                <span className="mono font-bold">1.</span>

                <span>
                  In the <b>Terminal</b> below, type{" "}
                  <code className="mono bg-white px-1 border border-black">
                    SET name akshay
                  </code>{" "}
                  and press Enter.
                </span>
              </li>

              <li className="flex gap-2">
                <span className="mono font-bold">2.</span>

                <span>
                  Find the node with the <b>👑 gold "LEADER" stamp</b> and click
                  its <b>Kill</b> button.
                </span>
              </li>

              <li className="flex gap-2">
                <span className="mono font-bold">3.</span>

                <span>
                  Watch a different node become leader within ~1 second. Now
                  type{" "}
                  <code className="mono bg-white px-1 border border-black">
                    GET name
                  </code>{" "}
                  — your data is still there, even though a server "died."
                </span>
              </li>
            </ol>
          </div>
        )}

        {/* Node section heading */}
        <div className="flex items-center justify-between mb-2">
          <h3 className="mono text-xs uppercase tracking-widest text-black/60">
            Live Cluster — 3 Nodes
          </h3>

          <span className="mono text-[10px] text-black/40">
            updates every 300ms
          </span>
        </div>

        {/* Node cards */}
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-6">
          {state.nodes.map((n) => (
            <NodeCard
              key={n.id}
              n={n}
              onKill={() => {
                post(`/api/chaos/kill?id=${n.id}`);

                pushEvent(
                  `${new Date().toLocaleTimeString()}  killing N${n.id}...`
                );
              }}
              onRestart={() => {
                post(`/api/chaos/restart?id=${n.id}`);

                pushEvent(
                  `${new Date().toLocaleTimeString()}  restarting N${n.id}...`
                );
              }}
            />
          ))}
        </div>

        {/* Chaos + Terminal + Events */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-6">

          {/* Chaos Controls */}
          <div className="brutal-box p-4 bg-white">
            <h2 className="mono text-xs uppercase tracking-widest mb-3 border-b-2 border-black pb-2">
              Chaos Controls
            </h2>

            <div className="flex flex-wrap gap-2 mb-4">
              {/* Partition */}
              <button
                onClick={() => {
                  post("/api/chaos/partition");
                  pushEvent("partitioning N0 from the rest...");
                }}
                className="brutal-btn bg-white px-3 py-1.5 text-xs flex items-center gap-1.5 hover:bg-[#D9481F] hover:text-white"
              >
                <Split size={14} />
                Partition
              </button>

              {/* Heal */}
              <button
                onClick={() => {
                  post("/api/chaos/heal");
                  pushEvent("healing partitions...");
                }}
                className="brutal-btn bg-white px-3 py-1.5 text-xs flex items-center gap-1.5 hover:bg-[#4B7F52] hover:text-white"
              >
                <Zap size={14} />
                Heal All
              </button>

              {/* Reset */}
              <button
                onClick={() => {
                  post("/api/reset");
                  pushEvent("resetting cluster...");
                }}
                className="brutal-btn bg-white px-3 py-1.5 text-xs flex items-center gap-1.5 hover:bg-black hover:text-white"
              >
                <RefreshCw size={14} />
                Reset
              </button>
            </div>

            <div className="mono text-[11px] uppercase tracking-widest mb-2 text-black/60">
              Event Log
            </div>

            <div className="h-40 overflow-y-auto mono text-xs space-y-1 border-2 border-black p-2 bg-[#F2ECDC]">
              {events.length === 0 && (
                <div className="text-black/40">
                  waiting for events...
                </div>
              )}

              {events.map((e, i) => (
                <div key={i}>{e}</div>
              ))}
            </div>
          </div>

          {/* Terminal */}
          <Terminal />
        </div>

        {/* Footer */}
        <p className="text-center mono text-[11px] text-black/50">
          simulation mode: nodes run in one process over a simulated network —
          same raft code as the real multi-process cluster. built from scratch,
          no consensus libraries used.
        </p>
      </div>
    </div>
  );
}