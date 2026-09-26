import { Crown, Power, Skull } from "lucide-react";
import type { NodeView } from "./useClusterStream";

const ROLE_BG: Record<string, string> = {
  leader: "bg-[#E8A33D]",
  candidate: "bg-[#6B4E9E] text-white",
  follower: "bg-white",
};

export function NodeCard({
  n,
  onKill,
  onRestart,
}: {
  n: NodeView;
  onKill: () => void;
  onRestart: () => void;
}) {
  const bg = n.alive ? ROLE_BG[n.role] ?? "bg-white" : "bg-[#B23A2E] text-white";

  return (
    <div className={`brutal-box p-4 relative ${bg} ${n.role === "leader" && n.alive ? "stamp" : ""}`}>
      {n.role === "leader" && n.alive && (
        <div className="absolute -top-4 -right-3 rotate-6 border-2 border-black bg-black text-[#E8A33D] px-2 py-0.5 text-[10px] font-bold uppercase tracking-widest mono">
          Leader
        </div>
      )}

      <div className="flex items-center justify-between mb-3">
        <span className="text-xl font-bold flex items-center gap-1.5">
          {n.alive && n.role === "leader" && <Crown size={20} strokeWidth={2.5} />}
          {!n.alive && <Skull size={18} strokeWidth={2.5} />}
          NODE {n.id}
        </span>
        <span className="mono text-[10px] uppercase tracking-widest border-2 border-black px-1.5 py-0.5">
          {n.alive ? n.role : "down"}
        </span>
      </div>

      <div className="grid grid-cols-3 gap-2 mb-4">
        {[
          ["TERM", n.term],
          ["COMMIT", n.commitIndex],
          ["LOG", n.logLen],
        ].map(([label, val]) => (
          <div key={label as string} className="border-2 border-black px-2 py-1.5 bg-white/70 text-center">
            <div className="text-[9px] uppercase tracking-widest mono">{label}</div>
            <div className="mono text-lg font-bold">{val}</div>
          </div>
        ))}
      </div>

      {n.alive ? (
        <button
          onClick={onKill}
          className="brutal-btn bg-white px-3 py-1.5 text-xs w-full flex items-center justify-center gap-1.5 hover:bg-[#B23A2E] hover:text-white"
        >
          <Power size={14} /> Kill
        </button>
      ) : (
        <button
          onClick={onRestart}
          className="brutal-btn bg-white px-3 py-1.5 text-xs w-full flex items-center justify-center gap-1.5 hover:bg-[#4B7F52] hover:text-white"
        >
          <Power size={14} /> Restart
        </button>
      )}
    </div>
  );
}