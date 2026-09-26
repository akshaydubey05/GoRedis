import { useState, useRef, useEffect } from "react";
import { API } from "./useClusterStream";

type Line = { text: string; kind: "cmd" | "ok" | "err" };

export function Terminal() {
  const inputRef = useRef<HTMLInputElement>(null);
  const [lines, setLines] = useState<Line[]>([
    { text: "GoRedis Cluster terminal — type a command below", kind: "ok" },
    { text: "try: SET name akshay | GET name | INCR counter", kind: "ok" },
  ]);
  const [input, setInput] = useState("");
  const [history, setHistory] = useState<string[]>([]);
  const [histIdx, setHistIdx] = useState(-1);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [lines]);

  async function run(cmd: string) {
    if (!cmd.trim()) return;
    setLines((l) => [...l, { text: `> ${cmd}`, kind: "cmd" }]);
    setHistory((h) => [cmd, ...h]);
    setHistIdx(-1);

    const t0 = performance.now();
    try {
      const res = await fetch(`${API}/api/command`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ cmd }),
      });
      const data = await res.json();
      const ms = Math.round(performance.now() - t0);
      setLines((l) => [
        ...l,
        {
          text: `${data.reply}  (N${data.handledBy} · ${ms}ms)`,
          kind: data.kind === "err" ? "err" : "ok",
        },
      ]);
    } catch {
      setLines((l) => [...l, { text: "connection error — is the gateway running?", kind: "err" }]);
    }
  }

  function onKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "Enter") {
      run(input);
      setInput("");
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      const next = Math.min(histIdx + 1, history.length - 1);
      setHistIdx(next);
      if (history[next]) setInput(history[next]);
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      const next = Math.max(histIdx - 1, -1);
      setHistIdx(next);
      setInput(next === -1 ? "" : history[next]);
    }
  }

  return (
    <div
    className="brutal-box p-4 bg-black text-[#4B7F52] scanlines flex flex-col h-full"
    style={{ position: "relative" }}
    >
      <div className="mono text-xs uppercase tracking-widest text-white mb-2 border-b-2 border-[#4B7F52] pb-2">
        &gt;_ Terminal
      </div>
      <div className="flex-1 overflow-y-auto mono text-sm space-y-1 mb-3 min-h-[180px] max-h-[260px]">
        {lines.map((l, i) => (
          <div
            key={i}
            className={
              l.kind === "cmd" ? "text-white" : l.kind === "err" ? "text-[#D9481F]" : "text-[#4B7F52]"
            }
          >
            {l.text}
          </div>
        ))}
        <div ref={bottomRef} />
      </div>
      <div className="flex items-center gap-2 border-t-2 border-[#4B7F52] pt-2">
        <span className="mono text-black">$</span>
            <input
                ref={inputRef}
                type="text"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={onKeyDown}
                placeholder="... command here"
                autoComplete="off"
                spellCheck={false}
                className="terminal-input flex-1 outline-none border-none bg-transparent mono"
                style={{
                    color: "#4B7F52",
                    WebkitTextFillColor: "#4B7F52",
                    backgroundColor: "transparent",
                    fontFamily: "'JetBrains Mono', monospace",
                    fontSize: "14px",
                    fontWeight: 500,
                    lineHeight: "20px",
                    opacity: 1,
                    caretColor: "#4B7F52",
                    position: "relative",
                    zIndex: 100,
                }}
            />
        <span className="mono text-white cursor-blink">▌</span>
      </div>
    </div>
  );
}