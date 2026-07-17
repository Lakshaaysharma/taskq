"use client";

import { useCallback, useEffect, useState } from "react";

interface QueueSummary {
  name: string;
  pending: number;
  scheduled: number;
  active: number;
  completed: number;
  dead: number;
  paused: boolean;
}

const STATES = ["pending", "scheduled", "active", "completed", "dead"] as const;
const REFRESH_MS = 2000;

export default function Dashboard() {
  const [queues, setQueues] = useState<QueueSummary[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [lastOk, setLastOk] = useState<number>(0);
  const [busy, setBusy] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const res = await fetch("/api/queues", { cache: "no-store" });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error ?? "request failed");
      setQueues(data.queues);
      setError(null);
      setLastOk(Date.now());
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to load");
    }
  }, []);

  useEffect(() => {
    load();
    const t = setInterval(load, REFRESH_MS);
    return () => clearInterval(t);
  }, [load]);

  async function togglePause(q: QueueSummary) {
    setBusy(q.name);
    await fetch(`/api/queues/${encodeURIComponent(q.name)}/pause`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ paused: !q.paused }),
    });
    await load();
    setBusy(null);
  }

  async function requeueDead(q: QueueSummary) {
    setBusy(q.name);
    await fetch(`/api/queues/${encodeURIComponent(q.name)}/requeue`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ from: "dead" }),
    });
    await load();
    setBusy(null);
  }

  const totals = STATES.reduce(
    (acc, s) => {
      acc[s] = queues.reduce((sum, q) => sum + (q[s] as number), 0);
      return acc;
    },
    {} as Record<string, number>,
  );

  const stale = lastOk > 0 && Date.now() - lastOk > REFRESH_MS * 3;

  return (
    <div className="wrap">
      <div className="header">
        <div className="logo">
          task<span className="q">q</span>
        </div>
        <div className="status">
          <span className={`dot${stale || error ? " stale" : ""}`} />
          {error ? "connection error" : "live"}
        </div>
      </div>
      <div className="subtitle">
        Real-time monitoring for the taskq task queue · refreshes every{" "}
        {REFRESH_MS / 1000}s
      </div>

      {error && <div className="error">Cannot reach Redis: {error}</div>}

      <div className="totals">
        {STATES.map((s) => (
          <div className="stat" key={s}>
            <div className="n">{totals[s] ?? 0}</div>
            <div className="l">{s}</div>
          </div>
        ))}
      </div>

      {queues.length === 0 && !error ? (
        <div className="empty">
          No queues yet. Enqueue a task from your C++ app to see it here.
        </div>
      ) : (
        queues.map((q) => (
          <div className="card" key={q.name}>
            <div className="card-head">
              <div className="qname">
                {q.name}
                {q.paused && <span className="badge paused">paused</span>}
              </div>
              <div className="actions">
                <button
                  disabled={busy === q.name}
                  onClick={() => togglePause(q)}
                >
                  {q.paused ? "Resume" : "Pause"}
                </button>
                <button
                  className="danger"
                  disabled={busy === q.name || q.dead === 0}
                  onClick={() => requeueDead(q)}
                >
                  Requeue dead ({q.dead})
                </button>
              </div>
            </div>
            <div className="counts">
              {STATES.map((s) => (
                <div className={`count ${s}`} key={s}>
                  <span className="v">{q[s] as number}</span>
                  <span className="k">{s}</span>
                </div>
              ))}
            </div>
          </div>
        ))
      )}

      <div className="footer">
        taskq · header-only Redis-backed task queue for C++17
      </div>
    </div>
  );
}
