import { useEffect, useRef, useState } from "react";
import { Link, Route, Routes, useLocation } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import WorkflowsPage from "./pages/Workflows";
import WorkflowDetailPage from "./pages/WorkflowDetail";
import RunDetailPage from "./pages/RunDetail";
import SettingsPage from "./pages/Settings";
import { InboxPanel } from "./components/InboxPanel";
import { ErrorBoundary } from "./components/ErrorBoundary";
import { getHealth, listInbox } from "./lib/api";

export default function App() {
  const [inboxOpen, setInboxOpen] = useState(false);
  const { pathname } = useLocation();
  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <Header onOpenInbox={() => setInboxOpen(true)} />
      <InboxPanel open={inboxOpen} onClose={() => setInboxOpen(false)} />
      <main className="max-w-6xl mx-auto px-6 py-8">
        {/* A render error in any routed page is contained here instead of
            white-screening the whole app; keyed on the route so navigating
            away clears it. */}
        <ErrorBoundary resetKey={pathname}>
          <Routes>
            <Route path="/" element={<WorkflowsPage />} />
            <Route path="/workflows/:id" element={<WorkflowDetailPage />} />
            <Route path="/runs/:id" element={<RunDetailPage />} />
            <Route path="/settings" element={<SettingsPage />} />
            <Route path="*" element={<NotFound />} />
          </Routes>
        </ErrorBoundary>
      </main>
    </div>
  );
}

function Header({ onOpenInbox }: { onOpenInbox: () => void }) {
  const { data: health } = useQuery({
    queryKey: ["health"],
    queryFn: getHealth,
    refetchInterval: 5_000,
  });

  // Background poll the inbox so we can show the unread badge AND
  // chime when new items arrive — independent of whether the user
  // has opened the InboxPanel.
  const { data: inbox } = useQuery({
    queryKey: ["inbox-headpoll"],
    queryFn: () => listInbox({ limit: 50 }),
    refetchInterval: 3_000,
  });
  const unread = inbox?.unread ?? 0;
  const lastUnreadRef = useRef<number>(unread);
  useEffect(() => {
    if (unread > lastUnreadRef.current) {
      // New items arrived → play a tiny beep.
      void playChime();
    }
    lastUnreadRef.current = unread;
  }, [unread]);

  return (
    <header className="bg-white border-b border-slate-200">
      <div className="max-w-6xl mx-auto px-6 py-3 flex items-center justify-between">
        <Link to="/" className="flex items-center gap-2">
          <div className="w-7 h-7 bg-gradient-to-br from-blue-500 to-violet-500
                          rounded-md grid place-items-center text-white font-bold">
            A
          </div>
          <span className="font-semibold text-lg tracking-tight">
            AgentKit Console
          </span>
        </Link>
        <div className="text-xs text-slate-500 tabular-nums flex items-center gap-3">
          {health ? (
            <>
              <span
                className={
                  health.ok
                    ? "h-2 w-2 rounded-full bg-emerald-500"
                    : "h-2 w-2 rounded-full bg-rose-500"
                }
              />
              <span>v{health.version}</span>
              <span>·</span>
              <span>{health.deployed_workflows.length} workflows</span>
            </>
          ) : (
            <span className="text-slate-400">connecting…</span>
          )}
          <button
            onClick={onOpenInbox}
            className="relative text-slate-500 hover:text-slate-900 ml-2"
            title={unread > 0 ? `${unread} unread notifications` : "Inbox"}
          >
            📬
            {unread > 0 && (
              <span className="absolute -top-1 -right-2 bg-rose-500 text-white
                                text-[9px] font-bold rounded-full px-1 min-w-[14px]
                                h-[14px] flex items-center justify-center
                                tabular-nums leading-none">
                {unread > 99 ? "99+" : unread}
              </span>
            )}
          </button>
          <Link
            to="/settings"
            className="text-slate-500 hover:text-slate-900 ml-2"
            title="System settings"
          >
            ⚙ Settings
          </Link>
        </div>
      </div>
    </header>
  );
}

/** Tiny synth chime via Web Audio API — no asset to fetch / load.
 *  A two-note up-step (~150ms total). Fails silently in browsers
 *  without AudioContext (or when the user hasn't interacted yet —
 *  modern browsers gate audio behind a user gesture). */
async function playChime(): Promise<void> {
  try {
    type AC = typeof AudioContext;
    const Ctor: AC | undefined =
      (window as unknown as { AudioContext?: AC }).AudioContext
      ?? (window as unknown as { webkitAudioContext?: AC }).webkitAudioContext;
    if (!Ctor) return;
    const ctx = new Ctor();
    const note = (freq: number, start: number, dur: number) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = "sine";
      osc.frequency.value = freq;
      gain.gain.setValueAtTime(0, ctx.currentTime + start);
      gain.gain.linearRampToValueAtTime(0.18, ctx.currentTime + start + 0.01);
      gain.gain.exponentialRampToValueAtTime(
        0.001, ctx.currentTime + start + dur,
      );
      osc.connect(gain).connect(ctx.destination);
      osc.start(ctx.currentTime + start);
      osc.stop(ctx.currentTime + start + dur);
    };
    note(660, 0, 0.10);     // E5
    note(880, 0.08, 0.12);  // A5
    setTimeout(() => ctx.close().catch(() => {}), 600);
  } catch {
    // ignore — no sound is fine
  }
}

function NotFound() {
  return (
    <div className="text-center text-slate-500 py-20">
      <p className="text-lg">Not found.</p>
      <Link to="/" className="text-blue-600 hover:underline">
        Back home
      </Link>
    </div>
  );
}
