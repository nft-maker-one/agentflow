import { Component, type ErrorInfo, type ReactNode } from "react";

interface Props {
  children: ReactNode;
  /** Changing this value resets the boundary (we key it on the route
   *  pathname so navigating away from a broken page recovers without a
   *  full page reload). */
  resetKey?: string;
}

interface State {
  error: Error | null;
  info: ErrorInfo | null;
}

/**
 * App-level error boundary.
 *
 * Without one, a single render throw anywhere in the tree propagates to
 * the React root and unmounts the WHOLE app — the dreaded blank white
 * screen. This was observable on the live-run views: a transient render
 * error during the high-frequency event re-render (e.g. when an agent's
 * terminal `*.out` event arrives) would take the entire console down.
 *
 * With this boundary the failure is contained to the routed page, the
 * actual error + component stack are shown (so the root cause is visible
 * instead of guessed at), and the rest of the shell (header, inbox,
 * navigation) keeps working. Navigating to another route clears it.
 */
export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null, info: null };

  static getDerivedStateFromError(error: Error): Partial<State> {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    this.setState({ info });
    // Surface to the console with the component stack for debugging.
    // eslint-disable-next-line no-console
    console.error("[ErrorBoundary] render error:", error, info.componentStack);
  }

  componentDidUpdate(prev: Props): void {
    // Reset when the route changes so leaving the broken page recovers.
    if (this.state.error && prev.resetKey !== this.props.resetKey) {
      this.setState({ error: null, info: null });
    }
  }

  render(): ReactNode {
    const { error, info } = this.state;
    if (!error) return this.props.children;

    return (
      <div className="max-w-2xl mx-auto my-12">
        <div className="bg-white border border-rose-200 rounded-lg overflow-hidden">
          <div className="px-5 py-4 border-b border-rose-100 bg-rose-50">
            <div className="text-xs text-rose-500 uppercase tracking-wide">
              Something went wrong on this page
            </div>
            <div className="font-mono text-sm font-semibold text-rose-900 mt-0.5 break-words">
              {error.name}: {error.message}
            </div>
          </div>
          <div className="px-5 py-4 space-y-3">
            <p className="text-sm text-slate-600">
              The rest of the console is still working — only this view
              failed to render. You can retry, or go back to your workflows.
            </p>
            {info?.componentStack && (
              <details className="text-xs">
                <summary className="cursor-pointer text-slate-500 hover:text-slate-800">
                  Component stack
                </summary>
                <pre className="mt-1.5 bg-slate-50 rounded p-2 overflow-x-auto thin-scroll text-[11px] text-slate-600">
                  {info.componentStack}
                </pre>
              </details>
            )}
            <div className="flex gap-2 pt-1">
              <button
                type="button"
                onClick={() => this.setState({ error: null, info: null })}
                className="text-xs px-3 py-1.5 bg-slate-700 hover:bg-slate-900 text-white rounded"
              >
                Retry
              </button>
              <a
                href="/"
                className="text-xs px-3 py-1.5 bg-white border border-slate-300 hover:bg-slate-50 text-slate-700 rounded"
              >
                Back to workflows
              </a>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
