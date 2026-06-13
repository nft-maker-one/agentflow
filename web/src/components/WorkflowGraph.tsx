import { useEffect, useMemo, useRef } from "react";
import ReactFlow, {
  Background,
  Controls,
  MarkerType,
  Position,
  ReactFlowProvider,
  useReactFlow,
  type Edge as RFEdge,
  type Node as RFNode,
} from "reactflow";
import type { GraphNode, GraphEdge } from "../lib/api";

/**
 * React Flow error handler.
 *
 * We never pass custom ``nodeTypes``/``edgeTypes`` (the default stable
 * module-level objects are used), so error #002 ("you've created a new
 * nodeTypes/edgeTypes object") can only fire as the documented React 18
 * ``<StrictMode>`` dev-only false-positive — StrictMode double-invokes
 * render and trips React Flow's internal shallow-key check. Swallow that
 * one code; forward everything else so real issues still surface.
 * Defined at module scope so the reference is stable across renders.
 */
function handleReactFlowError(code: string, message: string): void {
  if (code === "002") return;
  // eslint-disable-next-line no-console
  console.warn(`[React Flow] ${message}`);
}

interface Props {
  nodes: GraphNode[];
  edges: GraphEdge[];
  visitedNodes?: Set<string>;
  activeNodes?: Set<string>;
  descriptions?: Record<string, string>;
  /** Single-click — usually used for "select agent". */
  onNodeClick?: (nodeId: string) => void;
  /** Double-click — usually used for "open drawer / edit". */
  onNodeDoubleClick?: (nodeId: string) => void;
  /** Click handler for __start__ / __end__ / __error__ nodes. */
  onTerminalClick?: (kind: "start" | "end" | "error") => void;
  /** Run status — used to auto-refit the viewport when a run ends
   *  so the user sees the full topology again instead of being stuck
   *  on whatever zoom they were at during the live run. */
  runStatus?: string | null;
  /** The currently-selected agent (single-clicked). Drives a visual
   *  highlight ring. ``null`` means no selection. */
  selectedNodeId?: string | null;
  /** External I/O — rendered as virtual source/sink nodes
   *  flanking the regular __start__/__end__ nodes.  Topic-based
   *  edges are auto-synthesised against agent subscribe/publish. */
  externalSources?: ExtIOItem[];
  externalSinks?: ExtIOItem[];
  /** topic → list of agent template_keys subscribing to it.  Used
   *  to synthesise source→agent edges even when no IR edge has
   *  this topic as ``via``. */
  agentsBySubscribeTopic?: Record<string, string[]>;
  /** topic → list of agent template_keys publishing to it.  Used
   *  to synthesise agent→sink edges even when the topic isn't
   *  consumed by any downstream IR edge. */
  agentsByPublishTopic?: Record<string, string[]>;
}

/** A pared-down shape of ``ExternalIOItem`` from api.ts; we only
 *  need a few fields and avoid a hard cross-import to keep the
 *  graph component independent. */
interface ExtIOItem {
  name: string;
  kind: string;
  topic: string;
  health: { running: boolean; events_total: number; last_error: string | null };
}

/**
 * React Flow wrapper that renders the workflow DAG. The exported
 * component wraps the inner one in a ``ReactFlowProvider`` so we can
 * use ``useReactFlow`` to drive the viewport from effects.
 */
export function WorkflowGraph(props: Props) {
  return (
    <ReactFlowProvider>
      <WorkflowGraphInner {...props} />
    </ReactFlowProvider>
  );
}

function WorkflowGraphInner({
  nodes, edges,
  visitedNodes = new Set(),
  activeNodes = new Set(),
  descriptions = {},
  onNodeClick: onNodeClickProp,
  onNodeDoubleClick: onNodeDoubleClickProp,
  onTerminalClick,
  runStatus = null,
  selectedNodeId = null,
  externalSources = [],
  externalSinks = [],
  agentsBySubscribeTopic = {},
  agentsByPublishTopic = {},
}: Props) {
  const rf = useReactFlow();

  // ── Two-stage memo so the HIGHLIGHT (visited/active/selected) never
  // rebuilds the graph STRUCTURE. ──
  //
  // Stage 1 (`layout`) computes node POSITIONS + all edges. It depends
  // only on the topology, so its `position` objects keep a STABLE
  // reference across highlight changes. Stage 2 (`rfNodes`) re-styles
  // on highlight but REUSES those stable positions. Without this split,
  // every 200 ms run-highlight tick handed React Flow a wholesale-new
  // node array (new position objects), which — in controlled mode —
  // makes it flicker to blank and mis-fit onto the one active node.
  // (That was the "graph blanks during a run / only one node after" bug.)
  //
  // NOTE: this split alone is NOT enough — the caller must also pass
  // CONTENT-STABLE `visitedNodes`/`activeNodes` Sets (see WorkflowDetail).
  // If those are new refs every tick, `rfNodes` still rebuilds, React Flow
  // resets node measurement to 0×0, and the canvas goes blank (the
  // event-driven white-screen).
  const layout = useMemo(
    () => layoutGraph(
      nodes, edges, descriptions,
      externalSources, externalSinks,
      agentsBySubscribeTopic, agentsByPublishTopic,
    ),
    [nodes, edges, descriptions, externalSources, externalSinks,
     agentsBySubscribeTopic, agentsByPublishTopic],
  );

  const rfNodes = useMemo<RFNode[]>(
    () => layout.positioned.map((p) => ({
      id: p.id,
      data: { label: p.label, kind: p.kind },
      position: p.position,                  // STABLE ref — unchanged on highlight
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
      style: {
        ...nodeStyle(
          p.styleNode,
          p.highlightable && visitedNodes.has(p.id),
          p.highlightable && activeNodes.has(p.id),
          selectedNodeId === p.id,
        ),
        cursor: "pointer",
      },
      title: p.title,
      className: p.highlightable && activeNodes.has(p.id) ? "node-active" : "",
    } as RFNode)),
    [layout, visitedNodes, activeNodes, selectedNodeId],
  );

  const rfEdges = layout.rfEdges;   // edges never depend on highlight

  // ---- Manual double-click detection ----
  // ReactFlow's built-in ``onNodeDoubleClick`` is flaky in some
  // browsers/versions (the ``onNodeClick`` fires twice with no
  // ``onNodeDoubleClick`` ever being emitted). We do our own
  // detection: same node clicked twice within 400 ms = double-click.
  const lastClickRef = useRef<{ id: string; time: number } | null>(null);
  const DBL_CLICK_MS = 400;

  // Re-fit the viewport whenever the node set changes (so virtual
  // nodes don't get clipped after edits) AND whenever the run finishes
  // (so the user goes from "live partial view" back to "full DAG").
  const lastRunStatusRef = useRef<string | null>(null);
  useEffect(() => {
    const prev = lastRunStatusRef.current;
    const isTerminal =
      runStatus !== null &&
      runStatus !== prev &&
      runStatus !== "Running" &&
      runStatus !== "Pending";
    lastRunStatusRef.current = runStatus;

    // Defer one frame so React Flow has applied the new node positions.
    const id = window.setTimeout(() => {
      rf.fitView({ padding: 0.2, duration: isTerminal ? 350 : 0 });
    }, 50);
    return () => window.clearTimeout(id);
  }, [rf, rfNodes.length, rfEdges.length, runStatus]);

  return (
    <div className="h-[420px] border border-slate-200 rounded-lg bg-white relative">
      <ReactFlow
        nodes={rfNodes}
        edges={rfEdges}
        fitView
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable
        zoomOnDoubleClick={false}
        proOptions={{ hideAttribution: true }}
        onError={handleReactFlowError}
        onNodeClick={(_evt, node) => {
          const kind = node.data?.kind as string | undefined;
          // Built-in start/end/error get a single click handler
          // (no select/edit semantics).
          if (kind === "start" || kind === "end" || kind === "error") {
            if (onTerminalClick) onTerminalClick(kind);
            lastClickRef.current = null;
            return;
          }
          // Agent + ext_source + ext_sink all share the same
          // single = select / double = edit semantics.
          const isInteractive =
            kind === "agent" || kind === "ext_source" || kind === "ext_sink";
          if (!isInteractive) return;

          const now = Date.now();
          const last = lastClickRef.current;
          const isDouble = !!(
            last && last.id === node.id && now - last.time <= DBL_CLICK_MS
          );
          lastClickRef.current = isDouble ? null : { id: node.id, time: now };

          if (isDouble) {
            if (onNodeDoubleClickProp) onNodeDoubleClickProp(node.id);
          } else {
            if (onNodeClickProp) onNodeClickProp(node.id);
          }
        }}
      >
        <Background gap={16} size={1} color="#e2e8f0" />
        <Controls position="bottom-right" showInteractive={false} />
      </ReactFlow>

      {/* Manual "Reset view" button — top-right. Always available. */}
      <button
        type="button"
        onClick={() => rf.fitView({ padding: 0.2, duration: 300 })}
        className="absolute top-2 right-2 z-10 text-xs px-2 py-1
                   bg-white border border-slate-300 rounded shadow-sm
                   hover:bg-slate-50 text-slate-700"
        title="Reset zoom — show all nodes"
      >
        ⤢ Fit view
      </button>
    </div>
  );
}

// ----------------------------------------------------------------
// Trivial BFS layout — good enough for ≤ 30 nodes.
// ----------------------------------------------------------------

/** A node with a computed (stable) position, before highlight styling
 *  is applied. ``styleNode`` carries the minimal shape ``nodeStyle``
 *  needs; ``highlightable`` is false for external source/sink nodes
 *  (which are never part of run highlighting). */
interface PositionedNode {
  id: string;
  /** "start" | "end" | "error" | "agent" | "ext_source" | "ext_sink". */
  kind: string;
  label: string;
  title: string;
  position: { x: number; y: number };
  /** Minimal shape ``nodeStyle`` needs (it only reads ``kind``). */
  styleNode: { kind: string };
  highlightable: boolean;
}

function layoutGraph(
  nodes: GraphNode[],
  edges: GraphEdge[],
  descriptions: Record<string, string>,
  externalSources: ExtIOItem[],
  externalSinks: ExtIOItem[],
  agentsBySubscribeTopic: Record<string, string[]>,
  agentsByPublishTopic: Record<string, string[]>,
): { positioned: PositionedNode[]; rfEdges: RFEdge[] } {
  const layer = new Map<string, number>();
  const adj = new Map<string, string[]>();
  for (const e of edges) {
    if (!adj.has(e.source)) adj.set(e.source, []);
    adj.get(e.source)!.push(e.target);
  }

  const startId = nodes.find((n) => n.kind === "start")?.id;
  if (startId) {
    const queue: [string, number][] = [[startId, 0]];
    while (queue.length) {
      const [id, d] = queue.shift()!;
      if (layer.has(id) && layer.get(id)! <= d) continue;
      layer.set(id, d);
      for (const nxt of adj.get(id) ?? []) {
        if (!layer.has(nxt) || layer.get(nxt)! > d + 1) {
          queue.push([nxt, d + 1]);
        }
      }
    }
  }
  const maxLayer = Math.max(0, ...Array.from(layer.values()));
  for (const n of nodes) {
    if (!layer.has(n.id)) {
      layer.set(n.id, n.kind === "error" ? maxLayer + 1 : maxLayer);
    }
  }

  const layerCount = new Map<number, number>();
  const layerIndex = new Map<string, number>();
  for (const n of nodes) {
    const l = layer.get(n.id) ?? 0;
    const idx = layerCount.get(l) ?? 0;
    layerIndex.set(n.id, idx);
    layerCount.set(l, idx + 1);
  }

  const X_GAP = 220;
  const Y_GAP = 90;

  const positioned: PositionedNode[] = nodes.map((n) => {
    const l = layer.get(n.id) ?? 0;
    const idx = layerIndex.get(n.id) ?? 0;
    const total = layerCount.get(l) ?? 1;
    return {
      id: n.id,
      kind: n.kind,
      label: nodeLabel(n),
      title: descriptions[n.id] ?? "",
      position: {
        x: l * X_GAP,
        y: idx * Y_GAP - ((total - 1) * Y_GAP) / 2,
      },
      styleNode: n,
      highlightable: true,
    };
  });

  const rfEdges: RFEdge[] = edges.map((e) => {
    // Synthetic edges injected by the workflow_edit router so that an
    // agent fed only by an external source is reachable from __start__
    // (IR validation requirement). Mark them visually so users don't
    // mistake them for an explicit edge they themselves wired.
    const isSynthExtStart = e.id.startsWith("e_ext_start_");
    if (isSynthExtStart) {
      return {
        id: e.id,
        source: e.source,
        target: e.target,
        label: `↺ ${e.via ?? ""}`,
        labelStyle: { fontSize: 10, fill: "#94a3b8", fontStyle: "italic" },
        labelBgStyle: { fill: "#f8fafc", stroke: "#e2e8f0" },
        labelBgPadding: [3, 2],
        style: {
          stroke: "#cbd5e1",                  // slate-300
          strokeWidth: 1,
          strokeDasharray: "3 5",
          opacity: 0.6,
        },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: "#cbd5e1",
          width: 10,
          height: 10,
        },
      } as RFEdge;
    }
    return {
      id: e.id,
      source: e.source,
      target: e.target,
      label: e.case ? `${e.case}` : undefined,
      labelStyle: { fontSize: 11, fill: "#64748b" },
      labelBgPadding: [4, 2],
      labelBgStyle: { fill: "#f8fafc", stroke: "#e2e8f0" },
      style: e.target === "__error__"
        ? {
            // Error edges are auto-injected fallback channels — dim them
            // so they don't visually compete with the happy path.
            stroke: "#e2e8f0",                    // slate-200
            strokeWidth: 1,
            strokeDasharray: "2 4",
            opacity: 0.7,
          }
        : {
            stroke: e.is_switch ? "#7c3aed" : "#94a3b8",
            strokeWidth: 1.5,
            strokeDasharray: e.is_switch ? "4 3" : undefined,
          },
      markerEnd: e.target === "__error__"
        ? {
            type: MarkerType.ArrowClosed,
            color: "#e2e8f0",
            width: 10,
            height: 10,
          }
        : {
            type: MarkerType.ArrowClosed,
            color: e.is_switch ? "#7c3aed" : "#94a3b8",
            width: 14,
            height: 14,
          },
    } as RFEdge;
  });

  // ----------------------------------------------------------------
  // External I/O virtual nodes & synthesised edges.
  //
  // Sources sit ABOVE __start__ on the same x-coordinate (layer 0
  // moved down by total/2 above), sinks sit AFTER the last layer.
  // We don't merge into the BFS layout — they're just placed on the
  // far ends so the graph stays readable.
  // ----------------------------------------------------------------

  // topic → [agent_key,...] maps. We prefer the parent-supplied
  // maps (built from the authoritative agent.subscribe / .publish
  // arrays in WorkflowDetail) and FALL BACK to edge-derived ones for
  // older callers. Without this, a sink whose topic has no downstream
  // IR consumer wouldn't get its incoming edge drawn.
  const agentsBySubscribe = new Map<string, string[]>(
    Object.entries(agentsBySubscribeTopic),
  );
  const agentsByPublish = new Map<string, string[]>(
    Object.entries(agentsByPublishTopic),
  );
  for (const e of edges) {
    if (e.via && e.target && e.target !== "__error__"
        && !agentsBySubscribe.has(e.via)) {
      agentsBySubscribe.set(e.via, [e.target]);
    }
    if (e.via && e.source && !agentsByPublish.has(e.via)) {
      agentsByPublish.set(e.via, [e.source]);
    }
  }

  // Source nodes — placed at x = -X_GAP, fanning vertically.
  externalSources.forEach((s, idx) => {
    const id = `__ext_source__${s.name}`;
    const total = externalSources.length;
    positioned.push({
      id,
      kind: "ext_source",
      label: `🔌 ${s.name}`,
      title: `External Source · ${s.kind}\nTopic: ${s.topic}\nClick to select · double-click to edit · Delete to remove`,
      position: {
        x: -X_GAP,
        y: idx * Y_GAP - ((total - 1) * Y_GAP) / 2,
      },
      styleNode: { kind: "ext_source" },
      highlightable: false,
    });

    // Edges from this source to any agent that subscribes to s.topic.
    const targets = agentsBySubscribe.get(s.topic) ?? [];
    for (const t of targets) {
      rfEdges.push({
        id: `__ext_src_edge__${s.name}__${t}`,
        source: id,
        target: t,
        label: s.topic,
        labelStyle: { fontSize: 10, fill: "#7c3aed", fontStyle: "italic" },
        labelBgStyle: { fill: "#f5f3ff", stroke: "#ddd6fe" },
        labelBgPadding: [3, 2],
        style: { stroke: "#7c3aed", strokeWidth: 1.5, strokeDasharray: "5 4" },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: "#7c3aed",
          width: 12,
          height: 12,
        },
      });
    }
  });

  // Sink nodes — placed at x = (maxLayer+2)*X_GAP, fanning vertically.
  const sinkX = (maxLayer + 2) * X_GAP;
  externalSinks.forEach((s, idx) => {
    const id = `__ext_sink__${s.name}`;
    const total = externalSinks.length;
    positioned.push({
      id,
      kind: "ext_sink",
      label: `📤 ${s.name}`,
      title: `External Sink · ${s.kind}\nTopic: ${s.topic}\nClick to select · double-click to edit · Delete to remove`,
      position: {
        x: sinkX,
        y: idx * Y_GAP - ((total - 1) * Y_GAP) / 2,
      },
      styleNode: { kind: "ext_sink" },
      highlightable: false,
    });

    // Edges from any agent that publishes to s.topic into this sink.
    const sources = agentsByPublish.get(s.topic) ?? [];
    for (const a of sources) {
      if (a === "__start__" || a === "__end__" || a === "__error__") continue;
      rfEdges.push({
        id: `__ext_sink_edge__${a}__${s.name}`,
        source: a,
        target: id,
        label: s.topic,
        labelStyle: { fontSize: 10, fill: "#d97706", fontStyle: "italic" },
        labelBgStyle: { fill: "#fffbeb", stroke: "#fde68a" },
        labelBgPadding: [3, 2],
        style: { stroke: "#d97706", strokeWidth: 1.5, strokeDasharray: "5 4" },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: "#d97706",
          width: 12,
          height: 12,
        },
      });
    }
  });

  return { positioned, rfEdges };
}

function nodeLabel(n: GraphNode): string {
  if (n.kind === "agent") return n.label;
  if (n.kind === "start") return "▶ Start";
  if (n.kind === "end") return "■ End";
  if ((n.kind as string) === "ext_source") return n.label;
  if ((n.kind as string) === "ext_sink") return n.label;
  return "✗ Error";
}

function nodeStyle(
  n: { kind: string }, visited: boolean, active: boolean, selected: boolean = false,
): React.CSSProperties {
  const baseStyle: React.CSSProperties = {
    // Use longhand border props only — never mix the `border` shorthand
    // with `borderWidth`/`borderColor` longhands. Some branches below
    // override `borderWidth` (active/selected) and every branch sets
    // `borderColor`; keeping these as longhands that are ALWAYS present
    // avoids React's "removing borderWidth while border is set" warning.
    borderStyle: "solid",
    borderWidth: "1.5px",
    borderColor: "#cbd5e1",   // default; overridden per-kind below
    borderRadius: 8,
    padding: "10px 14px",
    fontSize: 13,
    fontWeight: 500,
    minWidth: 110,
    textAlign: "center",
  };
  if ((n.kind as string) === "ext_source") {
    baseStyle.background = "#7c3aed";   // violet-600
    baseStyle.color = "#fff";
    baseStyle.borderColor = "#5b21b6";  // violet-800
  } else if ((n.kind as string) === "ext_sink") {
    baseStyle.background = "#d97706";   // amber-600
    baseStyle.color = "#fff";
    baseStyle.borderColor = "#92400e";  // amber-800
  } else if (n.kind === "start") {
    baseStyle.background = "#334155";
    baseStyle.color = "#fff";
    baseStyle.borderColor = "#1e293b";
  } else if (n.kind === "end") {
    baseStyle.background = "#059669";
    baseStyle.color = "#fff";
    baseStyle.borderColor = "#047857";
  } else if (n.kind === "error") {
    baseStyle.background = "#dc2626";
    baseStyle.color = "#fff";
    baseStyle.borderColor = "#b91c1c";
  } else {
    // Agent
    if (active) {
      baseStyle.background = "#dbeafe";       // blue-100
      baseStyle.color = "#0f172a";
      baseStyle.borderColor = "#3b82f6";      // blue-500
      baseStyle.borderWidth = "2px";
    } else if (visited) {
      baseStyle.background = "#ecfeff";       // cyan-50
      baseStyle.color = "#0f172a";
      baseStyle.borderColor = "#06b6d4";      // cyan-500
    } else {
      baseStyle.background = "#ffffff";
      baseStyle.color = "#0f172a";
      baseStyle.borderColor = "#cbd5e1";
    }
  }
  // Selection ring layered ON TOP of any of the above.
  if (selected) {
    baseStyle.boxShadow =
      "0 0 0 3px rgba(99, 102, 241, 0.55), " +
      "0 4px 14px rgba(99, 102, 241, 0.35)";
    baseStyle.borderColor = "#6366f1";   // indigo-500
    baseStyle.borderWidth = "2px";
  }
  return baseStyle;
}
