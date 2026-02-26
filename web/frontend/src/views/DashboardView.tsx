import { useState, useCallback } from "react";
import GraphView from "./GraphView";
import RiverView from "./RiverView";
import HeatmapView from "./HeatmapView";
import TimelineView from "./TimelineView";
import GeoView from "./GeoView";
import CompareView from "./CompareView";
import SearchView from "./SearchView";

const VIEW_OPTIONS = [
  { id: "graph", label: "Graph" },
  { id: "river", label: "River" },
  { id: "heatmap", label: "Heatmap" },
  { id: "timeline", label: "Timeline" },
  { id: "geo", label: "Regions" },
  { id: "compare", label: "Compare" },
  { id: "search", label: "Search" },
] as const;

type ViewId = (typeof VIEW_OPTIONS)[number]["id"];

function renderView(id: ViewId) {
  switch (id) {
    case "graph": return <GraphView />;
    case "river": return <RiverView />;
    case "heatmap": return <HeatmapView />;
    case "timeline": return <TimelineView />;
    case "geo": return <GeoView />;
    case "compare": return <CompareView />;
    case "search": return <SearchView />;
  }
}

const DEFAULT_PANES: ViewId[] = ["graph", "heatmap", "timeline", "geo"];

export default function DashboardView() {
  const [panes, setPanes] = useState<ViewId[]>(DEFAULT_PANES);
  const [expanded, setExpanded] = useState<number | null>(null);

  const changePane = useCallback((index: number, viewId: ViewId) => {
    setPanes((prev) => {
      const next = [...prev];
      next[index] = viewId;
      return next;
    });
  }, []);

  const toggleExpand = useCallback((index: number) => {
    setExpanded((prev) => (prev === index ? null : index));
  }, []);

  if (expanded !== null) {
    const viewId = panes[expanded];
    return (
      <div className="dashboard-expanded">
        <div className="dashboard-pane-header">
          <select
            value={viewId}
            onChange={(e) => changePane(expanded, e.target.value as ViewId)}
            className="pane-select"
          >
            {VIEW_OPTIONS.map((v) => (
              <option key={v.id} value={v.id}>{v.label}</option>
            ))}
          </select>
          <button className="pane-btn" onClick={() => setExpanded(null)} title="Back to grid">
            ⊞
          </button>
        </div>
        <div className="dashboard-pane-content">
          {renderView(viewId)}
        </div>
      </div>
    );
  }

  return (
    <div className="dashboard-grid">
      {panes.map((viewId, i) => (
        <div key={i} className="dashboard-pane">
          <div className="dashboard-pane-header">
            <select
              value={viewId}
              onChange={(e) => changePane(i, e.target.value as ViewId)}
              className="pane-select"
            >
              {VIEW_OPTIONS.map((v) => (
                <option key={v.id} value={v.id}>{v.label}</option>
              ))}
            </select>
            <button className="pane-btn" onClick={() => toggleExpand(i)} title="Expand">
              ⤢
            </button>
          </div>
          <div className="dashboard-pane-content">
            {renderView(viewId)}
          </div>
        </div>
      ))}
    </div>
  );
}
