import { useEffect, useState, useCallback, useRef, Component, type ReactNode } from "react";
import ForceGraph2D from "react-force-graph-2d";
import { useFilters } from "../context/FilterContext";
import {
  fetchEntityGraph,
  fetchEntityEgo,
  fetchEntityArticles,
  fetchCooccurrenceArticles,
  type GraphData,
  type Article,
} from "../api/client";
import ArticlePanel from "../components/ArticlePanel";

class GraphErrorBoundary extends Component<{ children: ReactNode }, { error: string | null }> {
  state = { error: null as string | null };
  static getDerivedStateFromError(err: Error) { return { error: err.message }; }
  render() {
    if (this.state.error) return <div style={{ color: "#e57373", padding: 20 }}>Graph error: {this.state.error}</div>;
    return this.props.children;
  }
}

const TYPE_COLORS: Record<string, string> = {
  person: "#4fc3f7",
  organisation: "#81c784",
  place: "#e57373",
  event: "#ffb74d",
  work: "#ba68c8",
  product: "#f06292",
  concept: "#90a4ae",
  technology: "#4dd0e1",
  species: "#aed581",
  substance: "#fff176",
  legislation: "#a1887f",
  statistic: "#78909c",
  medical_condition: "#ef5350",
};

interface PanelState {
  title: string;
  articles: Article[];
}

export default function GraphView() {
  const filters = useFilters();
  const [graphData, setGraphData] = useState<GraphData>({ nodes: [], edges: [] });
  const [loading, setLoading] = useState(true);
  const [panel, setPanel] = useState<PanelState | null>(null);
  const [query, setQuery] = useState("");
  const [searchLabel, setSearchLabel] = useState<string | null>(null);
  const graphRef = useRef<any>(null);

  const loadGraph = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchEntityGraph({
        time_from: filters.timeFrom || undefined,
        time_to: filters.timeTo || undefined,
        entity_type: filters.entityType || undefined,
        min_cooccurrence: filters.minCooccurrence,
        min_articles: filters.minArticles,
        limit: 150,
      });
      setGraphData({
        nodes: data.nodes,
        edges: data.edges.map((e) => ({
          ...e,
          source: e.source,
          target: e.target,
        })),
      });
    } catch (err) {
      console.error("Failed to load graph:", err);
    } finally {
      setLoading(false);
    }
  }, [filters.timeFrom, filters.timeTo, filters.entityType, filters.minCooccurrence, filters.minArticles]);

  useEffect(() => {
    if (!searchLabel) loadGraph();
  }, [loadGraph, searchLabel]);

  const handleSubmit = useCallback(async () => {
    const q = query.trim();
    if (!q) return;
    setLoading(true);
    try {
      // Fetch ego graph for this entity from the backend
      const data = await fetchEntityEgo(q, {
        time_from: filters.timeFrom || undefined,
        time_to: filters.timeTo || undefined,
        entity_type: filters.entityType || undefined,
        limit: 50,
      });
      if (data.nodes.length === 0) {
        // No entity found — reload default graph
        setSearchLabel(null);
        await loadGraph();
        return;
      }
      setGraphData({
        nodes: data.nodes,
        edges: data.edges.map((e) => ({
          ...e,
          source: e.source,
          target: e.target,
        })),
      });
      setSearchLabel(q);
      setTimeout(() => graphRef.current?.zoomToFit(400, 40), 200);
    } catch (err) {
      console.error("Search failed:", err);
    } finally {
      setLoading(false);
    }
  }, [query, filters.timeFrom, filters.timeTo, filters.entityType, loadGraph]);

  const handleClear = useCallback(() => {
    setQuery("");
    setSearchLabel(null);
    setSelectedNode(null);
    loadGraph();
  }, [loadGraph]);

  const handleNodeClick = useCallback(
    async (node: any) => {
      setPanel({ title: `Loading articles for "${node.name}"...`, articles: [] });
      try {
        const articles = await fetchEntityArticles(node.name, {
          time_from: filters.timeFrom || undefined,
          time_to: filters.timeTo || undefined,
        });
        setPanel({ title: `${node.name} (${articles.length} articles)`, articles });
      } catch (err) {
        console.error(err);
        setPanel({ title: `Error loading articles for "${node.name}"`, articles: [] });
      }
    },
    [filters.timeFrom, filters.timeTo]
  );

  const handleLinkClick = useCallback(
    async (link: any) => {
      const sourceName = typeof link.source === "object" ? link.source.id : link.source;
      const targetName = typeof link.target === "object" ? link.target.id : link.target;
      setPanel({ title: `Loading...`, articles: [] });
      try {
        const articles = await fetchCooccurrenceArticles(sourceName, targetName, {
          time_from: filters.timeFrom || undefined,
          time_to: filters.timeTo || undefined,
        });
        setPanel({
          title: `${sourceName} + ${targetName} (${articles.length} articles)`,
          articles,
        });
      } catch (err) {
        console.error(err);
        setPanel({ title: "Error loading articles", articles: [] });
      }
    },
    [filters.timeFrom, filters.timeTo]
  );

  const [selectedNode, setSelectedNode] = useState<string | null>(null);

  const maxArticles = Math.max(...graphData.nodes.map((n) => n.article_count), 10);

  const handleZoom = useCallback((delta: number) => {
    if (!graphRef.current) return;
    const currentZoom = graphRef.current.zoom();
    graphRef.current.zoom(currentZoom * delta, 300);
  }, []);

  const handleFitToView = useCallback(() => {
    if (!graphRef.current) return;
    graphRef.current.zoomToFit(400, 40);
  }, []);

  return (
    <div className="graph-view">
      <div className="graph-toolbar">
        <input
          type="text"
          placeholder='Search entity: e.g. "Pocock", "climate", "NHS"'
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleSubmit()}
          className="search-input nl-input"
          disabled={loading}
        />
        <button onClick={handleSubmit} className="search-btn" disabled={loading}>
          {loading ? "..." : "Search"}
        </button>
        {searchLabel && (
          <button onClick={handleClear} className="search-btn search-btn-find">Show all</button>
        )}
        <div className="zoom-controls">
          <button onClick={() => handleZoom(1.5)} className="zoom-btn" title="Zoom in">+</button>
          <button onClick={() => handleZoom(1 / 1.5)} className="zoom-btn" title="Zoom out">−</button>
          <button onClick={handleFitToView} className="zoom-btn zoom-fit" title="Fit to view">⊞</button>
        </div>
        <span className="node-count">
          {loading ? "Loading..." : `${graphData.nodes.length} entities, ${graphData.edges.length} connections`}
        </span>
      </div>
      {searchLabel && (
        <div className="nl-pills">
          <span className="nl-pill pill-topic">entity: {searchLabel}</span>
          <button onClick={handleClear} className="nl-clear" title="Clear search">x</button>
        </div>
      )}
      <div className="graph-container">
        <GraphErrorBoundary>
          {!loading && graphData.nodes.length > 0 && (
            <ForceGraph2D
              ref={graphRef}
              graphData={{
                nodes: graphData.nodes as any[],
                links: graphData.edges as any[],
              }}
              nodeId="id"
              nodeLabel={(node: any) => `${node.name} (${node.type}) — ${node.article_count} articles`}
              nodeColor={(node: any) => TYPE_COLORS[node.type] || "#999"}
              nodeVal={(node: any) => Math.max(2, Math.min(20, (node.article_count / maxArticles) * 15))}
              linkWidth={(link: any) => {
                if (!selectedNode) return Math.max(0.5, Math.log2(link.weight));
                const src = typeof link.source === "object" ? link.source.id : link.source;
                const tgt = typeof link.target === "object" ? link.target.id : link.target;
                return (src === selectedNode || tgt === selectedNode)
                  ? Math.max(2, Math.log2(link.weight) * 2)
                  : Math.max(0.3, Math.log2(link.weight) * 0.5);
              }}
              linkColor={(link: any) => {
                if (!selectedNode) return "rgba(255,255,255,0.15)";
                const src = typeof link.source === "object" ? link.source.id : link.source;
                const tgt = typeof link.target === "object" ? link.target.id : link.target;
                return (src === selectedNode || tgt === selectedNode)
                  ? "rgba(79,195,247,0.6)"
                  : "rgba(255,255,255,0.05)";
              }}
              onNodeClick={(node: any) => {
                setSelectedNode(node.id);
                handleNodeClick(node);
                if (graphRef.current) {
                  graphRef.current.centerAt(node.x, node.y, 600);
                  graphRef.current.zoom(3, 600);
                }
              }}
              onLinkClick={handleLinkClick}
              backgroundColor="#1a1a2e"
              nodeCanvasObject={(node: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
                const size = Math.max(3, Math.min(14, (node.article_count / maxArticles) * 12));
                const baseColor = TYPE_COLORS[node.type] || "#999";
                const isSelected = node.id === selectedNode;

                const isNeighbor = selectedNode && graphData.edges.some((e) => {
                  const src = typeof e.source === "object" ? (e.source as any).id : e.source;
                  const tgt = typeof e.target === "object" ? (e.target as any).id : e.target;
                  return (src === selectedNode && tgt === node.id) || (tgt === selectedNode && src === node.id);
                });

                const dimmed = selectedNode && !isSelected && !isNeighbor;
                const color = dimmed ? "rgba(100,100,100,0.3)" : baseColor;

                if (isSelected) {
                  ctx.beginPath();
                  ctx.arc(node.x, node.y, size + 3, 0, 2 * Math.PI);
                  ctx.strokeStyle = "#fff";
                  ctx.lineWidth = 2;
                  ctx.stroke();
                }

                ctx.beginPath();
                ctx.arc(node.x, node.y, size, 0, 2 * Math.PI);
                ctx.fillStyle = color;
                ctx.fill();

                const showLabel = isSelected || isNeighbor || globalScale > 1.5 || node.article_count > maxArticles * 0.1;
                if (showLabel && !dimmed) {
                  ctx.font = `${Math.max(3, 12 / globalScale)}px sans-serif`;
                  ctx.textAlign = "center";
                  ctx.textBaseline = "top";
                  ctx.fillStyle = isSelected ? "#fff" : "#e0e0e0";
                  ctx.fillText(node.name, node.x, node.y + size + 2);
                }
              }}
              nodePointerAreaPaint={(node: any, color: string, ctx: CanvasRenderingContext2D) => {
                const size = Math.max(3, Math.min(14, (node.article_count / maxArticles) * 12));
                ctx.beginPath();
                ctx.arc(node.x, node.y, size + 4, 0, 2 * Math.PI);
                ctx.fillStyle = color;
                ctx.fill();
              }}
              warmupTicks={50}
              cooldownTicks={100}
              onBackgroundClick={() => {
                setSelectedNode(null);
              }}
              enableZoomInteraction={true}
              enablePanInteraction={true}
            />
          )}
        </GraphErrorBoundary>
      </div>
      {panel && (
        <ArticlePanel
          title={panel.title}
          articles={panel.articles}
          onClose={() => setPanel(null)}
        />
      )}
    </div>
  );
}
