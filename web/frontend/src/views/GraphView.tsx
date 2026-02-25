import { useEffect, useState, useCallback, useRef, Component, type ReactNode } from "react";
import ForceGraph2D from "react-force-graph-2d";
import { useFilters } from "../context/FilterContext";
import {
  fetchEntityGraph,
  fetchEntityArticles,
  fetchCooccurrenceArticles,
  fetchNLQuery,
  type GraphData,
  type Article,
  type NLFilters,
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
  const [search, setSearch] = useState("");
  const [nlQuery, setNlQuery] = useState("");
  const [nlFilters, setNlFilters] = useState<NLFilters | null>(null);
  const [nlLoading, setNlLoading] = useState(false);
  const graphRef = useRef<any>(null);

  const loadGraph = useCallback(async (overrides?: { topic?: string; region?: string; entity_type?: string; time_from?: string; time_to?: string }) => {
    setLoading(true);
    try {
      const data = await fetchEntityGraph({
        time_from: overrides?.time_from || filters.timeFrom || undefined,
        time_to: overrides?.time_to || filters.timeTo || undefined,
        entity_type: overrides?.entity_type || filters.entityType || undefined,
        topic: overrides?.topic,
        region: overrides?.region,
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
    if (!nlFilters) loadGraph();
  }, [loadGraph, nlFilters]);

  const handleNlSubmit = useCallback(async () => {
    if (!nlQuery.trim()) return;
    setNlLoading(true);
    try {
      const result = await fetchNLQuery(nlQuery.trim());
      setNlFilters(result);
      // Apply first topic/region to graph filters
      const overrides: Record<string, string | undefined> = {};
      if (result.topics?.length) overrides.topic = result.topics[0];
      if (result.regions?.length) overrides.region = result.regions[0];
      if (result.entity_type) overrides.entity_type = result.entity_type;
      if (result.time_from) overrides.time_from = result.time_from;
      if (result.time_to) overrides.time_to = result.time_to;
      await loadGraph(overrides);
    } catch (err) {
      console.error("NL query failed:", err);
    } finally {
      setNlLoading(false);
    }
  }, [nlQuery, loadGraph]);

  const clearNlFilters = useCallback(() => {
    setNlFilters(null);
    setNlQuery("");
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

  const handleSearch = useCallback(() => {
    if (!search || !graphRef.current) return;
    const node = graphData.nodes.find(
      (n) => n.name.toLowerCase().includes(search.toLowerCase())
    );
    if (node) {
      graphRef.current.centerAt(
        (node as any).x,
        (node as any).y,
        800
      );
      graphRef.current.zoom(3, 800);
    }
  }, [search, graphData.nodes]);

  const [selectedNode, setSelectedNode] = useState<string | null>(null);

  const maxArticles = Math.max(...graphData.nodes.map((n) => n.article_count), 1);

  const handleZoom = useCallback((delta: number) => {
    if (!graphRef.current) return;
    const currentZoom = graphRef.current.zoom();
    graphRef.current.zoom(currentZoom * delta, 300);
  }, []);

  const handleFitToView = useCallback(() => {
    if (!graphRef.current) return;
    graphRef.current.zoomToFit(400, 40);
  }, []);

  const formatLabel = (s: string) => s.replace(/_/g, " ");

  return (
    <div className="graph-view">
      <div className="graph-toolbar">
        <input
          type="text"
          placeholder="Ask: e.g. &quot;climate articles in Asia&quot;"
          value={nlQuery}
          onChange={(e) => setNlQuery(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleNlSubmit()}
          className="search-input nl-input"
          disabled={nlLoading}
        />
        <button onClick={handleNlSubmit} className="search-btn" disabled={nlLoading}>
          {nlLoading ? "..." : "Query"}
        </button>
        <div className="toolbar-divider" />
        <input
          type="text"
          placeholder="Find entity..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleSearch()}
          className="search-input find-input"
        />
        <button onClick={handleSearch} className="search-btn search-btn-find">Find</button>
        <div className="zoom-controls">
          <button onClick={() => handleZoom(1.5)} className="zoom-btn" title="Zoom in">+</button>
          <button onClick={() => handleZoom(1 / 1.5)} className="zoom-btn" title="Zoom out">−</button>
          <button onClick={handleFitToView} className="zoom-btn zoom-fit" title="Fit to view">⊞</button>
        </div>
        <span className="node-count">
          {loading ? "Loading..." : `${graphData.nodes.length} entities, ${graphData.edges.length} connections`}
        </span>
      </div>
      {nlFilters && (
        <div className="nl-pills">
          {nlFilters.topics?.map((t) => (
            <span key={t} className="nl-pill pill-topic">{formatLabel(t)}</span>
          ))}
          {nlFilters.regions?.map((r) => (
            <span key={r} className="nl-pill pill-region">{formatLabel(r)}</span>
          ))}
          {nlFilters.entity_type && (
            <span className="nl-pill pill-entity-type">{formatLabel(nlFilters.entity_type)}</span>
          )}
          {nlFilters.time_from && (
            <span className="nl-pill pill-time">from {nlFilters.time_from}</span>
          )}
          {nlFilters.time_to && (
            <span className="nl-pill pill-time">to {nlFilters.time_to}</span>
          )}
          <button onClick={clearNlFilters} className="nl-clear" title="Clear NL filters">x</button>
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
              nodeVal={(node: any) => Math.max(2, (node.article_count / maxArticles) * 30)}
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
                // Center on clicked node and zoom in
                if (graphRef.current) {
                  graphRef.current.centerAt(node.x, node.y, 600);
                  graphRef.current.zoom(3, 600);
                }
              }}
              onLinkClick={handleLinkClick}
              backgroundColor="#1a1a2e"
              nodeCanvasObject={(node: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
                const size = Math.max(3, (node.article_count / maxArticles) * 20);
                const baseColor = TYPE_COLORS[node.type] || "#999";
                const isSelected = node.id === selectedNode;

                // Check if this node is a neighbor of the selected node
                const isNeighbor = selectedNode && graphData.edges.some((e) => {
                  const src = typeof e.source === "object" ? (e.source as any).id : e.source;
                  const tgt = typeof e.target === "object" ? (e.target as any).id : e.target;
                  return (src === selectedNode && tgt === node.id) || (tgt === selectedNode && src === node.id);
                });

                const dimmed = selectedNode && !isSelected && !isNeighbor;
                const color = dimmed ? "rgba(100,100,100,0.3)" : baseColor;

                // Highlight ring for selected node
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

                // Show labels: always for selected/neighbor, otherwise based on zoom/importance
                const showLabel = isSelected || isNeighbor || globalScale > 1.5 || node.article_count > maxArticles * 0.1;
                if (showLabel && !dimmed) {
                  ctx.font = `${Math.max(3, 12 / globalScale)}px sans-serif`;
                  ctx.textAlign = "center";
                  ctx.textBaseline = "top";
                  ctx.fillStyle = isSelected ? "#fff" : isNeighbor ? "#e0e0e0" : "#e0e0e0";
                  ctx.fillText(node.name, node.x, node.y + size + 2);
                }
              }}
              nodePointerAreaPaint={(node: any, color: string, ctx: CanvasRenderingContext2D) => {
                const size = Math.max(3, (node.article_count / maxArticles) * 20);
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
