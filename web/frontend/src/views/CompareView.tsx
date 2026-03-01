import { useEffect, useState, useRef, useCallback } from "react";
import * as d3 from "d3";
import { useFilters } from "../context/FilterContext";
import {
  fetchEntityTimeline,
  fetchEntityArticles,
  fetchCooccurrenceArticles,
  summarizeArticle,
  summarizeArticles,
  type TimelineData,
  type Article,
} from "../api/client";

const COLOR_A = "#4fc3f7";
const COLOR_B = "#ef5350";

export default function CompareView() {
  const filters = useFilters();
  const svgRef = useRef<SVGSVGElement>(null);
  const [entityA, setEntityA] = useState("");
  const [entityB, setEntityB] = useState("");
  const [activeA, setActiveA] = useState("");
  const [activeB, setActiveB] = useState("");
  const [data, setData] = useState<TimelineData | null>(null);
  const [loading, setLoading] = useState(false);
  const [articlesA, setArticlesA] = useState<Article[]>([]);
  const [articlesB, setArticlesB] = useState<Article[]>([]);
  const [articlesBoth, setArticlesBoth] = useState<Article[]>([]);
  const [articlesLoading, setArticlesLoading] = useState(false);
  const [articleSummaries, setArticleSummaries] = useState<Record<string, string>>({});
  const [summarizingId, setSummarizingId] = useState<string | null>(null);
  const [syntheses, setSyntheses] = useState<Record<string, string>>({});
  const [synthesizingKey, setSynthesizingKey] = useState<string | null>(null);

  const handleSummarizeOne = (articleId: string) => {
    setSummarizingId(articleId);
    summarizeArticle(articleId)
      .then((result) => {
        setArticleSummaries((prev) => ({ ...prev, [articleId]: result.error ? `Error: ${result.error}` : result.summary }));
      })
      .catch((err) => setArticleSummaries((prev) => ({ ...prev, [articleId]: `Error: ${err.message}` })))
      .finally(() => setSummarizingId(null));
  };

  const handleSummarizeColumn = (key: string, arts: Article[], label: string) => {
    if (arts.length === 0) return;
    setSynthesizingKey(key);
    summarizeArticles(arts.map((a) => a.article_id), label)
      .then((result) => {
        setSyntheses((prev) => ({ ...prev, [key]: result.error ? `Error: ${result.error}` : result.synthesis }));
      })
      .catch((err) => setSyntheses((prev) => ({ ...prev, [key]: `Error: ${err.message}` })))
      .finally(() => setSynthesizingKey(null));
  };

  const handleCompare = useCallback(async () => {
    const a = entityA.trim().toLowerCase();
    const b = entityB.trim().toLowerCase();
    if (!a || !b) return;
    setActiveA(a);
    setActiveB(b);
    setLoading(true);
    setArticlesLoading(true);
    try {
      const timeFilters = {
        time_from: filters.timeFrom || undefined,
        time_to: filters.timeTo || undefined,
      };
      // Fetch timeline first to resolve entity names
      const timeline = await fetchEntityTimeline([a, b], { ...timeFilters, bucket: "week" });
      setData(timeline);

      const resolvedA = timeline.series.find((s) => s.entity.includes(a))?.entity || a;
      const resolvedB = timeline.series.find((s) => s.entity.includes(b))?.entity || b;
      setActiveA(resolvedA);
      setActiveB(resolvedB);

      // Fetch articles using resolved names
      const [artsA, artsB, artsBoth] = await Promise.all([
        fetchEntityArticles(resolvedA, timeFilters).catch(() => [] as Article[]),
        fetchEntityArticles(resolvedB, timeFilters).catch(() => [] as Article[]),
        fetchCooccurrenceArticles(resolvedA, resolvedB, timeFilters).catch(() => [] as Article[]),
      ]);
      const bothIds = new Set(artsBoth.map((x) => x.article_id));
      setArticlesA(artsA.filter((x) => !bothIds.has(x.article_id)));
      setArticlesB(artsB.filter((x) => !bothIds.has(x.article_id)));
      setArticlesBoth(artsBoth);
    } catch (err) {
      console.error("Compare failed:", err);
    } finally {
      setLoading(false);
      setArticlesLoading(false);
    }
  }, [entityA, entityB, filters.timeFrom, filters.timeTo]);

  useEffect(() => {
    if (!data || !svgRef.current) return;
    renderCompareChart(data, svgRef.current, activeA, activeB);
  }, [data, activeA, activeB]);

  return (
    <div className="compare-view">
      <div className="compare-controls">
        <div className="compare-input-group">
          <span className="compare-dot" style={{ background: COLOR_A }} />
          <input
            type="text"
            placeholder="Entity A"
            value={entityA}
            onChange={(e) => setEntityA(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleCompare()}
            className="search-input"
            style={{ width: 180 }}
          />
        </div>
        <span className="compare-vs">vs</span>
        <div className="compare-input-group">
          <span className="compare-dot" style={{ background: COLOR_B }} />
          <input
            type="text"
            placeholder="Entity B"
            value={entityB}
            onChange={(e) => setEntityB(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleCompare()}
            className="search-input"
            style={{ width: 180 }}
          />
        </div>
        <button onClick={handleCompare} className="search-btn" disabled={loading}>
          {loading ? "..." : "Compare"}
        </button>
      </div>
      {loading && <div className="loading-msg">Loading comparison...</div>}
      <svg ref={svgRef} className="compare-svg" />
      {activeA && activeB && !articlesLoading && (
        <div className="compare-articles">
          {([
            { key: "a", arts: articlesA, label: `Only ${activeA}`, color: COLOR_A },
            { key: "both", arts: articlesBoth, label: "Both", color: "#fff", className: "compare-both" },
            { key: "b", arts: articlesB, label: `Only ${activeB}`, color: COLOR_B },
          ] as const).map(({ key, arts, label, color, className }) => (
            <div key={key} className={`compare-column ${className || ""}`}>
              <h3 style={{ color }}>{label} ({arts.length})</h3>
              {arts.length > 0 && (
                <button
                  className="summarize-btn"
                  style={{ marginBottom: 8, width: "100%" }}
                  onClick={() => handleSummarizeColumn(key, arts, `${label}: ${activeA} vs ${activeB}`)}
                  disabled={synthesizingKey === key}
                >
                  {synthesizingKey === key ? "Summarizing..." : "Summarize"}
                </button>
              )}
              {syntheses[key] && (
                <div className="synthesis-box" style={{ marginBottom: 8 }}>
                  <div className="synthesis-label">Synthesis</div>
                  <div className="synthesis-text">{syntheses[key]}</div>
                </div>
              )}
              <div className="compare-list">
                {arts.slice(0, 20).map((a) => (
                  <div key={a.article_id} className="article-card">
                    <a href={a.url} target="_blank" rel="noopener noreferrer" className="article-title">
                      {a.headline || a.title}
                    </a>
                    <div className="article-meta">
                      <span>{a.section}</span>
                      <span>{a.published_at?.slice(0, 10)}</span>
                      {!articleSummaries[a.article_id] && (
                        <button
                          className="summarize-one-btn"
                          onClick={() => handleSummarizeOne(a.article_id)}
                          disabled={summarizingId === a.article_id}
                        >
                          {summarizingId === a.article_id ? "..." : "Summarize"}
                        </button>
                      )}
                    </div>
                    {articleSummaries[a.article_id] && (
                      <div className="article-ai-summary">{articleSummaries[a.article_id]}</div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function renderCompareChart(
  data: TimelineData,
  svgEl: SVGSVGElement,
  entityA: string,
  entityB: string,
) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const container = svgEl.parentElement!;
  const width = container.clientWidth;
  const height = 250;
  const margin = { top: 20, right: 30, bottom: 30, left: 50 };

  svg.attr("width", width).attr("height", height);

  if (!data.timestamps.length) return;

  const timestamps = data.timestamps.map((s) => new Date(s));
  const seriesA = data.series.find((s) => s.entity === entityA);
  const seriesB = data.series.find((s) => s.entity === entityB);

  const x = d3.scaleTime()
    .domain(d3.extent(timestamps) as [Date, Date])
    .range([margin.left, width - margin.right]);

  const allVals = [
    ...(seriesA?.values || []),
    ...(seriesB?.values || []),
  ];
  const yMax = d3.max(allVals) ?? 10;
  const y = d3.scaleLinear()
    .domain([0, yMax * 1.1])
    .range([height - margin.bottom, margin.top]);

  const line = d3.line<number>()
    .x((_, i) => x(timestamps[i]))
    .y((d) => y(d))
    .curve(d3.curveMonotoneX);

  // Grid
  svg.append("g")
    .attr("transform", `translate(${margin.left},0)`)
    .call(d3.axisLeft(y).ticks(4).tickSize(-(width - margin.left - margin.right)))
    .call((g) => g.selectAll(".tick line").attr("stroke", "rgba(255,255,255,0.05)"))
    .call((g) => g.selectAll(".tick text").attr("fill", "#888"))
    .call((g) => g.select(".domain").remove());

  if (seriesA) {
    svg.append("path")
      .datum(seriesA.values)
      .attr("fill", "none")
      .attr("stroke", COLOR_A)
      .attr("stroke-width", 2.5)
      .attr("d", line);
  }

  if (seriesB) {
    svg.append("path")
      .datum(seriesB.values)
      .attr("fill", "none")
      .attr("stroke", COLOR_B)
      .attr("stroke-width", 2.5)
      .attr("d", line);
  }

  svg.append("g")
    .attr("transform", `translate(0,${height - margin.bottom})`)
    .call(d3.axisBottom(x).ticks(8))
    .attr("color", "#888");

  svg.append("g")
    .attr("transform", `translate(${margin.left},0)`)
    .call(d3.axisLeft(y).ticks(4))
    .attr("color", "#888");

  // Legend
  const legend = svg.append("g").attr("transform", `translate(${margin.left + 10}, ${margin.top + 5})`);
  legend.append("line").attr("x1", 0).attr("x2", 20).attr("y1", 0).attr("y2", 0)
    .attr("stroke", COLOR_A).attr("stroke-width", 2.5);
  legend.append("text").attr("x", 25).attr("y", 4).attr("fill", COLOR_A)
    .attr("font-size", "12px").text(entityA);
  legend.append("line").attr("x1", 0).attr("x2", 20).attr("y1", 18).attr("y2", 18)
    .attr("stroke", COLOR_B).attr("stroke-width", 2.5);
  legend.append("text").attr("x", 25).attr("y", 22).attr("fill", COLOR_B)
    .attr("font-size", "12px").text(entityB);
}
