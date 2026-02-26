import { useEffect, useState, useRef, useCallback } from "react";
import * as d3 from "d3";
import { useFilters } from "../context/FilterContext";
import {
  fetchEntityTimeline,
  fetchEntityArticles,
  type TimelineData,
  type Article,
} from "../api/client";
import ArticlePanel from "../components/ArticlePanel";

const COLORS = d3.schemeCategory10;

interface PanelState {
  title: string;
  articles: Article[];
}

export default function TimelineView() {
  const filters = useFilters();
  const svgRef = useRef<SVGSVGElement>(null);
  const [entities, setEntities] = useState<string[]>([]);
  const [inputVal, setInputVal] = useState("");
  const [data, setData] = useState<TimelineData | null>(null);
  const [loading, setLoading] = useState(false);
  const [panel, setPanel] = useState<PanelState | null>(null);
  const [hiddenEntities, setHiddenEntities] = useState<Set<string>>(new Set());

  const loadData = useCallback(async (ents: string[]) => {
    if (ents.length === 0) { setData(null); return; }
    setLoading(true);
    try {
      const d = await fetchEntityTimeline(ents, {
        time_from: filters.timeFrom || undefined,
        time_to: filters.timeTo || undefined,
        bucket: "week",
      });
      setData(d);
      // Update entity names to resolved names from the API
      if (d.series.length > 0) {
        setEntities((prev) => {
          const resolved = [...prev];
          for (const s of d.series) {
            const idx = resolved.findIndex((e) => s.entity.includes(e));
            if (idx >= 0 && resolved[idx] !== s.entity) {
              resolved[idx] = s.entity;
            }
          }
          return resolved;
        });
      }
    } catch (err) {
      console.error("Failed to load timeline:", err);
    } finally {
      setLoading(false);
    }
  }, [filters.timeFrom, filters.timeTo]);

  useEffect(() => {
    if (entities.length > 0) loadData(entities);
  }, [loadData, entities]);

  const addEntity = useCallback(() => {
    const val = inputVal.trim().toLowerCase();
    if (val && !entities.includes(val)) {
      setEntities((prev) => [...prev, val]);
    }
    setInputVal("");
  }, [inputVal, entities]);

  const removeEntity = useCallback((name: string) => {
    setEntities((prev) => prev.filter((e) => e !== name));
    setHiddenEntities((prev) => { const next = new Set(prev); next.delete(name); return next; });
  }, []);

  const toggleEntity = useCallback((name: string) => {
    setHiddenEntities((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name); else next.add(name);
      return next;
    });
  }, []);

  const handlePointClick = useCallback(async (entity: string, ts: string) => {
    setPanel({ title: `Loading ${entity} articles...`, articles: [] });
    try {
      const weekEnd = new Date(ts);
      weekEnd.setDate(weekEnd.getDate() + 7);
      const articles = await fetchEntityArticles(entity, {
        time_from: ts,
        time_to: weekEnd.toISOString().slice(0, 10),
      });
      setPanel({
        title: `${entity} — week of ${ts} (${articles.length})`,
        articles,
      });
    } catch (err) {
      console.error(err);
    }
  }, []);

  useEffect(() => {
    if (!data || !svgRef.current) return;
    renderTimeline(data, svgRef.current, entities, hiddenEntities, handlePointClick);
  }, [data, entities, hiddenEntities, handlePointClick]);

  return (
    <div className="timeline-view">
      <div className="timeline-controls">
        <input
          type="text"
          placeholder='Add entity: e.g. "trump", "nhs"'
          value={inputVal}
          onChange={(e) => setInputVal(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && addEntity()}
          className="search-input"
          style={{ width: 220 }}
        />
        <button onClick={addEntity} className="search-btn">Add</button>
        <div className="entity-pills">
          {entities.map((name, i) => (
            <span
              key={name}
              className="entity-pill"
              style={{
                background: COLORS[i % COLORS.length],
                opacity: hiddenEntities.has(name) ? 0.3 : 1,
              }}
              onClick={() => toggleEntity(name)}
            >
              {name}
              <button
                className="pill-remove"
                onClick={(e) => { e.stopPropagation(); removeEntity(name); }}
              >x</button>
            </span>
          ))}
        </div>
      </div>
      {loading && <div className="loading-msg">Loading timeline...</div>}
      {entities.length === 0 && !loading && (
        <div className="loading-msg" style={{ color: "#888" }}>
          Add entities above to see their mention frequency over time.
        </div>
      )}
      <svg ref={svgRef} className="timeline-svg" />
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

function renderTimeline(
  data: TimelineData,
  svgEl: SVGSVGElement,
  entities: string[],
  hiddenEntities: Set<string>,
  onClick: (entity: string, ts: string) => void,
) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const container = svgEl.parentElement!;
  const width = container.clientWidth;
  const height = Math.max(400, container.clientHeight - 80);
  const margin = { top: 20, right: 30, bottom: 40, left: 50 };

  svg.attr("width", width).attr("height", height);

  if (!data.timestamps.length || !data.series.length) {
    svg.append("text")
      .attr("x", width / 2).attr("y", height / 2)
      .attr("text-anchor", "middle").attr("fill", "#888")
      .text("No data found for these entities");
    return;
  }

  const parseTime = (s: string) => new Date(s);
  const timestamps = data.timestamps.map(parseTime);
  const visibleSeries = data.series.filter((s) => !hiddenEntities.has(s.entity));

  const x = d3.scaleTime()
    .domain(d3.extent(timestamps) as [Date, Date])
    .range([margin.left, width - margin.right]);

  const yMax = d3.max(visibleSeries, (s) => d3.max(s.values)) ?? 10;
  const y = d3.scaleLinear()
    .domain([0, yMax * 1.1])
    .range([height - margin.bottom, margin.top]);

  const colorMap = new Map<string, string>();
  entities.forEach((e, i) => colorMap.set(e, COLORS[i % COLORS.length]));

  const line = d3.line<number>()
    .x((_, i) => x(timestamps[i]))
    .y((d) => y(d))
    .curve(d3.curveMonotoneX);

  // Tooltip
  const tooltip = d3.select(container).selectAll(".timeline-tooltip").data([0]);
  const tip = tooltip.enter()
    .append("div")
    .attr("class", "timeline-tooltip")
    .style("position", "absolute")
    .style("display", "none")
    .style("background", "#2a2a3e")
    .style("color", "#e0e0e0")
    .style("padding", "6px 10px")
    .style("border-radius", "4px")
    .style("font-size", "13px")
    .style("pointer-events", "none")
    .style("z-index", "30")
    .merge(tooltip as any);

  // Grid lines
  svg.append("g")
    .attr("transform", `translate(${margin.left},0)`)
    .call(d3.axisLeft(y).ticks(5).tickSize(-(width - margin.left - margin.right)))
    .call((g) => g.selectAll(".tick line").attr("stroke", "rgba(255,255,255,0.05)"))
    .call((g) => g.selectAll(".tick text").attr("fill", "#888"))
    .call((g) => g.select(".domain").remove());

  // Lines
  for (const series of visibleSeries) {
    const c = colorMap.get(series.entity) || "#999";

    svg.append("path")
      .datum(series.values)
      .attr("fill", "none")
      .attr("stroke", c)
      .attr("stroke-width", 2.5)
      .attr("d", line);

    // Data points
    svg.selectAll(`.dot-${series.entity}`)
      .data(series.values)
      .enter()
      .append("circle")
      .attr("cx", (_, i) => x(timestamps[i]))
      .attr("cy", (d) => y(d))
      .attr("r", 4)
      .attr("fill", c)
      .attr("stroke", "#1a1a2e")
      .attr("stroke-width", 1.5)
      .style("cursor", "pointer")
      .on("mouseover", function (event, d) {
        d3.select(this).attr("r", 6);
        const i = series.values.indexOf(d);
        tip.style("display", "block")
          .html(`<strong>${series.entity}</strong><br/>${d} articles<br/>${data.timestamps[i]}`)
          .style("left", `${event.offsetX + 12}px`)
          .style("top", `${event.offsetY - 10}px`);
      })
      .on("mouseout", function () {
        d3.select(this).attr("r", 4);
        tip.style("display", "none");
      })
      .on("click", (_, d) => {
        const i = series.values.indexOf(d);
        onClick(series.entity, data.timestamps[i]);
      });
  }

  // Axes
  svg.append("g")
    .attr("transform", `translate(0,${height - margin.bottom})`)
    .call(d3.axisBottom(x).ticks(8))
    .attr("color", "#888");

  svg.append("g")
    .attr("transform", `translate(${margin.left},0)`)
    .call(d3.axisLeft(y).ticks(5))
    .attr("color", "#888");
}
