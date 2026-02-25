import { useEffect, useState, useRef, useCallback } from "react";
import * as d3 from "d3";
import { useFilters } from "../context/FilterContext";
import {
  fetchTopicRiver,
  fetchArticles,
  fetchNLQuery,
  type TopicRiverData,
  type Article,
  type NLFilters,
} from "../api/client";
import ArticlePanel from "../components/ArticlePanel";

const TOPIC_COLORS = d3.schemeTableau10.concat(d3.schemePaired).slice(0, 30);

interface PanelState {
  title: string;
  articles: Article[];
}

export default function RiverView() {
  const filters = useFilters();
  const svgRef = useRef<SVGSVGElement>(null);
  const [riverData, setRiverData] = useState<TopicRiverData | null>(null);
  const [loading, setLoading] = useState(true);
  const [panel, setPanel] = useState<PanelState | null>(null);
  const [focusedTopics, setFocusedTopics] = useState<Set<string>>(new Set());
  const [nlQuery, setNlQuery] = useState("");
  const [nlFilters, setNlFilters] = useState<NLFilters | null>(null);
  const [nlLoading, setNlLoading] = useState(false);

  const loadData = useCallback(async (region?: string) => {
    setLoading(true);
    try {
      const bucket = filters.timeFrom && daysDiff(filters.timeFrom) <= 7 ? "hour" : "day";
      const data = await fetchTopicRiver({
        time_from: filters.timeFrom || undefined,
        time_to: filters.timeTo || undefined,
        region,
        bucket,
      });
      setRiverData(data);
    } catch (err) {
      console.error("Failed to load river data:", err);
    } finally {
      setLoading(false);
    }
  }, [filters.timeFrom, filters.timeTo]);

  useEffect(() => {
    if (!nlFilters) loadData();
  }, [loadData, nlFilters]);

  const handleNlSubmit = useCallback(async () => {
    if (!nlQuery.trim()) return;
    setNlLoading(true);
    try {
      const result = await fetchNLQuery(nlQuery.trim());
      setNlFilters(result);
      // Focus on returned topics
      if (result.topics?.length) {
        setFocusedTopics(new Set(result.topics));
      }
      // Reload with region filter if present
      await loadData(result.regions?.[0]);
    } catch (err) {
      console.error("NL query failed:", err);
    } finally {
      setNlLoading(false);
    }
  }, [nlQuery, loadData]);

  const clearNlFilters = useCallback(() => {
    setNlFilters(null);
    setNlQuery("");
    setFocusedTopics(new Set());
    loadData();
  }, [loadData]);

  const toggleTopic = useCallback((topic: string) => {
    setFocusedTopics((prev) => {
      const next = new Set(prev);
      if (next.has(topic)) {
        next.delete(topic);
      } else {
        next.add(topic);
      }
      return next;
    });
  }, []);

  useEffect(() => {
    if (!riverData || !svgRef.current) return;
    renderStreamgraph(
      riverData,
      svgRef.current,
      focusedTopics,
      async (topic: string, timestamp: string) => {
        setPanel({ title: `Loading ${topic} articles...`, articles: [] });
        try {
          const articles = await fetchArticles({
            topic,
            region: nlFilters?.regions?.[0],
            time_from: timestamp,
            time_to: nextBucket(timestamp, filters.timeFrom),
            limit: 30,
          });
          setPanel({ title: `${topic.replace(/_/g, " ")} — ${articles.length} articles`, articles });
        } catch (err) {
          console.error(err);
        }
      },
      toggleTopic,
    );
  }, [riverData, filters.timeFrom, focusedTopics, nlFilters, toggleTopic]);

  const formatLabel = (s: string) => s.replace(/_/g, " ");

  return (
    <div className="river-view">
      <div className="graph-toolbar">
        <input
          type="text"
          placeholder='Ask: e.g. "economy in Europe"'
          value={nlQuery}
          onChange={(e) => setNlQuery(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleNlSubmit()}
          className="search-input nl-input"
          disabled={nlLoading}
        />
        <button onClick={handleNlSubmit} className="search-btn" disabled={nlLoading}>
          {nlLoading ? "..." : "Query"}
        </button>
        {focusedTopics.size > 0 && (
          <button onClick={() => setFocusedTopics(new Set())} className="search-btn search-btn-find">
            Show all
          </button>
        )}
        {nlFilters && (
          <button onClick={clearNlFilters} className="nl-clear" title="Clear NL filters">x</button>
        )}
      </div>
      {nlFilters && (
        <div className="nl-pills" style={{ top: 42 }}>
          {nlFilters.topics?.map((t) => (
            <span key={t} className="nl-pill pill-topic">{formatLabel(t)}</span>
          ))}
          {nlFilters.regions?.map((r) => (
            <span key={r} className="nl-pill pill-region">{formatLabel(r)}</span>
          ))}
          {nlFilters.time_from && (
            <span className="nl-pill pill-time">from {nlFilters.time_from}</span>
          )}
          {nlFilters.time_to && (
            <span className="nl-pill pill-time">to {nlFilters.time_to}</span>
          )}
        </div>
      )}
      {loading && <div className="loading-msg">Loading topic river...</div>}
      <svg ref={svgRef} className="river-svg" />
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

function daysDiff(dateStr: string): number {
  const d = new Date(dateStr);
  return Math.ceil((Date.now() - d.getTime()) / (1000 * 60 * 60 * 24));
}

function nextBucket(timestamp: string, timeFrom?: string): string {
  const d = new Date(timestamp);
  if (timeFrom && daysDiff(timeFrom) <= 7) {
    d.setHours(d.getHours() + 1);
  } else {
    d.setDate(d.getDate() + 1);
  }
  return d.toISOString().slice(0, 10);
}

function renderStreamgraph(
  data: TopicRiverData,
  svgEl: SVGSVGElement,
  focusedTopics: Set<string>,
  onClick: (topic: string, timestamp: string) => void,
  onLegendClick: (topic: string) => void,
) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const container = svgEl.parentElement!;
  const width = container.clientWidth;
  const height = Math.max(400, container.clientHeight - 60);
  const margin = { top: 20, right: 200, bottom: 40, left: 50 };

  svg.attr("width", width).attr("height", height);

  if (!data.timestamps.length || !data.series.length) {
    svg.append("text")
      .attr("x", width / 2)
      .attr("y", height / 2)
      .attr("text-anchor", "middle")
      .attr("fill", "#888")
      .text("No data for this time range");
    return;
  }

  const topics = data.series.map((s) => s.topic);
  const parseTime = (s: string) => new Date(s);
  const timestamps = data.timestamps.map(parseTime);
  const hasFocus = focusedTopics.size > 0;

  // Build stack data
  const stackData = data.timestamps.map((ts, i) => {
    const row: Record<string, number | Date> = { date: parseTime(ts) };
    for (const s of data.series) {
      row[s.topic] = s.values[i] || 0;
    }
    return row;
  });

  const stack = d3.stack<Record<string, number | Date>>()
    .keys(topics)
    .offset(d3.stackOffsetWiggle)
    .order(d3.stackOrderInsideOut);

  const layers = stack(stackData as any);

  const x = d3.scaleTime()
    .domain(d3.extent(timestamps) as [Date, Date])
    .range([margin.left, width - margin.right]);

  const yMin = d3.min(layers, (l) => d3.min(l, (d) => d[0])) ?? 0;
  const yMax = d3.max(layers, (l) => d3.max(l, (d) => d[1])) ?? 1;

  const y = d3.scaleLinear()
    .domain([yMin, yMax])
    .range([height - margin.bottom, margin.top]);

  const color = d3.scaleOrdinal<string>()
    .domain(topics)
    .range(TOPIC_COLORS);

  const area = d3.area<any>()
    .x((d: any) => x(d.data.date))
    .y0((d: any) => y(d[0]))
    .y1((d: any) => y(d[1]))
    .curve(d3.curveBasis);

  // Tooltip
  const tooltip = d3.select(container).selectAll(".river-tooltip").data([0]);
  const tip = tooltip.enter()
    .append("div")
    .attr("class", "river-tooltip")
    .style("position", "absolute")
    .style("display", "none")
    .style("background", "#2a2a3e")
    .style("color", "#e0e0e0")
    .style("padding", "6px 10px")
    .style("border-radius", "4px")
    .style("font-size", "13px")
    .style("pointer-events", "none")
    .merge(tooltip as any);

  // Streams
  svg.selectAll("path.stream")
    .data(layers)
    .enter()
    .append("path")
    .attr("class", "stream")
    .attr("d", area)
    .attr("fill", (d: any) => color(d.key))
    .attr("opacity", (d: any) => {
      if (!hasFocus) return 0.85;
      return focusedTopics.has(d.key) ? 0.95 : 0.1;
    })
    .style("cursor", "pointer")
    .on("mouseover", function (_event: any, d: any) {
      const targetOpacity = hasFocus && !focusedTopics.has(d.key) ? 0.25 : 1;
      d3.select(this).attr("opacity", targetOpacity);
      tip.style("display", "block");
    })
    .on("mousemove", function (event: any, d: any) {
      const [mx] = d3.pointer(event, svgEl);
      const date = x.invert(mx);
      const idx = d3.bisectLeft(timestamps.map(t => t.getTime()), date.getTime());
      const val = idx < d.length ? (d[idx][1] - d[idx][0]).toFixed(0) : "?";
      tip
        .html(`<strong>${d.key.replace(/_/g, " ")}</strong><br/>${val} articles`)
        .style("left", `${event.offsetX + 12}px`)
        .style("top", `${event.offsetY - 10}px`);
    })
    .on("mouseout", function (_event: any, d: any) {
      const baseOpacity = hasFocus ? (focusedTopics.has(d.key) ? 0.95 : 0.1) : 0.85;
      d3.select(this).attr("opacity", baseOpacity);
      tip.style("display", "none");
    })
    .on("click", function (_event: any, d: any) {
      const [mx] = d3.pointer(event, svgEl);
      const date = x.invert(mx);
      onClick(d.key, date.toISOString().slice(0, 10));
    });

  // X axis
  svg.append("g")
    .attr("transform", `translate(0,${height - margin.bottom})`)
    .call(d3.axisBottom(x).ticks(8))
    .attr("color", "#888");

  // Legend (clickable)
  const legend = svg.append("g")
    .attr("transform", `translate(${width - margin.right + 15}, ${margin.top})`);

  topics.forEach((topic, i) => {
    const g = legend.append("g")
      .attr("transform", `translate(0, ${i * 18})`)
      .style("cursor", "pointer")
      .on("click", () => onLegendClick(topic));

    const isFocused = !hasFocus || focusedTopics.has(topic);

    g.append("rect")
      .attr("width", 12)
      .attr("height", 12)
      .attr("fill", color(topic))
      .attr("opacity", isFocused ? 1 : 0.2);
    g.append("text")
      .attr("x", 16)
      .attr("y", 10)
      .attr("fill", isFocused ? "#ccc" : "#555")
      .attr("font-size", "11px")
      .attr("font-weight", focusedTopics.has(topic) ? "bold" : "normal")
      .text(topic.replace(/_/g, " "));
  });
}
