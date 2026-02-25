import { useEffect, useState, useRef, useCallback } from "react";
import * as d3 from "d3";
import { useFilters } from "../context/FilterContext";
import { fetchTopicRiver, fetchArticles, type TopicRiverData, type Article } from "../api/client";
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

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const bucket = filters.timeFrom && daysDiff(filters.timeFrom) <= 7 ? "hour" : "day";
      const data = await fetchTopicRiver({
        time_from: filters.timeFrom || undefined,
        time_to: filters.timeTo || undefined,
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
    loadData();
  }, [loadData]);

  useEffect(() => {
    if (!riverData || !svgRef.current) return;
    renderStreamgraph(riverData, svgRef.current, async (topic: string, timestamp: string) => {
      setPanel({ title: `Loading ${topic} articles...`, articles: [] });
      try {
        const articles = await fetchArticles({
          topic,
          time_from: timestamp,
          time_to: nextBucket(timestamp, filters.timeFrom),
          limit: 30,
        });
        setPanel({ title: `${topic.replace(/_/g, " ")} — ${articles.length} articles`, articles });
      } catch (err) {
        console.error(err);
      }
    });
  }, [riverData, filters.timeFrom]);

  return (
    <div className="river-view">
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
  onClick: (topic: string, timestamp: string) => void
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
    .attr("opacity", 0.85)
    .style("cursor", "pointer")
    .on("mouseover", function (_event: any, _d: any) {
      d3.select(this).attr("opacity", 1);
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
    .on("mouseout", function () {
      d3.select(this).attr("opacity", 0.85);
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

  // Legend
  const legend = svg.append("g")
    .attr("transform", `translate(${width - margin.right + 15}, ${margin.top})`);

  topics.forEach((topic, i) => {
    const g = legend.append("g").attr("transform", `translate(0, ${i * 18})`);
    g.append("rect").attr("width", 12).attr("height", 12).attr("fill", color(topic));
    g.append("text")
      .attr("x", 16)
      .attr("y", 10)
      .attr("fill", "#ccc")
      .attr("font-size", "11px")
      .text(topic.replace(/_/g, " "));
  });
}
