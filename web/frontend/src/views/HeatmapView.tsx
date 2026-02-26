import { useEffect, useState, useRef, useCallback } from "react";
import * as d3 from "d3";
import { useFilters } from "../context/FilterContext";
import {
  fetchSentimentHeatmap,
  fetchArticles,
  type HeatmapData,
  type Article,
} from "../api/client";
import ArticlePanel from "../components/ArticlePanel";

interface PanelState {
  title: string;
  articles: Article[];
}

export default function HeatmapView() {
  const filters = useFilters();
  const svgRef = useRef<SVGSVGElement>(null);
  const [data, setData] = useState<HeatmapData | null>(null);
  const [loading, setLoading] = useState(true);
  const [panel, setPanel] = useState<PanelState | null>(null);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const d = await fetchSentimentHeatmap({
        time_from: filters.timeFrom || undefined,
        time_to: filters.timeTo || undefined,
        bucket: "week",
      });
      setData(d);
    } catch (err) {
      console.error("Failed to load heatmap:", err);
    } finally {
      setLoading(false);
    }
  }, [filters.timeFrom, filters.timeTo]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const handleCellClick = useCallback(async (topic: string, ts: string) => {
    setPanel({ title: `Loading ${topic.replace(/_/g, " ")} articles...`, articles: [] });
    try {
      const weekEnd = new Date(ts);
      weekEnd.setDate(weekEnd.getDate() + 7);
      const articles = await fetchArticles({
        topic,
        time_from: ts,
        time_to: weekEnd.toISOString().slice(0, 10),
        limit: 30,
      });
      setPanel({
        title: `${topic.replace(/_/g, " ")} — week of ${ts} (${articles.length})`,
        articles,
      });
    } catch (err) {
      console.error(err);
    }
  }, []);

  useEffect(() => {
    if (!data || !svgRef.current) return;
    renderHeatmap(data, svgRef.current, handleCellClick);
  }, [data, handleCellClick]);

  return (
    <div className="heatmap-view">
      {loading && <div className="loading-msg">Loading sentiment heatmap...</div>}
      <svg ref={svgRef} className="heatmap-svg" />
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

function renderHeatmap(
  data: HeatmapData,
  svgEl: SVGSVGElement,
  onClick: (topic: string, ts: string) => void,
) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const container = svgEl.parentElement!;
  const width = container.clientWidth;
  const height = Math.max(500, container.clientHeight - 20);
  const margin = { top: 30, right: 30, bottom: 80, left: 180 };

  svg.attr("width", width).attr("height", height);

  if (!data.timestamps.length || !data.topics.length) {
    svg.append("text")
      .attr("x", width / 2).attr("y", height / 2)
      .attr("text-anchor", "middle").attr("fill", "#888")
      .text("No data for this time range");
    return;
  }

  const cellLookup = new Map<string, { avg_sentiment: number; count: number }>();
  for (const c of data.cells) {
    cellLookup.set(`${c.topic}|${c.ts}`, { avg_sentiment: c.avg_sentiment, count: c.count });
  }

  const x = d3.scaleBand()
    .domain(data.timestamps)
    .range([margin.left, width - margin.right])
    .padding(0.05);

  const y = d3.scaleBand()
    .domain(data.topics)
    .range([margin.top, height - margin.bottom])
    .padding(0.05);

  const color = d3.scaleSequential(d3.interpolateRdYlGn).domain([-1, 1]);

  // Tooltip
  const tooltip = d3.select(container).selectAll(".heatmap-tooltip").data([0]);
  const tip = tooltip.enter()
    .append("div")
    .attr("class", "heatmap-tooltip")
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

  // Cells
  for (const topic of data.topics) {
    for (const ts of data.timestamps) {
      const cell = cellLookup.get(`${topic}|${ts}`);
      if (!cell) continue;

      svg.append("rect")
        .attr("x", x(ts)!)
        .attr("y", y(topic)!)
        .attr("width", x.bandwidth())
        .attr("height", y.bandwidth())
        .attr("fill", color(cell.avg_sentiment))
        .attr("rx", 2)
        .style("cursor", "pointer")
        .on("mouseover", function (event) {
          d3.select(this).attr("stroke", "#fff").attr("stroke-width", 2);
          tip.style("display", "block")
            .html(
              `<strong>${topic.replace(/_/g, " ")}</strong><br/>` +
              `Week: ${ts}<br/>` +
              `Sentiment: ${cell.avg_sentiment.toFixed(2)}<br/>` +
              `Articles: ${cell.count}`
            )
            .style("left", `${event.offsetX + 12}px`)
            .style("top", `${event.offsetY - 10}px`);
        })
        .on("mousemove", function (event) {
          tip
            .style("left", `${event.offsetX + 12}px`)
            .style("top", `${event.offsetY - 10}px`);
        })
        .on("mouseout", function () {
          d3.select(this).attr("stroke", "none");
          tip.style("display", "none");
        })
        .on("click", () => onClick(topic, ts));
    }
  }

  // X axis
  const xAxis = svg.append("g")
    .attr("transform", `translate(0,${height - margin.bottom})`)
    .call(
      d3.axisBottom(x)
        .tickValues(data.timestamps.filter((_, i) => i % Math.max(1, Math.floor(data.timestamps.length / 12)) === 0))
        .tickFormat((d) => (d as string).slice(5))
    )
    .attr("color", "#888");
  xAxis.selectAll("text").attr("transform", "rotate(-45)").style("text-anchor", "end");

  // Y axis
  svg.append("g")
    .attr("transform", `translate(${margin.left},0)`)
    .call(d3.axisLeft(y).tickFormat((d) => (d as string).replace(/_/g, " ")))
    .attr("color", "#888");

  // Color legend
  const legendWidth = 200;
  const legendHeight = 12;
  const legendX = margin.left;
  const legendY = height - 15;

  const defs = svg.append("defs");
  const gradient = defs.append("linearGradient").attr("id", "heatmap-gradient");
  const steps = 10;
  for (let i = 0; i <= steps; i++) {
    gradient.append("stop")
      .attr("offset", `${(i / steps) * 100}%`)
      .attr("stop-color", color(-1 + (2 * i) / steps));
  }

  svg.append("rect")
    .attr("x", legendX).attr("y", legendY)
    .attr("width", legendWidth).attr("height", legendHeight)
    .attr("fill", "url(#heatmap-gradient)")
    .attr("rx", 2);

  svg.append("text").attr("x", legendX).attr("y", legendY - 3)
    .attr("fill", "#888").attr("font-size", "10px").text("-1 (negative)");
  svg.append("text").attr("x", legendX + legendWidth).attr("y", legendY - 3)
    .attr("fill", "#888").attr("font-size", "10px").attr("text-anchor", "end").text("+1 (positive)");
}
