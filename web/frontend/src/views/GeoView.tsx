import { useEffect, useState, useCallback } from "react";
import * as d3 from "d3";
import { useFilters } from "../context/FilterContext";
import {
  fetchRegionOverview,
  fetchArticles,
  fetchEntityArticles,
  type RegionOverviewData,
  type RegionInfo,
  type Article,
} from "../api/client";
import ArticlePanel from "../components/ArticlePanel";

const REGION_POSITIONS: Record<string, [number, number]> = {
  north_america: [0.20, 0.30],
  latin_america_caribbean: [0.25, 0.60],
  europe: [0.48, 0.25],
  middle_east: [0.58, 0.45],
  africa: [0.48, 0.60],
  asia_pacific: [0.72, 0.35],
  oceania: [0.78, 0.65],
  global: [0.48, 0.85],
};

const REGION_LABELS: Record<string, string> = {
  north_america: "North America",
  latin_america_caribbean: "Latin America",
  europe: "Europe",
  middle_east: "Middle East",
  africa: "Africa",
  asia_pacific: "Asia Pacific",
  oceania: "Oceania",
  global: "Global",
};

interface PanelState {
  title: string;
  articles: Article[];
}

export default function GeoView() {
  const filters = useFilters();
  const [data, setData] = useState<RegionOverviewData | null>(null);
  const [loading, setLoading] = useState(true);
  const [panel, setPanel] = useState<PanelState | null>(null);
  const [hovered, setHovered] = useState<string | null>(null);
  const [selectedRegion, setSelectedRegion] = useState<RegionInfo | null>(null);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const d = await fetchRegionOverview({
        time_from: filters.timeFrom || undefined,
        time_to: filters.timeTo || undefined,
      });
      setData(d);
    } catch (err) {
      console.error("Failed to load regions:", err);
    } finally {
      setLoading(false);
    }
  }, [filters.timeFrom, filters.timeTo]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const handleBubbleClick = useCallback((region: RegionInfo) => {
    setSelectedRegion(region);
    setPanel(null);
  }, []);

  const handleEntityClick = useCallback(async (entityName: string, region: string) => {
    setPanel({ title: `Loading ${entityName} in ${REGION_LABELS[region]}...`, articles: [] });
    try {
      const articles = await fetchEntityArticles(entityName, {
        time_from: filters.timeFrom || undefined,
        time_to: filters.timeTo || undefined,
        region,
      });
      setPanel({
        title: `${entityName} in ${REGION_LABELS[region]} (${articles.length})`,
        articles,
      });
    } catch (err) {
      console.error(err);
    }
  }, [filters.timeFrom, filters.timeTo]);

  const handleRecentArticles = useCallback(async (region: string) => {
    setPanel({ title: `Loading latest from ${REGION_LABELS[region]}...`, articles: [] });
    try {
      const articles = await fetchArticles({
        region,
        time_from: filters.timeFrom || undefined,
        time_to: filters.timeTo || undefined,
        limit: 50,
      });
      setPanel({
        title: `Latest from ${REGION_LABELS[region]} (${articles.length})`,
        articles,
      });
    } catch (err) {
      console.error(err);
    }
  }, [filters.timeFrom, filters.timeTo]);

  const maxCount = data ? Math.max(...data.regions.map((r) => r.article_count), 1) : 1;
  const color = d3.scaleSequential(d3.interpolateRdYlGn).domain([-1, 1]);

  return (
    <div className="geo-view">
      {loading && <div className="loading-msg">Loading regions...</div>}
      {data && (
        <div className="geo-canvas">
          {data.regions.map((r) => {
            const pos = REGION_POSITIONS[r.region];
            if (!pos) return null;
            const radius = Math.max(30, Math.sqrt(r.article_count / maxCount) * 100);
            const isHovered = hovered === r.region;
            const isSelected = selectedRegion?.region === r.region;
            return (
              <div
                key={r.region}
                className="geo-bubble"
                style={{
                  left: `${pos[0] * 100}%`,
                  top: `${pos[1] * 100}%`,
                  width: radius * 2,
                  height: radius * 2,
                  backgroundColor: color(r.avg_sentiment),
                  transform: `translate(-50%, -50%) scale(${isHovered || isSelected ? 1.1 : 1})`,
                  zIndex: isHovered || isSelected ? 10 : 1,
                  borderColor: isSelected ? "#fff" : undefined,
                  borderWidth: isSelected ? 3 : undefined,
                }}
                onMouseEnter={() => setHovered(r.region)}
                onMouseLeave={() => setHovered(null)}
                onClick={() => handleBubbleClick(r)}
              >
                <span className="geo-label">{REGION_LABELS[r.region] || r.region}</span>
                <span className="geo-count">{r.article_count.toLocaleString()}</span>
                {isHovered && !isSelected && (
                  <div className="geo-tooltip">
                    <div>Sentiment: {r.avg_sentiment.toFixed(2)}</div>
                    <div style={{ marginTop: 4, fontSize: 11 }}>
                      {r.top_entities.slice(0, 3).map((e) => (
                        <div key={e.name}>{e.name} ({e.count})</div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
      {selectedRegion && (
        <div className="geo-detail-panel">
          <div className="panel-header">
            <h2>{REGION_LABELS[selectedRegion.region]} — {selectedRegion.article_count.toLocaleString()} articles</h2>
            <button className="close-btn" onClick={() => setSelectedRegion(null)}>×</button>
          </div>
          <div className="geo-detail-body">
            <div className="geo-detail-stat">
              Avg sentiment: <strong>{selectedRegion.avg_sentiment.toFixed(2)}</strong>
            </div>
            <h3>Top entities</h3>
            <div className="geo-entity-list">
              {selectedRegion.top_entities.map((e) => (
                <button
                  key={e.name}
                  className="geo-entity-btn"
                  onClick={() => handleEntityClick(e.name, selectedRegion.region)}
                >
                  {e.name} <span className="geo-entity-count">({e.count})</span>
                </button>
              ))}
            </div>
            <button
              className="search-btn"
              style={{ marginTop: 12, width: "100%" }}
              onClick={() => handleRecentArticles(selectedRegion.region)}
            >
              Latest articles
            </button>
          </div>
        </div>
      )}
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
