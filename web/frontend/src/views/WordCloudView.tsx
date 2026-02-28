import { useState, useEffect, useCallback } from "react";
import { useFilters } from "../context/FilterContext";
import type { WordCloudItem, Article } from "../api/client";
import {
  fetchWordCloudEntities,
  fetchWordCloudTags,
  fetchWordCloudSmokeTerms,
  fetchWordCloudHeadlines,
  fetchTextSearch,
  fetchEntityArticles,
} from "../api/client";

type Source = "entities" | "tags" | "smoke" | "headlines";

const SOURCE_LABELS: Record<Source, string> = {
  entities: "Entities",
  tags: "Keywords",
  smoke: "Smoke Terms",
  headlines: "Headlines",
};

const ENTITY_TYPE_COLORS: Record<string, string> = {
  person: "#4fc3f7",
  organisation: "#81c784",
  place: "#ffb74d",
  event: "#ce93d8",
  legislation: "#f06292",
  statistic: "#90a4ae",
  work: "#a1887f",
  product: "#fff176",
  species: "#aed581",
  substance: "#ef9a9a",
  concept: "#80cbc4",
  medical_condition: "#e57373",
  technology: "#7986cb",
};

function wordColor(item: WordCloudItem, source: Source): string {
  if (source === "entities" && item.type) {
    return ENTITY_TYPE_COLORS[item.type] || "#4fc3f7";
  }
  if (source === "tags") return "#81c784";
  if (source === "smoke") return "#ef9a9a";
  if (source === "headlines") return "#ffb74d";
  return "#4fc3f7";
}

function computeFontSize(count: number, maxCount: number, minCount: number): number {
  if (maxCount === minCount) return 24;
  const ratio = (count - minCount) / (maxCount - minCount);
  // Scale from 12px to 56px
  return Math.round(12 + ratio * 44);
}

export default function WordCloudView() {
  const { timeFrom, timeTo } = useFilters();
  const [source, setSource] = useState<Source>("entities");
  const [words, setWords] = useState<WordCloudItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [articles, setArticles] = useState<Article[]>([]);
  const [articlesLoading, setArticlesLoading] = useState(false);

  const loadWords = useCallback(() => {
    setLoading(true);
    setSelected(null);
    setArticles([]);
    const filters = {
      time_from: timeFrom || undefined,
      time_to: timeTo || undefined,
      limit: 150,
    };
    const fetcher = {
      entities: fetchWordCloudEntities,
      tags: fetchWordCloudTags,
      smoke: fetchWordCloudSmokeTerms,
      headlines: fetchWordCloudHeadlines,
    }[source];
    fetcher(filters)
      .then(setWords)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [source, timeFrom, timeTo]);

  useEffect(() => { loadWords(); }, [loadWords]);

  const handleWordClick = (word: string) => {
    setSelected(word);
    setArticlesLoading(true);
    const filters = {
      time_from: timeFrom || undefined,
      time_to: timeTo || undefined,
      limit: 30,
    };
    if (source === "entities") {
      fetchEntityArticles(word, filters)
        .then(setArticles)
        .catch(console.error)
        .finally(() => setArticlesLoading(false));
    } else {
      fetchTextSearch(word, filters)
        .then(setArticles)
        .catch(console.error)
        .finally(() => setArticlesLoading(false));
    }
  };

  const maxCount = words.length > 0 ? words[0].count : 1;
  const minCount = words.length > 0 ? words[words.length - 1].count : 1;

  return (
    <div className="wordcloud-view">
      <div className="wordcloud-tabs">
        {(Object.keys(SOURCE_LABELS) as Source[]).map((s) => (
          <button
            key={s}
            className={`wordcloud-tab ${source === s ? "active" : ""}`}
            onClick={() => setSource(s)}
          >
            {SOURCE_LABELS[s]}
          </button>
        ))}
        <span className="wordcloud-count">
          {words.length} words
        </span>
      </div>

      <div className="wordcloud-body">
        <div className={`wordcloud-cloud ${selected ? "cloud-with-panel" : ""}`}>
          {loading && <div className="loading-msg">Loading...</div>}
          {!loading && words.length === 0 && (
            <div className="loading-msg">No data for this time range</div>
          )}
          {!loading && words.map((w) => (
            <span
              key={w.text + (w.type || "")}
              className={`cloud-word ${selected === w.text ? "cloud-word-selected" : ""}`}
              style={{
                fontSize: computeFontSize(w.count, maxCount, minCount),
                color: wordColor(w, source),
                opacity: selected && selected !== w.text ? 0.3 : 1,
              }}
              onClick={() => handleWordClick(w.text)}
              title={`${w.text}: ${w.count}${w.type ? ` (${w.type})` : ""}`}
            >
              {w.text}
            </span>
          ))}
        </div>

        {selected && (
          <div className="wordcloud-panel">
            <div className="panel-header">
              <h2>{selected}</h2>
              <button className="close-btn" onClick={() => { setSelected(null); setArticles([]); }}>
                ×
              </button>
            </div>
            <div className="panel-body">
              {articlesLoading && <div className="empty">Loading articles...</div>}
              {!articlesLoading && articles.length === 0 && (
                <div className="empty">No articles found</div>
              )}
              {articles.map((a) => (
                <div key={a.article_id} className="article-card">
                  <a
                    href={a.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="article-title"
                  >
                    {a.title}
                  </a>
                  <div className="article-meta">
                    <span>{a.section}</span>
                    <span>
                      {a.published_at && new Date(a.published_at).toLocaleDateString()}
                    </span>
                  </div>
                  {a.summary && <div className="summary">{a.summary}</div>}
                  {a.standfirst && !a.summary && (
                    <div className="standfirst">{a.standfirst}</div>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
