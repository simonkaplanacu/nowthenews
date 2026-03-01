import { useState, useEffect, useCallback, useRef } from "react";
import { useFilters } from "../context/FilterContext";
import type { WordCloudItem, Article } from "../api/client";
import {
  fetchWordCloudEntities,
  fetchWordCloudTags,
  fetchWordCloudSmokeTerms,
  fetchWordCloudHeadlines,
  fetchWordCloudTopics,
  fetchTextSearch,
  fetchEntityArticles,
  summarizeArticle,
  summarizeArticles,
} from "../api/client";
import cloud from "d3-cloud";

type Source = "entities" | "tags" | "smoke" | "headlines" | "topics";

const SOURCE_LABELS: Record<Source, string> = {
  entities: "Entities",
  tags: "Keywords",
  smoke: "Smoke Terms",
  headlines: "Headlines",
  topics: "Hot Topics",
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
  if (source === "topics") return "#ce93d8";
  return "#4fc3f7";
}

interface LayoutWord {
  text: string;
  size: number;
  x: number;
  y: number;
  rotate: number;
  color: string;
  count: number;
  type?: string;
}

export default function WordCloudView() {
  const { timeFrom, timeTo } = useFilters();
  const [source, setSource] = useState<Source>("entities");
  const [words, setWords] = useState<WordCloudItem[]>([]);
  const [layoutWords, setLayoutWords] = useState<LayoutWord[]>([]);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [articles, setArticles] = useState<Article[]>([]);
  const [articlesLoading, setArticlesLoading] = useState(false);
  const [searchInput, setSearchInput] = useState("");
  const [activeSearch, setActiveSearch] = useState("");
  const [synthesis, setSynthesis] = useState<string | null>(null);
  const [synthesisLoading, setSynthesisLoading] = useState(false);
  const [articleSummaries, setArticleSummaries] = useState<Record<string, string>>({});
  const [summarizingId, setSummarizingId] = useState<string | null>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const loadWords = useCallback(() => {
    setLoading(true);
    setSelected(null);
    setArticles([]);
    setSynthesis(null);
    const filters = {
      q: activeSearch || undefined,
      time_from: timeFrom || undefined,
      time_to: timeTo || undefined,
      limit: 150,
    };
    const fetcher = {
      entities: fetchWordCloudEntities,
      tags: fetchWordCloudTags,
      smoke: fetchWordCloudSmokeTerms,
      headlines: fetchWordCloudHeadlines,
      topics: fetchWordCloudTopics,
    }[source];
    fetcher(filters)
      .then(setWords)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [source, timeFrom, timeTo, activeSearch]);

  useEffect(() => { loadWords(); }, [loadWords]);

  // Run d3-cloud layout when words or container size changes
  useEffect(() => {
    if (words.length === 0 || !containerRef.current) {
      setLayoutWords([]);
      return;
    }

    const rect = containerRef.current.getBoundingClientRect();
    const width = rect.width || 800;
    const height = rect.height || 500;

    const maxCount = words[0].count;
    const minCount = words[words.length - 1].count;
    const range = maxCount - minCount || 1;

    // Scale font sizes based on container area
    const area = width * height;
    const scaleFactor = Math.sqrt(area) / 40;
    const minFont = Math.max(10, scaleFactor * 0.3);
    const maxFont = Math.min(72, scaleFactor * 1.5);

    const colorMap = new Map<string, string>();
    words.forEach((w) => colorMap.set(w.text, wordColor(w, source)));

    const layout = cloud()
      .size([width, height])
      .words(
        words.map((w) => ({
          text: w.text,
          size: minFont + ((w.count - minCount) / range) * (maxFont - minFont),
          count: w.count,
          type: w.type,
        }))
      )
      .padding(4)
      .rotate(() => (Math.random() > 0.7 ? 90 : 0))
      .font("system-ui, -apple-system, sans-serif")
      .fontSize((d: any) => d.size)
      .spiral("archimedean")
      .on("end", (output: any[]) => {
        setLayoutWords(
          output.map((d) => ({
            text: d.text!,
            size: d.size!,
            x: d.x!,
            y: d.y!,
            rotate: d.rotate!,
            color: colorMap.get(d.text!) || "#4fc3f7",
            count: d.count,
            type: d.type,
          }))
        );
      });

    layout.start();
  }, [words, source]);

  const handleWordClick = (word: string) => {
    setSelected(word);
    setArticlesLoading(true);
    setSynthesis(null);
    setArticleSummaries({});
    const filters = {
      time_from: timeFrom || undefined,
      time_to: timeTo || undefined,
      limit: 30,
    };
    if (source === "entities") {
      fetchEntityArticles(word, { ...filters, q: activeSearch || undefined })
        .then(setArticles)
        .catch(console.error)
        .finally(() => setArticlesLoading(false));
    } else {
      // Combine active search + clicked word for text search (space = AND)
      const combined = activeSearch ? `${activeSearch} ${word}` : word;
      fetchTextSearch(combined, filters)
        .then(setArticles)
        .catch(console.error)
        .finally(() => setArticlesLoading(false));
    }
  };

  const handleSearch = () => {
    setActiveSearch(searchInput.trim());
  };

  const handleClearSearch = () => {
    setSearchInput("");
    setActiveSearch("");
  };

  const handleSummarizeAll = () => {
    if (articles.length === 0) return;
    setSynthesisLoading(true);
    setSynthesis(null);
    const ids = articles.map((a) => a.article_id);
    const query = activeSearch ? `${activeSearch} — ${selected}` : selected || undefined;
    summarizeArticles(ids, query)
      .then((result) => {
        if (result.error) {
          setSynthesis(`Error: ${result.error}`);
        } else {
          setSynthesis(result.synthesis);
        }
      })
      .catch((err) => setSynthesis(`Error: ${err.message}`))
      .finally(() => setSynthesisLoading(false));
  };

  const handleSummarizeOne = (articleId: string) => {
    setSummarizingId(articleId);
    summarizeArticle(articleId)
      .then((result) => {
        if (result.error) {
          setArticleSummaries((prev) => ({ ...prev, [articleId]: `Error: ${result.error}` }));
        } else {
          setArticleSummaries((prev) => ({ ...prev, [articleId]: result.summary }));
        }
      })
      .catch((err) => setArticleSummaries((prev) => ({ ...prev, [articleId]: `Error: ${err.message}` })))
      .finally(() => setSummarizingId(null));
  };

  return (
    <div className="wordcloud-view">
      <div className="wordcloud-tabs">
        <div className="wordcloud-search-group">
          <input
            className="wordcloud-search-input"
            placeholder="(A | B) topic:environment ..."
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleSearch()}
          />
          <button className="search-btn" onClick={handleSearch}>Search</button>
          {activeSearch && (
            <button className="wordcloud-clear-btn" onClick={handleClearSearch} title="Clear search">
              x
            </button>
          )}
        </div>
        <div className="wordcloud-source-tabs">
          {(Object.keys(SOURCE_LABELS) as Source[]).map((s) => (
            <button
              key={s}
              className={`wordcloud-tab ${source === s ? "active" : ""}`}
              onClick={() => setSource(s)}
            >
              {SOURCE_LABELS[s]}
            </button>
          ))}
        </div>
        <span className="wordcloud-count">
          {layoutWords.length} words
          {activeSearch && <span className="wordcloud-search-pill">{activeSearch}</span>}
        </span>
      </div>

      <div className="wordcloud-body">
        <div
          ref={containerRef}
          className={`wordcloud-cloud ${selected ? "cloud-with-panel" : ""}`}
        >
          {loading && <div className="loading-msg">Loading...</div>}
          {!loading && words.length === 0 && (
            <div className="loading-msg">No data for this time range</div>
          )}
          {!loading && layoutWords.length > 0 && (
            <svg
              ref={svgRef}
              className="wordcloud-svg"
              width="100%"
              height="100%"
              viewBox={containerRef.current
                ? `${-containerRef.current.getBoundingClientRect().width / 2} ${-containerRef.current.getBoundingClientRect().height / 2} ${containerRef.current.getBoundingClientRect().width} ${containerRef.current.getBoundingClientRect().height}`
                : "-400 -250 800 500"
              }
            >
              {layoutWords.map((w) => (
                <text
                  key={w.text + (w.type || "")}
                  className="cloud-word-svg"
                  textAnchor="middle"
                  transform={`translate(${w.x},${w.y}) rotate(${w.rotate})`}
                  style={{
                    fontSize: w.size,
                    fill: w.color,
                    opacity: selected && selected !== w.text ? 0.2 : 1,
                    cursor: "pointer",
                    fontWeight: w.size > 30 ? 600 : 400,
                    fontFamily: "system-ui, -apple-system, sans-serif",
                  }}
                  onClick={() => handleWordClick(w.text)}
                >
                  <title>{`${w.text}: ${w.count}${w.type ? ` (${w.type})` : ""}`}</title>
                  {w.text}
                </text>
              ))}
            </svg>
          )}
        </div>

        {selected && (
          <div className="wordcloud-panel">
            <div className="panel-header">
              <h2>{selected}</h2>
              <button className="close-btn" onClick={() => { setSelected(null); setArticles([]); setSynthesis(null); setArticleSummaries({}); }}>
                x
              </button>
            </div>
            <div className="panel-body">
              {articlesLoading && <div className="empty">Loading articles...</div>}
              {!articlesLoading && articles.length === 0 && (
                <div className="empty">No articles found</div>
              )}
              {!articlesLoading && articles.length > 0 && (
                <div className="summarize-all-bar">
                  <span className="article-count-label">{articles.length} articles</span>
                  <button
                    className="summarize-btn"
                    onClick={handleSummarizeAll}
                    disabled={synthesisLoading}
                  >
                    {synthesisLoading ? "Summarizing..." : "Summarize all"}
                  </button>
                </div>
              )}
              {synthesis && (
                <div className="synthesis-box">
                  <div className="synthesis-label">Synthesis</div>
                  <div className="synthesis-text">{synthesis}</div>
                </div>
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
                  {!articleSummaries[a.article_id] && a.summary && <div className="summary">{a.summary}</div>}
                  {!articleSummaries[a.article_id] && a.standfirst && !a.summary && (
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
