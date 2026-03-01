import { useState, useCallback } from "react";
import { useFilters } from "../context/FilterContext";
import { fetchTextSearch, summarizeArticle, summarizeArticles, type Article } from "../api/client";
import { exportCSV, exportJSON } from "../utils/export";

export default function SearchView() {
  const filters = useFilters();
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<Article[]>([]);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const [synthesis, setSynthesis] = useState<string | null>(null);
  const [synthesisLoading, setSynthesisLoading] = useState(false);
  const [articleSummaries, setArticleSummaries] = useState<Record<string, string>>({});
  const [summarizingId, setSummarizingId] = useState<string | null>(null);

  const handleSearch = useCallback(async () => {
    if (!query.trim()) return;
    setLoading(true);
    setSearched(true);
    setSynthesis(null);
    setArticleSummaries({});
    try {
      const data = await fetchTextSearch(query.trim(), {
        time_from: filters.timeFrom || undefined,
        time_to: filters.timeTo || undefined,
        limit: 100,
      });
      setResults(data);
    } catch (err) {
      console.error("Search failed:", err);
      setResults([]);
    } finally {
      setLoading(false);
    }
  }, [query, filters.timeFrom, filters.timeTo]);

  const handleSummarizeAll = () => {
    if (results.length === 0) return;
    setSynthesisLoading(true);
    setSynthesis(null);
    const ids = results.map((a) => a.article_id);
    summarizeArticles(ids, query)
      .then((result) => {
        setSynthesis(result.error ? `Error: ${result.error}` : result.synthesis);
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
    <div className="search-view">
      <div className="search-view-bar">
        <input
          type="text"
          placeholder="(A | B) topic:environment ... use | for OR, topic: for topics"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleSearch()}
          className="search-view-input"
          disabled={loading}
        />
        <button onClick={handleSearch} className="search-btn" disabled={loading}>
          {loading ? "Searching..." : "Search"}
        </button>
        {searched && (
          <span className="search-result-count">
            {loading ? "" : `${results.length} result${results.length !== 1 ? "s" : ""}`}
          </span>
        )}
        {results.length > 0 && (
          <>
            <button className="export-btn" onClick={() => exportCSV(results)}>CSV</button>
            <button className="export-btn" onClick={() => exportJSON(results)}>JSON</button>
            <button
              className="summarize-btn"
              onClick={handleSummarizeAll}
              disabled={synthesisLoading}
            >
              {synthesisLoading ? "Summarizing..." : "Summarize all"}
            </button>
          </>
        )}
      </div>
      {synthesis && (
        <div className="synthesis-box" style={{ margin: "8px 16px" }}>
          <div className="synthesis-label">Synthesis</div>
          <div className="synthesis-text">{synthesis}</div>
        </div>
      )}
      <div className="search-results">
        {!searched && (
          <div className="search-hint">
            Search across all article text. Use <code>|</code> for OR, <code>topic:X</code> for topic filter, parentheses to group.
          </div>
        )}
        {searched && !loading && results.length === 0 && (
          <div className="search-hint">No articles found.</div>
        )}
        {results.map((a) => (
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
            {!articleSummaries[a.article_id] && a.standfirst && <p className="standfirst">{a.standfirst}</p>}
          </div>
        ))}
      </div>
    </div>
  );
}
