import { useState, useCallback } from "react";
import { useFilters } from "../context/FilterContext";
import { fetchTextSearch, type Article } from "../api/client";

export default function SearchView() {
  const filters = useFilters();
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<Article[]>([]);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);

  const handleSearch = useCallback(async () => {
    if (!query.trim()) return;
    setLoading(true);
    setSearched(true);
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

  return (
    <div className="search-view">
      <div className="search-view-bar">
        <input
          type="text"
          placeholder="Search article text... (comma-separate for AND, e.g. &quot;Camp David, Mar-a-Lago&quot;)"
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
      </div>
      <div className="search-results">
        {!searched && (
          <div className="search-hint">
            Search across all article text — title, headline, and body.
            Separate multiple terms with commas to require all of them.
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
            </div>
            {a.standfirst && <p className="standfirst">{a.standfirst}</p>}
          </div>
        ))}
      </div>
    </div>
  );
}
