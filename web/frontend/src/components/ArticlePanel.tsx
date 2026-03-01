import { useState } from "react";
import type { Article } from "../api/client";
import { summarizeArticle, summarizeArticles } from "../api/client";
import { exportCSV, exportJSON } from "../utils/export";

interface Props {
  title: string;
  articles: Article[];
  onClose: () => void;
  extraAction?: { label: string; onClick: () => void };
}

export default function ArticlePanel({ title, articles, onClose, extraAction }: Props) {
  const [synthesis, setSynthesis] = useState<string | null>(null);
  const [synthesisLoading, setSynthesisLoading] = useState(false);
  const [articleSummaries, setArticleSummaries] = useState<Record<string, string>>({});
  const [summarizingId, setSummarizingId] = useState<string | null>(null);

  const handleSummarizeAll = () => {
    if (articles.length === 0) return;
    setSynthesisLoading(true);
    setSynthesis(null);
    const ids = articles.map((a) => a.article_id);
    summarizeArticles(ids, title)
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
    <aside className="article-panel">
      <div className="panel-header">
        <h2>{title}</h2>
        <div className="panel-actions">
          {extraAction && (
            <button className="export-btn" onClick={extraAction.onClick}>{extraAction.label}</button>
          )}
          {articles.length > 0 && (
            <>
              <button className="export-btn" onClick={() => exportCSV(articles)} title="Export CSV">CSV</button>
              <button className="export-btn" onClick={() => exportJSON(articles)} title="Export JSON">JSON</button>
            </>
          )}
          <button className="close-btn" onClick={onClose}>x</button>
        </div>
      </div>
      <div className="panel-body">
        {articles.length === 0 && <p className="empty">No articles found.</p>}
        {articles.length > 0 && (
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
            <a href={a.url} target="_blank" rel="noopener noreferrer" className="article-title">
              {a.title || a.headline}
            </a>
            <div className="article-meta">
              <span className="source">{a.source}</span>
              <span className="section">{a.section}</span>
              <span className="date">
                {a.published_at ? new Date(a.published_at).toLocaleDateString() : ""}
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
            {!articleSummaries[a.article_id] && a.standfirst && <p className="standfirst">{a.standfirst}</p>}
            {!articleSummaries[a.article_id] && a.summary && <p className="summary">{a.summary}</p>}
            {a.sentiment && (
              <span className={`badge sentiment-${a.sentiment}`}>{a.sentiment}</span>
            )}
            {a.content_type && (
              <span className="badge content-type">{a.content_type.replace(/_/g, " ")}</span>
            )}
          </div>
        ))}
      </div>
    </aside>
  );
}
