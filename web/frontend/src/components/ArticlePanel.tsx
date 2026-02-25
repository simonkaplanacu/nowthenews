import type { Article } from "../api/client";

interface Props {
  title: string;
  articles: Article[];
  onClose: () => void;
}

export default function ArticlePanel({ title, articles, onClose }: Props) {
  return (
    <aside className="article-panel">
      <div className="panel-header">
        <h2>{title}</h2>
        <button className="close-btn" onClick={onClose}>×</button>
      </div>
      <div className="panel-body">
        {articles.length === 0 && <p className="empty">No articles found.</p>}
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
            </div>
            {a.standfirst && <p className="standfirst">{a.standfirst}</p>}
            {a.summary && <p className="summary">{a.summary}</p>}
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
