import { useState, useEffect, useCallback } from "react";
import type { Alert, SavedSearch, SearchMatch } from "../api/client";
import {
  fetchAlerts,
  acknowledgeAlert,
  fetchSavedSearches,
  createSavedSearch,
  deleteSavedSearch,
  fetchSearchMatches,
} from "../api/client";

const SEVERITY_COLORS: Record<string, string> = {
  critical: "#c62828",
  warning: "#e65100",
  info: "#1565c0",
};

const TYPE_LABELS: Record<string, string> = {
  enrichment_crash: "Enrichment Crash",
  api_limit: "API Limit",
  ingestion_failure: "Ingestion Failure",
  stale_db: "Stale DB",
  search_match: "Search Match",
};

export default function AlertsView() {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [searches, setSearches] = useState<SavedSearch[]>([]);
  const [matches, setMatches] = useState<SearchMatch[]>([]);
  const [typeFilter, setTypeFilter] = useState("");
  const [showAcknowledged, setShowAcknowledged] = useState(false);
  const [newLabel, setNewLabel] = useState("");
  const [newQuery, setNewQuery] = useState("");

  const loadAlerts = useCallback(() => {
    const params: Record<string, string | number> = { limit: 100 };
    if (typeFilter) params.alert_type = typeFilter;
    if (!showAcknowledged) params.acknowledged = 0;
    fetchAlerts(params).then(setAlerts).catch(console.error);
  }, [typeFilter, showAcknowledged]);

  const loadSearches = useCallback(() => {
    fetchSavedSearches().then(setSearches).catch(console.error);
    fetchSearchMatches({ limit: 50 }).then(setMatches).catch(console.error);
  }, []);

  useEffect(() => { loadAlerts(); }, [loadAlerts]);
  useEffect(() => { loadSearches(); }, [loadSearches]);

  const handleAcknowledge = async (id: string) => {
    await acknowledgeAlert(id);
    loadAlerts();
  };

  const handleCreateSearch = async () => {
    if (!newLabel.trim() || !newQuery.trim()) return;
    await createSavedSearch(newLabel.trim(), newQuery.trim());
    setNewLabel("");
    setNewQuery("");
    loadSearches();
  };

  const handleDeleteSearch = async (id: string) => {
    await deleteSavedSearch(id);
    loadSearches();
  };

  return (
    <div className="alerts-view">
      <div className="alerts-columns">
        {/* System Alerts */}
        <div className="alerts-section">
          <div className="alerts-section-header">
            <h2>System Alerts</h2>
            <div className="alerts-filters">
              <select
                value={typeFilter}
                onChange={(e) => setTypeFilter(e.target.value)}
                className="alerts-select"
              >
                <option value="">All types</option>
                {Object.entries(TYPE_LABELS).map(([k, v]) => (
                  <option key={k} value={k}>{v}</option>
                ))}
              </select>
              <label className="alerts-checkbox">
                <input
                  type="checkbox"
                  checked={showAcknowledged}
                  onChange={(e) => setShowAcknowledged(e.target.checked)}
                />
                Show acknowledged
              </label>
            </div>
          </div>
          <div className="alerts-list">
            {alerts.length === 0 && (
              <div className="alerts-empty">No alerts</div>
            )}
            {alerts.map((a) => (
              <div key={a.alert_id} className={`alert-card ${a.acknowledged ? "alert-acked" : ""}`}>
                <div className="alert-header">
                  <span
                    className="alert-severity"
                    style={{ background: SEVERITY_COLORS[a.severity] || "#455a64" }}
                  >
                    {a.severity}
                  </span>
                  <span className="alert-type">
                    {TYPE_LABELS[a.alert_type] || a.alert_type}
                  </span>
                  <span className="alert-time">
                    {new Date(a.created_at).toLocaleString()}
                  </span>
                </div>
                <div className="alert-message">{a.message}</div>
                {!a.acknowledged && (
                  <button
                    className="alert-ack-btn"
                    onClick={() => handleAcknowledge(a.alert_id)}
                  >
                    Acknowledge
                  </button>
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Saved Searches */}
        <div className="alerts-section">
          <div className="alerts-section-header">
            <h2>Saved Searches</h2>
          </div>

          {/* Create form */}
          <div className="search-create-form">
            <input
              className="search-create-input"
              placeholder="Label (e.g. David Pocock)"
              value={newLabel}
              onChange={(e) => setNewLabel(e.target.value)}
            />
            <input
              className="search-create-input"
              placeholder="Search query"
              value={newQuery}
              onChange={(e) => setNewQuery(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleCreateSearch()}
            />
            <button className="search-create-btn" onClick={handleCreateSearch}>
              Add
            </button>
          </div>

          {/* Existing searches */}
          <div className="saved-searches-list">
            {searches.map((s) => (
              <div key={s.search_id} className="saved-search-card">
                <div className="saved-search-info">
                  <span className="saved-search-label">{s.label}</span>
                  <span className="saved-search-query">"{s.query}"</span>
                  <span className="saved-search-date">
                    since {new Date(s.created_at).toLocaleDateString()}
                  </span>
                </div>
                <button
                  className="saved-search-delete"
                  onClick={() => handleDeleteSearch(s.search_id)}
                  title="Delete"
                >
                  x
                </button>
              </div>
            ))}
          </div>

          {/* Recent matches */}
          <div className="matches-header">
            <h3>Recent Matches</h3>
          </div>
          <div className="matches-list">
            {matches.length === 0 && (
              <div className="alerts-empty">No matches yet</div>
            )}
            {matches.map((m) => (
              <div key={m.match_id} className="match-card">
                <div className="match-search-label">{m.search_label}</div>
                <a
                  href={m.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="match-title"
                >
                  {m.title}
                </a>
                <div className="match-meta">
                  {m.published_at && new Date(m.published_at).toLocaleDateString()}
                  {" — matched "}
                  {new Date(m.matched_at).toLocaleString()}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
