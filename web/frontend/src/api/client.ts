type FetchParams = Record<string, string | number | undefined>;

async function get<T>(path: string, params?: FetchParams): Promise<T> {
  const qs = new URLSearchParams();
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== null && v !== "") {
        qs.set(k, String(v));
      }
    }
  }
  const query = qs.toString();
  const url = path + (query ? `?${query}` : "");
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`API ${resp.status}: ${resp.statusText}`);
  return resp.json();
}

export interface Entity {
  name: string;
  type: string;
  article_count: number;
}

export interface GraphNode {
  id: string;
  name: string;
  type: string;
  article_count: number;
}

export interface GraphEdge {
  source: string;
  target: string;
  weight: number;
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface Article {
  article_id: string;
  title: string;
  headline: string;
  standfirst: string;
  source: string;
  section: string;
  published_at: string;
  url: string;
  sentiment?: string;
  content_type?: string;
  summary?: string;
}

export interface TopicSeries {
  topic: string;
  values: number[];
}

export interface TopicRiverData {
  timestamps: string[];
  series: TopicSeries[];
}

export interface Stats {
  total_articles: number;
  enriched_articles: number;
}

export interface Filters {
  time_from?: string;
  time_to?: string;
  entity_type?: string;
  topic?: string;
  region?: string;
  min_cooccurrence?: number;
  min_articles?: number;
}

export interface NLFilters {
  topics?: string[];
  regions?: string[];
  entity_type?: string;
  time_from?: string;
  time_to?: string;
}

export function fetchEntities(filters: Filters & { limit?: number }) {
  return get<Entity[]>("/api/entities", { ...filters });
}

export function fetchEntityGraph(filters: Filters & { limit?: number }) {
  return get<GraphData>("/api/entity-graph", { ...filters });
}

export function fetchEntityArticles(name: string, filters?: Filters) {
  return get<Article[]>(`/api/entity/${encodeURIComponent(name)}/articles`, filters ? { ...filters } : undefined);
}

export function fetchCooccurrenceArticles(a: string, b: string, filters?: Filters) {
  return get<Article[]>(
    `/api/cooccurrence/${encodeURIComponent(a)}/${encodeURIComponent(b)}/articles`,
    filters ? { ...filters } : undefined
  );
}

export function fetchEntityEgo(name: string, filters?: { time_from?: string; time_to?: string; entity_type?: string; limit?: number }) {
  return get<GraphData>(`/api/entity-ego/${encodeURIComponent(name)}`, filters ? { ...filters } : undefined);
}

export function fetchTopicRiver(filters?: { time_from?: string; time_to?: string; region?: string; bucket?: string }) {
  return get<TopicRiverData>("/api/topic-river", filters);
}

export function fetchArticles(filters?: Record<string, string | number | undefined>) {
  return get<Article[]>("/api/articles", filters);
}

export function fetchStats() {
  return get<Stats>("/api/stats");
}

export function fetchNLQuery(q: string): Promise<NLFilters> {
  return get<NLFilters>("/api/nl-query", { q });
}

export function fetchTextSearch(q: string, filters?: { time_from?: string; time_to?: string; limit?: number; offset?: number }) {
  return get<Article[]>("/api/text-search", { q, ...filters });
}

// --- Sentiment Heatmap ---

export interface HeatmapCell {
  ts: string;
  topic: string;
  avg_sentiment: number;
  count: number;
}

export interface HeatmapData {
  timestamps: string[];
  topics: string[];
  cells: HeatmapCell[];
}

export function fetchSentimentHeatmap(filters?: { time_from?: string; time_to?: string; region?: string; bucket?: string }) {
  return get<HeatmapData>("/api/sentiment-heatmap", filters);
}

// --- Entity Timeline ---

export interface TimelineSeries {
  entity: string;
  values: number[];
}

export interface TimelineData {
  timestamps: string[];
  series: TimelineSeries[];
}

export function fetchEntityTimeline(entities: string[], filters?: { time_from?: string; time_to?: string; bucket?: string }) {
  return get<TimelineData>("/api/entity-timeline", { entities: entities.join(","), ...filters });
}

// --- Region Overview ---

export interface RegionInfo {
  region: string;
  article_count: number;
  avg_sentiment: number;
  top_entities: { name: string; count: number }[];
}

export interface RegionOverviewData {
  regions: RegionInfo[];
}

export function fetchRegionOverview(filters?: { time_from?: string; time_to?: string; topic?: string }) {
  return get<RegionOverviewData>("/api/region-overview", filters);
}

// --- Topic Trends ---

export interface TopicTrend {
  topic: string;
  current_count: number;
  previous_count: number;
  pct_change: number;
}

export interface TopicTrendsData {
  trends: TopicTrend[];
}

export function fetchTopicTrends(filters?: { weeks?: number; region?: string }) {
  return get<TopicTrendsData>("/api/topic-trends", filters);
}

// --- Alerts ---

export interface Alert {
  alert_id: string;
  alert_type: string;
  severity: string;
  message: string;
  context: string;
  created_at: string;
  acknowledged: number;
}

export function fetchAlerts(params?: { alert_type?: string; severity?: string; acknowledged?: number; limit?: number }) {
  return get<Alert[]>("/api/alerts", params);
}

export async function acknowledgeAlert(alertId: string): Promise<{ status: string }> {
  const resp = await fetch(`/api/alerts/${alertId}/acknowledge`, { method: "POST" });
  if (!resp.ok) throw new Error(`API ${resp.status}`);
  return resp.json();
}

// --- Saved Searches ---

export interface SavedSearch {
  search_id: string;
  label: string;
  query: string;
  email: string;
  active: number;
  created_at: string;
}

export function fetchSavedSearches() {
  return get<SavedSearch[]>("/api/saved-searches");
}

export async function createSavedSearch(label: string, query: string): Promise<{ search_id: string }> {
  const resp = await fetch("/api/saved-searches", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ label, query }),
  });
  if (!resp.ok) throw new Error(`API ${resp.status}`);
  return resp.json();
}

export async function deleteSavedSearch(searchId: string): Promise<{ status: string }> {
  const resp = await fetch(`/api/saved-searches/${searchId}`, { method: "DELETE" });
  if (!resp.ok) throw new Error(`API ${resp.status}`);
  return resp.json();
}

// --- Search Matches ---

export interface SearchMatch {
  match_id: string;
  search_id: string;
  article_id: string;
  matched_at: string;
  search_label: string;
  search_query: string;
  title: string;
  published_at: string;
  url: string;
}

export function fetchSearchMatches(params?: { search_id?: string; limit?: number }) {
  return get<SearchMatch[]>("/api/search-matches", params);
}
