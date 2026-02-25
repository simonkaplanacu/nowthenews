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
