import { useFilters } from "../context/FilterContext";

const ENTITY_TYPES = [
  { value: "", label: "All types" },
  { value: "person", label: "Person" },
  { value: "organisation", label: "Organisation" },
  { value: "place", label: "Place" },
  { value: "event", label: "Event" },
  { value: "work", label: "Work" },
  { value: "product", label: "Product" },
  { value: "concept", label: "Concept" },
  { value: "technology", label: "Technology" },
];

const TIME_PRESETS = [
  { label: "All time", from: "", to: "" },
  { label: "Last 7d", from: daysAgo(7), to: "" },
  { label: "Last 30d", from: daysAgo(30), to: "" },
  { label: "Last 90d", from: daysAgo(90), to: "" },
];

function daysAgo(n: number): string {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString().slice(0, 10);
}

export default function FilterBar() {
  const f = useFilters();

  return (
    <div className="filter-bar">
      <div className="filter-group">
        <label>Time</label>
        <select
          value={f.timeFrom || "all"}
          onChange={(e) => {
            const preset = TIME_PRESETS.find(p => (p.from || "all") === e.target.value);
            if (preset) {
              f.setTimeFrom(preset.from);
              f.setTimeTo(preset.to);
            }
          }}
        >
          {TIME_PRESETS.map(p => (
            <option key={p.label} value={p.from || "all"}>{p.label}</option>
          ))}
        </select>
      </div>

      <div className="filter-group">
        <label>Entity type</label>
        <select value={f.entityType} onChange={(e) => f.setEntityType(e.target.value)}>
          {ENTITY_TYPES.map(t => (
            <option key={t.value} value={t.value}>{t.label}</option>
          ))}
        </select>
      </div>

      <div className="filter-group">
        <label>Min co-occurrence</label>
        <input
          type="range"
          min={1}
          max={20}
          value={f.minCooccurrence}
          onChange={(e) => f.setMinCooccurrence(Number(e.target.value))}
        />
        <span className="filter-value">{f.minCooccurrence}</span>
      </div>

      <div className="filter-group">
        <label>Min articles</label>
        <input
          type="range"
          min={1}
          max={50}
          value={f.minArticles}
          onChange={(e) => f.setMinArticles(Number(e.target.value))}
        />
        <span className="filter-value">{f.minArticles}</span>
      </div>
    </div>
  );
}
