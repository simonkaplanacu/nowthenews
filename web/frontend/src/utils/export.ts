import type { Article } from "../api/client";

function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

export function exportCSV(articles: Article[], filename = "articles.csv") {
  const headers = ["title", "headline", "section", "published_at", "sentiment", "content_type", "url"];
  const escape = (s: string | undefined) => {
    if (!s) return "";
    return `"${s.replace(/"/g, '""')}"`;
  };
  const rows = articles.map((a) =>
    headers.map((h) => escape(String((a as any)[h] ?? ""))).join(",")
  );
  const csv = [headers.join(","), ...rows].join("\n");
  downloadBlob(new Blob([csv], { type: "text/csv" }), filename);
}

export function exportJSON(articles: Article[], filename = "articles.json") {
  const json = JSON.stringify(articles, null, 2);
  downloadBlob(new Blob([json], { type: "application/json" }), filename);
}
