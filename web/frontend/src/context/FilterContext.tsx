import { createContext, useContext, useState, useCallback, type ReactNode } from "react";
import { useNavigate } from "react-router-dom";

interface FilterState {
  timeFrom: string;
  timeTo: string;
  entityType: string;
  topic: string;
  minCooccurrence: number;
  minArticles: number;
}

interface FilterContextValue extends FilterState {
  setTimeFrom: (v: string) => void;
  setTimeTo: (v: string) => void;
  setEntityType: (v: string) => void;
  setTopic: (v: string) => void;
  setMinCooccurrence: (v: number) => void;
  setMinArticles: (v: number) => void;
  drillToGraph: (topic: string) => void;
}

const FilterContext = createContext<FilterContextValue | null>(null);

export function FilterProvider({ children }: { children: ReactNode }) {
  const [timeFrom, setTimeFrom] = useState("");
  const [timeTo, setTimeTo] = useState("");
  const [entityType, setEntityType] = useState("");
  const [topic, setTopic] = useState("");
  const [minCooccurrence, setMinCooccurrence] = useState(2);
  const [minArticles, setMinArticles] = useState(3);

  // drillToGraph is a no-op placeholder here; actual navigation
  // happens in components that have access to useNavigate.
  const drillToGraph = useCallback((_topic: string) => {}, []);

  return (
    <FilterContext.Provider
      value={{
        timeFrom, setTimeFrom,
        timeTo, setTimeTo,
        entityType, setEntityType,
        topic, setTopic,
        minCooccurrence, setMinCooccurrence,
        minArticles, setMinArticles,
        drillToGraph,
      }}
    >
      {children}
    </FilterContext.Provider>
  );
}

export function useFilters() {
  const ctx = useContext(FilterContext);
  if (!ctx) throw new Error("useFilters must be inside FilterProvider");
  return ctx;
}
