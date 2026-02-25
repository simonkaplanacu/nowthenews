import { createContext, useContext, useState, type ReactNode } from "react";

interface FilterState {
  timeFrom: string;
  timeTo: string;
  entityType: string;
  minCooccurrence: number;
  minArticles: number;
}

interface FilterContextValue extends FilterState {
  setTimeFrom: (v: string) => void;
  setTimeTo: (v: string) => void;
  setEntityType: (v: string) => void;
  setMinCooccurrence: (v: number) => void;
  setMinArticles: (v: number) => void;
}

const FilterContext = createContext<FilterContextValue | null>(null);

export function FilterProvider({ children }: { children: ReactNode }) {
  const [timeFrom, setTimeFrom] = useState("");
  const [timeTo, setTimeTo] = useState("");
  const [entityType, setEntityType] = useState("");
  const [minCooccurrence, setMinCooccurrence] = useState(2);
  const [minArticles, setMinArticles] = useState(3);

  return (
    <FilterContext.Provider
      value={{
        timeFrom, setTimeFrom,
        timeTo, setTimeTo,
        entityType, setEntityType,
        minCooccurrence, setMinCooccurrence,
        minArticles, setMinArticles,
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
