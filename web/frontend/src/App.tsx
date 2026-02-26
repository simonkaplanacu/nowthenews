import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import GraphView from "./views/GraphView";
import RiverView from "./views/RiverView";
import SearchView from "./views/SearchView";
import HeatmapView from "./views/HeatmapView";
import TimelineView from "./views/TimelineView";
import GeoView from "./views/GeoView";
import CompareView from "./views/CompareView";
import DashboardView from "./views/DashboardView";
import { FilterProvider } from "./context/FilterContext";
import FilterBar from "./components/FilterBar";
import "./App.css";

function AppContent() {
  return (
    <BrowserRouter>
      <div className="app">
        <header className="top-bar">
          <h1 className="logo">NowTheNews</h1>
          <nav className="nav-tabs">
            <NavLink to="/" end className={({ isActive }) => isActive ? "tab active" : "tab"}>
              Dashboard
            </NavLink>
            <NavLink to="/graph" className={({ isActive }) => isActive ? "tab active" : "tab"}>
              Graph
            </NavLink>
            <NavLink to="/river" className={({ isActive }) => isActive ? "tab active" : "tab"}>
              River
            </NavLink>
            <NavLink to="/heatmap" className={({ isActive }) => isActive ? "tab active" : "tab"}>
              Heatmap
            </NavLink>
            <NavLink to="/timeline" className={({ isActive }) => isActive ? "tab active" : "tab"}>
              Timeline
            </NavLink>
            <NavLink to="/geo" className={({ isActive }) => isActive ? "tab active" : "tab"}>
              Regions
            </NavLink>
            <NavLink to="/compare" className={({ isActive }) => isActive ? "tab active" : "tab"}>
              Compare
            </NavLink>
            <NavLink to="/search" className={({ isActive }) => isActive ? "tab active" : "tab"}>
              Search
            </NavLink>
          </nav>
          <FilterBar />
        </header>
        <main className="main-content">
          <Routes>
            <Route path="/" element={<DashboardView />} />
            <Route path="/graph" element={<GraphView />} />
            <Route path="/river" element={<RiverView />} />
            <Route path="/heatmap" element={<HeatmapView />} />
            <Route path="/timeline" element={<TimelineView />} />
            <Route path="/geo" element={<GeoView />} />
            <Route path="/compare" element={<CompareView />} />
            <Route path="/search" element={<SearchView />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}

export default function App() {
  return (
    <FilterProvider>
      <AppContent />
    </FilterProvider>
  );
}
