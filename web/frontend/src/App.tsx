import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import GraphView from "./views/GraphView";
import RiverView from "./views/RiverView";
import SearchView from "./views/SearchView";
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
              Graph
            </NavLink>
            <NavLink to="/river" className={({ isActive }) => isActive ? "tab active" : "tab"}>
              River
            </NavLink>
            <NavLink to="/search" className={({ isActive }) => isActive ? "tab active" : "tab"}>
              Search
            </NavLink>
          </nav>
          <FilterBar />
        </header>
        <main className="main-content">
          <Routes>
            <Route path="/" element={<GraphView />} />
            <Route path="/river" element={<RiverView />} />
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
