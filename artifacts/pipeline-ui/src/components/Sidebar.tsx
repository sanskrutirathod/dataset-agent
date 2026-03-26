import { Link, useLocation } from "wouter";
import { cn } from "@/lib/utils";
import { Database, Play, List, Settings, Zap } from "lucide-react";

const NAV = [
  { href: "/", icon: List, label: "Runs" },
  { href: "/new", icon: Play, label: "New Run" },
];

export function Sidebar() {
  const [location] = useLocation();

  return (
    <aside className="flex flex-col w-56 min-h-screen bg-gray-950 text-white border-r border-gray-800">
      <div className="flex items-center gap-2 px-5 py-5 border-b border-gray-800">
        <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-indigo-600">
          <Zap className="w-4 h-4 text-white" />
        </div>
        <div>
          <div className="text-sm font-semibold leading-tight">WEBSPACEAI</div>
          <div className="text-[10px] text-gray-400 leading-tight">Dataset Engine</div>
        </div>
      </div>

      <nav className="flex-1 px-3 py-4 space-y-1">
        {NAV.map(({ href, icon: Icon, label }) => {
          const active = location === href || (href !== "/" && location.startsWith(href));
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                "flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors",
                active
                  ? "bg-indigo-700 text-white"
                  : "text-gray-400 hover:text-white hover:bg-gray-800"
              )}
            >
              <Icon className="w-4 h-4 shrink-0" />
              {label}
            </Link>
          );
        })}
      </nav>

      <div className="px-5 py-4 border-t border-gray-800 text-xs text-gray-500">
        <div className="flex items-center gap-1">
          <Database className="w-3 h-3" />
          <span>8-stage AI pipeline</span>
        </div>
      </div>
    </aside>
  );
}
