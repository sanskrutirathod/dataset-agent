import { Link, useLocation } from "wouter";
import { cn } from "@/lib/utils";
import { Database, Play, List, Zap } from "lucide-react";

const NAV = [
  { href: "/", icon: List, label: "Runs" },
  { href: "/new", icon: Play, label: "New Run" },
];

export function Sidebar() {
  const [location] = useLocation();

  return (
    <aside className="flex flex-col w-56 min-h-screen bg-sidebar text-sidebar-foreground border-r border-sidebar-border">
      <div className="flex items-center gap-2.5 px-5 py-5 border-b border-sidebar-border">
        <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-primary">
          <Zap className="w-4 h-4 text-primary-foreground" />
        </div>
        <div>
          <div className="text-sm font-bold leading-tight tracking-wide">WEBSPACEAI</div>
          <div className="text-[10px] text-muted-foreground leading-tight mt-0.5">Dataset Engine</div>
        </div>
      </div>

      <nav className="flex-1 px-3 py-4 space-y-0.5">
        {NAV.map(({ href, icon: Icon, label }) => {
          const active = location === href || (href !== "/" && location.startsWith(href));
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                "flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-all",
                active
                  ? "bg-primary text-primary-foreground shadow-sm"
                  : "text-muted-foreground hover:text-sidebar-foreground hover:bg-sidebar-accent"
              )}
            >
              <Icon className="w-4 h-4 shrink-0" />
              {label}
            </Link>
          );
        })}
      </nav>

      <div className="px-5 py-4 border-t border-sidebar-border">
        <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
          <Database className="w-3 h-3" />
          <span>8-stage AI pipeline</span>
        </div>
      </div>
    </aside>
  );
}
