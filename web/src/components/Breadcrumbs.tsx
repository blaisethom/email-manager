import { Fragment } from 'react';
import { Link, useLocation } from 'react-router-dom';

export interface BreadcrumbEntry {
  label: string;
  path: string;
}

interface BreadcrumbsProps {
  current: string;
  defaultTrail: BreadcrumbEntry[];
}

/** Read breadcrumb trail from router location state. */
export function getBreadcrumbs(state: unknown): BreadcrumbEntry[] {
  const s = state as { breadcrumbs?: BreadcrumbEntry[] } | null;
  return s?.breadcrumbs ?? [];
}

/** Build location state for a link, extending the current trail with a new entry. */
export function extendBreadcrumbs(
  currentState: unknown,
  entry: BreadcrumbEntry,
): { breadcrumbs: BreadcrumbEntry[] } {
  return { breadcrumbs: [...getBreadcrumbs(currentState), entry] };
}

export default function Breadcrumbs({ current, defaultTrail }: BreadcrumbsProps) {
  const location = useLocation();
  const trail = getBreadcrumbs(location.state);
  const crumbs = trail.length > 0 ? trail : defaultTrail;

  return (
    <nav className="flex items-center gap-1.5 text-sm mb-6 min-w-0">
      {crumbs.map((crumb, i) => (
        <Fragment key={i}>
          <Link
            to={crumb.path}
            state={i > 0 ? { breadcrumbs: crumbs.slice(0, i) } : undefined}
            className="text-slate-500 hover:text-slate-700 transition-colors whitespace-nowrap"
          >
            {crumb.label}
          </Link>
          <span className="text-slate-300">/</span>
        </Fragment>
      ))}
      <span className="text-slate-700 font-medium truncate">{current}</span>
    </nav>
  );
}
