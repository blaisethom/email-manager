import { Outlet, NavLink } from 'react-router-dom';
import { useEffect, useState } from 'react';
import { api } from '../api';
import type { MetaResponse } from '../types';

function BuildingIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="3" width="18" height="18" rx="2" />
      <path d="M9 3v18" />
      <path d="M3 9h6" />
      <path d="M3 15h6" />
      <path d="M12 9h9" />
      <path d="M12 15h9" />
    </svg>
  );
}

function PersonIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="8" r="4" />
      <path d="M4 20c0-4 3.6-7 8-7s8 3 8 7" />
    </svg>
  );
}

function ChatIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
    </svg>
  );
}

function EnvelopeIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="2" y="4" width="20" height="16" rx="2" />
      <path d="m22 7-10 7L2 7" />
    </svg>
  );
}

export default function Layout() {
  const [meta, setMeta] = useState<MetaResponse | null>(null);

  useEffect(() => {
    api.getMeta().then(setMeta).catch(console.error);
  }, []);

  return (
    <div className="flex h-screen overflow-hidden bg-slate-50">
      {/* Sidebar */}
      <aside className="w-56 flex-shrink-0 bg-slate-900 flex flex-col">
        {/* Logo */}
        <div className="flex items-center gap-2.5 px-5 py-5 border-b border-slate-800">
          <div className="w-8 h-8 rounded-lg bg-blue-600 flex items-center justify-center text-white flex-shrink-0">
            <EnvelopeIcon />
          </div>
          <span className="text-white font-semibold text-sm">Email Manager</span>
        </div>

        {/* Nav */}
        <nav className="flex-1 px-3 py-4 space-y-1">
          <NavLink
            to="/companies"
            className={({ isActive }) =>
              `sidebar-link${isActive ? ' active' : ''}`
            }
          >
            <BuildingIcon />
            Companies
          </NavLink>
          <NavLink
            to="/contacts"
            className={({ isActive }) =>
              `sidebar-link${isActive ? ' active' : ''}`
            }
          >
            <PersonIcon />
            Contacts
          </NavLink>
          <NavLink
            to="/discussions"
            className={({ isActive }) =>
              `sidebar-link${isActive ? ' active' : ''}`
            }
          >
            <ChatIcon />
            Discussions
          </NavLink>
        </nav>

        {/* Stats footer */}
        {meta && (
          <div className="px-4 py-4 border-t border-slate-800">
            <p className="text-xs text-slate-500 uppercase tracking-wider mb-3 font-medium">Database</p>
            <div className="space-y-1.5">
              <div className="flex justify-between text-xs">
                <span className="text-slate-400">Companies</span>
                <span className="text-slate-200 font-medium">{meta.stats.companies.toLocaleString()}</span>
              </div>
              <div className="flex justify-between text-xs">
                <span className="text-slate-400">Contacts</span>
                <span className="text-slate-200 font-medium">{meta.stats.contacts.toLocaleString()}</span>
              </div>
              <div className="flex justify-between text-xs">
                <span className="text-slate-400">Discussions</span>
                <span className="text-slate-200 font-medium">{meta.stats.discussions.toLocaleString()}</span>
              </div>
              <div className="flex justify-between text-xs">
                <span className="text-slate-400">Emails</span>
                <span className="text-slate-200 font-medium">{meta.stats.emails.toLocaleString()}</span>
              </div>
            </div>
          </div>
        )}
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-y-auto">
        <Outlet />
      </main>
    </div>
  );
}
