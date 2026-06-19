import type {
  CompaniesResponse,
  CompanyDetail,
  CompanyInsights,
  ContactsResponse,
  ContactDetail,
  DiscussionsResponse,
  DiscussionDetail,
  ActionsResponse,
  MetaResponse,
  ThreadEmail,
  CalendarEventsResponse,
  ProposedAction,
  PipelineJob,
  PipelineStage,
  JobConfig,
  JobsResponse,
  SearchResponse,
  OrganizationsResponse,
  OrganizationDetail,
  PeopleResponse,
  PersonDetail,
  TasksResponse,
  HubSpotTaskDetail,
} from './types';

const BASE = '/api';

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  return res.json() as Promise<T>;
}

function buildQuery(params: Record<string, string | number | undefined>): string {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== '') {
      q.set(k, String(v));
    }
  }
  const s = q.toString();
  return s ? `?${s}` : '';
}

export interface CompaniesParams extends Record<string, string | number | undefined> {
  q?: string;
  label?: string;
  stale?: string;
  source?: string;
  sort?: string;
  order?: string;
  page?: number;
  limit?: number;
}

export interface ContactsParams extends Record<string, string | number | undefined> {
  q?: string;
  company?: string;
  source?: string;
  sort?: string;
  order?: string;
  page?: number;
  limit?: number;
}

export interface DiscussionsParams extends Record<string, string | number | undefined> {
  q?: string;
  category?: string;
  state?: string;
  exclude_states?: string;
  company_id?: string | number;
  sort?: string;
  order?: string;
  page?: number;
  limit?: number;
}

export interface ActionsParams extends Record<string, string | number | undefined> {
  q?: string;
  status?: string;
  assignee?: string;
  company_id?: string | number;
  discussion_id?: string | number;
  sort?: string;
  order?: string;
  page?: number;
  limit?: number;
}

export interface JobsParams extends Record<string, string | number | undefined> {
  status?: string;
  page?: number;
  limit?: number;
}

export interface SearchParams extends Record<string, string | number | undefined> {
  q: string;
  limit?: number;
  page?: number;
  company?: string;
  label?: string;
  from?: string;
  to?: string;
  category?: string;
  discussions?: string;
  model?: string;
}

export interface TasksParams extends Record<string, string | number | undefined> {
  status?: string;
  q?: string;
  page?: number;
  limit?: number;
}

export interface CalendarEventsParams extends Record<string, string | number | undefined> {
  q?: string;
  from?: string;
  to?: string;
  status?: string;
  sort?: string;
  order?: string;
  page?: number;
  limit?: number;
}

export const api = {
  getMeta(): Promise<MetaResponse> {
    return fetchJson<MetaResponse>(`${BASE}/meta`);
  },

  getCompanies(params: CompaniesParams = {}): Promise<CompaniesResponse> {
    return fetchJson<CompaniesResponse>(`${BASE}/companies${buildQuery(params)}`);
  },

  getCompany(id: number): Promise<CompanyDetail> {
    return fetchJson<CompanyDetail>(`${BASE}/companies/${id}`);
  },

  getCompanyHomepage(id: number): Promise<{ content: string; domain: string; fetched_at: string }> {
    return fetchJson(`${BASE}/companies/${id}/homepage`);
  },

  getCompanyInsights(id: number): Promise<CompanyInsights> {
    return fetchJson<CompanyInsights>(`${BASE}/companies/${id}/insights`);
  },

  getContacts(params: ContactsParams = {}): Promise<ContactsResponse> {
    return fetchJson<ContactsResponse>(`${BASE}/contacts${buildQuery(params)}`);
  },

  getContact(email: string): Promise<ContactDetail> {
    return fetchJson<ContactDetail>(`${BASE}/contacts/${encodeURIComponent(email)}`);
  },

  getDiscussions(params: DiscussionsParams = {}): Promise<DiscussionsResponse> {
    return fetchJson<DiscussionsResponse>(`${BASE}/discussions${buildQuery(params)}`);
  },

  getDiscussion(id: number): Promise<DiscussionDetail> {
    return fetchJson<DiscussionDetail>(`${BASE}/discussions/${id}`);
  },

  getProposedActions(discussionId: number): Promise<ProposedAction[]> {
    return fetchJson<ProposedAction[]>(`${BASE}/discussions/${discussionId}/proposed-actions`);
  },

  getThreadEmails(threadId: string, discussionId?: number): Promise<{ emails: ThreadEmail[] }> {
    const qs = discussionId ? `?discussion_id=${discussionId}` : '';
    return fetchJson(`${BASE}/threads/${encodeURIComponent(threadId)}/emails${qs}`);
  },

  getCalendarEvents(params: CalendarEventsParams = {}): Promise<CalendarEventsResponse> {
    return fetchJson<CalendarEventsResponse>(`${BASE}/calendar-events${buildQuery(params)}`);
  },

  getActions(params: ActionsParams = {}): Promise<ActionsResponse> {
    return fetchJson<ActionsResponse>(`${BASE}/actions${buildQuery(params)}`);
  },

  // ── Unified entities (organizations + people) ─────────────────────────

  getOrganizations(params: CompaniesParams = {}): Promise<OrganizationsResponse> {
    return fetchJson<OrganizationsResponse>(`${BASE}/organizations${buildQuery(params)}`);
  },

  getOrganization(orgId: number): Promise<OrganizationDetail> {
    return fetchJson<OrganizationDetail>(`${BASE}/organizations/${orgId}`);
  },

  mergeOrganizations(sourceId: number, targetId: number, notes?: string): Promise<{ moved: number }> {
    return fetch(`${BASE}/organizations/${sourceId}/merge`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target_id: targetId, notes }),
    }).then((r) => r.ok ? r.json() : r.text().then(t => { throw new Error(`HTTP ${r.status}: ${t}`); }));
  },

  getPeople(params: ContactsParams = {}): Promise<PeopleResponse> {
    return fetchJson<PeopleResponse>(`${BASE}/people${buildQuery(params)}`);
  },

  getPerson(personId: number): Promise<PersonDetail> {
    return fetchJson<PersonDetail>(`${BASE}/people/${personId}`);
  },

  mergePeople(sourceId: number, targetId: number, notes?: string): Promise<{ moved: number }> {
    return fetch(`${BASE}/people/${sourceId}/merge`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target_id: targetId, notes }),
    }).then((r) => r.ok ? r.json() : r.text().then(t => { throw new Error(`HTTP ${r.status}: ${t}`); }));
  },

  // ── Pipeline Jobs ──────────────────────────────────────────────────────

  getJobs(params: JobsParams = {}): Promise<JobsResponse> {
    return fetchJson<JobsResponse>(`${BASE}/jobs${buildQuery(params)}`);
  },

  getJob(id: number): Promise<PipelineJob> {
    return fetchJson<PipelineJob>(`${BASE}/jobs/${id}`);
  },

  getActiveJob(): Promise<{ active: { jobId: number; lastOutputAt: string } | null }> {
    return fetchJson(`${BASE}/jobs/active`);
  },

  getStages(): Promise<{ stages: PipelineStage[] }> {
    return fetchJson(`${BASE}/jobs/stages`);
  },

  async createJob(config: JobConfig): Promise<PipelineJob> {
    const res = await fetch(`${BASE}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => res.statusText);
      throw new Error(`HTTP ${res.status}: ${text}`);
    }
    return res.json() as Promise<PipelineJob>;
  },

  // ── HubSpot Tasks ─────────────────────────────────────────────────────

  getTasks(params: TasksParams = {}): Promise<TasksResponse> {
    return fetchJson<TasksResponse>(`${BASE}/tasks${buildQuery(params)}`);
  },

  getTask(id: string): Promise<HubSpotTaskDetail> {
    return fetchJson<HubSpotTaskDetail>(`${BASE}/tasks/${encodeURIComponent(id)}`);
  },

  search(params: SearchParams): Promise<SearchResponse> {
    return fetchJson<SearchResponse>(`${BASE}/search${buildQuery(params)}`);
  },

  async cancelJob(id: number): Promise<PipelineJob> {
    const res = await fetch(`${BASE}/jobs/${id}/cancel`, { method: 'POST' });
    if (!res.ok) {
      const text = await res.text().catch(() => res.statusText);
      throw new Error(`HTTP ${res.status}: ${text}`);
    }
    return res.json() as Promise<PipelineJob>;
  },

  // ── Feedback mutations ─────────────────────────────────────────────────

  async updateDiscussion(
    id: number,
    patch: { title?: string; summary?: string | null; reason?: string | null },
  ): Promise<{ id: number; updated: string[] }> {
    const res = await fetch(`${BASE}/discussions/${id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(patch),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => res.statusText);
      throw new Error(`HTTP ${res.status}: ${text}`);
    }
    return res.json();
  },

  async mergeDiscussions(
    targetId: number,
    sourceId: number,
    reason?: string | null,
  ): Promise<{ target_id: number; source_id: number }> {
    const res = await fetch(`${BASE}/discussions/${targetId}/merge`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ source_id: sourceId, reason }),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => res.statusText);
      throw new Error(`HTTP ${res.status}: ${text}`);
    }
    return res.json();
  },

  async removeThreadFromDiscussion(
    discussionId: number,
    threadId: string,
    reason?: string | null,
  ): Promise<void> {
    const url = `${BASE}/discussions/${discussionId}/threads/${encodeURIComponent(threadId)}${reason ? `?reason=${encodeURIComponent(reason)}` : ''}`;
    const res = await fetch(url, { method: 'DELETE' });
    if (!res.ok) {
      const text = await res.text().catch(() => res.statusText);
      throw new Error(`HTTP ${res.status}: ${text}`);
    }
  },

  async addThreadToDiscussion(
    discussionId: number,
    threadId: string,
    fromDiscussionId?: number,
    reason?: string | null,
  ): Promise<void> {
    const res = await fetch(`${BASE}/discussions/${discussionId}/threads`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ thread_id: threadId, from_discussion_id: fromDiscussionId, reason }),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => res.statusText);
      throw new Error(`HTTP ${res.status}: ${text}`);
    }
  },

  async updateEvent(
    id: string,
    patch: { type?: string; actor?: string | null; target?: string | null; event_date?: string | null; detail?: string | null; reason?: string | null },
  ): Promise<{ id: string; updated: string[] }> {
    const res = await fetch(`${BASE}/events/${encodeURIComponent(id)}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(patch),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => res.statusText);
      throw new Error(`HTTP ${res.status}: ${text}`);
    }
    return res.json();
  },

  async deleteEvent(id: string, reason?: string | null): Promise<void> {
    const url = `${BASE}/events/${encodeURIComponent(id)}${reason ? `?reason=${encodeURIComponent(reason)}` : ''}`;
    const res = await fetch(url, { method: 'DELETE' });
    if (!res.ok) {
      const text = await res.text().catch(() => res.statusText);
      throw new Error(`HTTP ${res.status}: ${text}`);
    }
  },
};
