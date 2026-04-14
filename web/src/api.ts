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
  sort?: string;
  order?: string;
  page?: number;
  limit?: number;
}

export interface ContactsParams extends Record<string, string | number | undefined> {
  q?: string;
  company?: string;
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

  getThreadEmails(threadId: string): Promise<{ emails: ThreadEmail[] }> {
    return fetchJson(`${BASE}/threads/${encodeURIComponent(threadId)}/emails`);
  },

  getCalendarEvents(params: CalendarEventsParams = {}): Promise<CalendarEventsResponse> {
    return fetchJson<CalendarEventsResponse>(`${BASE}/calendar-events${buildQuery(params)}`);
  },

  getActions(params: ActionsParams = {}): Promise<ActionsResponse> {
    return fetchJson<ActionsResponse>(`${BASE}/actions${buildQuery(params)}`);
  },
};
