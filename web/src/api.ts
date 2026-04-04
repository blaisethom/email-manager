import type {
  CompaniesResponse,
  CompanyDetail,
  ContactsResponse,
  ContactDetail,
  DiscussionsResponse,
  DiscussionDetail,
  MetaResponse,
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
  company_id?: string | number;
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
};
