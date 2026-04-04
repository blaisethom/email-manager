export interface Company {
  id: number;
  name: string;
  domain: string | null;
  email_count: number;
  first_seen: string | null;
  last_seen: string | null;
  homepage_fetched_at: string | null;
  description: string | null;
  labels: string[];
}

export interface CompanyLabel {
  label: string;
  confidence: number | null;
  reasoning: string | null;
  model_used: string | null;
  assigned_at: string | null;
}

export interface CompanyDetail extends Omit<Company, 'labels'> {
  labels: CompanyLabel[];
  contacts: ContactSummary[];
  discussions: DiscussionSummary[];
}

export interface ContactSummary {
  id: number;
  email: string;
  name: string | null;
  email_count: number;
  sent_count: number;
  received_count: number;
  last_seen: string | null;
}

export interface Contact {
  id: number;
  email: string;
  name: string | null;
  company: string | null;
  first_seen: string | null;
  last_seen: string | null;
  email_count: number;
  sent_count: number;
  received_count: number;
}

export interface ContactMemory {
  email: string;
  name: string | null;
  relationship: string | null;
  summary: string | null;
  discussions: Array<{ topic: string; status: string }>;
  key_facts: string[];
  model_used: string | null;
  strategy_used: string | null;
  generated_at: string | null;
}

export interface Thread {
  id: number;
  thread_id: string;
  subject: string | null;
  email_count: number;
  first_date: string | null;
  last_date: string | null;
  participants: string[];
  summary: string | null;
}

export interface ContactDetail extends Contact {
  memory: ContactMemory | null;
  threads: Thread[];
}

export interface DiscussionSummary {
  id: number;
  title: string;
  category: string | null;
  current_state: string | null;
  company_id: number | null;
  summary: string | null;
  participants: string[];
  first_seen: string | null;
  last_seen: string | null;
}

export interface Discussion extends DiscussionSummary {
  company_name: string | null;
  updated_at: string | null;
}

export interface StateHistoryEntry {
  id: number;
  state: string;
  entered_at: string | null;
  reasoning: string | null;
  model_used: string | null;
  detected_at: string | null;
}

export interface DiscussionDetail extends Discussion {
  state_history: StateHistoryEntry[];
  threads: Thread[];
}

export interface CompaniesResponse {
  items: Company[];
  total: number;
  labels: string[];
}

export interface ContactsResponse {
  items: Contact[];
  total: number;
  companies: string[];
}

export interface DiscussionsResponse {
  items: Discussion[];
  total: number;
  categories: string[];
  states: string[];
}

export interface MetaResponse {
  labels: string[];
  categories: string[];
  states: string[];
  stats: {
    companies: number;
    contacts: number;
    discussions: number;
    emails: number;
  };
}
