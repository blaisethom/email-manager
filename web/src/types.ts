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

export interface ThreadEmail {
  id: number;
  message_id: string;
  subject: string | null;
  from_address: string;
  from_name: string | null;
  to_addresses: string[];
  cc_addresses: string[];
  date: string;
  body_text: string | null;
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

export interface Action {
  id: number;
  discussion_id: number;
  description: string;
  assignee_emails: string[];
  target_date: string | null;
  status: string;
  source_date: string | null;
  completed_date: string | null;
  discussion_title: string | null;
  company_name: string | null;
  company_id: number | null;
}

export interface DiscussionAction {
  id: number;
  description: string;
  assignee_emails: string[];
  target_date: string | null;
  status: string;
  source_date: string | null;
  completed_date: string | null;
}

export interface CalendarEvent {
  id: number;
  event_id: string;
  title: string | null;
  description: string | null;
  location: string | null;
  start_time: string;
  end_time: string;
  all_day: boolean;
  status: string | null;
  organizer_email: string | null;
  attendees: Array<{ email: string; name?: string; response_status?: string }>;
  html_link: string | null;
  discussion_id?: number | null;
  discussion_title?: string | null;
  match_score?: number | null;
  match_reason?: string | null;
}

export interface CalendarEventsResponse {
  items: CalendarEvent[];
  total: number;
}

export interface EventLedgerEntry {
  id: string;
  domain: string;
  type: string;
  actor: string | null;
  target: string | null;
  event_date: string | null;
  detail: string | null;
  confidence: number | null;
  thread_id: string | null;
  source_email_id: string | null;
}

export interface Milestone {
  name: string;
  achieved: boolean;
  achieved_date: string | null;
  evidence_event_ids: string[];
  confidence: number | null;
}

export interface DiscussionDetail extends Discussion {
  state_history: StateHistoryEntry[];
  threads: Thread[];
  actions: DiscussionAction[];
  calendar_events: CalendarEvent[];
  events: EventLedgerEntry[];
  milestones: Milestone[];
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

export interface ActionsResponse {
  items: Action[];
  total: number;
  statuses: string[];
  assignees: string[];
}

export interface CategoryConfig {
  name: string;
  description: string;
  states: string[];
  terminal_states: string[];
}

export interface MetaResponse {
  labels: string[];
  categories: string[];
  states: string[];
  userEmails: string[];
  stats: {
    companies: number;
    contacts: number;
    discussions: number;
    actions: number;
    emails: number;
    calendar_events: number;
  };
  categoryConfig: CategoryConfig[];
}
