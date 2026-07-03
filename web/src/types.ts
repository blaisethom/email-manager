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
  last_analysed_at: string | null;
  is_stale: boolean;
}

export interface CompanyLabel {
  label: string;
  confidence: number | null;
  reasoning: string | null;
  model_used: string | null;
  assigned_at: string | null;
}

export interface CompanyThread {
  thread_id: string;
  subject: string | null;
  email_count: number;
  first_date: string | null;
  last_date: string | null;
  summary: string | null;
  discussions: Array<{
    id: number;
    title: string;
    category: string | null;
    current_state: string | null;
  }>;
}

export interface HubSpotCompany {
  id: string;
  name: string | null;
  domain: string | null;
  website: string | null;
  industry: string | null;
  description: string | null;
  about_us: string | null;
  city: string | null;
  state: string | null;
  country: string | null;
  phone: string | null;
  num_employees: number | null;
  annual_revenue: number | null;
  lifecycle_stage: string | null;
  type: string | null;
  owner_id: string | null;
  founded_year: string | null;
  linkedin_url: string | null;
  twitter_handle: string | null;
  hs_updated_at: string | null;
  hs_url: string | null;
}

export interface HubSpotContact {
  id: string;
  email: string | null;
  firstname: string | null;
  lastname: string | null;
  company_name: string | null;
  job_title: string | null;
  phone: string | null;
  city: string | null;
  state: string | null;
  country: string | null;
  address: string | null;
  lifecycle_stage: string | null;
  lead_status: string | null;
  owner_id: string | null;
  twitter_handle: string | null;
  linkedin_url: string | null;
  website: string | null;
  industry: string | null;
  salutation: string | null;
  hs_updated_at: string | null;
  hs_url: string | null;
}

export interface CompanyDetail extends Omit<Company, 'labels'> {
  labels: CompanyLabel[];
  contacts: ContactSummary[];
  discussions: DiscussionSummary[];
  threads: CompanyThread[];
  new_email_count: number;
  org_id: number | null;
  sources: Array<'email' | 'homepage' | 'hubspot'>;
  hubspot: HubSpotCompany | null;
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
  person_id: number | null;
  sources: Array<'email' | 'hubspot'>;
  hubspot: HubSpotContact | null;
}

export interface DiscussionSummary {
  id: number;
  title: string;
  category: string | null;
  current_state: string | null;
  company_id: number | null;
  parent_id: number | null;
  summary: string | null;
  participants: string[];
  first_seen: string | null;
  last_seen: string | null;
}

export interface Discussion extends DiscussionSummary {
  company_name: string | null;
  updated_at: string | null;
  proposed_action_count?: number;
  high_priority_count?: number;
  med_priority_count?: number;
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
  discussion_id: number | null;
}

export interface Milestone {
  id: number;
  name: string;
  achieved: boolean;
  achieved_date: string | null;
  evidence_event_ids: string[];
  confidence: number | null;
  source: string;
}

export interface ProposedAction {
  id: number;
  action: string;
  reasoning: string | null;
  priority: string;
  wait_until: string | null;
  assignee: string | null;
  created_at: string;
  status: string;
  source: string;
}

export interface ParentDiscussion {
  id: number;
  title: string;
  category: string | null;
  current_state: string | null;
  summary: string | null;
  first_seen: string | null;
  last_seen: string | null;
  company_name: string | null;
}

export interface HubSpotNote {
  id: string;
  body: string | null;
  created_at: string | null;
  updated_at: string | null;
  owner_id: string | null;
  hs_url: string | null;
}

export interface DiscussionDetail extends Discussion {
  parent: ParentDiscussion | null;
  state_history: StateHistoryEntry[];
  threads: Thread[];
  actions: DiscussionAction[];
  calendar_events: CalendarEvent[];
  events: EventLedgerEntry[];
  milestones: Milestone[];
  proposed_actions: ProposedAction[];
  children: Discussion[];
  notes: HubSpotNote[];
}

export interface ProcessingRun {
  id: number;
  mode: string;
  model: string | null;
  started_at: string;
  completed_at: string | null;
  events_created: number;
  discussions_created: number;
  discussions_updated: number;
  actions_proposed: number;
  input_tokens: number;
  output_tokens: number;
  llm_calls: number;
  total_llm_ms: number;
}

export interface LlmCallsByStage {
  stage: string;
  call_count: number;
  total_input: number;
  total_output: number;
}

export interface DiscussionInsight {
  id: number;
  title: string;
  category: string | null;
  current_state: string | null;
  summary: string | null;
  first_seen: string | null;
  last_seen: string | null;
  updated_at: string | null;
  parent_id: number | null;
  run_id: number | null;
  event_count: number;
  latest_event_created: string | null;
  action_count: number;
  milestones_achieved: number;
  milestones_total: number;
  last_run_mode: string | null;
  last_run_model: string | null;
}

export interface EventDomainSummary {
  domain: string;
  cnt: number;
  latest_event_date: string | null;
  latest_created: string | null;
}

export interface CompanyInsights {
  company: { id: number; domain: string; name: string };
  processing_runs: ProcessingRun[];
  llm_calls_by_stage: LlmCallsByStage[];
  discussions: DiscussionInsight[];
  events_by_domain: EventDomainSummary[];
  unprocessed_threads: number;
  pending_changes: number;
  proposed_actions: Array<ProposedAction & { discussion_id: number; discussion_title: string }>;
}

export interface CompaniesResponse {
  items: Company[];
  total: number;
  labels: string[];
  stale_count: number;
  up_to_date_count: number;
  never_analysed_count: number;
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

export interface LabelConfig {
  name: string;
  description: string;
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
  labelConfig: LabelConfig[];
}

// ── Review ─────────────────────────────────────────────────────────────────

export interface ReviewCompanyLabel {
  label: string;
  confidence: number | null;
  reasoning: string | null;
  model_used: string | null;
}

export interface ReviewCompany {
  company_id: number;
  domain: string | null;
  name: string | null;
  description: string | null;
  last_analysed_at: string | null;
  labels: ReviewCompanyLabel[];
}

export interface ReviewLabelsResponse {
  items: ReviewCompany[];
  total: number;
}

export interface PipelineCompany {
  company_id: number;
  name: string | null;
  domain: string | null;
  last_analysed_at: string | null;
  name_source: 'email' | 'hubspot' | 'human' | null;
  stages_run: string[];
}

export interface PipelineCompaniesResponse {
  items: PipelineCompany[];
  total: number;
  stages: string[];
}

export interface LearnedRule {
  id: number;
  layer: string;
  category: string | null;
  rule_text: string;
  active: boolean;
  created_at: string;
  source_feedback_ids: string | null;
}

export type Granularity = 'fewer' | 'balanced' | 'more';

// ── Config ──────────────────────────────────────────────────────────────────

export interface ProkuraService {
  service_name: string;
  service_slug: string;
  provider: string;
  credential_type: string;
  auth_scheme?: string;
  bearer_token_key?: string;
  account_email?: string;
  domain_pattern?: string;
  token_expires_at?: string | null;
  description?: string;
}

export interface ProkuraResourcesResponse {
  available: boolean;
  agent_name?: string;
  services: ProkuraService[];
  error?: string;
}

export interface EmailAccount {
  name: string;
  backend: 'gmail' | 'imap';
  gmail_credentials_path?: string;
  gmail_token_path?: string;
  gmail_labels?: string[];
  gmail_bearer_token?: string;
  imap_host?: string;
  imap_user?: string;
  imap_password?: string;
  imap_port?: number;
  imap_use_ssl?: boolean;
  imap_folders?: string[];
  hubspot_bearer_token?: string;
  hubspot_owner_email?: string;
}

export interface LabelDef {
  name: string;
  description: string;
}

export interface EventTypeDef {
  name: string;
  description: string;
}

export interface MilestoneDef {
  name: string;
  description: string;
}

export interface CategoryDef {
  name: string;
  description: string;
  workflow_states: string[];
  terminal_states: string[];
  sub_discussion?: boolean;
  event_types?: EventTypeDef[];
  terminal_event_types?: string[];
  milestones?: MilestoneDef[];
}

// ── Unified entity model ───────────────────────────────────────────────────

export type EntitySource = 'email' | 'homepage' | 'hubspot';

export interface Organization {
  org_id: number;
  email_company_id: number | null;
  hubspot_id: string | null;
  name: string | null;
  domain: string | null;
  description: string | null;
  email_count: number;
  first_seen: string | null;
  last_seen: string | null;
  homepage_fetched_at: string | null;
  industry: string | null;
  lifecycle_stage: string | null;
  country: string | null;
  is_stale: boolean;
  staleness_status: string;
  last_analysed_at: string | null;
  sources: EntitySource[];
  labels: string[];
}

export interface OrganizationsResponse {
  items: Organization[];
  total: number;
  labels: string[];
  stale_count: number;
  up_to_date_count: number;
  never_analysed_count: number;
}

export interface OrganizationDetail extends Omit<Organization, 'org_id' | 'sources' | 'labels'> {
  id: number;
  org_id: number;
  hubspot_url: string | null;
  sources: EntitySource[];
  identities: Array<{
    source: EntitySource;
    source_id: string;
    match_key: string | null;
    confidence: number;
    is_manual: number;
    created_at: string;
  }>;
  notes: string | null;
  website: string | null;
  type: string | null;
  owner_id: string | null;
  phone: string | null;
  city: string | null;
  state: string | null;
  num_employees: number | null;
  annual_revenue: number | null;
  linkedin_url: string | null;
  twitter_handle: string | null;
  founded_year: string | null;
}

export interface Person {
  person_id: number;
  email_contact_id: number | null;
  hubspot_id: string | null;
  name: string | null;
  email: string | null;
  company_name: string | null;
  email_count: number;
  sent_count: number;
  received_count: number;
  first_seen: string | null;
  last_seen: string | null;
  job_title: string | null;
  lifecycle_stage: string | null;
  lead_status: string | null;
  country: string | null;
  sources: Array<'email' | 'hubspot'>;
}

export interface PeopleResponse {
  items: Person[];
  total: number;
  companies: string[];
}

export interface PersonDetail extends Person {
  id: number;
  hubspot_url: string | null;
  identities: Array<{
    source: 'email' | 'hubspot';
    source_id: string;
    match_key: string | null;
    confidence: number;
    is_manual: number;
    created_at: string;
  }>;
  notes: string | null;
  phone: string | null;
  city: string | null;
  state: string | null;
  address: string | null;
  owner_id: string | null;
  linkedin_url: string | null;
  twitter_handle: string | null;
  industry: string | null;
  salutation: string | null;
  website: string | null;
}

// ── HubSpot Tasks ─────────────────────────────────────────────────────────

export interface HubSpotTask {
  id: string;
  subject: string | null;
  body: string | null;
  status: string | null;
  type: string | null;
  priority: string | null;
  due_date: string | null;
  completed_at: string | null;
  owner_id: string | null;
  associated_contact_ids: string[];
  associated_company_ids: string[];
  hs_url: string | null;
  fetched_at: string;
  contacts: Array<{ id: string; email: string | null; name: string | null }>;
  companies: Array<{ id: string; name: string | null; domain: string | null; hs_url: string | null; local_id: number | null }>;
  thread_count: number;
}

export interface HubSpotTaskThread {
  thread_id: string;
  subject: string | null;
  email_count: number;
  first_date: string | null;
  last_date: string | null;
  contact_email: string;
}

export interface HubSpotTaskDetail extends HubSpotTask {
  threads: HubSpotTaskThread[];
}

export interface TasksResponse {
  items: HubSpotTask[];
  total: number;
}

// ── Pipeline Jobs ──────────────────────────────────────────────────────────

export interface PipelineJob {
  id: number;
  job_type: string;
  status: 'queued' | 'running' | 'completed' | 'failed' | 'cancelled';
  config_json: string;
  pid: number | null;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
  exit_code: number | null;
  error_message: string | null;
  current_stage: string | null;
  progress_done: number;
  progress_total: number;
  current_company: string | null;
  prefect_flow_run_id: string | null;
  prefect_deployment_name: string | null;
}

// ── Prefect ──────────────────────────────────────────────────────────────────

export interface PrefectDeployment {
  id: string;
  name: string;
  flow_name: string;
  paused: boolean;
  schedules: Array<{ schedule: { cron?: string }; active: boolean }>;
  last_polled: string | null;
  next_scheduled_start_time: string | null;
}

export interface PrefectFlowRun {
  id: string;
  name: string;
  deployment_id: string | null;
  state_type: string;
  state_name: string;
  start_time: string | null;
  end_time: string | null;
  total_run_time: number;
  parameters: Record<string, unknown>;
}

export interface PrefectLog {
  id: string;
  timestamp: string;
  level: number;
  message: string;
}

export interface PipelineStage {
  name: string;
  scope: 'global' | 'company';
  needs_ai: boolean;
  depends_on: string[];
}

export interface JobConfig {
  job_type: 'sync' | 'analyse';
  stages?: string[] | null;
  company?: string | null;
  label?: string | null;
  force?: boolean;
  clean?: boolean;
  per_company?: boolean;
  concurrency?: number;
  new_emails?: boolean;
  stale_model?: boolean;
  stale_prompt?: boolean;
}

export interface JobsResponse {
  items: PipelineJob[];
  total: number;
}

// ── Search ─────────────────────────────────────────────────────────────────

export interface SearchResult {
  thread_id: string;
  subject: string | null;
  company_domain: string | null;
  company_name: string | null;
  participants: string[];
  first_date: string | null;
  last_date: string | null;
  email_count: number;
  snippet: string | null;
  score: number;
  score_type: string;
}

export interface DiscussionSearchResult {
  discussion_id: number;
  title: string;
  category: string | null;
  current_state: string | null;
  company_domain: string | null;
  company_name: string | null;
  first_seen: string | null;
  last_seen: string | null;
  snippet: string | null;
  score: number;
  score_type: string;
}

export interface SearchResponse {
  results: SearchResult[];
  discussion_results: DiscussionSearchResult[];
  total: number;
  discussion_total: number;
  query_time_ms: number;
  search_mode: string;
}
