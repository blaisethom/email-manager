# Market Research: Similar Products

*Research conducted April 2026*

The email-analyser is an AI-powered, local-first, pipeline-based CRM/PRM that extracts structured business events from email threads, clusters them into discussions, tracks workflow milestones, and proposes actions. Several products have overlapping features, but none match the exact architecture.

---

## Closest Competitors

### 1. Affinity (affinity.co)

The most architecturally similar commercial product. Affinity automatically ingests emails and calendar events, builds relationship intelligence, tracks deal pipelines, and scores relationship strength. Aimed at VCs, PE firms, and deal-driven teams.

- **Overlap**: Automatic email/calendar capture, relationship scoring, deal pipeline tracking, event extraction
- **Difference**: Cloud-hosted SaaS, no local/self-hosted option, no concept of domain-specific event vocabularies or configurable workflow state machines. Much more opinionated about use cases (investment, dealflow)

### 2. Cloze (cloze.com)

AI-powered relationship management that logs every email, call, text, and meeting. Analyzes communication frequency, detects cold relationships, and surfaces daily follow-up suggestions.

- **Overlap**: Automatic communication capture, relationship tracking, next-action suggestions, timeline building
- **Difference**: Pivoted heavily toward real estate (AI assistant "MAIA"). Cloud-only. Doesn't do structured event extraction or configurable domain vocabularies -- more about communication frequency than business event semantics

### 3. Folk (folk.app)

A collaborative CRM that unifies email, calendar, LinkedIn, and WhatsApp. AI detects inactive discussions and proposes follow-ups in your tone.

- **Overlap**: Email/calendar unification, discussion activity detection, AI-generated next actions
- **Difference**: Focused on outreach and networking rather than deep email analysis. No event extraction pipeline, no milestone/workflow tracking, no configurable taxonomies

### 4. Orvo (getorvo.com)

AI personal CRM with stakeholder influence mapping, meeting prep briefs, voice transcription, and smart email writing.

- **Overlap**: AI-powered relationship management, contact memory/profiles, contextual suggestions
- **Difference**: More people/networking-focused than deal/pipeline-focused. No structured event extraction from email threads. Cloud-hosted

### 5. Dex (getdex.com)

Personal CRM that auto-collects data from email, calendar, and LinkedIn. AI generates pre-meeting briefs and suggests follow-ups.

- **Overlap**: Auto-capture from email/calendar, follow-up suggestions, contact enrichment
- **Difference**: Lightweight -- no event extraction, no discussion clustering, no workflow states

### 6. Streak (streak.com)

CRM that lives inside Gmail. AI Co-Pilot summarizes deal timelines, answers questions from email history, and suggests next steps.

- **Overlap**: Email-native, AI summarization of threads, deal progression tracking
- **Difference**: Gmail-only, no structured event extraction, no configurable domain vocabulary

### 7. 4Degrees (4degrees.ai)

Relationship intelligence CRM for deal teams. Automatically logs emails and meetings, scores relationships, tracks pipeline.

- **Overlap**: Automatic email/meeting capture, relationship scoring, deal pipeline
- **Difference**: Cloud SaaS targeting PE/VC firms, no self-hosted option, no configurable event taxonomy

---

## Open Source / Self-Hosted Alternatives

### 8. n8n + Ollama (self-hosted workflow)

People are building DIY email analysis pipelines using n8n (open-source workflow automation) with self-hosted LLMs via Ollama. Closest to the email-analyser architecture in spirit -- local, privacy-first, pipeline-based.

- **Overlap**: Self-hosted, local LLM support, pipeline-based email processing
- **Difference**: No out-of-the-box CRM/PRM functionality -- event extraction, discussion clustering, and milestone tracking would need to be built from scratch. Infrastructure, not a product

### 9. Mail Zero (github.com/Mail-0/Zero)

Open-source email app focused on privacy. AI-powered but oriented toward inbox management rather than business intelligence.

---

## Differentiation Summary

| Capability | email-analyser | Closest Competitor |
|---|---|---|
| Configurable domain vocabulary (discussion_categories.yaml) | Yes | None -- all competitors use fixed taxonomies |
| Structured event extraction from email threads | Yes, multi-stage AI pipeline | Affinity does some, but less structured |
| Discussion clustering from events | Yes, AI-driven | No direct equivalent |
| Workflow state machines with milestones | Yes, per-domain | Streak/Affinity have simple pipelines |
| Local-first / SQLite | Yes | n8n+Ollama only |
| Ollama support (local LLM) | Yes | None of the CRM products |
| Change journal for incremental processing | Yes | No equivalent in any competitor |
| Provenance/lineage tracking (run_id, model_version) | Yes | None at this level |

The biggest differentiators are: **(1)** the configurable domain-specific event vocabulary, **(2)** the multi-stage AI pipeline with full provenance, and **(3)** local/self-hosted with optional local LLM support. No existing product combines all three. The commercial CRMs (Affinity, Cloze, Folk) are the closest functionally but are all cloud-hosted and use fixed, non-configurable analysis models.

The gap in the market: there is no open-source, self-hosted, AI-powered relationship intelligence tool with structured event extraction and configurable domain vocabularies. The email-analyser sits in that gap.

---

## Sources

- [Affinity CRM](https://www.affinity.co/product/crm)
- [Cloze CRM Review 2026](https://getdex.com/blog/cloze-crm-review/)
- [Folk CRM](https://www.folk.app/)
- [Orvo](https://www.getorvo.com/learn/best-ai-crm-2026)
- [Dex vs Orvo comparison](https://getdex.com/blog/dex-vs-orvo/)
- [Clay AI Review](https://autogpt.net/ai-tool/clay-ai/)
- [4Degrees Relationship Intelligence](https://www.4degrees.ai/blog/unlocking-the-power-of-relationship-intelligence-crm-for-deal-driven-teams)
- [Best AI Personal CRM 2026](https://www.folk.app/articles/best-ai-personal-crm)
- [AI Eats the CRM](https://www.pberg.com/blog/2025/10/22/ai-eats-the-crm/)
- [n8n + Ollama email pipeline](https://medium.com/@aravindhrk/simplify-your-gmail-inbox-using-self-hosted-n8n-with-ollama-llm-7332c2a85df6)
- [Mail Zero (open source)](https://github.com/mail-0/zero)
- [Relationship Intelligence Guide](https://www.introhive.com/blog-posts/relationship-intelligence-automation/)
