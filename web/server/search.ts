/**
 * Vector search and hybrid retrieval (BM25 + vector + RRF).
 *
 * - Embeds query via Voyage AI REST API
 * - Runs pgvector similarity search
 * - Merges with BM25 results using Reciprocal Rank Fusion
 */

import type { Database, DbRow } from './db.js';

const VOYAGE_API_URL = 'https://api.voyageai.com/v1/embeddings';
const VOYAGE_API_KEY = process.env.VOYAGE_API_KEY || 'VOYAGER_API_KEY'; // proxy swaps placeholder
const DEFAULT_FAST_MODEL = 'voyage-3-lite';
const DEFAULT_QUALITY_MODEL = 'voyage-3';
const RRF_K = 60; // standard RRF constant

// ── Query embedding ────────────────────────────────────────────────────────

// Simple LRU cache for query embeddings
const queryCache = new Map<string, { embedding: number[]; ts: number }>();
const CACHE_MAX = 100;
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

async function embedQuery(text: string, model: string): Promise<number[]> {
  const cacheKey = `${model}:${text}`;
  const cached = queryCache.get(cacheKey);
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) {
    return cached.embedding;
  }

  const resp = await fetch(VOYAGE_API_URL, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${VOYAGE_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      input: [text],
      model,
      input_type: 'query',
    }),
  });

  if (!resp.ok) {
    const body = await resp.text().catch(() => '');
    throw new Error(`Voyage API error ${resp.status}: ${body}`);
  }

  const data = await resp.json() as { data: Array<{ embedding: number[] }> };
  const embedding = data.data[0].embedding;

  // Cache it
  queryCache.set(cacheKey, { embedding, ts: Date.now() });
  if (queryCache.size > CACHE_MAX) {
    const oldest = queryCache.keys().next().value;
    if (oldest !== undefined) queryCache.delete(oldest);
  }

  return embedding;
}

// ── Vector search via pgvector ─────────────────────────────────────────────

interface VectorResult {
  thread_id: string;
  similarity: number;
}

async function vectorSearch(
  db: Database,
  queryEmbedding: number[],
  model: string,
  limit: number,
  filters?: { company?: string; label?: string },
): Promise<VectorResult[]> {
  const vecStr = '[' + queryEmbedding.join(',') + ']';
  // Params must match placeholder order in the SQL below:
  //   1. SELECT ... <=> ?::vector     (vecStr)
  //   2. WHERE te.model_name = ?      (model)
  //   3..N. filter placeholders
  //   N+1. ORDER BY ... <=> ?::vector (vecStr)
  //   N+2. LIMIT ?                    (limit)
  const params: unknown[] = [vecStr, model];
  const filterClauses: string[] = [];

  if (filters?.company) {
    filterClauses.push(
      'AND te.thread_id IN (SELECT thread_id FROM thread_search_docs WHERE company_domain = ?)'
    );
    params.push(filters.company);
  }
  if (filters?.label) {
    filterClauses.push(
      'AND te.thread_id IN (SELECT tsd.thread_id FROM thread_search_docs tsd WHERE tsd.company_domain IN (SELECT c.domain FROM companies c JOIN company_labels cl ON cl.company_id = c.id WHERE cl.label = ?))'
    );
    params.push(filters.label);
  }

  params.push(vecStr, limit);

  const results = await db.query<VectorResult>(
    `SELECT te.thread_id,
            1 - (te.embedding <=> ?::vector) AS similarity
     FROM thread_embeddings te
     WHERE te.model_name = ?
     ${filterClauses.join(' ')}
     ORDER BY te.embedding <=> ?::vector
     LIMIT ?`,
    ...params
  );

  return results;
}

// ── Reciprocal Rank Fusion ─────────────────────────────────────────────────

interface RankedResult {
  thread_id: string;
  bm25_rank: number | null;
  bm25_score: number | null;
  vector_rank: number | null;
  vector_score: number | null;
  rrf_score: number;
}

function reciprocalRankFusion(
  bm25Results: Array<{ thread_id: string; score: number }>,
  vectorResults: Array<{ thread_id: string; similarity: number }>,
): RankedResult[] {
  const merged = new Map<string, RankedResult>();

  // Add BM25 results
  bm25Results.forEach((r, i) => {
    merged.set(r.thread_id, {
      thread_id: r.thread_id,
      bm25_rank: i + 1,
      bm25_score: r.score,
      vector_rank: null,
      vector_score: null,
      rrf_score: 1 / (RRF_K + i + 1),
    });
  });

  // Add/merge vector results
  vectorResults.forEach((r, i) => {
    const existing = merged.get(r.thread_id);
    if (existing) {
      existing.vector_rank = i + 1;
      existing.vector_score = r.similarity;
      existing.rrf_score += 1 / (RRF_K + i + 1);
    } else {
      merged.set(r.thread_id, {
        thread_id: r.thread_id,
        bm25_rank: null,
        bm25_score: null,
        vector_rank: i + 1,
        vector_score: r.similarity,
        rrf_score: 1 / (RRF_K + i + 1),
      });
    }
  });

  // Sort by RRF score descending
  return Array.from(merged.values()).sort((a, b) => b.rrf_score - a.rrf_score);
}

// ── Public API ─────────────────────────────────────────────────────────────

export interface HybridSearchOptions {
  query: string;
  limit: number;
  offset?: number;
  model?: 'fast' | 'quality';
  company?: string;
  label?: string;
}

export interface HybridSearchResult {
  thread_id: string;
  rrf_score: number;
  bm25_score: number | null;
  vector_score: number | null;
}

/**
 * Check whether vector search is available (embeddings exist in the DB).
 */
export async function isVectorSearchAvailable(db: Database): Promise<boolean> {
  if (db.backend !== 'postgres') return false;
  try {
    const row = await db.queryOne<{ cnt: number }>(
      'SELECT COUNT(*) AS cnt FROM thread_embeddings LIMIT 1'
    );
    return (row?.cnt ?? 0) > 0;
  } catch {
    return false;
  }
}

/**
 * Run hybrid search: BM25 + vector with RRF fusion.
 * Returns thread_ids ranked by combined score.
 */
export async function hybridSearch(
  db: Database,
  options: HybridSearchOptions,
): Promise<{ results: HybridSearchResult[]; vector_available: boolean }> {
  const modelName = options.model === 'quality' ? DEFAULT_QUALITY_MODEL : DEFAULT_FAST_MODEL;
  const offset = options.offset ?? 0;
  // Pull enough candidates from each source to cover the requested page plus buffer for RRF overlap.
  const vectorLimit = Math.max(50, offset + options.limit + 20);

  // Try vector search
  let vectorResults: VectorResult[] = [];
  let vectorAvailable = false;

  try {
    const queryEmbedding = await embedQuery(options.query, modelName);
    vectorResults = await vectorSearch(
      db, queryEmbedding, modelName, vectorLimit,
      { company: options.company, label: options.label },
    );
    vectorAvailable = true;
  } catch (err) {
    // Vector search unavailable (no embeddings, API error, etc.)
    // Fall through to BM25-only
    console.log('[search] Vector search unavailable:', (err as Error).message);
  }

  if (!vectorAvailable || vectorResults.length === 0) {
    // BM25-only mode: return empty hybrid results, let caller use BM25 directly
    return { results: [], vector_available: false };
  }

  // Run BM25 to get scored results for RRF merging (dual-config: english + simple phrase)
  const q = options.query;
  const bm25Params: unknown[] = [q, q, q, q];
  const bm25Filters: string[] = [];
  if (options.company) { bm25Filters.push('AND tsd.company_domain = ?'); bm25Params.push(options.company); }
  if (options.label) {
    bm25Filters.push('AND tsd.company_domain IN (SELECT c.domain FROM companies c JOIN company_labels cl ON cl.company_id = c.id WHERE cl.label = ?)');
    bm25Params.push(options.label);
  }
  bm25Params.push(vectorLimit);

  const bm25Results = await db.query<{ thread_id: string; score: number }>(
    `WITH matched AS (
       SELECT thread_id FROM thread_search_docs WHERE doc_tsv @@ websearch_to_tsquery('english', ?)
       UNION
       SELECT thread_id FROM thread_search_docs WHERE doc_tsv_simple @@ phraseto_tsquery('simple', ?)
     )
     SELECT tsd.thread_id,
            ts_rank_cd(tsd.doc_tsv, websearch_to_tsquery('english', ?))
              + 10.0 * ts_rank_cd(tsd.doc_tsv_simple, phraseto_tsquery('simple', ?))
            AS score
     FROM matched m
     JOIN thread_search_docs tsd ON tsd.thread_id = m.thread_id
     WHERE 1=1 ${bm25Filters.join(' ')}
     ORDER BY score DESC
     LIMIT ?`,
    ...bm25Params
  );

  // Fuse with RRF
  const fused = reciprocalRankFusion(bm25Results, vectorResults);

  // Apply outreach boost: multiply rrf_score by (1 + 0.4 * ln(1 + outreach_score)) and re-sort.
  // Matches the multiplier used in the BM25-only path so behaviour is consistent.
  if (fused.length > 0) {
    const fusedIds = fused.map(r => r.thread_id);
    const placeholders = fusedIds.map(() => '?').join(',');
    const boostRows = await db.query<{ thread_id: string; outreach_score: number | null }>(
      `SELECT thread_id, outreach_score FROM thread_search_docs WHERE thread_id IN (${placeholders})`,
      ...fusedIds
    );
    const boostMap = new Map(boostRows.map(r => [r.thread_id, Number(r.outreach_score ?? 0)]));
    for (const r of fused) {
      const s = boostMap.get(r.thread_id) ?? 0;
      r.rrf_score *= 1 + 0.4 * Math.log1p(s);
    }
    fused.sort((a, b) => b.rrf_score - a.rrf_score);
  }

  return {
    results: fused.slice(offset, offset + options.limit).map(r => ({
      thread_id: r.thread_id,
      rrf_score: r.rrf_score,
      bm25_score: r.bm25_score,
      vector_score: r.vector_score,
    })),
    vector_available: true,
  };
}
