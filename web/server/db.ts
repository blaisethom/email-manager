/**
 * Database abstraction: supports both SQLite (node:sqlite) and PostgreSQL (pg).
 *
 * SQLite uses synchronous node:sqlite API.
 * PostgreSQL uses async pg Pool — all query methods return Promises.
 * The factory returns the same interface shape; callers must await.
 */

import path from 'path';
import { fileURLToPath } from 'url';
import { createRequire } from 'module';

const require = createRequire(import.meta.url);

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export interface DbRow {
  [key: string]: unknown;
}

export interface Database {
  query<T extends DbRow = DbRow>(sql: string, ...params: unknown[]): Promise<T[]>;
  queryOne<T extends DbRow = DbRow>(sql: string, ...params: unknown[]): Promise<T | undefined>;
  exec(sql: string): Promise<void>;
  backend: 'sqlite' | 'postgres';
}

/** Convert SQLite ? placeholders to PostgreSQL $1, $2, etc. */
function translateParams(sql: string): string {
  let idx = 0;
  let result = '';
  let inString = false;
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];
    if (inString) {
      result += ch;
      if (ch === "'") inString = false;
    } else if (ch === "'") {
      inString = true;
      result += ch;
    } else if (ch === '?') {
      idx++;
      result += `$${idx}`;
    } else {
      result += ch;
    }
  }
  return result;
}

function translateSql(sql: string): string {
  let out = sql;
  out = out.replace(/GROUP_CONCAT\(([^,]+),\s*'([^']*)'\)/gi, "STRING_AGG(CAST($1 AS TEXT), '$2')");
  out = translateParams(out);
  return out;
}

// ── SQLite backend (wraps sync in Promises) ─────────────────────────────

export function createSqliteDb(): Database {
  const { DatabaseSync } = require('node:sqlite');
  const DB_PATH = process.env.DB_PATH ?? path.resolve(__dirname, '../../data/email_manager.db');
  const raw = new DatabaseSync(DB_PATH, { open: true });
  raw.exec('PRAGMA journal_mode=WAL');

  return {
    backend: 'sqlite' as const,
    async query<T extends DbRow = DbRow>(sql: string, ...params: unknown[]): Promise<T[]> {
      return raw.prepare(sql).all(...params) as T[];
    },
    async queryOne<T extends DbRow = DbRow>(sql: string, ...params: unknown[]): Promise<T | undefined> {
      return raw.prepare(sql).get(...params) as T | undefined;
    },
    async exec(sql: string): Promise<void> {
      raw.exec(sql);
    },
  };
}

// ── PostgreSQL backend ──────────────────────────────────────────────────

export function createPgDb(): Database {
  const pg = require('pg') as typeof import('pg');
  const dbUrl = process.env.DB_URL!;

  const pool = new pg.Pool({
    connectionString: dbUrl,
    max: 10,
  });

  return {
    backend: 'postgres' as const,
    async query<T extends DbRow = DbRow>(sql: string, ...params: unknown[]): Promise<T[]> {
      const pgSql = translateSql(sql);
      const result = await pool.query(pgSql, params.length > 0 ? params : undefined);
      return result.rows as T[];
    },
    async queryOne<T extends DbRow = DbRow>(sql: string, ...params: unknown[]): Promise<T | undefined> {
      const pgSql = translateSql(sql);
      const result = await pool.query(pgSql, params.length > 0 ? params : undefined);
      return result.rows[0] as T | undefined;
    },
    async exec(sql: string): Promise<void> {
      const pgSql = translateSql(sql);
      // Split multiple statements
      const stmts = pgSql.split(';').map(s => s.trim()).filter(s => s.length > 0);
      for (const stmt of stmts) {
        try {
          await pool.query(stmt);
        } catch (e: any) {
          if (!e.message?.includes('already exists')) {
            console.warn(`DDL warning: ${e.message?.slice(0, 100)}`);
          }
        }
      }
    },
  };
}

// ── Factory ─────────────────────────────────────────────────────────────

export function createDb(): Database {
  const dbBackend = process.env.DB_BACKEND ?? '';

  if (dbBackend === 'postgres') {
    console.log('Using PostgreSQL backend');
    return createPgDb();
  }

  console.log('Using SQLite backend');
  return createSqliteDb();
}
