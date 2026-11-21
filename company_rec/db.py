"""Postgres schema and idempotent latest-wins persistence."""

from __future__ import annotations

import asyncpg

from company_rec.events import Event

DDL = """
CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE IF NOT EXISTS companies (
  registration_number TEXT PRIMARY KEY,
  company_name TEXT NOT NULL,
  nature_of_business TEXT,
  state TEXT,
  industry TEXT,
  updated_at TIMESTAMPTZ NOT NULL,
  message_id TEXT NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  embedding_model TEXT,
  embedding_surrogate BOOLEAN NOT NULL DEFAULT FALSE,
  embedding vector(384),
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS companies_embedding_hnsw
  ON companies USING hnsw (embedding vector_cosine_ops);
CREATE TABLE IF NOT EXISTS processed_messages (
  message_id TEXT PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

UPSERT = """
INSERT INTO companies (
  registration_number, company_name, nature_of_business, state, industry,
  updated_at, message_id, active, embedding_model, embedding_surrogate, embedding
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::vector)
ON CONFLICT (registration_number) DO UPDATE SET
  company_name = CASE WHEN EXCLUDED.active = FALSE THEN companies.company_name
                      WHEN EXCLUDED.company_name <> '' THEN EXCLUDED.company_name
                      ELSE companies.company_name END,
  nature_of_business = CASE WHEN EXCLUDED.active = FALSE THEN companies.nature_of_business
                            ELSE EXCLUDED.nature_of_business END,
  state = CASE WHEN EXCLUDED.active = FALSE THEN companies.state ELSE EXCLUDED.state END,
  industry = CASE WHEN EXCLUDED.active = FALSE THEN companies.industry ELSE EXCLUDED.industry END,
  updated_at = EXCLUDED.updated_at,
  message_id = EXCLUDED.message_id,
  active = EXCLUDED.active,
  embedding_model = EXCLUDED.embedding_model,
  embedding_surrogate = EXCLUDED.embedding_surrogate,
  embedding = EXCLUDED.embedding
WHERE EXCLUDED.updated_at > companies.updated_at
   OR (EXCLUDED.updated_at = companies.updated_at AND EXCLUDED.message_id > companies.message_id)
"""


def vector_literal(values: list[float]) -> str:
    """asyncpg has no pgvector codec; pass text and cast to ::vector in SQL."""
    return "[" + ",".join(f"{v:.7g}" for v in values) + "]"


async def init_schema(conn: asyncpg.Connection) -> None:
    await conn.execute(DDL)


async def claim(conn: asyncpg.Connection, message_id: str) -> bool:
    """Return True if newly claimed; False if already processed."""
    claimed = await conn.fetchval(
        "INSERT INTO processed_messages (message_id) VALUES ($1) ON CONFLICT DO NOTHING RETURNING message_id",
        message_id,
    )
    return claimed is not None


async def upsert(
    conn: asyncpg.Connection,
    event: Event,
    embedding: list[float] | None,
    model: str | None,
    surrogate: bool,
) -> bool:
    """Apply one event under D2's latest-wins guard. Returns True if the row changed."""
    active = event.kind == "company-upserted"
    result = await conn.execute(
        UPSERT,
        event.reg,
        event.company_name or ("Unknown" if not active else event.reg),
        event.nature or None,
        event.state or None,
        event.industry or None,
        event.updated_at,
        event.message_id,
        active,
        model if embedding is not None else None,
        surrogate,
        vector_literal(embedding) if embedding is not None else None,
    )
    return result.endswith("1")
