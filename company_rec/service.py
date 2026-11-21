"""Similar-companies API for the portal."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

import asyncpg
from fastapi import FastAPI, HTTPException, Query

from company_rec.config import Settings

settings = Settings.from_env()
_pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    global _pool
    _pool = await asyncpg.create_pool(settings.dsn)
    try:
        yield
    finally:
        await _pool.close()
        _pool = None


app = FastAPI(title="company-rec", lifespan=lifespan)


def _norm(value: str | None) -> str:
    return value.strip().lower() if value else ""


@asynccontextmanager
async def _conn():
    assert _pool is not None
    async with _pool.acquire() as conn:
        yield conn


@app.get("/health")
async def health() -> dict[str, str]:
    try:
        async with _conn() as conn:
            await conn.fetchval("SELECT 1")
        db = "ok"
    except Exception:
        db = "down"
    return {"status": "ok", "db": db}


@app.get("/companies/{reg}/similar")
async def similar(
    reg: str,
    limit: int = Query(4, ge=1, le=20),
    offset: int = Query(0, ge=0),
) -> dict:
    if limit + offset > settings.candidate_window:
        raise HTTPException(422, detail="limit + offset exceeds candidate window")
    try:
        async with _conn() as conn:
            query = await conn.fetchrow(
                "SELECT embedding::text AS embedding, industry, state, company_name FROM companies "
                "WHERE registration_number = $1 AND active = TRUE AND embedding IS NOT NULL",
                reg,
            )
            if query is None:
                raise HTTPException(404, detail="company not found")
            rows = await conn.fetch(
                "SELECT registration_number, company_name, nature_of_business, state, industry, "
                "1 - (embedding <=> $1::vector) AS score "
                "FROM companies "
                "WHERE active = TRUE AND embedding IS NOT NULL AND registration_number <> $2 "
                "ORDER BY embedding <=> $1::vector, registration_number LIMIT $3",
                query["embedding"],
                reg,
                settings.candidate_window,
            )
    except asyncpg.PostgresError as exc:
        raise HTTPException(503, detail="database unavailable") from exc

    industry = _norm(query["industry"])
    state = _norm(query["state"])
    ranked = []
    for row in rows:
        raw = float(row["score"])
        adjusted = raw
        if industry and _norm(row["industry"]) == industry:
            adjusted += settings.industry_bonus
        if state and _norm(row["state"]) == state:
            adjusted += settings.state_bonus
        ranked.append((adjusted, raw, row))
    ranked.sort(key=lambda item: (-item[0], item[2]["registration_number"]))
    page = ranked[offset : offset + limit]
    results = [
        {
            "registrationNumber": row["registration_number"],
            "companyName": row["company_name"],
            "natureOfBusiness": row["nature_of_business"],
            "state": row["state"],
            "industry": row["industry"],
            "score": round(raw, 6),
        }
        for _, raw, row in page
    ]
    return {
        "registrationNumber": reg,
        "companyName": query["company_name"],
        "has_more": offset + len(page) < len(ranked),
        "results": results,
    }
