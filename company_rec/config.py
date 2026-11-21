"""Environment-driven configuration for consumer and service."""

import os
from dataclasses import dataclass

DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/companies"


@dataclass(frozen=True)
class Settings:
    dsn: str
    amqp_url: str
    queue: str
    dlx: str
    dlq: str
    model: str
    limit_max: int
    candidate_window: int
    dlq_baseline: int
    industry_bonus: float
    state_bonus: float

    @classmethod
    def from_env(cls) -> "Settings":
        port = os.environ.get("RABBITMQ_PORT", "5672")
        return cls(
            dsn=os.environ.get("DATABASE_URL", DEFAULT_DSN),
            amqp_url=os.environ.get("RABBITMQ_URL", f"amqp://guest:guest@localhost:{port}/"),
            queue=os.environ.get("QUEUE", "company-events"),
            dlx=os.environ.get("DLX", "company-events.dlx"),
            dlq=os.environ.get("DLQ", "company-events.dlq"),
            model=os.environ.get("EMBED_MODEL", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"),
            limit_max=int(os.environ.get("LIMIT_MAX", "20")),
            candidate_window=int(os.environ.get("CANDIDATE_WINDOW", "40")),
            dlq_baseline=int(os.environ.get("DLQ_BASELINE", "25")),
            industry_bonus=float(os.environ.get("INDUSTRY_BONUS", "0.02")),
            state_bonus=float(os.environ.get("STATE_BONUS", "0.01")),
        )
