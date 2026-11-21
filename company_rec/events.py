"""Parse and normalise registry feed messages. Poison reasons are explicit, never guessed."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

SENTINEL = datetime(1970, 1, 1, tzinfo=timezone.utc)
MALAYSIA = timezone(timedelta(hours=8))


@dataclass(frozen=True)
class Event:
    reg: str
    kind: str
    updated_at: datetime
    message_id: str
    company_name: str
    nature: str
    state: str
    industry: str


@dataclass(frozen=True)
class Parsed:
    """Either an event or a poison reason. `shell` marks a first-seen malformed upsert."""

    event: Event | None
    reason: str | None
    message_id: str
    reg: str | None = None
    shell: bool = False


def normalize_updated_at(value: object) -> datetime:
    """ISO +08:00 / Z / naive / int epoch-ms / missing -> aware UTC."""
    if value is None or value == "":
        return SENTINEL
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    if not isinstance(value, str):
        return SENTINEL
    text = value.strip()
    if not text:
        return SENTINEL
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return SENTINEL
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MALAYSIA)
    return dt.astimezone(timezone.utc)


def _text(value: object) -> str:
    """Only usable strings; 0/None/objects are not company data."""
    return value.strip() if isinstance(value, str) and value.strip() else ""


def parse(raw: bytes | str, message_id: str | None = None) -> Parsed:
    body = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
    mid = message_id or hashlib.sha256(body.encode()).hexdigest()
    try:
        obj = json.loads(body, strict=True)
    except (ValueError, TypeError):
        return Parsed(None, "malformed-json", mid)
    if obj is None:
        return Parsed(None, "null", mid)
    if not isinstance(obj, dict):
        return Parsed(None, "non-object", mid)

    reg_value = obj.get("registrationNumber")
    reg = _text(reg_value)
    kind = obj.get("type")
    if not reg:
        return Parsed(None, "missing-registration-number", mid)
    if kind not in ("company-upserted", "company-struck-off"):
        return Parsed(None, f"unknown-type:{kind!s}", mid, reg=reg)

    data = obj.get("data")
    if kind == "company-upserted" and not isinstance(data, dict):
        return Parsed(None, "non-dict-data", mid, reg=reg, shell=True)
    data = data if isinstance(data, dict) else {}

    return Parsed(
        event=Event(
            reg=reg,
            kind=kind,
            updated_at=normalize_updated_at(obj.get("updatedAt")),
            message_id=mid,
            company_name=_text(data.get("companyName")),
            nature=_text(data.get("natureOfBusiness")),
            state=_text(data.get("state")),
            industry=_text(data.get("industry")),
        ),
        reason=None,
        message_id=mid,
        reg=reg,
    )


def surrogate_text(event: Event) -> str:
    """Fallback embedding input. Empty means no embedding (shell / all fields missing)."""
    return " ".join(p for p in (event.industry, event.company_name, event.state) if p)
