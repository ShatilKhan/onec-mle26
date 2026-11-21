"""Unit tests for feed parsing and timestamp normalisation."""

from datetime import datetime, timezone

from company_rec.events import SENTINEL, normalize_updated_at, parse, surrogate_text


def test_valid_upsert():
    line = '{"type":"company-upserted","registrationNumber":"SYN-1","updatedAt":"2026-08-14T09:00:00+08:00","data":{"companyName":"A","natureOfBusiness":"cafes","state":"Selangor","industry":"Food & Beverage"}}'
    p = parse(line, "mid-1")
    assert p.reason is None and p.event is not None
    assert p.event.updated_at == datetime(2026, 8, 14, 1, 0, tzinfo=timezone.utc)
    assert p.event.nature == "cafes"
    assert surrogate_text(p.event) == "Food & Beverage A Selangor"


def test_poison_reasons():
    assert parse("{bad", "m").reason == "malformed-json"
    assert parse("null", "m").reason == "null"
    assert parse("[1,2]", "m").reason == "non-object"
    assert parse('{"type":"company-upserted","data":{}}', "m").reason == "missing-registration-number"
    assert parse('{"type":"x","registrationNumber":"SYN-1"}', "m").reason == "unknown-type:x"
    p = parse('{"type":"company-upserted","registrationNumber":"SYN-1","data":"x"}', "m")
    assert p.reason == "non-dict-data" and p.shell and p.reg == "SYN-1"


def test_timestamp_shapes():
    assert normalize_updated_at(1786670837000) == datetime.fromtimestamp(1786670837, tz=timezone.utc)
    assert normalize_updated_at("2026-08-14T01:03:35Z") == datetime(2026, 8, 14, 1, 3, 35, tzinfo=timezone.utc)
    assert normalize_updated_at("2026-08-14T09:06:19") == datetime(2026, 8, 14, 1, 6, 19, tzinfo=timezone.utc)
    assert normalize_updated_at(None) == SENTINEL
    assert normalize_updated_at("garbage") == SENTINEL


def test_struck_off_has_no_data():
    p = parse('{"type":"company-struck-off","registrationNumber":"SYN-9","updatedAt":"2026-08-14T16:00:33+08:00"}', "m")
    assert p.reason is None and p.event is not None
    assert p.event.kind == "company-struck-off" and p.event.nature == ""


def test_message_id_fallback():
    p = parse("null")
    assert len(p.message_id) == 64
