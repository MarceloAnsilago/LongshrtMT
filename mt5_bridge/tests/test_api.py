from __future__ import annotations

import pytest

from mt5_bridge import api


class DummyResult:
    def __init__(self, position=None, deal=None):
        self.position = position
        self.deal = deal


class DummyDeal:
    def __init__(self, deal, position_id):
        self.deal = deal
        self.position_id = position_id


def test_resolve_position_id_prefers_direct_value(monkeypatch):
    result = DummyResult(position=42, deal=99)
    monkeypatch.setattr(api.mt5, "history_deals_get", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("should not call")))
    assert api._resolve_position_id(result) == 42


def test_resolve_position_id_uses_history_when_missing(monkeypatch):
    result = DummyResult(position=None, deal=1001)
    captured = {"called": False}

    def fake_history(start, end):
        captured["called"] = True
        return [DummyDeal(deal=1001, position_id=777)]

    monkeypatch.setattr(api.mt5, "history_deals_get", fake_history)
    assert api._resolve_position_id(result) == 777
    assert captured["called"]


def test_resolve_position_id_returns_none_when_no_matches(monkeypatch):
    result = DummyResult(position=None, deal=55)

    def fake_history(start, end):
        return [DummyDeal(deal=999, position_id=1)]

    monkeypatch.setattr(api.mt5, "history_deals_get", fake_history)
    assert api._resolve_position_id(result) is None
