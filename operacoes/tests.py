from __future__ import annotations

from decimal import Decimal
from datetime import timedelta
from unittest.mock import patch

import MetaTrader5 as mt5
from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone

from acoes.models import Asset
from operacoes.models import Operation, OperationMT5Trade, MT5IncidentEvent
from operacoes.services.mt5_reset import detect_demo_reset_for_open_trades
from operacoes.services.mt5_trade import _build_trade_payload, _simulation_expiration


class DemoResetDetectorTest(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_user(username="test", password="password")
        self.sell_asset = Asset.objects.create(ticker="PETR4")
        self.buy_asset = Asset.objects.create(ticker="VALE3")
        self.operation = Operation.objects.create(
            user=self.user,
            left_asset=self.sell_asset,
            right_asset=self.buy_asset,
            sell_asset=self.sell_asset,
            buy_asset=self.buy_asset,
            sell_quantity=1,
            buy_quantity=1,
            lot_size=1,
            lot_multiplier=1,
            sell_price=Decimal("10"),
            buy_price=Decimal("10"),
            sell_value=Decimal("10"),
            buy_value=Decimal("10"),
            net_value=Decimal("0"),
            capital_allocated=Decimal("20"),
        )
        self.base_now = timezone.now()
        self.trade = OperationMT5Trade.objects.create(
            operation=self.operation,
            leg="A",
            symbol="PETR4",
            ticket=123456,
            position_id=654321,
            side="SELL",
            volume=1.0,
            price_open=10.0,
        )

    def _set_trade_age(self, age: timedelta) -> None:
        opened_at = self.base_now - age
        OperationMT5Trade.objects.filter(pk=self.trade.pk).update(opened_at=opened_at)
        self.trade.refresh_from_db()

    @patch("operacoes.services.mt5_reset.fetch_mt5_account_info")
    @patch("operacoes.services.mt5_reset.fetch_mt5_history_deals")
    @patch("operacoes.services.mt5_reset.fetch_mt5_positions")
    def test_missing_trade_without_out_deal_triggers_reset(
        self,
        fetch_positions,
        fetch_history,
        fetch_account,
    ):
        fetch_positions.return_value = []
        fetch_history.return_value = []
        fetch_account.return_value = {
            "login": 1000,
            "server": "Demo",
            "balance": 1000.0,
            "equity": 1000.0,
            "margin": 10.0,
            "margin_free": 990.0,
            "margin_mode": mt5.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING,
        }
        self._set_trade_age(timedelta(minutes=10))

        detect_demo_reset_for_open_trades(now=self.base_now, request_id="test-id")

        self.trade.refresh_from_db()
        self.assertEqual(self.trade.status, OperationMT5Trade.STATUS_RESET)
        self.assertEqual(self.trade.close_reason, "DEMO_RESET_NO_DEAL_OUT")
        event = MT5IncidentEvent.objects.get(trade=self.trade)
        self.assertEqual(event.classification, "reset_demo_suspeito")
        self.assertEqual(event.account_server, "Demo")

    @patch("operacoes.services.mt5_reset.fetch_mt5_account_info")
    @patch("operacoes.services.mt5_reset.fetch_mt5_history_deals")
    @patch("operacoes.services.mt5_reset.fetch_mt5_positions")
    def test_missing_trade_with_out_deal_keeps_open(
        self,
        fetch_positions,
        fetch_history,
        fetch_account,
    ):
        fetch_positions.return_value = []
        fetch_history.return_value = [
            {
                "timestamp": self.base_now,
                "entry": mt5.DEAL_ENTRY_OUT,
                "reason": mt5.DEAL_REASON_SL,
                "ticket": self.trade.ticket,
                "order": self.trade.ticket,
            }
        ]
        fetch_account.return_value = {
            "login": 1001,
            "server": "Demo",
            "balance": 1000.0,
            "equity": 1000.0,
            "margin": 10.0,
            "margin_free": 990.0,
            "margin_mode": mt5.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING,
        }
        self._set_trade_age(timedelta(minutes=10))

        detect_demo_reset_for_open_trades(now=self.base_now, request_id="test-id")

        self.trade.refresh_from_db()
        self.assertEqual(self.trade.status, OperationMT5Trade.STATUS_OPEN)
        self.assertFalse(MT5IncidentEvent.objects.filter(trade=self.trade).exists())


class MT5TradePayloadTests(TestCase):
    def setUp(self) -> None:
        user_model = get_user_model()
        self.user = user_model.objects.create_user(username="payload-test", password="password")
        self.sell_asset = Asset.objects.create(ticker="PETR4")
        self.buy_asset = Asset.objects.create(ticker="VALE3")

    def _create_operation(self, *, is_real: bool) -> Operation:
        return Operation.objects.create(
            user=self.user,
            left_asset=self.sell_asset,
            right_asset=self.buy_asset,
            sell_asset=self.sell_asset,
            buy_asset=self.buy_asset,
            window=220,
            orientation="default",
            source="manual",
            sell_quantity=100,
            buy_quantity=100,
            lot_size=100,
            lot_multiplier=1,
            sell_price=Decimal("10.00"),
            buy_price=Decimal("10.00"),
            sell_value=Decimal("1000.00"),
            buy_value=Decimal("1000.00"),
            net_value=Decimal("0.00"),
            capital_allocated=Decimal("2000.00"),
            is_real=is_real,
        )

    def test_simulator_payload_adds_specified_expiration(self):
        operation = self._create_operation(is_real=False)
        expiration = _simulation_expiration()
        payload = _build_trade_payload(operation, "sell", expiration_at=expiration)
        self.assertEqual(payload["type_time"], "SPECIFIED")
        self.assertEqual(payload["expiration"], expiration.isoformat())
        self.assertEqual(payload["order_type"], "SELL_LIMIT")

    @patch("operacoes.services.mt5_trade.get_latest_price")
    def test_simulation_payload_uses_limit_order(self, latest_price):
        latest_price.return_value = 100.0
        operation = self._create_operation(is_real=False)
        expiration = _simulation_expiration()
        sell_payload = _build_trade_payload(operation, "sell", expiration_at=expiration)
        buy_payload = _build_trade_payload(operation, "buy", expiration_at=expiration)
        self.assertEqual(sell_payload["order_type"], "SELL_LIMIT")
        self.assertEqual(buy_payload["order_type"], "BUY_LIMIT")
        self.assertEqual(sell_payload["type_time"], "SPECIFIED")
        self.assertEqual(buy_payload["type_time"], "SPECIFIED")
        self.assertGreater(sell_payload["price"], 100.0)
        self.assertLess(buy_payload["price"], 100.0)

    def test_real_payload_defaults_to_gtc_without_expiration(self):
        operation = self._create_operation(is_real=True)
        payload = _build_trade_payload(operation, "buy")
        self.assertEqual(payload["type_time"], "GTC")
        self.assertNotIn("expiration", payload)
        self.assertIsNone(payload.get("order_type"))

    @patch("operacoes.services.mt5_reset.fetch_mt5_account_info")
    @patch("operacoes.services.mt5_reset.fetch_mt5_history_deals")
    @patch("operacoes.services.mt5_reset.fetch_mt5_positions")
    def test_trade_still_in_mt5_positions_is_skipped(
        self,
        fetch_positions,
        fetch_history,
        fetch_account,
    ):
        fetch_positions.return_value = [{"ticket": self.trade.ticket}]
        fetch_history.return_value = []
        fetch_account.return_value = {
            "login": 1002,
            "server": "Demo",
            "balance": 1000.0,
            "equity": 1000.0,
            "margin": 10.0,
            "margin_free": 990.0,
            "margin_mode": mt5.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING,
        }
        self._set_trade_age(timedelta(minutes=10))

        detect_demo_reset_for_open_trades(now=self.base_now, request_id="test-id")

        self.trade.refresh_from_db()
        fetch_history.assert_not_called()
        self.assertEqual(self.trade.status, OperationMT5Trade.STATUS_OPEN)
        self.assertFalse(MT5IncidentEvent.objects.filter(trade=self.trade).exists())

    @override_settings(MT5_RESET_MIN_AGE_SECONDS=300)
    @patch("operacoes.services.mt5_reset.fetch_mt5_account_info")
    @patch("operacoes.services.mt5_reset.fetch_mt5_history_deals")
    @patch("operacoes.services.mt5_reset.fetch_mt5_positions")
    def test_recent_trade_is_not_marked_even_without_history(
        self,
        fetch_positions,
        fetch_history,
        fetch_account,
    ):
        fetch_positions.return_value = []
        fetch_history.return_value = []
        fetch_account.return_value = {
            "login": 1003,
            "server": "Demo",
            "balance": 1000.0,
            "equity": 1000.0,
            "margin": 10.0,
            "margin_free": 990.0,
            "margin_mode": mt5.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING,
        }
        self._set_trade_age(timedelta(seconds=30))

        detect_demo_reset_for_open_trades(now=self.base_now, request_id="test-id")

        self.trade.refresh_from_db()
        self.assertEqual(self.trade.status, OperationMT5Trade.STATUS_OPEN)
        self.assertFalse(MT5IncidentEvent.objects.filter(trade=self.trade).exists())
