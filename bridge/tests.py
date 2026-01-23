from django.test import TestCase
from django.utils import timezone

from .models import Mt5Terminal, OrderRequest
from .services import claim_next_order, create_order, update_order_status


class BridgeServicesTests(TestCase):
    def setUp(self):
        self.terminal = Mt5Terminal.objects.create(terminal_id="VPS01")

    def test_create_order_generates_unique_client_id(self):
        first = create_order(
            terminal_id="VPS01",
            pair_id="PETR4_VALE3",
            side=OrderRequest.Side.BUY,
            symbol_a="PETR4",
            qty_a=10,
        )
        second = create_order(
            terminal_id="VPS01",
            pair_id="PETR4_VALE3",
            side=OrderRequest.Side.SELL,
            symbol_a="VALE3",
            qty_a=5,
        )
        self.assertNotEqual(first.client_order_id, second.client_order_id)
        self.assertEqual(first.status, OrderRequest.Status.QUEUED)

    def test_claim_next_order_is_idempotent(self):
        order = create_order(
            terminal_id="VPS01",
            pair_id="PETR4_VALE3",
            side=OrderRequest.Side.BUY,
            symbol_a="PETR4",
            qty_a=10,
        )
        claimed = claim_next_order("VPS01")
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed.id, order.id)
        claimed_again = claim_next_order("VPS01")
        self.assertIsNone(claimed_again)

    def test_update_status_sets_done_at(self):
        order = create_order(
            terminal_id="VPS01",
            pair_id="PETR4_VALE3",
            side=OrderRequest.Side.BUY,
            symbol_a="PETR4",
            qty_a=10,
        )
        updated = update_order_status(
            order_id=order.id,
            status=OrderRequest.Status.FILLED,
            payload={"ticket": 123},
            done=True,
            event_type="FILL",
        )
        self.assertEqual(updated.status, OrderRequest.Status.FILLED)
        self.assertIsNotNone(updated.done_at)
        self.assertLessEqual(updated.done_at, timezone.now())
