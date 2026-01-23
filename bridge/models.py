import uuid

from django.db import models
from django.utils import timezone


class Mt5Terminal(models.Model):
    terminal_id = models.CharField(primary_key=True, max_length=64)
    last_seen_at = models.DateTimeField(default=timezone.now)
    status = models.CharField(max_length=16, default="online")
    meta = models.JSONField(null=True, blank=True)

    def __str__(self) -> str:
        return self.terminal_id


class OrderRequest(models.Model):
    class Side(models.TextChoices):
        BUY = "BUY", "BUY"
        SELL = "SELL", "SELL"

    class OrderType(models.TextChoices):
        MARKET = "MARKET", "MARKET"
        LIMIT = "LIMIT", "LIMIT"

    class Status(models.TextChoices):
        QUEUED = "QUEUED", "QUEUED"
        CLAIMED = "CLAIMED", "CLAIMED"
        SENT = "SENT", "SENT"
        FILLED = "FILLED", "FILLED"
        REJECTED = "REJECTED", "REJECTED"
        CANCELLED = "CANCELLED", "CANCELLED"
        EXPIRED = "EXPIRED", "EXPIRED"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    terminal = models.ForeignKey(
        Mt5Terminal,
        on_delete=models.PROTECT,
        related_name="orders",
    )
    pair_id = models.CharField(max_length=64)
    side = models.CharField(max_length=16, choices=Side.choices)
    symbol_a = models.CharField(max_length=32)
    symbol_b = models.CharField(max_length=32, null=True, blank=True)
    qty_a = models.DecimalField(max_digits=18, decimal_places=6)
    qty_b = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    order_type = models.CharField(max_length=16, choices=OrderType.choices, default=OrderType.MARKET)
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.QUEUED)
    client_order_id = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    claimed_at = models.DateTimeField(null=True, blank=True)
    done_at = models.DateTimeField(null=True, blank=True)
    error = models.TextField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["terminal", "client_order_id"],
                name="uniq_terminal_client_order",
            )
        ]
        indexes = [
            models.Index(fields=["terminal", "status", "created_at"]),
            models.Index(fields=["pair_id"]),
            models.Index(fields=["claimed_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.id} ({self.pair_id})"


class OrderEvent(models.Model):
    class EventType(models.TextChoices):
        ACK = "ACK", "ACK"
        FILL = "FILL", "FILL"
        ERROR = "ERROR", "ERROR"
        INFO = "INFO", "INFO"

    id = models.BigAutoField(primary_key=True)
    order = models.ForeignKey(
        OrderRequest,
        on_delete=models.CASCADE,
        related_name="events",
    )
    event_type = models.CharField(max_length=32, choices=EventType.choices)
    payload = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return f"{self.order_id} - {self.event_type}"
