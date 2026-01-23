from django.contrib import admin

from .models import Mt5Terminal, OrderEvent, OrderRequest


@admin.register(Mt5Terminal)
class Mt5TerminalAdmin(admin.ModelAdmin):
    list_display = ("terminal_id", "status", "last_seen_at")
    search_fields = ("terminal_id",)


@admin.register(OrderRequest)
class OrderRequestAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "terminal",
        "pair_id",
        "side",
        "status",
        "created_at",
        "claimed_at",
        "done_at",
    )
    list_filter = ("status", "side", "order_type", "terminal")
    search_fields = ("id", "client_order_id", "pair_id", "symbol_a", "symbol_b")


@admin.register(OrderEvent)
class OrderEventAdmin(admin.ModelAdmin):
    list_display = ("order", "event_type", "created_at")
    list_filter = ("event_type",)
    search_fields = ("order__id",)
