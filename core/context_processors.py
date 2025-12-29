from __future__ import annotations

from django.conf import settings


def mt5_dry_run(_request):
    """Expose whether MT5 orders are in dry-run mode for all templates."""
    return {"MT5_DRY_RUN": getattr(settings, "MT5_DRY_RUN", False)}
