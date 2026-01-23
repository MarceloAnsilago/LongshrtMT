from __future__ import annotations


import pandas as pd

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.cache import cache
from django.http import HttpRequest, JsonResponse
from django.shortcuts import render, redirect
from django.urls import reverse_lazy
from django.utils import timezone
from django.views.decorators.http import require_http_methods, require_GET, require_POST
from django.views.generic import ListView, TemplateView
from django.db.models import Max

from acoes.models import Asset
from .models import QuoteDaily
from longshort.services.quotes import update_live_quotes


def _parse_ticker_filter(request: HttpRequest) -> list[str] | None:
    raw = request.GET.get("tickers", "")
    if not raw:
        return None
    parts = [segment.strip().upper() for segment in raw.split(",")]
    filtered = [part for part in parts if part]
    return filtered or None

@require_http_methods(["GET"])


def _build_pivot_context(
    request: HttpRequest,
    max_rows: int = 90,
    tickers_filter: list[str] | None = None,
):
    qs = QuoteDaily.objects.select_related("asset").order_by("-date")
    if not qs.exists():
        return {"cols": [], "rows": []}
    df = pd.DataFrame(list(qs.values("date", "asset__ticker", "close")))
    if df.empty:
        return {"cols": [], "rows": []}
    df_pivot = (
        df.pivot(index="date", columns="asset__ticker", values="close")
          .sort_index(ascending=False)
          .round(2)
    )
    if max_rows:
        df_pivot = df_pivot.head(max_rows)
    cols = list(df_pivot.columns)
    selected = set(tickers_filter) if tickers_filter else None
    if selected:
        cols = [col for col in cols if col.upper() in selected]
    rows = []
    for dt, row in df_pivot.iterrows():
        rows.append({
            "date": dt,
            "values": [("" if pd.isna(row[c]) else float(row[c])) for c in cols],
        })
    return {"cols": cols, "rows": rows}



class QuotesHomeView(LoginRequiredMixin, TemplateView):
    template_name = "cotacoes/quote_list.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)

        tickers_filter = _parse_ticker_filter(self.request)
        ctx["last_quotes"] = (
            QuoteDaily.objects.select_related("asset")
            .order_by("-date")[:30]
        )

        limit = 200 if tickers_filter else 60
        pivot_ctx = _build_pivot_context(
            self.request,
            max_rows=limit,
            tickers_filter=tickers_filter,
        )
        ctx["pivot_cols"] = pivot_ctx["cols"]
        ctx["pivot_rows"] = pivot_ctx["rows"]
        ctx["ticker_input"] = ",".join(tickers_filter or [])
        ctx["last_refresh_label"] = _get_last_refresh_label(self.request.user.id)
        latest_date = QuoteDaily.objects.aggregate(last=Max("date"))["last"]
        ctx["last_quote_label"] = latest_date.strftime("%d/%m/%Y") if latest_date else "--"

        return ctx


class QuoteDailyListView(LoginRequiredMixin, ListView):
    model = QuoteDaily
    template_name = "cotacoes/quote_table.html"
    context_object_name = "quotes"
    paginate_by = 100


@login_required
def update_quotes(request: HttpRequest):
    latest_date = QuoteDaily.objects.aggregate(last=Max("date"))["last"]
    refreshed_at = timezone.now()
    cache.set(LAST_REFRESH_KEY.format(uid=request.user.id), refreshed_at, timeout=60 * 60 * 24)
    latest_label = latest_date.strftime("%d/%m/%Y") if latest_date else "--"
    refreshed_label = _format_refresh_dt(refreshed_at)
    messages.success(
        request,
        f"Ultima cotacao diaria do Supabase: {latest_label}. Atualizado em {refreshed_label}.",
    )
    return redirect(reverse_lazy("cotacoes:home"))

def quotes_pivot(request: HttpRequest):
    tickers_filter = _parse_ticker_filter(request)
    limit = 200 if tickers_filter else None
    pivot_ctx = _build_pivot_context(
        request,
        max_rows=limit,
        tickers_filter=tickers_filter,
    )
    return render(request, "cotacoes/quote_pivot.html",
                  {"cols": pivot_ctx["cols"], "data": pivot_ctx["rows"]})



PROGRESS_KEY = "quotes_progress_user_{uid}"
LAST_REFRESH_KEY = "quotes_last_refresh_user_{uid}"


def _format_refresh_dt(dt_value) -> str:
    if not dt_value:
        return "--"
    try:
        localized = timezone.localtime(dt_value)
    except Exception:
        localized = dt_value
    try:
        return localized.strftime("%d/%m %H:%M")
    except Exception:
        return "--"


def _get_last_refresh_label(user_id: int) -> str:
    cached = cache.get(LAST_REFRESH_KEY.format(uid=user_id))
    if not cached:
        return "--"
    if isinstance(cached, str):
        try:
            cached = timezone.datetime.fromisoformat(cached)
        except Exception:
            return "--"
    return _format_refresh_dt(cached)

def _progress_set(user_id: int, **kwargs):
    key = PROGRESS_KEY.format(uid=user_id)
    payload = {"ts": timezone.now().isoformat(), **kwargs}
    cache.set(key, payload, timeout=60*10)

def _progress_get(user_id: int):
    key = PROGRESS_KEY.format(uid=user_id)
    return cache.get(key) or {}

@require_GET
@login_required
def quotes_progress(request: HttpRequest):
    return JsonResponse(_progress_get(request.user.id))

@login_required
@require_POST
def update_quotes_ajax(request: HttpRequest):
    latest_date = QuoteDaily.objects.aggregate(last=Max("date"))["last"]
    refreshed_at = timezone.now()
    cache.set(LAST_REFRESH_KEY.format(uid=request.user.id), refreshed_at, timeout=60 * 60 * 24)
    latest_label = latest_date.strftime("%d/%m/%Y") if latest_date else "--"
    refreshed_label = _format_refresh_dt(refreshed_at)
    _progress_set(
        request.user.id,
        ticker="",
        index=1,
        total=1,
        status="done",
        rows=0,
        deleted=0,
        last_refresh_label=refreshed_label,
        last_quote_label=latest_label,
    )
    return JsonResponse(
        {
            "ok": True,
            "last_quote_label": latest_label,
            "last_refresh_label": refreshed_label,
        }
    )


@login_required
def update_live_quotes_view(request: HttpRequest):
    """
    View que valida as cotacoes ao vivo no Supabase.
    """
    assets = Asset.objects.filter(is_active=True).order_by("id")
    n_updated, n_total = update_live_quotes(assets)

    messages.success(request, f"Cotacoes ao vivo verificadas: {n_updated}/{n_total} ativos.")
    return redirect("cotacoes:home")



