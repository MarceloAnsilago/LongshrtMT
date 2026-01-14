from __future__ import annotations

from datetime import date, timedelta
from urllib.parse import quote_plus
import pandas as pd

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.cache import cache
from django.http import HttpRequest, JsonResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse_lazy
from django.utils import timezone
from django.views.decorators.http import require_http_methods, require_GET, require_POST
from django.views.generic import ListView, TemplateView

from acoes.models import Asset
from mt5_bridge_client.mt5client import fetch_last_close_d1, MT5BridgeError
from .models import QuoteDaily, MissingQuoteLog

from longshort.services.quotes import (
    bulk_update_quotes,
    scan_all_assets_and_fix,
    find_missing_dates_for_asset,
    try_fetch_single_date,
    _date_to_unix,  # helper p/ montar link do Yahoo
)


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
        ctx["logs"] = MissingQuoteLog.objects.order_by("-created_at")[:20]

        limit = 200 if tickers_filter else 60
        pivot_ctx = _build_pivot_context(
            self.request,
            max_rows=limit,
            tickers_filter=tickers_filter,
        )
        ctx["pivot_cols"] = pivot_ctx["cols"]
        ctx["pivot_rows"] = pivot_ctx["rows"]
        ctx["ticker_input"] = ",".join(tickers_filter or [])

        bridge_error_entries = []
        bridge_errors = 0
        assets_for_bridge = Asset.objects.filter(is_active=True).order_by("ticker")[:12]
        for asset in assets_for_bridge:
            preco_d1 = None
            erro = None
            try:
                preco_d1 = fetch_last_close_d1(asset.ticker)
            except MT5BridgeError as exc:
                erro = str(exc)
                bridge_errors += 1
                bridge_error_entries.append({"asset": asset, "erro": erro})

        if bridge_errors:
            messages.warning(
                self.request,
                "Algumas cotações D1 não puderam ser carregadas via MT5 Bridge. Confira os logs.",
            )

        ctx["bridge_error_entries"] = bridge_error_entries
        return ctx


class QuoteDailyListView(LoginRequiredMixin, ListView):
    model = QuoteDaily
    template_name = "cotacoes/quote_table.html"
    context_object_name = "quotes"
    paginate_by = 100


def _prune_quotes_over_limit(assets, *, max_rows: int = 210) -> int:
    total_deleted = 0
    for asset in assets:
        ids = list(
            QuoteDaily.objects.filter(asset=asset)
            .order_by("-date")
            .values_list("id", flat=True)[max_rows:]
        )
        if not ids:
            continue
        deleted, _ = QuoteDaily.objects.filter(id__in=ids).delete()
        total_deleted += deleted
    return total_deleted


@login_required
def update_quotes(request: HttpRequest):
    assets = list(Asset.objects.filter(is_active=True).order_by("id"))
    n_assets, n_rows = bulk_update_quotes(assets, period="2y", interval="1d")
    deleted = _prune_quotes_over_limit(assets, max_rows=210)
    messages.success(
        request,
        f"Cota??es atualizadas: {n_assets} ativos, {n_rows} linhas inseridas, {deleted} removidas.",
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



@login_required
@require_POST
def clear_logs(request: HttpRequest):
    deleted = MissingQuoteLog.objects.filter(resolved_bool=False).delete()[0]
    messages.success(request, f"Logs limpos: {deleted} removidos.")
    return redirect("cotacoes:home")


PROGRESS_KEY = "quotes_progress_user_{uid}"

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
    assets = list(Asset.objects.filter(is_active=True).order_by("id"))

    def progress_cb(sym: str, idx: int, total: int, status: str, rows: int):
        _progress_set(request.user.id, ticker=sym, index=idx, total=total, status=status, rows=rows)

    total_assets = len(assets)
    _progress_set(request.user.id, ticker="", index=0, total=total_assets, status="starting", rows=0)
    n_assets, n_rows = bulk_update_quotes(assets, period="2y", interval="1d", progress_cb=progress_cb)
    deleted = _prune_quotes_over_limit(assets, max_rows=210)
    messages.success(
        request,
        f"Cota??es atualizadas: {n_assets} ativos, {n_rows} linhas inseridas, {deleted} removidas.",
    )
    _progress_set(
        request.user.id,
        ticker="",
        index=n_assets,
        total=total_assets,
        status="done",
        rows=n_rows,
        deleted=deleted,
    )
    return JsonResponse({"ok": True, "assets": n_assets, "rows": n_rows, "deleted": deleted})


@login_required
def update_live_quotes_view(request: HttpRequest):
    """
    View que atualiza os preços ao vivo (intervalo de 5 minutos via Yahoo Finance)
    e salva na tabela cotacoes_quotelive.
    """
    from longshort.services.quotes import update_live_quotes

    assets = Asset.objects.filter(is_active=True).order_by("id")
    n_updated, n_total = update_live_quotes(assets)

    messages.success(request, f"Cotações ao vivo atualizadas: {n_updated}/{n_total} ativos.")
    return redirect("cotacoes:home")



def faltantes(request):
    return redirect("cotacoes:faltantes_home")

@require_http_methods(["GET"])
def faltantes_home(request):
    """
    Mostra a página e um botão 'Escanear e corrigir'.
    Se já houver resultados em sessão (última execução), renderiza-os.
    """
    ctx = {
        "current": "faltantes",
        "results": request.session.pop("faltantes_results", None),
    }
    return render(request, "cotacoes/faltantes.html", ctx)

@require_http_methods(["POST"])
def faltantes_scan(request):
    use_stooq = bool(request.POST.get("use_stooq"))
    # exemplo limitando a janela a 18 meses (opcional):
    results = scan_all_assets_and_fix(use_stooq=use_stooq, since_months=18)

    n_fixed = sum(r["fixed"] for r in results)
    n_remaining = sum(len(r["remaining"]) for r in results)
    messages.info(request, f"Scanner concluído: {n_fixed} preenchido(s), {n_remaining} restante(s).")

    request.session["faltantes_results"] = results
    return redirect("cotacoes:faltantes_home")


from django.shortcuts import render, redirect, get_object_or_404
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from acoes.models import Asset
from longshort.services.quotes import (
    find_missing_dates_for_asset,
    try_fetch_single_date,
)



@require_http_methods(["GET"])
def faltantes_detail(request, ticker: str):
    asset = get_object_or_404(Asset, ticker=ticker.upper())
    # reescaneia só esse ativo pra pegar a lista atualizada
    missing = find_missing_dates_for_asset(asset)
    # monta linhas com link pro Yahoo e ação de tentar baixar
    google_query = quote_plus(f"{ticker.upper()} SA")
    google_url = f"https://www.google.com/search?q={google_query}"
    rows = []
    for d in missing:
        period1 = _date_to_unix(d)  # usa helper do services (ou recrie aqui)
        period2 = _date_to_unix(d + timedelta(days=1))
        yahoo_url = f"https://finance.yahoo.com/quote/{ticker.upper()}.SA/history?period1={period1}&period2={period2}"
        rows.append(
            {
                "date": d,
                "date_iso": d.isoformat(),
                "yahoo_url": yahoo_url,
                "google_url": google_url,
            }
        )
    ctx = {
        "current": "faltantes",
        "ticker": ticker.upper(),
        "rows": rows,
    }
    return render(request, "cotacoes/faltantes_detail.html", ctx)

@require_http_methods(["POST"])
def faltantes_fetch_one(request, ticker: str, dt: str):
    asset = get_object_or_404(Asset, ticker=ticker.upper())
    try:
        d = date.fromisoformat(dt)
    except Exception:
        messages.error(request, f"Data inválida: {dt}")
        return redirect("cotacoes:faltantes_detail", ticker=ticker)

    ok = try_fetch_single_date(asset, d, use_stooq=True)
    if ok:
        messages.success(request, f"{ticker} {d} inserido com sucesso.")
    else:
        messages.warning(request, f"{ticker} {d}: não há dado nas fontes.")
    return redirect("cotacoes:faltantes_detail", ticker=ticker)

@require_http_methods(["POST"])
def faltantes_insert_one(request, ticker: str):
    asset = get_object_or_404(Asset, ticker=ticker.upper())
    dt = request.POST.get("date")
    px = request.POST.get("price")
    try:
        d = date.fromisoformat(dt)
        price = float(px)
        QuoteDaily.objects.create(asset=asset, date=d, close=price)
        messages.success(request, f"Inserido manualmente: {ticker} {d} = {price:.2f}.")
    except Exception as e:
        messages.error(request, f"Falha ao inserir: {e}")
    return redirect("cotacoes:faltantes_detail", ticker=ticker)
