from __future__ import annotations

from typing import Sequence

import MetaTrader5 as mt5
from django.db import transaction

from operacoes.models import OperationMT5Trade

__all__ = ["MT5ReconciliationError", "reconcile_mt5_positions"]


class MT5ReconciliationError(Exception):
    """Erro ao tentar reconciliar posições entre o banco e o MT5."""


def reconcile_mt5_positions() -> Sequence[OperationMT5Trade]:
    """
    Reconcilia trades marcados como 'aberto' com as posições reais no MT5.

    Esta função inicializa o MT5, coleta as posições abertas e compara os tickets
    com os registros armazenados em OperationMT5Trade. Trades que não existem mais na
    lista de posições recebem o status 'encerrado_manual' e são salvos.
    """
    initialized = mt5.initialize()
    if not initialized:
        code, message = mt5.last_error()
        raise MT5ReconciliationError(
            f"Erro ao conectar com o terminal MT5: {message} ({code})"
        )

    try:
        positions = mt5.positions_get()
        if positions is None:
            raise MT5ReconciliationError("Erro ao ler posições abertas do MT5.")

        open_tickets: set[int] = set()
        for position in positions:
            ticket = getattr(position, "ticket", None)
            if ticket is None:
                continue
            try:
                open_tickets.add(int(ticket))
            except (TypeError, ValueError):
                continue
        reconciled: list[OperationMT5Trade] = []

        with transaction.atomic():
            trades = OperationMT5Trade.objects.filter(status=OperationMT5Trade.STATUS_OPEN)
            for trade in trades:
                if trade.ticket not in open_tickets:
                    trade.status = OperationMT5Trade.STATUS_MANUAL
                    trade.save(update_fields=["status"])
                    reconciled.append(trade)

        return tuple(reconciled)
    finally:
        if initialized:
            mt5.shutdown()
