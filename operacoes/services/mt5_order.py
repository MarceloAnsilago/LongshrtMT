from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

import MetaTrader5 as mt5
from django.conf import settings
from django.utils import timezone

from operacoes.models import Operation


class MT5OrderSendError(Exception):
    """Erro ao executar uma ordem diretamente no MT5."""


def _to_decimal(value: Any) -> Decimal | None:
    try:
        if value is None:
            return None
        return Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation):
        return None


def _cast_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _resolve_symbol(operation: Operation, override_symbol: str | None) -> str:
    symbol = (override_symbol or operation.symbol or "").strip().upper()
    if not symbol:
        raise MT5OrderSendError("O campo symbol da operação precisa estar preenchido.")
    return symbol


def _resolve_side(side: str | None) -> str:
    if side is None:
        raise MT5OrderSendError("É necessário informar o lado da ordem ('buy' ou 'sell').")
    normalized = side.strip().lower()
    if normalized not in {"buy", "sell"}:
        raise MT5OrderSendError("Lado inválido para a ordem; use 'buy' ou 'sell'.")
    return normalized


def enviar_ordem_mt5(
    operation: Operation,
    *,
    side: str | None = None,
    volume: float | None = None,
    stop_loss: float | None = None,
    take_profit: float | None = None,
    symbol: str | None = None,
    deviation: int | None = None,
) -> mt5.OrderSendResult:
    """
    Envia uma ordem diretamente para o MT5 e registra o preço de entrada.

    O preço armazenado em `operation.entry_price` depende de `operation.is_real`:
    - conta real: usa `result.price` retornado pelo MT5;
    - simulação: usa o tick atual (ask/bid) no momento da execução.
    """
    resolved_side = _resolve_side(side)
    resolved_symbol = _resolve_symbol(operation, symbol)

    try:
        volume_value = float(volume) if volume is not None else 0.0
    except (TypeError, ValueError):
        raise MT5OrderSendError("O volume precisa ser numérico.")
    if volume_value <= 0:
        raise MT5OrderSendError("O volume precisa ser maior que zero.")

    deviation_value = deviation if deviation is not None else getattr(settings, "MT5_TRADE_DEVIATION", 20)
    comment = getattr(settings, "MT5_TRADE_COMMENT", "LongShort")
    magic = getattr(settings, "MT5_TRADE_MAGIC", 0)

    initialized = mt5.initialize()
    if not initialized:
        code, message = mt5.last_error()
        raise MT5OrderSendError(f"Não foi possível inicializar o MT5 ({message} [{code}]).")

    try:
        symbol_info = mt5.symbol_info(resolved_symbol)
        if not symbol_info and not mt5.symbol_select(resolved_symbol, True):
            raise MT5OrderSendError(f"Símbolo {resolved_symbol} indisponível no MT5.")
        tick = mt5.symbol_info_tick(resolved_symbol)
        if not tick:
            raise MT5OrderSendError(f"Tick indisponível para {resolved_symbol}.")

        price_for_side = tick.ask if resolved_side == "buy" else tick.bid
        if not price_for_side or price_for_side <= 0:
            raise MT5OrderSendError(f"Não foi possível determinar o preço para o lado {resolved_side}.")

        trade_request: dict[str, object] = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": resolved_symbol,
            "volume": volume_value,
            "type": mt5.ORDER_TYPE_BUY if resolved_side == "buy" else mt5.ORDER_TYPE_SELL,
            "price": float(price_for_side),
            "deviation": int(deviation_value),
            "magic": int(magic) if magic is not None else 0,
            "comment": comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        if stop_loss is not None:
            trade_request["sl"] = float(stop_loss)
        if take_profit is not None:
            trade_request["tp"] = float(take_profit)

        result = mt5.order_send(trade_request)
        if result is None:
            raise MT5OrderSendError("O MT5 não respondeu à ordem.")
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            comment_text = getattr(result, "comment", None)
            raise MT5OrderSendError(f"Retcode {result.retcode}: {comment_text or 'erro desconhecido'}.")

        entry_price_decimal = _to_decimal(getattr(result, "price", None))
        if not entry_price_decimal or not operation.is_real:
            entry_price_decimal = _to_decimal(price_for_side)
        if entry_price_decimal is None:
            raise MT5OrderSendError("Não foi possível calcular o preço de entrada.")

        ticket_value = (
            _cast_int(getattr(result, "order", None))
            or _cast_int(getattr(result, "deal", None))
            or _cast_int(getattr(result, "ticket", None))
        )

        operation.entry_price = entry_price_decimal
        operation.mt5_ticket = ticket_value
        operation.executed_at = timezone.now()
        symbol_was_blank = not bool(operation.symbol)
        if symbol_was_blank:
            operation.symbol = resolved_symbol

        save_fields = ["entry_price", "mt5_ticket", "executed_at"]
        if symbol_was_blank:
            save_fields.append("symbol")
        if operation.pk:
            operation.save(update_fields=save_fields)
        else:
            operation.save()

        return result
    finally:
        if initialized:
            mt5.shutdown()
