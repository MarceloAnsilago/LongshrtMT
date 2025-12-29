from __future__ import annotations

import logging
from django.conf import settings

from acoes.models import Asset
from mt5_bridge_client.mt5client import MT5BridgeError, execute_trades
from operacoes.models import Operation

logger = logging.getLogger(__name__)


class MT5TradeExecutionError(MT5BridgeError):
    """Erro ao tentar executar uma operacao diretamente no MT5."""


def _normalize_symbol(asset: Asset | None) -> str | None:
    if asset is None:
        return None
    ticker = (asset.ticker or asset.ticker_yf or "").strip().upper()
    if not ticker:
        return None
    if ticker.endswith(".SA"):
        ticker = ticker[:-3]
    return ticker


def _build_comment(operation: Operation, role: str) -> str:
    base = getattr(settings, "MT5_TRADE_COMMENT", "LongShort")
    comment = f"{base} op#{operation.pk} {role}"
    user_id = getattr(operation.user, "id", None)
    if user_id:
        comment = f"{comment} u{user_id}"
    return comment[:31]  # MT5 comment max 31 chars


def _build_trade_payload(operation: Operation, role: str) -> dict[str, object]:
    if role not in {"sell", "buy"}:
        raise ValueError("role deve ser 'sell' ou 'buy'")
    asset = operation.sell_asset if role == "sell" else operation.buy_asset
    symbol = _normalize_symbol(asset)
    if not symbol:
        raise ValueError("Ativo sem ticker válido para MT5")

    quantity = operation.sell_quantity if role == "sell" else operation.buy_quantity
    price = operation.sell_price if role == "sell" else operation.buy_price
    if price is None:
        raise ValueError("Preço não informado para a ponta " + role)
    if quantity is None or quantity <= 0:
        raise ValueError("Quantidade inválida para a ponta " + role)

    volume = float(quantity)

    payload: dict[str, object] = {
        "symbol": symbol,
        "side": role,
        "lots": volume,
        "lot_size": 1,
        "quantity": int(quantity),
        "price": float(price),
        "deviation": int(getattr(settings, "MT5_TRADE_DEVIATION", 20)),
        "comment": _build_comment(operation, role),
        "type_time": "GTC",
        "type_filling": "IOC",
    }
    return payload


def execute_pair_trade(operation: Operation) -> list[dict[str, object]]:
    """Dispara as ordens de compra e venda via MT5 Bridge e retorna o resultado."""
    logger.info("MT5: iniciando execute_pair_trade para operação %s", operation.pk)

    try:
        sell_payload = _build_trade_payload(operation, "sell")
        buy_payload = _build_trade_payload(operation, "buy")
        trades = [sell_payload, buy_payload]
        logger.debug("MT5: payloads montados para operação %s: %s", operation.pk, trades)
    except ValueError as exc:
        logger.error("MT5: erro ao montar payload da operação %s: %s", operation.pk, exc)
        raise MT5TradeExecutionError(str(exc)) from exc

    try:
        logger.info("MT5: enviando trades para o bridge (operação %s)", operation.pk)
        if getattr(settings, "MT5_DRY_RUN", False):
            logger.info("MT5: dry run habilitado – não enviando trades para operação %s", operation.pk)
            logger.info("MT5: resultado simulado para operação %s: %s", operation.pk, trades)
            return [{"symbol": trade["symbol"], "ticket": 0, "retcode": 0, "price": trade["price"], "volume": trade["lots"], "comment": "dry-run"} for trade in trades]
        logger.info("MT5: payload final prontos para envio (symbol/lots/quantity): %s", [
            { "symbol": trade["symbol"], "lots": trade["lots"], "quantity": trade["quantity"] }
            for trade in trades
        ])
        result = execute_trades(trades)
        logger.info("MT5: resposta do bridge para operação %s: %s", operation.pk, result)
        return result
    except MT5BridgeError as exc:
        logger.error("Falha na execução das ordens MT5 para a operação %s: %s", operation.pk, exc)
        raise MT5TradeExecutionError(str(exc)) from exc
