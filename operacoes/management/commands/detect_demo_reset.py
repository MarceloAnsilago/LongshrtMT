from __future__ import annotations

import uuid

from django.core.management.base import BaseCommand, CommandError

from mt5_bridge_client.mt5client import MT5BridgeError
from operacoes.services.mt5_reset import detect_demo_reset_for_open_trades


class Command(BaseCommand):
    help = "Detecta resets demo reportados pelo MT5 e sincroniza o estado com o banco."

    def add_arguments(self, parser):
        parser.add_argument(
            "--request-id",
            dest="request_id",
            help="ID para correlacionar logs e auditoria (opcional).",
        )

    def handle(self, *args, **options):
        request_id = options.get("request_id") or str(uuid.uuid4())
        try:
            events = detect_demo_reset_for_open_trades(request_id=request_id)
        except MT5BridgeError as exc:
            raise CommandError(f"Erro ao consultar a ponte MT5: {exc}")

        if not events:
            self.stdout.write("Nenhum reset demo detectado.")
            return

        for event in events:
            trade_id = event.trade_id or "?"
            ticket = event.ticket or "?"
            self.stdout.write(
                f"[{event.detected_at.isoformat()}] Operação {trade_id} "
                f"(ticket={ticket}) marcada como reset demo."
            )
