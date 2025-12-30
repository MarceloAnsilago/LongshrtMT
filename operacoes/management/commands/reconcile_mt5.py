from __future__ import annotations

from django.core.management.base import BaseCommand

from operacoes.services.reconcile import MT5ReconciliationError, reconcile_mt5_positions


class Command(BaseCommand):
    help = "Reconcilia as ordens marcadas como abertas no banco com as posições abertas no MT5."

    def handle(self, *args, **options):
        print("Iniciando reconciliação entre MT5 e banco de dados...")

        try:
            reconciled = reconcile_mt5_positions()
        except MT5ReconciliationError as exc:
            print(f"Erro durante a reconciliação: {exc}")
            return

        if not reconciled:
            print("Nenhuma ordem precisou ser ajustada.")
        else:
            for trade in reconciled:
                print(
                    f"Trade {trade.ticket} ({trade.symbol}) marcado como 'encerrado_manual'."
                )

        print("Reconciliação concluída.")
