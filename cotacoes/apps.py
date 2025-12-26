from django.apps import AppConfig

from longshort.services.mt5_session import ensure_mt5_initialized


class CotacoesConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'cotacoes'

    def ready(self):
        ensure_mt5_initialized()
