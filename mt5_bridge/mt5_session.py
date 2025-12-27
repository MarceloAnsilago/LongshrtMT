import atexit
import logging
import MetaTrader5 as mt5

logger = logging.getLogger(__name__)

_initialized = False


def init_mt5():
    """
    Inicializa a sessão com o MetaTrader 5 apenas uma vez.
    """
    global _initialized

    if _initialized:
        return

    if not mt5.initialize():
        code, msg = mt5.last_error()
        raise RuntimeError(f"Erro ao inicializar MT5: {code} - {msg}")

    _initialized = True
    logger.info("MT5 inicializado com sucesso.")


def shutdown_mt5():
    """
    Encerra a sessão MT5 com segurança na saída do processo.
    """
    global _initialized

    if not _initialized:
        return

    mt5.shutdown()
    _initialized = False
    logger.info("Sessão MT5 encerrada.")


# garante que ao fechar o processo o MT5 seja desligado
atexit.register(shutdown_mt5)
