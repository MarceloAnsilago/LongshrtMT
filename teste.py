import MetaTrader5 as mt5

mt5.initialize()

symbol = "AXIA3"
tick = mt5.symbol_info_tick(symbol)

for vol in (1.0, 10.0, 100.0):
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": vol,
        "type": mt5.ORDER_TYPE_BUY,
        "price": tick.ask,
        "deviation": 20,
        "magic": 123456,
        "comment": f"teste_vol_{vol}",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    res = mt5.order_send(request)
    print("VOL", vol, "→ retcode:", res.retcode, "comment:", res.comment)
