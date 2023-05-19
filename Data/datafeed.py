import asyncio
import exchanges


async def main():
    poloniex = exchanges.init_poloniex()
    await poloniex.init_order_books()
    binance_us = exchanges.init_binance_us()
    await binance_us.init_order_books()

    await asyncio.gather(poloniex.parse_ws_messages(),
                         binance_us.parse_ws_messages(),
                         poloniex.track_orderbook(poloniex.symbols[0]),
                         binance_us.track_orderbook(binance_us.symbols[0]),
                         poloniex.track_BBA(poloniex.symbols[0].upper()),
                         binance_us.track_BBA(binance_us.symbols[0]))


asyncio.run(main())
