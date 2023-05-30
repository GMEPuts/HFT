import asyncio
from exchanges import binance_us
import json
from data_handling import order_message_handler, apply_balance_updates, orderbook_update_handler
import numpy as np
import fastparquet as fp
import pandas as pd


# use uv loop

class MasterDatafeed:

    def __init__(self,
                 exchange_list,
                 symbols
                 ):
        self.balances = {}
        self.open_orders = []
        self.open_positions = []
        self.symbols = symbols
        self.order_books = {}
        self.BBA = {}
        self.exchange_list = exchange_list
        self.order_books_active = {}
        self.orderbook_update_queue = asyncio.Queue()
        self.order_message_queue = asyncio.Queue()
        self.balance_message_queue = asyncio.Queue()
        self.trade_message_queue = asyncio.Queue()
        self.ob_stream_active = False
        self.userdata_stream_active = False
        self.equity_tracking = False

    async def initialize_queues(self):
        for exchange in self.exchange_list:
            for symbol in self.symbols:
                ob_id = exchange.name + '|' + symbol
                self.order_books[ob_id] = {}

    async def initialize_datafeeds(self):
        for exchange in self.exchange_list:
            await exchange.get_listen_key()
            await exchange.userdata_snapshot(balance_queue=self.balance_message_queue)
            await self.initialize_queues()

    async def get_orderbooks(self, exchange):

        for symbol in exchange.symbols:
            self.order_books_active[exchange.name + '|' + symbol] = False

        while not self.ob_stream_active:
            self.ob_stream_active = True
            while True:
                msg = await self.orderbook_update_queue.get()
                ob_id = msg['exchange'] + '|' + msg['symbol']

                if msg['messageType'] == 'orderbook_snapshot':
                    self.order_books[ob_id] = msg
                else:
                    self.order_books[ob_id] = await orderbook_update_handler(exchange, msg['symbol'],
                                                                             self.order_books[ob_id], msg)
                    if self.order_books[ob_id]['messageType'] == 'live_orderbook' \
                            and not self.order_books_active[ob_id]:
                        print(f'{ob_id} order book active')
                        self.order_books_active[ob_id] = True
                    else:
                        continue

    async def get_open_orders(self):
        while True:
            msg = await self.order_message_queue.get()

            self.open_orders, self.open_positions = order_message_handler(self.open_orders, self.open_positions,
                                                                          msg)

    async def get_balance(self, exchange):
        self.balances[exchange.name] = {}
        while True:
            msg = await self.balance_message_queue.get()

            self.balances[exchange.name] = apply_balance_updates(self.balances[exchange.name], msg)

            self.equity_tracking = True
            # lastTrade = self.open_positions[-1]
            # ts = lastTrade['transactTime']

    async def get_single_orderbook(self, exchange, symbol):
        ob_id = exchange.name + '|' + symbol
        while self.ob_stream_active:
            orderbook = self.order_books[ob_id]
            print(orderbook)
            await asyncio.sleep(1)

    async def get_positions(self):
        while self.open_positions:
            print(self.open_positions)
            await asyncio.sleep(5)

    async def track_bba(self, freq, exchange, symbol, df_size):
        msg_num = 0
        file_num = 0
        ob_id = exchange.name + '|' + symbol

        while True:
            try:
                ts = self.order_books[ob_id]['timestamp']
                best_bid = float(self.order_books[ob_id]['message']['bids'][0][0])
                best_ask = float(self.order_books[ob_id]['message']['asks'][0][0])
                mid = (best_bid + best_ask) / 2

                if msg_num == 0:
                    self.BBA[ob_id] = {'timestamp': [ts], 'best_bid': [best_bid], 'best_ask': [best_ask],
                                       'midprice': [mid]}

                if 0 < msg_num < df_size:
                    self.BBA[ob_id]['timestamp'].append(ts)
                    self.BBA[ob_id]['best_bid'].append(best_bid)
                    self.BBA[ob_id]['best_ask'].append(best_ask)
                    self.BBA[ob_id]['midprice'].append(mid)

                if msg_num >= df_size:
                    # pop first row, append msg
                    self.BBA[ob_id]['timestamp'].append(ts)
                    self.BBA[ob_id]['best_bid'].append(best_bid)
                    self.BBA[ob_id]['best_ask'].append(best_ask)
                    self.BBA[ob_id]['midprice'].append(mid)

                    self.BBA[ob_id]['timestamp'].pop(0)
                    self.BBA[ob_id]['best_bid'].pop(0)
                    self.BBA[ob_id]['best_ask'].pop(0)
                    self.BBA[ob_id]['midprice'].pop(0)

                    # store table to DB every N rows
                    if msg_num % df_size == 0:
                        df = pd.DataFrame(self.BBA[ob_id])
                        df.to_parquet(f'C:/Users/Rob Cheresh/Desktop/Algo Trading/'
                                      f'Datasets/HFT/BBA/BinanceUS/{symbol}{file_num}.parquet', engine='fastparquet')
                        file_num += 1
                msg_num += 1
                await asyncio.sleep(freq)
            except KeyError:
                await asyncio.sleep(1)
                continue

    async def track_equity(self, exchange):

        equity_value = []
        assets_tracking = []
        while self.equity_tracking:
            try:
                current_balances = self.balances[exchange.name]

                for bal in current_balances['balances']:
                    if float(bal[1]) == 0:
                        continue

                    if float(bal[1]) > 0 and bal[0] != 'USD':
                        symbol = bal[0] + 'USD'
                        quantity = float(bal[1])

                        if symbol not in assets_tracking:
                            update_time = current_balances['eventTime']
                            bba_ts = self.BBA[exchange.name + '|' + symbol]['timestamp']
                            bba_ts.reverse()

                            for i, ts in enumerate(bba_ts):

                                if update_time > ts:
                                    continue

                                if update_time <= ts:
                                    print(f'found {i - 1}')
                                    assets_tracking.append(symbol)
                                    timestamps = self.BBA[exchange.name + '|' + symbol]['timestamp'][-i + 1:-1]
                                    equity_value = list(
                                        np.dot(self.BBA[exchange.name + '|' + symbol]['midprice'][-i + 1:-1],
                                               quantity))
                                    print(equity_value)
                        else:
                            equity_value.append(self.BBA[exchange.name + '|' + symbol]['midprice'][-1] * quantity)
                            print(equity_value)
            except KeyError:
                await asyncio.sleep(1)
                continue
            await asyncio.sleep(.1)

    async def ws_datafeed(self):
        asyncio.ensure_future(self.get_open_orders())
        async with asyncio.TaskGroup() as tg:
            for exchange in self.exchange_list:
                #tg.create_task(exchange.marketdata_ws(ob_queue=self.orderbook_update_queue))
                tg.create_task(exchange.userdata_ws(order_queue=self.order_message_queue,
                                                    balance_queue=self.balance_message_queue))
                
                tg.create_task(exchange.connect_websocket(ws=exchange.depth_ws,
                                                         url=exchange.url+exchange.restDepth_endpoint,
                                                         on_message=exchange.on_depth_message,
                                                         queue=self.orderbook_update_queue,
                                                         payload=json.dumps(exchange.depth_payload))
                               
                 tg.create_task(exchange.connect_websocket(ws=exchange.trade_ws,
                                                         url=exchange.url+exchange.trade_endpoint,
                                                         on_message=exchange.on_trade_message,
                                                         queue=self.trade_message_queue,
                                                         payload=json.dumps(exchange.trade_payload))
                
                
                tg.create_task(self.get_orderbooks(exchange))
                tg.create_task(self.get_balance(exchange))
                tg.create_task(self.track_bba(.1, exchange, 'BTCUSD', 1000))
                tg.create_task(self.track_equity(exchange))


async def main():
    master = MasterDatafeed(exchange_list=[binance_us(symbols=['BTCUSD', 'ETHUSD', 'ADAUSD'])],
                            symbols=['BTCUSD', 'ETHUSD', 'ADAUSD'])
    await master.initialize_datafeeds()
    await master.ws_datafeed()


asyncio.run(main())
