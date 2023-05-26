import asyncio
from exchanges import binance_us
import json
from data_handling import order_message_handler, apply_balance_updates, orderbook_update_handler


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
        self.exchange_list = exchange_list
        self.orderbook_update_queue = asyncio.Queue()
        self.order_message_queue = asyncio.Queue()
        self.balance_message_queue = asyncio.Queue()
        self.ob_stream_active = False
        self.userdata_stream_active = False

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

    async def initialize_ob(self):
        for exchange in self.exchange_list:
            await exchange.init_order_books(ob_queue=self.orderbook_update_queue)

    async def get_orderbooks(self, exchange):
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

    async def get_open_orders(self):
        while not self.userdata_stream_active:
            self.userdata_stream_active = True
            while True:
                msg = await self.order_message_queue.get()
                self.open_orders, self.open_positions = order_message_handler(self.open_orders, self.open_positions,
                                                                              msg)
                print(self.open_orders)
                print(self.open_positions)

    async def get_balance(self):
        while not self.userdata_stream_active:
            self.userdata_stream_active = True
            while True:
                msg = await self.balance_message_queue.get()
                exchange = msg['exchange']
                self.balances[exchange] = apply_balance_updates(self.balances[exchange], msg)

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

    async def ws_datafeed(self):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.get_open_orders())
            tg.create_task(self.get_balance())
            for exchange in self.exchange_list:
                tg.create_task(exchange.marketdata_ws(ob_queue=self.orderbook_update_queue))
                tg.create_task(exchange.userdata_ws(order_queue=self.order_message_queue,
                                                    balance_queue=self.balance_message_queue))
                tg.create_task(self.get_orderbooks(exchange))
                #tg.create_task(self.get_single_orderbook(exchange, 'BTCUSD'))
                #tg.create_task(self.get_positions())

                # await asyncio.gather(asyncio.ensure_future(exchange.marketdata_ws(ob_queue=self.orderbook_update_queue)),
                #                      asyncio.ensure_future(exchange.userdata_ws(order_queue=self.order_message_queue,
                #                                                                 balance_queue=self.balance_message_queue)),
                #                      asyncio.ensure_future(self.get_orderbooks(exchange)),
                #                      asyncio.ensure_future(self.get_open_orders()),
                #                      asyncio.ensure_future(self.get_balance()),
                #                      self.get_single_orderbook(exchange, 'BTCUSD')
                #                      )


async def main():
    master = MasterDatafeed(exchange_list=[binance_us(symbols=['BTCUSD'])], symbols=['BTCUSD'])
    await master.initialize_datafeeds()
    await master.initialize_ob()
    await master.ws_datafeed()


asyncio.run(main())
