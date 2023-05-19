import websockets
import json
import asyncio
import aiohttp
from typing import Deque, Dict, List, Optional, Tuple
import time


def snapshot_message(symbol=None, last_update_id=None, bids=None, asks=None):
    msg = {'symbol': symbol,
           'lastUpdateId': last_update_id,
           'bids': bids,
           'asks': asks,
           }
    return msg


def orderbook_message(last_update_id=None, bids=None, asks=None):
    msg = {'lastUpdateId': last_update_id,
           'bids': bids,
           'asks': asks,
           }
    return msg


def orderbook_update(symbol=None, event_time=None,
                     first_update_id=None, last_update_id=None, bids=None, asks=None):
    msg = {'symbol': symbol,
           'eventTime': event_time,
           'firstUpdateId': first_update_id,
           'lastUpdateId': last_update_id,
           'bids': bids,
           'asks': asks
           }
    return msg


def apply_diffs(ob, update_msg):
    """
    Updates local order book's bid or ask lists based on the received update ([price, quantity])
    """

    order_book = {
        'messageType': 'live_orderbook',
        'message': {'lastUpdateId': update_msg['message']['lastUpdateId'],
                    'bids': ob['message']['bids'],
                    'asks': ob['message']['asks']
                    },
        'timestamp': time.time(),
        'symbol': ob['symbol'].upper(),
        'exchange': ob['exchange']
    }
    sides = ['bids', 'asks']

    for side in sides:
        for update in update_msg['message'][side]:
            price, quantity = update
            found = False
            # price exists: remove or update local order
            for i in range(0, len(order_book['message'][side])):

                if price == order_book['message'][side][i][0]:
                    # quantity is 0: remove
                    if float(quantity) == 0:
                        order_book['message'][side].pop(i)
                        found = True
                        break
                    else:
                        # quantity is not 0: update the order with new quantity
                        order_book['message'][side][i] = update
                        found = True
                        break

                # price not found: add new order
            if not found and float(quantity) != 0:
                order_book['message'][side].append(update)
                if side == 'asks':
                    order_book['message'][side] = sorted(order_book['message'][side])  # asks prices in ascendant order
                else:
                    order_book['message'][side] = sorted(order_book['message'][side],
                                                         reverse=True)  # bids prices in descendant order
                # maintain side depth <= 1000
                if len(order_book['message'][side]) > 1000:
                    order_book['message'][side].pop(len(order_book['message'][side]) - 1)

    return order_book


class ExchangeDataSource:

    def __init__(self,
                 symbols,
                 name,
                 ws_url,
                 rest_url,
                 rest_depth_endpoint,
                 trade_payload,
                 depth_payload,
                 modify_snapshot_msg,
                 ob_message,
                 ob_update,
                 channel_key_column,
                 event_key_column,
                 channel_keys,
                 event_keys,
                 snapshot_in_ws,
                 modify_update_msg,
                 ping_msg
                 ):

        self.trade_payload = trade_payload
        self.depth_payload = depth_payload
        self.snapshot_in_ws = snapshot_in_ws
        self.channel_key_column = channel_key_column
        self.event_key_column = event_key_column
        self.channel_keys = channel_keys
        self.modify_update_msg = modify_update_msg
        self.modify_snapshot_msg = modify_snapshot_msg
        self.event_keys = event_keys
        self.ob_message = ob_message
        self.ob_update = ob_update
        self.ping_msg = ping_msg
        self.ws_url = ws_url
        self.rest_url = rest_url
        self.rest_depth_endpoint = rest_depth_endpoint
        self.symbols = symbols
        self.depth_queue = asyncio.Queue()
        self.trade_queue = asyncio.Queue()
        self.name = name
        self.connection = websockets.connect(uri=self.ws_url)
        self.ws_active = False
        self._trade_messages_queue_key = "trade"
        self._diff_messages_queue_key = "order_book_diff"
        self._snapshot_messages_queue_key = "order_book_snapshot"
        self._tracking_message_queues: Dict[str, asyncio.Queue] = {}
        self._order_books_initialized = False
        self._active_ws_stream = False
        self._order_books: Dict[str, dict] = {}
        self._order_book_diff_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_trade_stream: asyncio.Queue = asyncio.Queue()
        self.depth_params = []
        self.trade_params = []
        self.separator = '/'

    def process_ob_msg(self, msg):
        update_msg = self.modify_update_msg(msg)
        return update_msg

    def process_ob_snapshot(self, msg):
        update_msg = self.modify_snapshot_msg(msg)
        return update_msg

    async def orderbook_snapshot(self, symbol):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.rest_url + self.rest_depth_endpoint.format(symbol)) as resp:
                snapshot = await resp.json()
                snapshot_timestamp = time.time()
                snapshot_msg = {
                    'messageType': 'orderbook_snapshot',
                    'message': self.process_ob_snapshot(snapshot),
                    'systemTime': snapshot_timestamp,
                    'symbol': symbol.upper(),
                    'exchange': self.name
                }

        return snapshot_msg

    async def init_order_books(self):

        for symbol in self.symbols:
            self._tracking_message_queues[symbol.upper()] = asyncio.Queue()

            if self.snapshot_in_ws:
                self._order_books[symbol.upper()] = {}
                # print('none')
            else:
                self._order_books[symbol.upper()] = await self.orderbook_snapshot(symbol)
        if not self.snapshot_in_ws:
            self._order_books_initialized = True

            print('orderbook initialized')
        await asyncio.sleep(3)

    async def parse_ws_messages(self):

        last_message_timestamp: float = time.time()
        messages_queued: int = 0
        messages_accepted: int = 0
        messages_rejected: int = 0

        while not self.ws_active:
            self.ws_active = True
            async with self.connection as ws:

                print('Trying to connect to trade data stream')
                await ws.send(json.dumps(self.trade_payload))
                print(f'Connected to {self.name} trade data stream')

                print('Trying to connect to depth data stream')
                await ws.send(json.dumps(self.depth_payload))
                print(f'Connected to {self.name} orderbook depth data stream')
                last_ping = time.time()
                while True:
                    try:
                        msg = await ws.recv()
                        msg = json.loads(msg)
                        # print(msg)

                        if msg[self.channel_key_column] == self.channel_keys['trade']:
                            trade_msg = []

                        # OB updates
                        if msg[self.channel_key_column] == self.channel_keys['depth'] and self.snapshot_in_ws:
                            if msg[self.event_key_column] == self.event_keys['snapshot']:
                                snapshot_msg = self.process_ob_snapshot(msg)
                                print(snapshot_msg)
                                self._order_books[snapshot_msg['symbol'].upper()] = {
                                    'messageType': 'orderbook_snapshot',
                                    'message': snapshot_msg,
                                    'timestamp': time.time(),
                                    'symbol': snapshot_msg['symbol'].upper(),
                                    'exchange': self.name
                                }
                                self._order_books_initialized = True

                        if msg[self.channel_key_column] == self.channel_keys['depth'] \
                                and (msg[self.event_key_column] == self.event_keys['update']
                                     or self.event_keys['update'] == 'None'):

                            depth_msg = self.modify_update_msg(msg)
                            # print(depth_msg)

                            # if symbol not in self._tracking_message_queues:
                            # print('not in msg queu')
                            # continue
                            message_queue = self._tracking_message_queues[depth_msg['symbol'].upper()]

                            await message_queue.put({
                                'messageType': 'orderbook_update',
                                'message': depth_msg,
                                'timestamp': time.time(),
                                'symbol': depth_msg['symbol'].upper(),
                                'exchange': self.name
                            })

                            messages_accepted += 1
                            # Log some statistics.
                            now: float = time.time()

                            if now - last_ping > 20 and self.ping_msg is not None:
                                await ws.send(json.dumps(self.ping_msg))
                                last_ping = time.time()

                            if int(now / 60.0) > int(last_message_timestamp / 60.0):
                                print(f"Diff messages processed: {messages_accepted}, "
                                      f"rejected: {messages_rejected}, queued: {messages_queued}")
                                messages_accepted = 0
                                messages_rejected = 0
                                messages_queued = 0
                            last_message_timestamp = now

                    except KeyError:
                        continue
                    except asyncio.CancelledError:
                        print(f'{self.name} Cancelled error')
                        raise
                    except Exception as e:
                        print(f'|{self.name}| Error: {e}, retrying in 1 second')
                        await asyncio.sleep(1)
                        self.ws_active = False
                        break

    async def track_orderbook(self, symbol):

        message_queue = self._tracking_message_queues[symbol.upper()]
        order_book = self._order_books[symbol.upper()]

        last_message_timestamp: float = time.time()
        diff_messages_accepted: int = 0
        messages_rejected = 0
        while self.ws_active:

            try:
                update_msg = await message_queue.get()

                # message handler - update snapshot
                if update_msg['message']['lastUpdateId'] <= self._order_books[symbol.upper()]['message'][
                    'lastUpdateId']:
                    print(f'{self.name} Not an update')
                    continue
                if update_msg['message']['firstUpdateId'] <= self._order_books[symbol.upper()]['message'][
                    'lastUpdateId'] + 1 <= \
                        update_msg['message']['lastUpdateId']:
                    # print('message good')
                    self._order_books[symbol.upper()] = apply_diffs(self._order_books[symbol.upper()], update_msg)
                    # print(self._order_books[symbol.upper()])
                else:
                    if self.snapshot_in_ws:
                        print(print(f'{self.name} Out of sync, re-syncing...'))
                    else:
                        self._order_books[symbol.upper()] = await self.orderbook_snapshot(symbol=symbol.upper())
                        print(f'{self.name} Out of sync, re-syncing...')

            except asyncio.queues.QueueEmpty:
                # await asyncio.sleep(0)
                continue

    async def get_trades(self):
        while True:
            msg = await self.trade_queue.get()
            print(msg)

    async def track_BBA(self, symbol, refresh_rate=1):
        while self.ws_active:
            if not self._order_books_initialized:
                await asyncio.sleep(.1)
                continue
            else:
                # print(self._order_books[symbol.upper()])
                best_bid = float(self._order_books[symbol.upper()]['message']['bids'][0][0])
                best_ask = float(self._order_books[symbol.upper()]['message']['asks'][0][0])
                mid = (best_bid + best_ask) / 2
                spread = (best_ask - best_bid) / best_bid
                msg = {'messageType': 'BBA',
                       'message': {'best bid': best_bid, 'best ask': best_ask, 'midprice': mid, 'Spread %': spread},
                       'timestamp': time.time(),
                       'symbol': symbol.upper(),
                       'exchange': self.name
                       }
                print(msg)
                await asyncio.sleep(refresh_rate)
