import websockets
import json
import asyncio
import aiohttp
from typing import Deque, Dict, List, Optional, Tuple
from datatypes import position_msg
import time


class ExchangeDataSource:

    def __init__(self,
                 symbols,
                 name,
                 api_key,
                 api_secret,
                 ws_url,
                 ws_url_private,
                 rest_url,
                 rest_depth_endpoint,
                 rest_userData_endpoint,
                 trade_payload,
                 depth_payload,
                 modify_snapshot_msg,
                 ob_message,
                 ob_update,
                 order_update_msg,
                 account_update_msg,
                 channel_key_column,
                 event_key_column,
                 channel_keys,
                 event_keys,
                 snapshot_in_ws,
                 modify_update_msg,
                 ping_msg,
                 auth_msg,
                 account_snapshot_msg,
                 user_snapshot_params,
                 account_snapshot_endpoint,
                 user_ws_method
                 ):

        self.trade_payload = trade_payload
        self.depth_payload = depth_payload
        self.snapshot_in_ws = snapshot_in_ws
        self.account_snapshot_msg = account_snapshot_msg
        self.user_snapshot_params = user_snapshot_params
        self.channel_key_column = channel_key_column
        self.event_key_column = event_key_column
        self.channel_keys = channel_keys
        self.modify_update_msg = modify_update_msg
        self.modify_snapshot_msg = modify_snapshot_msg
        self.event_keys = event_keys
        self.account_snapshot_endpoint = account_snapshot_endpoint
        self.ob_message = ob_message
        self.ob_update = ob_update
        self.user_ws_method = user_ws_method
        self.ws_session = None
        self.ping_msg = ping_msg
        self.ws_url = ws_url
        self.auth_msg = auth_msg
        self.account_balances = {}
        self.order_update_msg = order_update_msg
        self.account_update_msg = account_update_msg
        self.ws_url_private = ws_url_private
        self.rest_url = rest_url
        self.rest_depth_endpoint = rest_depth_endpoint
        self.rest_userData_endpoint = rest_userData_endpoint
        self.symbols = symbols
        self.depth_queue = asyncio.Queue()
        self.trade_queue = asyncio.Queue()
        self.balance_queue = asyncio.Queue()
        self.name = name
        self.is_on = False
        self.api_key = api_key
        self.api_secret = api_secret
        self.maker_fee = None
        self.taker_fee = None
        self.userListenKey = None
        self.connection = websockets.connect(uri=self.ws_url)
        self.private_connection = websockets.connect(uri=self.ws_url_private)
        self.ws_active = False
        self.user_ws_active = False
        self._tracking_message_queues: Dict[str, asyncio.Queue] = {}
        self._order_books_initialized = False
        self._order_books: Dict[str, dict] = {}
        self.open_orders = []
        self.open_positions = []
        self.depth_params = []
        self.trade_params = []
        self.symbols_active = {}
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

    async def marketdata_ws(self, ob_queue: asyncio.Queue):

        last_message_timestamp: float = time.time()
        messages_queued: int = 0
        messages_accepted: int = 0
        messages_rejected: int = 0

        for symbol in self.symbols:
            self.symbols_active[symbol] = False

        while not self.ws_active:
            self.ws_active = True
            async with self.connection as ws:

                await ws.send(json.dumps(self.trade_payload))
                print(f'Connected to {self.name} trade data stream')
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
                            continue
                        # OB updates
                        if msg[self.channel_key_column] == self.channel_keys['depth'] and self.snapshot_in_ws:
                            if msg[self.event_key_column] == self.event_keys['snapshot']:
                                snapshot_msg = self.process_ob_snapshot(msg)
                                await ob_queue.put({
                                    'messageType': 'orderbook_snapshot',
                                    'message': snapshot_msg,
                                    'timestamp': time.time(),
                                    'symbol': snapshot_msg['symbol'].upper(),
                                    'exchange': self.name
                                })
                                self.symbols_active[snapshot_msg['symbol'].upper()] = True

                        if msg[self.channel_key_column] == self.channel_keys['depth'] \
                                and (msg[self.event_key_column] == self.event_keys['update']
                                     or self.event_keys['update'] == 'None'):

                            depth_msg = self.modify_update_msg(msg)

                            # if no snapshot yet, use snapshot first
                            if not self.symbols_active[depth_msg['symbol'].upper()]:
                                snapshot_msg = await self.orderbook_snapshot(depth_msg['symbol'].upper())
                                await ob_queue.put(snapshot_msg)
                                self.symbols_active[depth_msg['symbol'].upper()] = True
                                continue

                            if self.symbols_active[depth_msg['symbol'].upper()]:
                                await ob_queue.put({
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
                    # except asyncio.CancelledError:
                    # print(f'{self.name} Cancelled error')
                    except Exception as e:
                        print(f'|{self.name}| Error: {e}, retrying in 1 second')
                        await asyncio.sleep(1)
                        self.ws_active = False
                        break

    async def userdata_snapshot(self, balance_queue: asyncio.Queue):
        params, headers = self.user_snapshot_params(time.time(), self.api_key, self.api_secret)
        async with aiohttp.ClientSession() as session:
            async with session.get(self.rest_url + self.account_snapshot_endpoint, params=params,
                                   headers=headers) as resp:
                msg = await resp.json()
                account_snapshot = self.account_snapshot_msg(msg)

                balances = []
                for x in account_snapshot.get('balances'):
                    #      asset       free balance       locked balance
                    b = [x.get('asset'), float(x.get('free')), float(x.get('locked'))]
                    balances.append(b)

                snapshot_msg = {'msgType': 'snapshot',
                                'eventTime': time.time(),
                                'balances': balances
                                }
                await balance_queue.put(snapshot_msg)
                self.maker_fee = float(account_snapshot.get('makerFee'))
                self.taker_fee = float(account_snapshot.get('takerFee'))

    async def on_userdata_message(self, balance_queue, order_queue):
        async for msg in self.ws_session:
            if msg.type == aiohttp.WSMsgType.text:

                msg = msg.json()

                if msg['e'] == "outboundAccountPosition":
                    account_msg = self.account_update_msg(msg)
                    account_msg2 = {'msgType': 'update',
                                    'eventTime': time.time(),
                                    'balances': account_msg['balances']
                                    }
                    await balance_queue.put(account_msg2)

                if msg['e'] == "executionReport":
                    order_msg = self.order_update_msg(msg)
                    await order_queue.put(order_msg)

            elif msg.type == aiohttp.WSMsgType.closed:
                self.user_ws_active = False
                break

    async def get_listen_key(self):
        headers = {'X-MBX-APIKEY': self.api_key}
        async with aiohttp.ClientSession() as session:
            async with session.post(self.rest_url + self.rest_userData_endpoint, headers=headers) as resp:
                key = await resp.json()
                self.userListenKey = key.get('listenKey')

    async def userdata_ws(self, balance_queue: asyncio.Queue, order_queue: asyncio.Queue):
        if self.user_ws_method == 'listenkey' and self.userListenKey is not None:
            while not self.user_ws_active:
                self.user_ws_active = True
                async with aiohttp.ClientSession() as session:
                    url = self.ws_url_private + self.userListenKey
                    self.ws_session = await session.ws_connect(url)
                    print(f'Connected to {self.name} user data stream')
                    await self.on_userdata_message(balance_queue, order_queue)

    async def authenticate_ws(self):

        while not self.user_ws_active:
            self.user_ws_active = True

            async with self.private_connection as ws:
                print('Trying to connect to user data stream')
                await ws.send(json.dumps(self.auth_msg))
                print(f'Connected to {self.name} user data stream')

                while True:
                    msg = await ws.recv()
                    print(msg)
