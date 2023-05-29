from marketdata import ExchangeDataSource
from datatypes import snapshot_message, orderbook_update, account_update_msg, \
    order_update_msg, account_snapshot_msg, position_msg
import hmac
import time
import base64
import hashlib
import urllib.parse
import urllib.request
import urllib
import json


def binance_us(symbols):
    def get_ob_update(msg):
        update_msg = orderbook_update(msg.get('s'), msg.get('E'),
                                      msg.get('U'), msg.get('u'),
                                      msg.get('b'), msg.get('a'))
        return update_msg

    def get_ob_snapshot(msg):
        snapshot_msg = snapshot_message(msg.get('s'), msg.get('lastUpdateId'),
                                        msg.get('bids'), msg.get('asks'))
        return snapshot_msg

    def get_acct_snapshot(msg):
        snapshot_msg = account_snapshot_msg(msg.get('updateTime'), msg.get('commissionRates').get('maker'),
                                            msg.get('commissionRates').get('taker'), msg.get('balances'))
        return snapshot_msg

    def get_order_update(msg):
        order_msg = order_update_msg('BinanceUS', msg.get('s'), msg.get('E'), msg.get('S'), msg.get('q'), msg.get('p'),
                                     msg.get('z'), msg.get('Z'), msg.get('c'), msg.get('T'), msg.get('x'), msg.get('o'),
                                     msg.get('f'), msg.get('C'), msg.get('r'))
        return order_msg

    def get_account_update(msg):
        account_msg = account_update_msg(msg.get('E'), msg.get('B'))
        return account_msg

    def create_signature(data, secret):
        postdata = urllib.parse.urlencode(data)
        message = postdata.encode()
        byte_key = bytes(secret, 'UTF-8')
        mac = hmac.new(byte_key, message, hashlib.sha256).hexdigest()
        return mac

    def user_data_params(timestamp, api_key, api_secret):
        data = {
            "timestamp": int(round(timestamp * 1000)),
        }
        headers = {'X-MBX-APIKEY': api_key}
        signature = create_signature(data, api_secret)
        params = {
            **data,
            "signature": signature,
        }
        return params, headers

    exchange = ExchangeDataSource(ws_url='wss://stream.binance.us:9443/ws',
                                  ws_url_private='wss://stream.binance.us:9443/ws/',
                                  rest_url='https://api.binance.us/api/v3/',
                                  rest_depth_endpoint=f'depth?symbol={{}}',
                                  rest_userData_endpoint='userDataStream',
                                  account_snapshot_endpoint='account',
                                  api_key='',
                                  api_secret='',
                                  user_snapshot_params=user_data_params,
                                  order_update_msg=get_order_update,
                                  account_update_msg=get_account_update,
                                  account_snapshot_msg=get_acct_snapshot,
                                  user_ws_method='listenkey',
                                  auth_msg=None,
                                  trade_payload={
                                      "method": "SUBSCRIBE",
                                      "params": [],
                                      "id": 1
                                  },
                                  depth_payload={
                                      "method": "SUBSCRIBE",
                                      "params": [],
                                      "id": 2
                                  },
                                  ob_message=['lastUpdateId', 'bids', 'asks'],
                                  ob_update=['s', 'E', 'U', 'u', 'b', 'a'],
                                  modify_snapshot_msg=get_ob_snapshot,
                                  modify_update_msg=get_ob_update,

                                  channel_key_column='e',
                                  event_key_column='s',
                                  channel_keys={'trade': 'trade',
                                                'depth': 'depthUpdate'
                                                },
                                  event_keys={
                                      'update': 'None',
                                      'trades': 'None',
                                      'snapshot': 'None'
                                  },
                                  # event type, event time, first update, last
                                  # update, bids, asks
                                  snapshot_in_ws=False,
                                  ping_msg=None,
                                  symbols=symbols,
                                  name='BinanceUS')
    trade_params = []
    depth_params = []
    for symbol in exchange.symbols:
        trade_params.append(f"{symbol.lower()}@trade")
        depth_params.append(f"{symbol.lower()}@depth@100ms")
    exchange.trade_payload['params'] = trade_params
    exchange.depth_payload['params'] = depth_params
    return exchange


def gemini():
    def get_ob_update(msg):
        update_msg = orderbook_update(msg.get('s'), msg.get('E'),
                                      msg.get('U'), msg.get('u'),
                                      msg.get('b'), msg.get('a'))
        return update_msg

    def get_ob_snapshot(msg):
        snapshot_msg = snapshot_message(msg.get('s'), msg.get('lastUpdateId'),
                                        msg.get('bids'), msg.get('asks'))
        return snapshot_msg

    exchange = ExchangeDataSource(ws_url='wss://api.gemini.com/v2/marketdata',
                                  ws_url_private='ss://stream.binance.us:9443/ws',
                                  rest_url='https://api.binance.us/api/v3/',
                                  rest_depth_endpoint=f'depth?symbol={{}}',
                                  rest_userData_endpoint='userDataStream',
                                  api_key='',
                                  api_secret='',
                                  trade_payload={
                                      "method": "SUBSCRIBE",
                                      "params": [],
                                      "id": 1
                                  },
                                  depth_payload={
                                      "method": "SUBSCRIBE",
                                      "params": [],
                                      "id": 2
                                  },
                                  ob_message=['lastUpdateId', 'bids', 'asks'],
                                  ob_update=['s', 'E', 'U', 'u', 'b', 'a'],
                                  modify_snapshot_msg=get_ob_snapshot,
                                  modify_update_msg=get_ob_update,

                                  channel_key_column='e',
                                  event_key_column='s',
                                  channel_keys={'trade': 'trade',
                                                'depth': 'depthUpdate'
                                                },
                                  event_keys={
                                      'update': 'None',
                                      'trades': 'None',
                                      'snapshot': 'None'
                                  },
                                  # event type, event time, first update, last
                                  # update, bids, asks
                                  snapshot_in_ws=False,
                                  ping_msg=None,
                                  symbols=['BTCUSD'],
                                  name='BinanceUS')
    trade_params = []
    depth_params = []
    for symbol in exchange.symbols:
        trade_params.append(f"{symbol.lower()}@trade")
        depth_params.append(f"{symbol.lower()}@depth@100ms")
    exchange.trade_payload['params'] = trade_params
    exchange.depth_payload['params'] = depth_params
    return exchange


'''
def init_poloniex():
    def get_ob_update(msg):
        update_msg = orderbook_update(msg.get('data')[0].get('symbol'), msg.get('data')[0].get('ts'),
                                      msg.get('data')[0].get('lastId'), msg.get('data')[0].get('id'),
                                      msg.get('data')[0].get('bids'), msg.get('data')[0].get('asks'))
        return update_msg

    def get_ob_snapshot(msg):
        snapshot_msg = snapshot_message(msg.get('data')[0].get('symbol'), msg.get('data')[0].get('id'),
                                        msg.get('data')[0].get('bids'),
                                        msg.get('data')[0].get('asks'))
        return snapshot_msg

    def create_signature(method, path, params, api_secret):
        timestamp = time.time()
        if method.upper() == "GET" and params is not None:
            params.update({"signTimestamp": timestamp})
            sorted_params = sorted(params.items(), key=lambda d: d[0], reverse=False)
            encode_params = urllib.parse.urlencode(sorted_params)
            del params["signTimestamp"]
        if params is None:
            encode_params = "signTimestamp=".format(timestamp)
        else:
            requestBody = json.dumps(params)
            encode_params = "requestBody={}&signTimestamp={}".format(
                requestBody, timestamp
            )
        sign_params_first = [method.upper(), path, encode_params]
        sign_params_second = "\n".join(sign_params_first)
        sign_params = sign_params_second.encode(encoding="UTF8")
        secret_key = api_secret.encode(encoding="UTF8")
        digest = hmac.new(secret_key, sign_params, digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest)
        signature = signature.decode()
        return signature, timestamp

    def auth_msg(api_key, api_secret):
        signature, timestamp = create_signature(method='GET', path='/ws', params=None, api_secret=api_secret)
        msg = {
            "event": "subscribe",
            "channel": ["auth"],
            "params": {
                "key": api_key,
                "signTimestamp": timestamp,
                "signature": signature
            }
        }
        return msg

    exchange = ExchangeDataSource(ws_url='wss://ws.poloniex.com/ws/public',
                                  ws_url_private='wss://ws.poloniex.com/ws/private',
                                  rest_url='https://api.poloniex.com/markets/',
                                  rest_depth_endpoint=f'{{}}/orderBook?scale={{}}&limit={{}}',
                                  rest_userData_endpoint=None,
                                  api_key='',
                                  api_secret='',
                                  auth_msg=None,
                                  depth_payload={
                                      "event": "subscribe",
                                      "channel": ['book_lv2'],
                                      "symbols": []
                                  },  # provides snapshots and depth updates
                                  trade_payload={
                                      "event": "subscribe",
                                      "channel": ['trades'],
                                      "symbols": []
                                  },
                                  ob_message=['symbol', 'id', 'bids', 'asks'],
                                  ob_update=['symbol', 'ts', 'lastId', 'id', 'bids', 'asks'],
                                  modify_update_msg=get_ob_update,
                                  modify_snapshot_msg=get_ob_snapshot,

                                  channel_key_column='channel',
                                  event_key_column='action',
                                  channel_keys={'trade': 'trades',
                                                'depth': 'book_lv2'
                                                },
                                  event_keys={'update': 'update',
                                              'trades': 'trades',
                                              'snapshot': 'snapshot'
                                              },
                                  # event type, event time, first update, last
                                  # update, bids, asks
                                  snapshot_in_ws=True,
                                  ping_msg={
                                      "event": "ping"
                                  },
                                  symbols=['btc_usdt'],
                                  name='Poloniex'
                                  )

    exchange.auth_msg = auth_msg(exchange.api_key, exchange.api_secret)

    for symbol in exchange.symbols:
        exchange.trade_payload['symbols'].append(symbol.lower())
        exchange.depth_payload['symbols'].append(symbol.lower())
    return exchange'''
