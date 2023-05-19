from marketdata import ExchangeDataSource, snapshot_message, orderbook_update


def init_binance_us():
    def get_ob_update(msg):
        update_msg = orderbook_update(msg.get('s'), msg.get('E'),
                                      msg.get('U'), msg.get('u'),
                                      msg.get('b'), msg.get('a'))
        return update_msg

    def get_ob_snapshot(msg):
        snapshot_msg = snapshot_message(msg.get('s'), msg.get('lastUpdateId'),
                                        msg.get('bids'), msg.get('asks'))
        return snapshot_msg

    exchange = ExchangeDataSource(ws_url='wss://stream.binance.us:9443/ws',
                                  rest_url='https://api.binance.us/api/v3/',
                                  rest_depth_endpoint=f'depth?symbol={{}}',
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

    exchange = ExchangeDataSource(ws_url='wss://ws.poloniex.com/ws/public',
                                  rest_url='https://api.poloniex.com/markets/',
                                  rest_depth_endpoint=f'{{}}/orderBook?scale={{}}&limit={{}}',
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
    for symbol in exchange.symbols:
        exchange.trade_payload['symbols'].append(symbol.lower())
        exchange.depth_payload['symbols'].append(symbol.lower())
    return exchange
