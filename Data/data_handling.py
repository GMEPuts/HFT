import asyncio
import time
from datatypes import position_msg
from exchanges import binance_us

exchange_key = {
    'BinanceUS': binance_us
}


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


def apply_balance_updates(snapshot, update_msg):
    snapshot = snapshot[update_msg['exchange']]
    update_bals = update_msg['balances']
    snapshot_bals = snapshot['balances']

    for x in update_bals:
        #      asset       free balance       locked balance
        b = [x.get('a'), float(x.get('f')), float(x.get('l'))]
        for i, bal in enumerate(snapshot_bals):
            # match found -> remove old balance -> add new balance
            if bal[0] == b[0]:
                snapshot_bals.pop(i)
                snapshot_bals.append(b)
            else:
                continue
    msg = {'exchange': update_msg['exchange'],
           'eventTime': update_msg['eventTime'],
           'balances': snapshot_bals
           }
    return msg


def order_message_handler(open_orders, open_positions, msg):
    symbol = msg['symbol']
    side = msg['side']
    exchange = msg['exchange']

    quantity = float(msg['quantity'])
    price = float(msg['price'])
    orderId = msg['clientOrderID']
    ts = msg['eventTime']

    # add order to open_orders
    if msg['executionType'] == 'NEW' and msg['orderType'] == 'LIMIT':
        open_orders.append(msg)
        print('NEW LIMIT ORDER')

    # cancellation - remove order from open_orders
    if msg['executionType'] == 'CANCELED':
        for i, order in enumerate(open_orders):
            if order['clientOrderID'] == msg['origClientOrderID']:
                open_orders.pop(i)
                print('CANCELLED LIMIT ORDER')

    # trade - remove order from open_orders, add position to open_positions
    if msg['executionType'] == 'TRADE':
        if msg['orderType'] == 'LIMIT':
            for i, order in enumerate(open_orders):
                if order['clientOrderID'] == msg['origClientOrderID']:
                    open_orders.pop(i)
                    open_positions.append(position_msg(msg, exchange.name))
                    print(f'| TRADE EXECUTED | {exchange} | {symbol} | {side} | LIMIT_ORDER |'
                          f' quantity: {quantity} | price: {price} |'
                          f' fees: {quantity * price} |')

        if msg['orderType'] == 'MARKET':
            open_positions.append(position_msg(msg, exchange))
            print(f'| TRADE EXECUTED | {exchange} | {symbol} | {side} | MARKET_ORDER |'
                  f' quantity: {quantity} | price: {price} |'
                  f' fees: {quantity * price} |')

    # order rejected - log rejection message
    if msg['executionType'] == 'REJECTED':
        print(f'| ORDER REJECTED | {exchange} | {symbol} | {ts} | {orderId} |')

    # order expired - remove from open_orders if limit order, send no_fill message
    if msg['executionType'] == 'EXPIRED':
        if msg['orderType'] == 'LIMIT':
            for i, order in enumerate(open_orders):
                if order['clientOrderID'] == msg['origClientOrderID']:
                    open_orders.pop(i)
        print(f'| ORDER EXPIRED | {exchange.name} | {symbol} | {ts} | {orderId} |')

    return open_orders, open_positions


async def orderbook_update_handler(exchange, symbol, orderbook, update_msg):
    # message handler - update snapshot
    if update_msg['message']['lastUpdateId'] <= orderbook['message']['lastUpdateId']:
        print(f'{exchange.name} Not an update')
    if update_msg['message']['firstUpdateId'] <= orderbook['message'][
        'lastUpdateId'] + 1 <= \
            update_msg['message']['lastUpdateId']:
        # print('message good')
        orderbook = apply_diffs(orderbook, update_msg)
    else:
        if exchange.snapshot_in_ws:
            print(print(f'{exchange.name} Out of sync, re-syncing...'))
        else:
            orderbook = await exchange.orderbook_snapshot(symbol=symbol.upper())
            print(f'{exchange.name} Out of sync, re-syncing...')
    return orderbook
