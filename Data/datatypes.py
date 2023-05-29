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


def account_update_msg(eventTime=None, balances=None):
    msg = {
           'eventTime': eventTime,
           'balances': balances
           }
    return msg


def order_update_msg(exchange=None, symbol=None, eventTime=None, side=None, quantity=None, price=None,
                     cu_fill_quant=None,cu_quote_quant=None,clientOrderID=None,
                     transactTime=None, executionType=None, orderType=None, tif=None,
                     origClientOrderID=None, rejectReason=None):
    msg = {'exchange': exchange,
           'symbol': symbol,
           'eventTime': eventTime,
           'executionType': executionType,
           'orderType': orderType,
           'side': side,
           'order_quantity': quantity,
           'order_price': price,
           'fill_quantity': cu_fill_quant,
           'fill_quote_quant': cu_quote_quant,
           'clientOrderID': clientOrderID,
           'transactTime': transactTime,
           'tif': tif,
           'origClientOrderID': origClientOrderID,
           'rejectReason': rejectReason
           }
    return msg


def account_snapshot_msg(updateTime=None, makerFee=None, takerFee=None, balances=None):
    msg = {'updateTime': updateTime,
           'makerFee': makerFee,
           'takerFee': takerFee,
           'balances': balances
           }
    return msg


def position_msg(msg, exchange):
    message = {'exchange': exchange,
               'symbol': msg['symbol'],
               'side': msg['side'],
               'fill_quantity': float(msg['fill_quantity']),
               'avg_fill_price': float(msg['fill_quote_quant'])/float(msg['fill_quantity']),
               'transactTime': msg['transactTime']
               }
    return message
