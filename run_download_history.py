# flake8: noqa
import datetime as dt
import time

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from tqdm.auto import tqdm

from Pandora.constant import SymbolSuffix
from vnpy.event import EventEngine
from Pandora.constant import Product, Interval, Exchange

from vnpy.trader.engine import MainEngine

from vnpy.app.vnpy_datamanager import ManagerEngine

from Pandora.helper import TDays


def download_future_history():
    """"""
    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    data_manager = ManagerEngine(main_engine, event_engine)

    count = data_manager.download_contract_data(Product.FUTURES, False)
    print(f"contract download {count}")

    # start = dt.datetime.now() - dt.timedelta(days=3)
    contracts = data_manager.load_contract_data(product=Product.FUTURES, start=None, end=None)

    contracts = {contract.name: contract for contract in contracts}
                 # if contract.exchange == Exchange.CZCE}
                 # contract.product_id.upper() not in {"IF", "T", "AG", "AP", "JM", "SI"}}

    contracts = list(contracts.values())

    with tqdm(total=len(contracts)) as pbar:
        for contract in contracts:
            count = data_manager.delete_bar_data(
                contract.symbol,
                contract.exchange,
                contract.product,
                None,
                start=contract.list_date,
                end=contract.expire_date.replace(hour=23, minute=59, second=59)
            )
            print(f"{contract.symbol} delete {count}")

            bars = data_manager.download_bar_data(contract=contract, interval="1m", output=print, return_data=True)
            count = data_manager.download_bar_data(contract=contract, interval="d", output=print, return_data=False)
            print(f"{contract.symbol} download {count}")

            count = data_manager.rebuild_bar_data_from_data(bars, "recorder", contract.product)
            print(f"{contract.symbol} rebuild {count}")

            pbar.update()

    main_engine.close()


def download_future_tick_history():
    """"""
    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    data_manager = ManagerEngine(main_engine, event_engine)

    contracts = data_manager.load_contract_data(product=Product.FUTURES, start=None, end=None)
    mc = [
        contract for contract in contracts
        if contract.symbol == contract.product_id + SymbolSuffix.MC and contract.expire_date > dt.datetime(2015, 1, 1)
    ]

    contracts_raw = {
        contract.name: contract for contract in contracts
        if contract.symbol != contract.product_id + SymbolSuffix.MC and contract.symbol != contract.product_id + SymbolSuffix.MNC
    }

    contracts_selected = {
        (contract.name, contract.list_date, contract.expire_date): contracts_raw[contract.name]
        for contract in mc
    }

    with tqdm(total=len(contracts_selected)) as pbar:
        # _start = False
        for (ticker, start, end), contract in contracts_selected.items():
            # if ticker == 'RU1605':
            #     _start = True

            # if not _start:
            #     pbar.update()
            #     print(f"Skip {ticker}")
            #     continue

            # ticks = data_manager.load_tick_data(contract.symbol, contract.exchange, start, end)
            # if ticks:
            #     pbar.update()
            #     print(f"Skip {ticker}: {len(ticks)}")
            #     continue

            periods = TDays.period(start, end, None)

            n = 10
            for i in range(0, len(periods), n):
                periods_ = periods[i:i+n]

                prev_tday, _, _ = TDays.interval(periods_[0], fmt=None, end_hour=0)
                start_ = dt.datetime.combine(prev_tday, dt.time(hour=20))
                end_ = dt.datetime.combine(periods_[-1], dt.time(hour=20))

                count = data_manager.download_tick_data(
                    symbol = contract.symbol,
                    exchange = contract.exchange,
                    start=start_,
                    end=end_,
                    output=print,
                    contract=contract
                )

                print(f"{contract.name} download {count} from {start_} to {end_} @ {dt.datetime.now()}")

            pbar.update()

    main_engine.close()


def download_option_history():
    """"""
    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    data_manager = ManagerEngine(main_engine, event_engine)

    periods = TDays.period(dt.date(2015, 2, 9), dt.date.today(), fmt=None)

    with tqdm(total=len(periods), desc="Downloading option contracts history") as pbar:
        for date in periods:
            count = data_manager.download_contract_data(Product.OPTION, start=date, end=date)

            pbar.set_description(f"{date}: {count}")
            pbar.update()

    contracts = data_manager.load_contract_data(product=Product.OPTION, start=None, end=None)

    with tqdm(total=len(contracts)) as pbar:
        for contract in contracts:
            count = data_manager.delete_bar_data(
                contract.symbol,
                contract.exchange,
                contract.product,
                None,
                start=contract.list_date,
                end=contract.expire_date.replace(hour=23, minute=59, second=59)
            )
            print(f"{contract.symbol} delete {count}")

            count = data_manager.download_bar_data(contract=contract, interval="d", output=print, return_data=False)
            pbar.set_description(f"{contract.symbol} download {count}")

            if contract.exchange in {Exchange.SSE, Exchange.SZSE, Exchange.CFFEX}:
                bars = data_manager.download_bar_data(contract=contract, interval="1m", output=print, return_data=True)

                count = data_manager.rebuild_bar_data_from_data(bars, "recorder", contract.product)
                pbar.set_description(f"{contract.symbol} rebuild {count}")

                if not count:
                    print(f"{contract.symbol} download {count}")

            pbar.update()

    main_engine.close()


def download_option_underlying_history():
    """"""
    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    data_manager = ManagerEngine(main_engine, event_engine)

    count = data_manager.download_contract_data(Product.ETF, True)
    print(f"ETF contract download {count}")

    count = data_manager.download_contract_data(Product.INDEX, True)
    print(f"Index contract download {count}")

    # start = dt.datetime.now() - dt.timedelta(days=3)
    etfs = data_manager.load_contract_data(product=Product.ETF)
    idxs = data_manager.load_contract_data(product=Product.INDEX)

    contracts = etfs + idxs
    # contracts = [i for i in idxs if i.symbol == "000905"]

    with tqdm(total=len(contracts)) as pbar:
        for contract in contracts:
            count = data_manager.delete_bar_data(
                contract.symbol,
                contract.exchange,
                contract.product,
                None,
                start=contract.list_date,
                end=contract.expire_date.replace(hour=23, minute=59, second=59)
            )
            print(f"{contract.symbol} delete {count}")

            bars = data_manager.download_bar_data(contract=contract, interval="1m", start=dt.datetime(2014, 1, 1), output=print, return_data=True)
            count = data_manager.download_bar_data(contract=contract, interval="d", output=print, return_data=False)
            print(f"{contract.symbol} download {count}")

            count = data_manager.rebuild_bar_data_from_data(bars, "recorder", contract.product)
            print(f"{contract.symbol} rebuild {count}")

            pbar.update()

    main_engine.close()


def calculate_option_greeks(start_date, end_date, output=print, symbol_underlyings=None):
    from vnpy.app.vnpy_optionmaster.pricing import (
        black_76, binomial_tree, black_scholes
    )
    from vnpy.app.vnpy_optionmaster.time import ANNUAL_DAYS

    from Pandora.helper import TDays
    from Pandora.data_manager import get_api

    option_model_map = {
        Product.ETF: black_scholes,
        Product.INDEX: black_76,
        Product.FUTURES: binomial_tree,
    }

    api = get_api()

    calendar = TDays.get_trading_calendar()
    calendar = calendar.reset_index()

    rf = api.get_risk_free_rate(symbol='IRSHIBOR1Y', begin_date=dt.date(2015, 2, 9))
    rf.columns = rf.columns.str.lower()
    rf = rf[['date', 'rate']]
    rf['rate'] /= 100
    rf['rate'] = rf['rate'].rolling(2, min_periods=1).mean()

    if symbol_underlyings is None:
        symbol_underlyings = [
            '510050',
            '510300',
            '510500',
            '588000',
            '588080',
            '159901',
            '159915',
        ]

    elif isinstance(symbol_underlyings, str):
        symbol_underlyings = [symbol_underlyings]

    product = Product.ETF
    model = option_model_map[product]

    periods = TDays.period(start_date, end_date, None)
    prev_tday, _, _ = TDays.interval(periods[0], fmt=None, end_hour=0)

    start = dt.datetime.combine(prev_tday, dt.time(hour=21))
    end = dt.datetime.combine(periods[-1], dt.time(hour=15, minute=30))

    output(f"calculating greeks from {start} to {end}")
    for underlying_symbol in symbol_underlyings:
        data = api.get_option_chain(underlying_symbol, product, start, end, None, option_fields=['datetime', 'symbol', 'interval', 'close_price'])

        option_chain = data[underlying_symbol]
        spot = option_chain['bar_underlying'][['datetime', 'exchange', 'interval', 'close_price']]
        option_price = option_chain['bar_options']

        mat = option_price.merge(spot, on=['datetime', 'interval'], how='left', suffixes=('', '_spot'))
        mat['date'] = pd.to_datetime(mat['datetime'].dt.date)

        contract_options = option_chain['contract_options'][['symbol', 'datetime', 'size', 'option_type', 'option_strike', 'expire_date']]
        contract_options = contract_options.merge(calendar, left_on='expire_date', right_on='Date',
                                                  suffixes=('', '_exp'))
        contract_options = contract_options.merge(calendar, left_on='datetime', right_on='Date', suffixes=('', '_dt'))

        contract_options['ptm_trade_day'] = contract_options['index'] - contract_options['index_dt']

        info = contract_options[
            ['symbol', 'datetime', 'size', 'option_type', 'option_strike', 'ptm_trade_day']]
        info = info.rename(columns={'datetime': 'date'})
        info['option_type'] = np.where(info.loc[:, 'option_type'] == '看涨期权', 1, -1)
        info['time_to_expire'] = info.loc[:, 'ptm_trade_day'] / ANNUAL_DAYS

        mat = mat.merge(info, how='left').merge(rf, how='left').dropna()

        greeks = Parallel(n_jobs=-1)(  # n_jobs=-1 表示使用所有CPU核心
            delayed(calc_option_greeks)(model, row)
            for idx, row in mat.iterrows()
        )

        if greeks:
            greeks = pd.DataFrame(greeks)
            table_name = api.dolphindb.get_table_name("option_greeks", Product.OPTION)
            api.dolphindb.upsert(table_name, greeks, on='datetime')


def calc_option_greeks(model, d):
    iv = model.calculate_impv(
        d['close_price'],
        d['close_price_spot'],
        d['option_strike'],
        d['rate'],
        d['time_to_expire'],
        d['option_type']
    )

    if iv:
        price, delta, gamma, theta, vega = model.calculate_greeks(
            d['close_price_spot'],
            d['option_strike'],
            d['rate'],
            d['time_to_expire'],
            iv,
            d['option_type']
        )

    else:
        delta = gamma = theta = vega = 0

    return {
        'symbol': d['symbol'],
        'exchange': d['exchange'],
        'datetime': d['datetime'],
        'interval': d['interval'],
        'iv': iv,
        'delta': delta,
        'gamma': gamma,
        'vega': vega,
        'theta': theta,
    }


if __name__ == "__main__":
    # download_option_history()

    # calculate_option_greeks('2015-02-09', '2024-06-04', symbol_underlyings='510050')
    # calculate_option_greeks('2015-02-09', '2024-06-04', symbol_underlyings='510300')
    # start = dt.datetime(2024, 6, 20)
    # end = dt.datetime.now()
    #
    # start_ = start
    # while start_ <= end:
    #     end_ = start_ + dt.timedelta(days=30)
    #
    #     print(f"calculating greeks from {start_} to {end_}")
    #     calculate_option_greeks(start_, end_)
    #
    #     start_ = end_

    # start, end, _ = TDays.interval(end_hour=0, fmt=None)

    # calculate_option_greeks(start, end)

    # download_future_tick_history()

    download_option_underlying_history()
