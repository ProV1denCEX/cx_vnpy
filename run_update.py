# flake8: noqa
import datetime as dt

from tqdm.auto import tqdm

from Pandora.constant import SymbolSuffix
from vnpy.event import EventEngine
from Pandora.constant import Product, Exchange
from vnpy.trader.engine import MainEngine
from vnpy.app.vnpy_datamanager import ManagerEngine
from vnpy.app.vnpy_tinysoft.tinysoft_datafeed import ALL_SYMBOL, LISTING_SYMBOL, MC_SYMBOL

from Pandora.helper import TDays


def delete_quote(start_date, end_date, output=print):
    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    data_manager = ManagerEngine(main_engine, event_engine)

    periods = TDays.period(start_date, end_date, None)

    with tqdm(total=len(periods), desc=f"updating quote data from {start_date} to {end_date}") as pbar:
        for trade_date in periods:
            output(f"deleting quote data @{trade_date}...")
            prev_tday, tday, _ = TDays.interval(trade_date, fmt=None, end_hour=0)

            start = dt.datetime.combine(prev_tday, dt.time(hour=21))
            end = dt.datetime.combine(tday, dt.time(hour=15, minute=30))

            count = data_manager.delete_bar_data(start=start, end=end)
            output(f"delete bar from {start} to {end} {count}")

    main_engine.close()


def update_futures(start_date, end_date, output=print, symbol_set=LISTING_SYMBOL):
    """"""
    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    data_manager = ManagerEngine(main_engine, event_engine)

    count = data_manager.download_contract_data(Product.FUTURES, symbol_set == LISTING_SYMBOL)
    output(f"future contract download {count}")

    periods = TDays.period(start_date, end_date, None)

    with tqdm(total=len(periods), desc=f"updating quote data from {start_date} to {end_date}") as pbar:
        for trade_date in periods:
            output(f"updating quote data @{trade_date}...")
            prev_tday, tday, _ = TDays.interval(trade_date, fmt=None, end_hour=0)

            start = dt.datetime.combine(prev_tday, dt.time(hour=21))
            end = dt.datetime.combine(tday, dt.time(hour=15, minute=30))

            try:
                count = data_manager.delete_bar_data(product=Product.FUTURES, start=start, end=end)
                output(f"delete bar from {start} to {end} {count}")

            except Exception as e:
                output(f"failed to delete bar from {start} to {end}, exception {e} !!!")

            bars = data_manager.download_bar_data(
                symbol_set,
                Exchange.LOCAL,
                Product.FUTURES,
                "1m",
                start=start,
                end=end,
                output=output,
                return_data=True
            )
            output(f"{symbol_set} download 1m from {start} to {end} {len(bars)}")

            count = data_manager.download_bar_data(
                symbol_set,
                Exchange.LOCAL,
                Product.FUTURES,
                "d",
                start=start,
                end=end,
                output=output
            )
            output(f"{symbol_set} download daily from {start} to {end} {count}")

            bars_to_build = {}
            for bar in bars:
                if bar.symbol in bars_to_build:
                    bars_to_build[bar.symbol].append(bar)

                else:
                    bars_to_build[bar.symbol] = [bar]

            total = 0
            for symbol, bars_ in bars_to_build.items():
                bars_.sort(key=lambda x: x.datetime)
                count = data_manager.rebuild_bar_data_from_data(bars_, "recorder", Product.FUTURES)
                total += count

            output(f"{symbol_set} rebuild recorder from {start} to {end} {total}")
            pbar.update()

    main_engine.close()


def update_futures_ticks(start_date, end_date, output=print, symbol_set=MC_SYMBOL):
    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    data_manager = ManagerEngine(main_engine, event_engine)

    contracts = data_manager.load_contract_data(product=Product.FUTURES, start=start_date, end=end_date)
    if symbol_set == MC_SYMBOL:
        mc = [
            contract for contract in contracts
            if contract.symbol == contract.product_id + SymbolSuffix.MC
        ]

        contracts_raw = {
            contract.name: contract for contract in contracts
            if contract.symbol != contract.product_id + SymbolSuffix.MC and contract.symbol != contract.product_id + SymbolSuffix.MNC
        }

        contracts_selected = {
            (contract.name, max(contract.list_date, start_date), min(contract.expire_date, end_date)): contracts_raw[contract.name]
            for contract in mc
        }

    else:
        contracts_selected = {
            (contract.name, max(contract.list_date, start_date), min(contract.expire_date, end_date)): contract
            for contract in contracts
            if contract.symbol != contract.product_id + SymbolSuffix.MC and contract.symbol != contract.product_id + SymbolSuffix.MNC
        }

    with tqdm(total=len(contracts_selected)) as pbar:
        for (ticker, start, end), contract in contracts_selected.items():
            periods = TDays.period(start, end, None)

            n = 10
            for i in range(0, len(periods), n):
                periods_ = periods[i:i+n]

                prev_tday, _, _ = TDays.interval(periods_[0], fmt=None, end_hour=0)
                start_ = dt.datetime.combine(prev_tday, dt.time(hour=20))
                end_ = dt.datetime.combine(periods_[-1], dt.time(hour=20))

                # count = data_manager.delete_tick_data(contract.symbol, contract.exchange, contract.product, start_, end_)
                # output(f"delete tick {ticker} from {start_} to {end_} {count}")
                ticks = data_manager.load_tick_data(contract.symbol, contract.exchange, start_, end_)
                count1 = len(ticks)

                count2 = data_manager.download_tick_data(
                    symbol = contract.symbol,
                    exchange = contract.exchange,
                    start=start_,
                    end=end_,
                    output=output,
                    contract=contract
                )

                ticks = data_manager.load_tick_data(contract.symbol, contract.exchange, start_, end_)
                count3 = len(ticks)

                output(f"{contract.name} download {count1} - {count2} - {count3} from {start_} to {end_} @ {dt.datetime.now()}")

            pbar.update()

    main_engine.close()

def update_options(start_date, end_date, output=print, symbol_set=LISTING_SYMBOL):
    """"""
    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    data_manager = ManagerEngine(main_engine, event_engine)

    count = data_manager.download_contract_data(
        Product.OPTION,
        symbol_set == LISTING_SYMBOL,
        start=start_date,
        end=end_date
    )
    output(f"option contract download {count}")

    periods = TDays.period(start_date, end_date, None)
    prev_tday, _, _ = TDays.interval(periods[0], fmt=None, end_hour=0)

    start = dt.datetime.combine(prev_tday, dt.time(hour=21))
    end = dt.datetime.combine(periods[-1], dt.time(hour=15, minute=30))

    try:
        count = data_manager.delete_bar_data(product=Product.OPTION, start=start, end=end)
        output(f"delete bar from {start} to {end} {count}")

    except Exception as e:
        output(f"failed to delete bar from {start} to {end}, exception {e} !!!")

    count = data_manager.download_bar_data(
        symbol_set,
        Exchange.LOCAL,
        Product.OPTION,
        "d",
        start=start,
        end=end,
        output=output,
    )
    output(f"{symbol_set} download daily from {start} to {end} {count}")

    contracts = data_manager.load_contract_data(product=Product.OPTION, start=start, end=end)
    contracts = [i for i in contracts if i.exchange in {Exchange.SSE, Exchange.SZSE, Exchange.CFFEX}]

    with tqdm(total=len(contracts), desc=f"updating quote data from {start_date} to {end_date}") as pbar:
        for contract in contracts:
            output(f"updating quote data @{contract.symbol}...")

            bars = data_manager.download_bar_data(
                contract=contract,
                interval="1m",
                start=start,
                end=end,
                output=output,
                return_data=True
            )
            total = len(bars)

            bars.sort(key=lambda x: x.datetime)
            count = data_manager.rebuild_bar_data_from_data(bars, "recorder", contract.product)
            total += count

            output(f"{contract.symbol} min data from {start} to {end} {total}")

            pbar.update()

    main_engine.close()


def update_option_underlyings(start_date, end_date, output=print, symbol_set=LISTING_SYMBOL):
    """"""
    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    data_manager = ManagerEngine(main_engine, event_engine)

    etfs = data_manager.load_contract_data(product=Product.ETF)
    idxs = data_manager.load_contract_data(product=Product.INDEX)

    contracts = etfs + idxs

    periods = TDays.period(start_date, end_date, None)
    prev_tday, _, _ = TDays.interval(periods[0], fmt=None, end_hour=0)

    start = dt.datetime.combine(prev_tday, dt.time(hour=21))
    end = dt.datetime.combine(periods[-1], dt.time(hour=15, minute=30))

    try:
        count = data_manager.delete_bar_data(start=start, end=end)
        output(f"delete bar from {start} to {end} {count}")

    except Exception as e:
        output(f"failed to delete bar from {start} to {end}, exception {e} !!!")

    with tqdm(total=len(contracts), desc=f"updating quote data from {start_date} to {end_date}") as pbar:
        for contract in contracts:
            output(f"updating quote data @{contract.symbol}...")
            count = data_manager.download_bar_data(
                contract=contract,
                interval="d",
                start=start,
                end=end,
                output=output,
                return_data=False
            )
            output(f"{contract.symbol} download daily from {start} to {end} {count}")

            # we need to iter listing contracts, equity op only
            bars = data_manager.download_bar_data(
                contract=contract,
                interval="1m",
                start=start,
                end=end,
                output=output,
                return_data=True
            )
            total = len(bars)

            bars.sort(key=lambda x: x.datetime)
            count = data_manager.rebuild_bar_data_from_data(bars, "recorder", contract.product)
            total += count

            output(f"{contract.symbol} min data from {start} to {end} {total}")
            pbar.update()

    main_engine.close()


if __name__ == "__main__":
    # start = end = "2024-01-16"
    # start, end, _ = TDays.interval(end_hour=0, fmt=None)
    days = TDays.interval(days=5, fmt=None)

    # start = end = TDays.get_tday(end_hour=0)
    # run(start, end)

    update_futures_ticks(days[0], days[5], symbol_set=ALL_SYMBOL)

    # update_futures(start, end)
    # update_options(start, end)
    # update_option_underlyings(start, end)

    # days = TDays.interval(days=5, fmt=None)
    # start = days[0]
    # end = days[4]
    #
    # update_options(start, end)

    # days = TDays.interval(days=5)
    # start = days[0]
    # end = days[4]
    #
    # run(start, end, print, "all")


