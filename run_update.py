# flake8: noqa
import datetime as dt

from tqdm.auto import tqdm

from vnpy.event import EventEngine
from vnpy.trader.constant import Product, Exchange
from vnpy.trader.engine import MainEngine
from vnpy.app.vnpy_datamanager import ManagerEngine
from vnpy.app.vnpy_tinysoft.tinysoft_datafeed import ALL_SYMBOL, LISTING_SYMBOL

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

            count = data_manager.delete_bar_data(start=start, end=end)
            output(f"delete bar from {start} to {end} {count}")

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
                count = data_manager.rebuild_bar_data_from_data(bars_, "recorder")
                total += count

            output(f"{symbol_set} rebuild recorder from {start} to {end} {total}")
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

    with tqdm(total=len(periods), desc=f"updating quote data from {start_date} to {end_date}") as pbar:
        for trade_date in periods:
            output(f"updating quote data @{trade_date}...")
            prev_tday, tday, _ = TDays.interval(trade_date, fmt=None, end_hour=0)

            start = dt.datetime.combine(prev_tday, dt.time(hour=21))
            end = dt.datetime.combine(tday, dt.time(hour=15, minute=30))

            daily_bars = data_manager.download_bar_data(
                symbol_set,
                Exchange.LOCAL,
                Product.OPTION,
                "d",
                start=tday,
                end=tday,
                output=output,
                return_data=True
            )
            output(f"{symbol_set} download daily from {start} to {end} {len(daily_bars)}")

            # we need to iter listing contracts, equity op only
            total = 0
            bars_to_min = [bar for bar in daily_bars if bar.exchange in {Exchange.SSE, Exchange.SZSE, Exchange.CFFEX}]
            with tqdm(total=len(bars_to_min), desc=f"updating min data from {start} to {end}") as pbar_min:
                for bar in bars_to_min:
                    bars = data_manager.download_bar_data(
                        bar.symbol,
                        bar.exchange,
                        Product.OPTION,
                        "1m",
                        start=start,
                        end=end,
                        output=output,
                        return_data=True
                    )
                    total += len(bars)

                    bars.sort(key=lambda x: x.datetime)
                    count = data_manager.rebuild_bar_data_from_data(bars, "recorder")
                    total += count

                    if len(bars) == 0 and bar.volume != 0:
                        output(f"{bar.symbol} min data failed from {start} to {end} !")

                    pbar_min.update()

            output(f"{symbol_set} min data from {start} to {end} {total}")
            pbar.update()

    main_engine.close()


if __name__ == "__main__":
    start = end = "2024-01-16"
    start, end, _ = TDays.interval(end_hour=0, fmt=None)

    # start = end = TDays.get_tday(end_hour=0)
    # run(start, end)

    # update_futures(start, end)
    update_options(start, end)


    # days = TDays.interval(days=5)
    # start = days[0]
    # end = days[4]
    #
    # run(start, end, print, "all")

