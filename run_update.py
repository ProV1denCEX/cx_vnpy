# flake8: noqa
import datetime as dt

from tqdm.auto import tqdm

from vnpy.event import EventEngine
from vnpy.trader.constant import Product, Exchange

from vnpy.trader.engine import MainEngine

from vnpy.app.vnpy_datamanager import ManagerEngine


def main():
    """"""
    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    data_manager = ManagerEngine(main_engine, event_engine)

    count = data_manager.download_contract_data(Product.FUTURES, True)
    print(f"contract download {count}")

    start = (dt.datetime.now() - dt.timedelta(days=3)).replace(hour=16, minute=0, second=0, microsecond=0)

    count = data_manager.delete_bar_data(start=start)
    print(f"delete {count}")

    bars = data_manager.download_bar_data("listing_only", Exchange.LOCAL, Product.FUTURES, "1m", start=start,
                                          output=print, return_data=True)
    print(f"listing_only download 1m {len(bars)}")

    count = data_manager.download_bar_data("listing_only", Exchange.LOCAL, Product.FUTURES, "d", start=start, output=print)
    print(f"listing_only download daily {count}")

    bars_to_build = {}
    for bar in bars:
        if bar.symbol in bars_to_build:
            bars_to_build[bar.symbol].append(bar)

        else:
            bars_to_build[bar.symbol] = [bar]

    total = 0
    with tqdm(total=len(bars_to_build), desc="rebuilding bars...") as pbar:
        for symbol, bars_ in bars_to_build.items():
            bars_.sort(key=lambda x: x.datetime)
            count = data_manager.rebuild_bar_data_from_data(bars_, "recorder")
            total += count
            pbar.update()

    print(f"listing_only rebuild recorder {total}")

    main_engine.close()


if __name__ == "__main__":
    main()
