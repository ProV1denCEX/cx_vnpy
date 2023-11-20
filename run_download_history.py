# flake8: noqa
import datetime as dt

from tqdm.auto import tqdm

from vnpy.event import EventEngine
from vnpy.trader.constant import Product

from vnpy.trader.engine import MainEngine

from vnpy.app.vnpy_datamanager import ManagerEngine


def main():
    """"""
    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    data_manager = ManagerEngine(main_engine, event_engine)

    # count = data_manager.download_contract_data(Product.FUTURES, False)

    contracts = data_manager.load_contract_data(product=Product.FUTURES, start=dt.datetime(2023, 11, 16),
                                                end=dt.datetime.now())

    contracts = contracts[:10]
    with tqdm(total=len(contracts)) as pbar:
        for contract in contracts:
            start = contract.list_date
            # start = max(contract.list_date, dt.datetime.now() - dt.timedelta(days=3))
            count = data_manager.download_bar_data(contract.symbol, contract.exchange, contract.product, "1m",
                                                   start=start, output=print)
            print(f"{contract.symbol} download {count}")

            count = data_manager.rebuild_bar_data(contract.symbol, contract.exchange, "recorder",
                                                  start=contract.list_date)
            print(f"{contract.symbol} rebuild {count}")

            pbar.update()

    main_engine.close()


if __name__ == "__main__":
    main()
