# flake8: noqa
import datetime as dt

from tqdm.auto import tqdm

from vnpy.event import EventEngine
from vnpy.trader.constant import Product, Interval

from vnpy.trader.engine import MainEngine

from vnpy.app.vnpy_datamanager import ManagerEngine


def main():
    """"""
    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    data_manager = ManagerEngine(main_engine, event_engine)

    # count = data_manager.download_contract_data(Product.FUTURES, False)
    # print(f"contract download {count}")

    # start = dt.datetime.now() - dt.timedelta(days=3)
    contracts = data_manager.load_contract_data(product=Product.FUTURES, start=None, end=None)

    contracts = {contract.symbol: contract for contract in contracts if
                 # contract.symbol == "j2401"}
                 contract.product_id.upper() in {"IF", "T", "AG", "AP", "JM", "SI"}}

    contracts = list(contracts.values())

    with tqdm(total=len(contracts)) as pbar:
        for contract in contracts:
            start_ = contract.list_date
            # start_ = max(contract.list_date, dt.datetime.now() - dt.timedelta(days=3))
            count = data_manager.delete_bar_data(contract.symbol, None, None)
            print(f"{contract.symbol} delete {count}")

            count = data_manager.download_bar_data(contract.symbol, contract.exchange, contract.product, "1m", start=start_, output=print)
            count = data_manager.download_bar_data(contract.symbol, contract.exchange, contract.product, "d", start=start_, output=print)
            print(f"{contract.symbol} download {count}")

            count = data_manager.rebuild_bar_data(contract.symbol, contract.exchange, "recorder", start=start_)
            print(f"{contract.symbol} rebuild {count}")

            pbar.update()

    main_engine.close()


if __name__ == "__main__":
    main()
