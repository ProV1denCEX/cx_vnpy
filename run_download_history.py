# flake8: noqa
import datetime as dt

from tqdm.auto import tqdm

from vnpy.event import EventEngine
from vnpy.trader.constant import Product, Interval, Exchange

from vnpy.trader.engine import MainEngine

from vnpy.app.vnpy_datamanager import ManagerEngine


def main():
    """"""
    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    data_manager = ManagerEngine(main_engine, event_engine)

    count = data_manager.download_contract_data(Product.FUTURES, False)
    print(f"contract download {count}")

    # start = dt.datetime.now() - dt.timedelta(days=3)
    contracts = data_manager.load_contract_data(product=Product.FUTURES, start=None, end=None)

    contracts = {contract.name: contract for contract in contracts
                 }
                 # if contract.exchange == Exchange.CZCE}
                 # contract.product_id.upper() not in {"IF", "T", "AG", "AP", "JM", "SI"}}

    contracts = list(contracts.values())

    with tqdm(total=len(contracts)) as pbar:
        for contract in contracts:
            count = data_manager.delete_bar_data(
                contract.symbol,
                contract.exchange,
                None,
                start=contract.list_date,
                end=contract.expire_date.replace(hour=23, minute=59, second=59)
            )
            print(f"{contract.symbol} delete {count}")

            bars = data_manager.download_bar_data(contract=contract, interval="1m", output=print, return_data=True)
            count = data_manager.download_bar_data(contract=contract, interval="d", output=print, return_data=False)
            print(f"{contract.symbol} download {count}")

            count = data_manager.rebuild_bar_data_from_data(bars, "recorder")
            print(f"{contract.symbol} rebuild {count}")

            pbar.update()

    main_engine.close()


if __name__ == "__main__":
    main()
