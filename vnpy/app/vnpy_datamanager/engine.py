import csv
from datetime import datetime
from typing import List, Optional, Callable

from vnpy.trader.engine import BaseEngine, MainEngine, EventEngine
from vnpy.trader.constant import Interval, Exchange, Product
from vnpy.trader.object import BarData, TickData, ContractData, HistoryRequest
from vnpy.trader.database import BaseDatabase, get_database, BarOverview, DB_TZ
from vnpy.trader.datafeed import BaseDatafeed, get_datafeed
from vnpy.trader.utility import ZoneInfo, BarGenerator, generate_vt_symbol

APP_NAME = "DataManager"


class ManagerEngine(BaseEngine):
    """"""

    def __init__(
        self,
        main_engine: MainEngine,
        event_engine: EventEngine,
    ) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.database: BaseDatabase = get_database()
        self.datafeed: BaseDatafeed = get_datafeed()

    def import_data_from_csv(
        self,
        file_path: str,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        tz_name: str,
        datetime_head: str,
        open_head: str,
        high_head: str,
        low_head: str,
        close_head: str,
        volume_head: str,
        turnover_head: str,
        open_interest_head: str,
        datetime_format: str
    ) -> tuple:
        """"""
        with open(file_path, "rt") as f:
            buf: list = [line.replace("\0", "") for line in f]

        reader: csv.DictReader = csv.DictReader(buf, delimiter=",")

        bars: List[BarData] = []
        start: datetime = None
        count: int = 0
        tz = ZoneInfo(tz_name)

        for item in reader:
            if datetime_format:
                dt: datetime = datetime.strptime(item[datetime_head], datetime_format)
            else:
                dt: datetime = datetime.fromisoformat(item[datetime_head])
            dt = dt.replace(tzinfo=tz)

            turnover = item.get(turnover_head, 0)
            open_interest = item.get(open_interest_head, 0)

            bar: BarData = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=dt,
                interval=interval,
                volume=float(item[volume_head]),
                open_price=float(item[open_head]),
                high_price=float(item[high_head]),
                low_price=float(item[low_head]),
                close_price=float(item[close_head]),
                turnover=float(turnover),
                open_interest=float(open_interest),
                gateway_name="DB",
            )

            bars.append(bar)

            # do some statistics
            count += 1
            if not start:
                start = bar.datetime

        end: datetime = bar.datetime

        # insert into database
        self.database.save_bar_data(bars)

        return start, end, count

    def output_data_to_csv(
        self,
        file_path: str,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> bool:
        """"""
        bars: List[BarData] = self.load_bar_data(symbol, exchange, interval, start, end)

        fieldnames: list = [
            "symbol",
            "exchange",
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
            "open_interest"
        ]

        try:
            with open(file_path, "w") as f:
                writer: csv.DictWriter = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
                writer.writeheader()

                for bar in bars:
                    d: dict = {
                        "symbol": bar.symbol,
                        "exchange": bar.exchange.value,
                        "datetime": bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                        "open": bar.open_price,
                        "high": bar.high_price,
                        "low": bar.low_price,
                        "close": bar.close_price,
                        "turnover": bar.turnover,
                        "volume": bar.volume,
                        "open_interest": bar.open_interest,
                    }
                    writer.writerow(d)

            return True
        except PermissionError:
            return False

    def get_bar_overview(self) -> List[BarOverview]:
        """"""
        return self.database.get_bar_overview()

    def load_contract_data(
            self,
            symbol: str = None,
            product: Product = Product.FUTURES,
            start: datetime = None,
            end: datetime = None
    ) -> list[ContractData]:
        """
        Load contract data from database.
        """
        contracts = self.database.load_contract_data(
            symbol,
            product,
            start,
            end
        )

        return contracts

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """"""
        bars: List[BarData] = self.database.load_bar_data(
            symbol,
            exchange,
            interval,
            start,
            end
        )

        return bars

    def load_tick_data(
            self,
            symbol: str,
            exchange: Exchange,
            start: datetime,
            end: datetime
    ):
        ticks = self.database.load_tick_data(symbol, exchange, start, end)
        return ticks

    def delete_bar_data(
        self,
        symbol: str = None,
        exchange: Exchange = None,
        interval: Interval = None,
        start: datetime = None,
        end: datetime = None,
    ) -> int:
        """"""
        count: int = self.database.delete_bar_data(
            symbol,
            exchange,
            interval,
            start,
            end
        )

        return count

    def download_contract_data(
        self,
        product: Product,
        listing_only: bool = True,
        output: Callable = print
    ):
        symbol = "listing_only" if listing_only else "all"
        req: HistoryRequest = HistoryRequest(
            symbol=symbol,
            exchange=Exchange.LOCAL,
            product=product,
            start=datetime.now(DB_TZ),
        )

        data: List[ContractData] = self.datafeed.query_contract_history(req, output)

        if data:
            self.database.save_contract_data(data)
            return len(data)

        return 0

    def download_bar_data(
        self,
        symbol: str = "",
        exchange: Exchange = None,
        product: Product = Product.FUTURES,
        interval: str = "1m",
        start: datetime = datetime(2010, 1, 1),
        end: datetime = datetime.now(DB_TZ),

        output: Callable = print,
        return_data: bool = False,

        contract: ContractData = None
    ):
        """
        Query bar data from datafeed.
        """

        assert symbol or contract

        if contract:
            req: HistoryRequest = HistoryRequest(
                symbol=contract.symbol,
                exchange=contract.exchange,
                product=contract.product,
                interval=Interval(interval),
                start=start,
                end=end
            )

            req.start = max(start, contract.list_date)

        else:
            req: HistoryRequest = HistoryRequest(
                symbol=symbol,
                exchange=exchange,
                product=product,
                interval=Interval(interval),
                start=start,
                end=end
            )

            vt_symbol: str = f"{symbol}.{exchange.value}"
            contract: Optional[ContractData] = self.main_engine.get_contract(vt_symbol)

        req.contract = contract

        # If history data provided in gateway, then query
        if contract and contract.history_data:
            data: List[BarData] = self.main_engine.query_history(
                req, contract.gateway_name
            )
        # Otherwise use datafeed to query data
        else:
            data: List[BarData] = self.datafeed.query_bar_history(req, output)

        if data:
            self.database.save_bar_data(data)

        if return_data:
            return data

        else:
            return len(data)

    def download_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        output: Callable
    ) -> int:
        """
        Query tick data from datafeed.
        """
        req: HistoryRequest = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            start=start,
            end=datetime.now(DB_TZ)
        )

        data: List[TickData] = self.datafeed.query_tick_history(req, output)

        if data:
            self.database.save_tick_data(data)
            return len(data)

        return 0

    def rebuild_bar_data(
            self,
            symbol: str,
            exchange: Exchange,
            interval: str,
            start: datetime,
            end: datetime = datetime.now(DB_TZ)
    ):
        bars_to_save = []

        def record_bar(bar):
            bars_to_save.append(bar)

        if interval == "1m":
            ticks = self.load_tick_data(symbol, exchange, start, end)

            bg = BarGenerator(record_bar)
            for tick in ticks:
                bg.update_tick(tick)

        else:
            bars = self.load_bar_data(symbol, exchange, Interval.MINUTE, start, end)

            bgs = []
            if interval == "recorder":
                # bgs.append(BarGenerator(None, interval=Interval.DAILY, on_window_bar=record_bar))

                windows = [2, 3, 5, 15]

            else:
                windows = [Interval.to_window(interval)]

            for i in windows:
                bgs.append(BarGenerator(None, window=i, on_window_bar=record_bar))

            for bar in bars:
                for bg in bgs:
                    bg.update_bar(bar)

        if bars_to_save:
            self.database.save_bar_data(bars_to_save)
            return len(bars_to_save)

        return 0

    def rebuild_bar_data_from_data(self, data: list, interval: Interval):
        bars_to_save = []

        def record_bar(bar):
            bars_to_save.append(bar)

        if interval == "1m":
            bg = BarGenerator(record_bar)
            for tick in data:
                bg.update_tick(tick)

        else:
            bgs = []
            if interval == "recorder":
                # bgs.append(BarGenerator(None, interval=Interval.DAILY, on_window_bar=record_bar))

                windows = [2, 3, 5, 15]

            else:
                windows = [Interval.to_window(interval)]

            for i in windows:
                bgs.append(BarGenerator(None, window=i, on_window_bar=record_bar))

            for bar in data:
                for bg in bgs:
                    bg.update_bar(bar)

        if bars_to_save:
            self.database.save_bar_data(bars_to_save)
            return len(bars_to_save)

        return 0
