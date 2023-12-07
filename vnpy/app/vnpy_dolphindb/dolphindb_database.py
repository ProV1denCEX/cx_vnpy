from datetime import datetime

import numpy as np
import pandas as pd
import dolphindb as ddb

from vnpy.trader.constant import Exchange, Interval, Product, OptionType
from vnpy.trader.object import BarData, TickData, ContractData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    TickOverview,
    DB_TZ,
    convert_tz
)
from vnpy.trader.setting import SETTINGS

from .dolphindb_script import SCRIPTS_FUNC


class DolphindbDatabase(BaseDatabase):
    """DolphinDB数据库接口"""

    def __init__(self) -> None:
        """构造函数"""
        self.user: str = SETTINGS["database.user"]
        self.password: str = SETTINGS["database.password"]
        self.host: str = SETTINGS["database.host"]
        self.port: int = SETTINGS["database.port"]
        self.db_path: str = "dfs://" + SETTINGS["database.database"]

        self.table_name = SETTINGS["database.table_name"]

        # 连接数据库
        self.session: ddb.session = ddb.session(keepAliveTime=600)
        self.session.connect(self.host, self.port, self.user, self.password)

        # 创建连接池（用于数据写入）
        self.pool: ddb.DBConnectionPool = ddb.DBConnectionPool(self.host, self.port, 5, self.user, self.password)

        # 初始化数据库和数据表
        if not self.session.existsDatabase(self.db_path):
            self.session.run(SCRIPTS_FUNC['database'](SETTINGS["database.database"]))

        for k, v in self.table_name.items():
            if not self.session.existsTable(self.db_path, v):
                if k in SCRIPTS_FUNC:
                    self.session.run(SCRIPTS_FUNC[k](SETTINGS["database.database"], v))

    def __del__(self) -> None:
        """析构函数"""
        if not self.session.isClosed():
            self.session.close()

    def save_contract_data(self, contracts: list[ContractData]) -> bool:
        futures_to_db = []
        options_to_db = []

        dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        for contract in contracts:
            if contract.product == Product.FUTURES:
                d: dict = {
                    "symbol": contract.symbol,
                    "exchange": contract.exchange.value,
                    "datetime": dt,
                    "name": contract.name,
                    "product_id": contract.product_id,
                    "size": contract.size,
                    "pricetick": contract.pricetick,
                    "list_date": contract.list_date,
                    "expire_date": contract.expire_date,
                    "min_volume": contract.min_volume,
                }

                futures_to_db.append(d)

            elif contract.product == Product.OPTION:
                d: dict = {
                    "symbol": contract.symbol,
                    "exchange": contract.exchange.value,
                    "datetime": dt,
                    "name": contract.name,
                    "product_id": contract.product_id,
                    "size": contract.size,
                    "pricetick": contract.pricetick,
                    "list_date": contract.option_listed,
                    "expire_date": contract.option_expiry,
                    "min_volume": contract.min_volume,

                    "option_strike": contract.option_strike,
                    "option_underlying": contract.option_underlying,
                    "option_type": contract.option_type.value,
                    "option_portfolio": contract.option_portfolio,
                    "option_index": contract.option_index,
                }

                options_to_db.append(d)

        if futures_to_db:
            df: pd.DataFrame = pd.DataFrame.from_records(futures_to_db)
            appender: ddb.PartitionedTableAppender = ddb.PartitionedTableAppender(self.db_path, self.table_name["contract_futures"], "expire_date", self.pool)
            appender.append(df)

        if options_to_db:
            df: pd.DataFrame = pd.DataFrame.from_records(options_to_db)
            appender: ddb.PartitionedTableAppender = ddb.PartitionedTableAppender(self.db_path, self.table_name["contract_options"], "datetime", self.pool)
            appender.append(df)

        return True

    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        """保存k线数据"""
        bars_to_db: list[dict] = []

        for bar in bars:
            dt: np.datetime64 = np.datetime64(convert_tz(bar.datetime))

            d: dict = {
                "symbol": bar.symbol,
                "exchange": bar.exchange.value,
                "datetime": dt,
                "interval": bar.interval.value,
                "volume": float(bar.volume),
                "turnover": float(bar.turnover),
                "open_interest": float(bar.open_interest),
                "open_price": float(bar.open_price),
                "high_price": float(bar.high_price),
                "low_price": float(bar.low_price),
                "close_price": float(bar.close_price)
            }

            bars_to_db.append(d)

        df: pd.DataFrame = pd.DataFrame.from_records(bars_to_db)
        appender: ddb.PartitionedTableAppender = ddb.PartitionedTableAppender(self.db_path, self.table_name["bar"], "datetime", self.pool)
        appender.append(df)

        return True

    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
        """保存TICK数据"""
        ticks_to_db: list[dict] = []

        for tick in ticks:
            dt: np.datetime64 = np.datetime64(convert_tz(tick.datetime))

            d: dict = {
                "symbol": tick.symbol,
                "exchange": tick.exchange.value,
                "datetime": dt,

                "name": tick.name,
                "volume": float(tick.volume),
                "turnover": float(tick.turnover),
                "open_interest": float(tick.open_interest),
                "last_price": float(tick.last_price),
                "last_volume": float(tick.last_volume),
                "limit_up": float(tick.limit_up),
                "limit_down": float(tick.limit_down),

                "open_price": float(tick.open_price),
                "high_price": float(tick.high_price),
                "low_price": float(tick.low_price),
                "pre_close": float(tick.pre_close),

                "bid_price_1": float(tick.bid_price_1),
                "bid_price_2": float(tick.bid_price_2),
                "bid_price_3": float(tick.bid_price_3),
                "bid_price_4": float(tick.bid_price_4),
                "bid_price_5": float(tick.bid_price_5),

                "ask_price_1": float(tick.ask_price_1),
                "ask_price_2": float(tick.ask_price_2),
                "ask_price_3": float(tick.ask_price_3),
                "ask_price_4": float(tick.ask_price_4),
                "ask_price_5": float(tick.ask_price_5),

                "bid_volume_1": float(tick.bid_volume_1),
                "bid_volume_2": float(tick.bid_volume_2),
                "bid_volume_3": float(tick.bid_volume_3),
                "bid_volume_4": float(tick.bid_volume_4),
                "bid_volume_5": float(tick.bid_volume_5),

                "ask_volume_1": float(tick.ask_volume_1),
                "ask_volume_2": float(tick.ask_volume_2),
                "ask_volume_3": float(tick.ask_volume_3),
                "ask_volume_4": float(tick.ask_volume_4),
                "ask_volume_5": float(tick.ask_volume_5),

                "localtime": np.datetime64(tick.localtime),
            }

            ticks_to_db.append(d)

        df: pd.DataFrame = pd.DataFrame.from_records(ticks_to_db)
        appender: ddb.PartitionedTableAppender = ddb.PartitionedTableAppender(self.db_path, self.table_name["tick"], "datetime", self.pool)
        appender.append(df)

        return True

    def load_contract_data(
            self,
            symbol: str = None,
            product: Product = Product.FUTURES,
            start: datetime = None,
            end: datetime = None
    ) -> list[ContractData]:

        contracts = []
        if product == Product.FUTURES:
            table_name = self.table_name["contract_futures"]

            table: ddb.Table = self.session.loadTable(tableName=table_name, dbPath=self.db_path)

            query = table.select('*')
            if symbol:
                query = query.where(f'symbol="{symbol}"')

            if start:
                start = np.datetime64(start)
                start: str = str(start).replace("-", ".")
                query = query.where(f'expire_date>={start}')

            if end:
                end = np.datetime64(end)
                end: str = str(end).replace("-", ".")
                query = query.where(f'list_date<={end}')

            df: pd.DataFrame = query.toDF()

            if df.empty:
                return []

            for tp in df.itertuples():
                contract = ContractData(
                    symbol=tp.symbol,
                    exchange=Exchange[tp.exchange],
                    name=tp.name,
                    product=product,
                    product_id=tp.product_id,
                    size=tp.size,
                    pricetick=tp.pricetick,
                    list_date=tp.list_date.to_pydatetime(),
                    expire_date=tp.expire_date.to_pydatetime(),
                    min_volume=tp.min_volume,

                    gateway_name="DB"
                )
                contracts.append(contract)

        elif product == Product.OPTION:
            table_name = self.table_name["contract_options"]

            table: ddb.Table = self.session.loadTable(tableName=table_name, dbPath=self.db_path)

            query = table.select('*')
            if symbol:
                query = query.where(f'symbol="{symbol}"')

            if start:
                start = np.datetime64(start)
                start: str = str(start).replace("-", ".")
                query = query.where(f'datetime>={start}')

            if end:
                end = np.datetime64(end)
                end: str = str(end).replace("-", ".")
                query = query.where(f'datetime<={end}')

            df: pd.DataFrame = query.toDF()

            if df.empty:
                return []

            for tp in df.itertuples():
                contract = ContractData(
                    symbol=tp.symbol,
                    exchange=Exchange[tp.exchange],
                    name=tp.name,
                    product=product,
                    product_id=tp.product_id,
                    size=tp.size,
                    pricetick=tp.pricetick,
                    list_date=tp.list_date.to_pydatetime(),
                    expire_date=tp.expire_date.to_pydatetime(),
                    min_volume=tp.min_volume,

                    option_strike=tp.option_strike,
                    option_underlying=tp.option_underlying,
                    option_type=OptionType.from_str(tp.option_type),
                    option_portfolio=tp.option_portfolio,
                    option_index=tp.option_index,

                    option_listed=tp.list_date.to_pydatetime(),
                    option_expiry=tp.expire_date.to_pydatetime(),

                    gateway_name="DB"
                )
                contracts.append(contract)

        else:
            raise NotImplementedError

        return contracts

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> list[BarData]:
        """读取K线数据"""
        # 转换时间格式
        start = np.datetime64(start)
        start: str = str(start).replace("-", ".")

        end = np.datetime64(end)
        end: str = str(end).replace("-", ".")

        table: ddb.Table = self.session.loadTable(tableName=self.table_name["bar"], dbPath=self.db_path)

        df: pd.DataFrame = (
            table.select('*')
            .where(f'symbol="{symbol}"')
            .where(f'exchange="{exchange.value}"')
            .where(f'interval="{interval.value}"')
            .where(f'datetime>={start}')
            .where(f'datetime<={end}')
            .toDF()
        )

        if df.empty:
            return []

        df.set_index("datetime", inplace=True)
        df = df.tz_localize(DB_TZ.key)

        # 转换为BarData格式
        bars: list[BarData] = []

        for tp in df.itertuples():
            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=tp.Index.to_pydatetime(),
                interval=interval,
                volume=tp.volume,
                turnover=tp.turnover,
                open_interest=tp.open_interest,
                open_price=tp.open_price,
                high_price=tp.high_price,
                low_price=tp.low_price,
                close_price=tp.close_price,
                gateway_name="DB"
            )
            bars.append(bar)

        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> list[TickData]:
        """读取Tick数据"""
        # 转换时间格式
        start = np.datetime64(start)
        start: str = str(start).replace("-", ".")

        end = np.datetime64(end)
        end: str = str(end).replace("-", ".")

        # 读取数据DataFrame
        table: ddb.Table = self.session.loadTable(tableName=self.table_name["tick"], dbPath=self.db_path)

        df: pd.DataFrame = (
            table.select('*')
            .where(f'symbol="{symbol}"')
            .where(f'exchange="{exchange.value}"')
            .where(f'datetime>={start}')
            .where(f'datetime<={end}')
            .toDF()
        )

        if df.empty:
            return []

        df.set_index("datetime", inplace=True)
        df = df.tz_localize(DB_TZ.key)

        # 转换为TickData格式
        ticks: list[TickData] = []

        for tp in df.itertuples():
            tick: TickData = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=tp.Index.to_pydatetime(),
                name=tp.name,
                volume=tp.volume,
                turnover=tp.turnover,
                open_interest=tp.open_interest,
                last_price=tp.last_price,
                last_volume=tp.last_volume,
                limit_up=tp.limit_up,
                limit_down=tp.limit_down,
                open_price=tp.open_price,
                high_price=tp.high_price,
                low_price=tp.low_price,
                pre_close=tp.pre_close,
                bid_price_1=tp.bid_price_1,
                bid_price_2=tp.bid_price_2,
                bid_price_3=tp.bid_price_3,
                bid_price_4=tp.bid_price_4,
                bid_price_5=tp.bid_price_5,
                ask_price_1=tp.ask_price_1,
                ask_price_2=tp.ask_price_2,
                ask_price_3=tp.ask_price_3,
                ask_price_4=tp.ask_price_4,
                ask_price_5=tp.ask_price_5,
                bid_volume_1=tp.bid_volume_1,
                bid_volume_2=tp.bid_volume_2,
                bid_volume_3=tp.bid_volume_3,
                bid_volume_4=tp.bid_volume_4,
                bid_volume_5=tp.bid_volume_5,
                ask_volume_1=tp.ask_volume_1,
                ask_volume_2=tp.ask_volume_2,
                ask_volume_3=tp.ask_volume_3,
                ask_volume_4=tp.ask_volume_4,
                ask_volume_5=tp.ask_volume_5,
                localtime=tp.localtime,
                gateway_name="DB"
            )
            ticks.append(tick)

        return ticks

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange = None,
        interval: Interval = None,
        start: datetime = None,
        end: datetime = None,
    ) -> int:
        """删除K线数据"""
        # 加载数据表
        table: ddb.Table = self.session.loadTable(tableName=self.table_name["bar"], dbPath=self.db_path)

        # 统计数据量
        query = table.select('count(*)')
        if symbol:
            query = query.where(f'symbol="{symbol}"')

        if exchange:
            query = query.where(f'exchange="{exchange.value}"')

        if interval:
            query = query.where(f'interval="{interval.value}"')

        if start:
            start = np.datetime64(start)
            start: str = str(start).replace("-", ".")

            query = query.where(f'datetime >= {start}')

        if end:
            end = np.datetime64(end)
            end: str = str(end).replace("-", ".")

            query = query.where(f'datetime <= {end}')

        df: pd.DataFrame = query.toDF()
        count: int = df["count"][0]

        # 删除K线数据
        query = table.delete()
        if symbol:
            query = query.where(f'symbol="{symbol}"')

        if exchange:
            query = query.where(f'exchange="{exchange.value}"')

        if interval:
            query = query.where(f'interval="{interval.value}"')

        if start:
            query = query.where(f'datetime >= {start}')

        if end:
            query = query.where(f'datetime <= {end}')

        query.execute()

        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除Tick数据"""
        # 加载数据表
        table: ddb.Table = self.session.loadTable(tableName=self.table_name["tick"], dbPath=self.db_path)

        # 统计数据量
        df: pd.DataFrame = (
            table.select('count(*)')
            .where(f'symbol="{symbol}"')
            .where(f'exchange="{exchange.value}"')
            .toDF()
        )
        count: int = df["count"][0]

        # 删除Tick数据
        (
            table.delete()
            .where(f'symbol="{symbol}"')
            .where(f'exchange="{exchange.value}"')
            .execute()
        )

        return count

    def get_bar_overview(self) -> list[BarOverview]:
        """"查询数据库中的K线汇总信息"""
        table: ddb.Table = self.session.loadTable(tableName=self.table_name["baroverview"], dbPath=self.db_path)
        df: pd.DataFrame = table.select('*').toDF()

        overviews: list[BarOverview] = []

        for tp in df.itertuples():
            overview: BarOverview = BarOverview(
                symbol=tp.symbol,
                exchange=Exchange(tp.exchange),
                interval=Interval(tp.interval),
                count=tp.count,
                start=datetime.fromtimestamp(tp.start.to_pydatetime().timestamp(), DB_TZ),
                end=datetime.fromtimestamp(tp.end.to_pydatetime().timestamp(), DB_TZ),
            )
            overviews.append(overview)

        return overviews

    def get_tick_overview(self) -> list[TickOverview]:
        """"查询数据库中的K线汇总信息"""
        table: ddb.Table = self.session.loadTable(tableName=self.table_name["tickoverview"], dbPath=self.db_path)
        df: pd.DataFrame = table.select('*').toDF()

        overviews: list[TickOverview] = []

        for tp in df.itertuples():
            overview: TickOverview = TickOverview(
                symbol=tp.symbol,
                exchange=Exchange(tp.exchange),
                count=tp.count,
                start=datetime.fromtimestamp(tp.start.to_pydatetime().timestamp(), DB_TZ),
                end=datetime.fromtimestamp(tp.end.to_pydatetime().timestamp(), DB_TZ),
            )
            overviews.append(overview)

        return overviews
