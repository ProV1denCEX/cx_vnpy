from datetime import datetime
from enum import Enum

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

from Pandora.helper import DateFmt


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
        self.pool: ddb.DBConnectionPool = ddb.DBConnectionPool(self.host, self.port, 2, self.user, self.password)

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

    def get_table_name(self, kind, product):
        if kind == "bar":
            if product == Product.FUTURES:
                return self.table_name["bar_futures"]
            elif product == Product.OPTION:
                return self.table_name["bar_options"]
            else:
                return self.table_name["bar"]

        elif kind == "contract":
            if product == Product.FUTURES:
                return self.table_name["contract_futures"]
            elif product == Product.OPTION:
                return self.table_name["contract_options"]
            else:
                return self.table_name["contract"]

        else:
            return self.table_name[kind]

    def query(self, table, **kwargs):
        table: ddb.Table = self.session.loadTable(tableName=table, dbPath=self.db_path)

        fields = kwargs.pop("fields", "*")

        query = table.select(fields)
        for k, v in kwargs.items():
            if not v:
                continue

            if k == "start":
                start = v.strftime(DateFmt.dolphin_datetime.value)
                query = query.where(f'datetime>={start}')

            elif k == "end":
                end = v.strftime(DateFmt.dolphin_datetime.value)
                query = query.where(f'datetime<={end}')

            elif k == "where":
                query = query.where(v)

            elif isinstance(v, Enum):
                query = query.where(f'{k}="{v.value}"')

            else:
                query = query.where(f'{k}="{v}"')

        df: pd.DataFrame = query.toDF()

        return df

    def save_contract_data(self, contracts: list[ContractData]) -> bool:
        contracts_to_db = []
        futures_to_db = []
        options_to_db = []

        dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        for contract in contracts:
            if contract.product == Product.FUTURES:
                d: dict = {
                    "symbol": contract.symbol,
                    "exchange": contract.exchange.value,
                    "datetime": contract.datetime or dt,
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
                    "datetime": contract.datetime or dt,
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

            else:
                d: dict = {
                    "symbol": contract.symbol,
                    "exchange": contract.exchange.value,
                    "datetime": contract.datetime or dt,
                    "name": contract.name,
                    "product": contract.product.value,
                    "product_id": contract.product_id,
                    "size": contract.size,
                    "pricetick": contract.pricetick,
                    "list_date": contract.list_date,
                    "expire_date": contract.expire_date,
                    "min_volume": contract.min_volume,
                }

                contracts_to_db.append(d)

        if futures_to_db:
            df: pd.DataFrame = pd.DataFrame.from_records(futures_to_db)
            appender: ddb.PartitionedTableAppender = ddb.PartitionedTableAppender(self.db_path,
                                                                                  self.table_name["contract_futures"],
                                                                                  "expire_date", self.pool)
            appender.append(df)

        if options_to_db:
            df: pd.DataFrame = pd.DataFrame.from_records(options_to_db)
            appender: ddb.PartitionedTableAppender = ddb.PartitionedTableAppender(self.db_path,
                                                                                  self.table_name["contract_options"],
                                                                                  "datetime", self.pool)
            appender.append(df)

        if contracts_to_db:
            df: pd.DataFrame = pd.DataFrame.from_records(contracts_to_db)
            appender: ddb.PartitionedTableAppender = ddb.PartitionedTableAppender(self.db_path,
                                                                                  self.table_name["contract"],
                                                                                  "expire_date", self.pool)
            appender.append(df)

        return True

    def save_bar_data(self, bars: list[BarData], stream: bool = False, product: Product = None) -> bool:
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

        table_name = self.get_table_name("bar", product)

        appender: ddb.PartitionedTableAppender = ddb.PartitionedTableAppender(
            self.db_path,
            table_name,
            "datetime",
            self.pool
        )
        appender.append(df)

        return True

    def save_tick_data(self, ticks: list[TickData], stream: bool = False, product: Product = None) -> bool:
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
        table_name = self.get_table_name("tick", product)

        appender: ddb.PartitionedTableAppender = ddb.PartitionedTableAppender(
            self.db_path,
            table_name,
            "datetime",
            self.pool
        )
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
        table_name = self.get_table_name("contract", product)
        table: ddb.Table = self.session.loadTable(tableName=table_name, dbPath=self.db_path)

        query = table.select('*')

        if symbol:
            query = query.where(f'symbol="{symbol}"')

        if product == Product.FUTURES:
            if start:
                start = start.strftime(DateFmt.dolphin_datetime.value)
                query = query.where(f'expire_date>={start}')

            if end:
                end = end.strftime(DateFmt.dolphin_datetime.value)
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
            if start:
                start = start.strftime(DateFmt.dolphin_datetime.value)
                query = query.where(f'datetime >= {start}')

            if end:
                end = end.strftime(DateFmt.dolphin_datetime.value)
                query = query.where(f'datetime <= {end}')

            df: pd.DataFrame = query.sort("datetime").toDF()

            if df.empty:
                return []

            for symbol, info in df.groupby("symbol"):
                last_info = info.iloc[-1, :]

                contract = ContractData(
                    symbol=symbol,
                    exchange=Exchange[last_info['exchange']],
                    name=last_info['name'],
                    product=product,
                    product_id=last_info['product_id'],
                    size=last_info['size'],
                    pricetick=last_info['pricetick'],

                    list_date=last_info['list_date'].to_pydatetime(),
                    expire_date=last_info['expire_date'].to_pydatetime(),
                    datetime=last_info['datetime'].to_pydatetime(),

                    min_volume=last_info['min_volume'],

                    option_strike=last_info['option_strike'],
                    option_underlying=last_info['option_underlying'],
                    option_type=OptionType.from_str(last_info['option_type']),
                    option_portfolio=last_info['option_portfolio'],
                    option_index=last_info['option_index'],

                    option_listed=last_info['list_date'].to_pydatetime(),
                    option_expiry=last_info['expire_date'].to_pydatetime(),

                    gateway_name="DB"
                )
                contracts.append(contract)

        else:
            if product:
                query = query.where(f'product="{product.value}"')

            if start:
                start = start.strftime(DateFmt.dolphin_datetime.value)
                query = query.where(f'expire_date>={start}')

            if end:
                end = end.strftime(DateFmt.dolphin_datetime.value)
                query = query.where(f'list_date<={end}')

            df: pd.DataFrame = query.toDF()

            if df.empty:
                return []

            for tp in df.itertuples():
                contract = ContractData(
                    symbol=tp.symbol,
                    exchange=Exchange[tp.exchange],
                    name=tp.name,
                    product=Product(tp.product),
                    product_id=tp.product_id,
                    size=tp.size,
                    pricetick=tp.pricetick,
                    list_date=tp.list_date.to_pydatetime(),
                    expire_date=tp.expire_date.to_pydatetime(),
                    min_volume=tp.min_volume,

                    gateway_name="DB"
                )
                contracts.append(contract)

        return contracts

    def load_bar_data(
            self,
            symbol: str,
            exchange: Exchange,
            product: Product,
            interval: Interval,
            start: datetime,
            end: datetime
    ) -> list[BarData]:
        """读取K线数据"""
        table_name = self.get_table_name("bar", product)

        # 加载数据表
        table: ddb.Table = self.session.loadTable(tableName=table_name, dbPath=self.db_path)

        # 转换时间格式
        start = start.strftime(DateFmt.dolphin_datetime.value)
        end = end.strftime(DateFmt.dolphin_datetime.value)

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
        start = start.strftime(DateFmt.dolphin_datetime.value)
        end = end.strftime(DateFmt.dolphin_datetime.value)

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
            product: Product = None,
            interval: Interval = None,
            start: datetime = None,
            end: datetime = None,
    ) -> int:
        """删除K线数据"""
        table_name = self.get_table_name("bar", product)

        # 加载数据表
        table: ddb.Table = self.session.loadTable(tableName=table_name, dbPath=self.db_path)

        # 统计数据量
        query = table.select('count(*)')
        if symbol:
            query = query.where(f'symbol="{symbol}"')

        if exchange:
            query = query.where(f'exchange="{exchange.value}"')

        if interval:
            query = query.where(f'interval="{interval.value}"')

        if start:
            start = start.strftime(DateFmt.dolphin_datetime.value)
            query = query.where(f'datetime >= {start}')

        if end:
            end = end.strftime(DateFmt.dolphin_datetime.value)
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
            exchange: Exchange,
            product: Product = None,
            start: datetime = None,
            end: datetime = None,
    ) -> int:
        """删除Tick数据"""
        # 加载数据表
        table_name = self.get_table_name("tick", product)
        table: ddb.Table = self.session.loadTable(tableName=table_name, dbPath=self.db_path)

        # 统计数据量
        query = table.select('count(*)')
        if symbol:
            query = query.where(f'symbol="{symbol}"')

        if exchange:
            query = query.where(f'exchange="{exchange.value}"')

        if start:
            start = start.strftime(DateFmt.dolphin_datetime.value)
            query = query.where(f'datetime >= {start}')

        if end:
            end = end.strftime(DateFmt.dolphin_datetime.value)
            query = query.where(f'datetime <= {end}')

        df: pd.DataFrame = query.toDF()
        count: int = df["count"][0]

        # 删除Tick数据
        query = table.delete()
        if symbol:
            query = query.where(f'symbol="{symbol}"')

        if exchange:
            query = query.where(f'exchange="{exchange.value}"')

        if start:
            query = query.where(f'datetime >= {start}')

        if end:
            query = query.where(f'datetime <= {end}')

        query.execute()

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
