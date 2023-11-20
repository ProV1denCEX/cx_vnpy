from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Callable

import pandas as pd
from pyTSL import Client, DoubleToDatetime

from vnpy.trader.database import get_database
from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval, Product
from vnpy.trader.object import BarData, TickData, HistoryRequest, ContractData
from vnpy.trader.utility import ZoneInfo, generate_ticker
from vnpy.trader.datafeed import BaseDatafeed

EXCHANGE_MAP: Dict[Exchange, str] = {
    Exchange.SSE: "SH",
    Exchange.SZSE: "SZ"
}

EXCHANGE_CHINESE_MAP: Dict[str, Exchange] = {
    "大连商品交易所": Exchange.DCE,
    "郑州商品交易所": Exchange.CZCE,
    "上海期货交易所": Exchange.SHFE,
    "上海国际能源交易中心": Exchange.INE,
    "中国金融期货交易所": Exchange.CFFEX,
    "广州期货交易所": Exchange.GFEX,
}


INTERVAL_MAP: Dict[Interval, str] = {
    Interval.MINUTE: "cy_1m",
    Interval.HOUR: "cy_60m",
    Interval.DAILY: "cy_day",
}

SHIFT_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
}

CHINA_TZ = ZoneInfo("Asia/Shanghai")


class TinysoftDatafeed(BaseDatafeed):
    """天软数据服务接口"""

    def __init__(self):
        """"""
        self.username: str = SETTINGS["datafeed.username"]
        self.password: str = SETTINGS["datafeed.password"]

        self.client: Client = None
        self.inited: bool = False

        self.database = get_database()

    def init(self, output: Callable = print) -> bool:
        """初始化"""
        if self.inited:
            return True

        self.client = Client(
            self.username,
            self.password,
            "tsl.tinysoft.com.cn",
            443
        )

        n: int = self.client.login()
        if n != 1:
            output("天软数据服务初始化失败：用户名密码错误！")
            return False

        self.inited = True
        return True

    def query_contract_history(self, req: HistoryRequest, output: Callable = print) -> Optional[List[ContractData]]:
        """
        Query history contract data.
        """
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        if req.product == Product.OPTION:
            raise NotImplementedError

        if req.symbol == "listing_only":
            bk_func = "GetBk"
            bks = "'国内商品期货;股指期货;国债期货'"

        else:
            bk_func = "GetBkAll"
            bks = "'国内商品期货;股指期货;国债期货'"

        cmd = (f"symbols := {bk_func}({bks}); "
               "return select * from infotable 703 of symbols end;")

        contracts = []
        result = self.client.exec(cmd)
        if not result.error():
            data = pd.DataFrame(result.value())
            if not data.empty:
                loc = data["变动日"] > 20100101
                data = data[loc].sort_values('变动日')
                for name, d in data.groupby('StockID'):
                    if len(d) > 1:
                        assert d["变动日"].iat[0] < d["变动日"].iat[-1]

                    contract: ContractData = ContractData(
                        symbol=d["StockName"].iat[0],
                        exchange=EXCHANGE_CHINESE_MAP.get(d["上市地"].iat[0]),
                        name=d["StockID"],
                        product_id=d["交易代码"].iat[0],
                        product=req.product,
                        size=d["合约乘数"].iat[-1],
                        pricetick=d["最小变动价位"].iat[-1],

                        list_date=datetime.strptime(str(d["变动日"].iat[0]), "%Y%m%d"),
                        expire_date=datetime.strptime(str(d["最后交易日"].iat[-1]), "%Y%m%d"),

                        gateway_name="TSL"
                    )

                    contracts.append(contract)

        return contracts

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> Optional[List[BarData]]:
        """查询K线数据"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        symbol, exchange = req.symbol, req.exchange

        contracts = self.database.load_contract_data(symbol=req.symbol, product=req.product)
        if contracts:
            contract = contracts[0]

        else:
            raise ValueError("Contract data not found in db")

        ticker = contract.name

        tsl_exchange: str = EXCHANGE_MAP.get(exchange, "")
        tsl_interval: str = INTERVAL_MAP[req.interval]

        bars: List[BarData] = []

        start_str: str = req.start.strftime("%Y%m%d")
        end_str: str = req.end.strftime("%Y%m%d")

        cmd: str = (
            f"setsysparam(pn_cycle(),{tsl_interval}());"
            "return select * from markettable "
            f"datekey {start_str}T to {end_str}T "
            f"of '{tsl_exchange}{ticker}' end;"
        )
        result = self.client.exec(cmd)

        if not result.error():
            data = pd.DataFrame(result.value())
            if not data.empty:
                data['dt'] = data['date'].apply(DoubleToDatetime)

                shift: timedelta = SHIFT_MAP.get(req.interval, None)
                if shift:
                    data['dt'] -= shift

                data = self.fix_amount(data, multiplier=contract.size)

                for _, d in data.iterrows():
                    bar: BarData = BarData(
                        symbol=d["StockName"],
                        exchange=exchange,
                        datetime=d["dt"].replace(tzinfo=CHINA_TZ),
                        interval=req.interval,
                        open_price=d["open"],
                        high_price=d["high"],
                        low_price=d["low"],
                        close_price=d["close"],
                        volume=d["vol"],
                        turnover=d["amount"],
                        gateway_name="TSL"
                    )

                    # 期货则获取持仓量字段
                    if not tsl_exchange:
                        bar.open_interest = d["sectional_cjbs"]

                    bars.append(bar)

        return bars

    def query_tick_history(self, req: HistoryRequest, output: Callable = print) -> Optional[List[TickData]]:
        """查询Tick数据"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        symbol, exchange = req.symbol, req.exchange
        tsl_exchange: str = EXCHANGE_MAP.get(exchange, "")

        ticks: List[TickData] = []
        dts: Set[datetime] = set()

        dt: datetime = req.start
        while dt <= req.end:
            date_str: str = dt.strftime("%Y%m%d")
            cmd: str = f"return select * from tradetable datekey {date_str}T to {date_str}T+16/24 of '{tsl_exchange}{symbol}' end ; "
            result = self.client.exec(cmd)

            if not result.error():
                data = result.value()
                for d in data:
                    dt: datetime = DoubleToDatetime(d["date"])
                    dt: datetime = dt.replace(tzinfo=CHINA_TZ)

                    # 解决期货缺失毫秒时间戳的问题
                    if dt in dts:
                        dt = dt.replace(microsecond=500000)
                    dts.add(dt)

                    tick: TickData = TickData(
                        symbol=symbol,
                        exchange=exchange,
                        name=d["StockName"],
                        datetime=dt,
                        open_price=d["sectional_open"],
                        high_price=d["sectional_high"],
                        low_price=d["sectional_low"],
                        last_price=d["price"],
                        volume=d["sectional_vol"],
                        turnover=d["sectional_amount"],
                        bid_price_1=d["buy1"],
                        bid_price_2=d["buy2"],
                        bid_price_3=d["buy3"],
                        bid_price_4=d["buy4"],
                        bid_price_5=d["buy5"],
                        ask_price_1=d["sale1"],
                        ask_price_2=d["sale2"],
                        ask_price_3=d["sale3"],
                        ask_price_4=d["sale4"],
                        ask_price_5=d["sale5"],
                        bid_volume_1=d["bc1"],
                        bid_volume_2=d["bc2"],
                        bid_volume_3=d["bc3"],
                        bid_volume_4=d["bc4"],
                        bid_volume_5=d["bc5"],
                        ask_volume_1=d["sc1"],
                        ask_volume_2=d["sc2"],
                        ask_volume_3=d["sc3"],
                        ask_volume_4=d["sc4"],
                        ask_volume_5=d["sc5"],
                        localtime=dt,
                        gateway_name="TSL"
                    )

                    # 期货则获取持仓量字段
                    if not tsl_exchange:
                        tick.open_interest = d["sectional_cjbs"]

                    # 基金获取IOPV字段
                    if d["syl2"]:
                        iopv: float = d["syl2"]
                        if exchange == Exchange.SZSE:  # 深交所的IOPV要除以100才是每股
                            iopv /= 100

                        tick.extra = {"iopv": iopv}

                    ticks.append(tick)

            dt += timedelta(days=1)

        return ticks

    @staticmethod
    def fix_amount(data, **kwargs):
        thres_lower = kwargs.get('thres_lower', 0.8)
        thres_upper = kwargs.get('thres_lower', 1.2)

        col_price = kwargs.get('col_price', 'close')
        col_volume = kwargs.get('col_volume', 'vol')
        col_amount = kwargs.get('col_amount', 'amount')

        multiplier = kwargs.get('multiplier', 1)

        after = data.copy()

        def fix_multiplier(data_):
            return data_.loc[:, col_amount] * multiplier

        def fix_10000(data_):
            return data_.loc[:, col_amount] * 10000

        def fix_multiplier_and_10000(data_):
            return data_.loc[:, col_amount] * 10000 * multiplier

        def fix_replace_w_pvm(data_):
            return data_.loc[:, col_price] * data_.loc[:, col_volume] * multiplier

        to_fix = data.copy()
        bias = to_fix.loc[:, col_amount] / (
                to_fix.loc[:, col_price] * to_fix.loc[:, col_volume] * multiplier)
        loc = (bias > thres_lower) & (bias < thres_upper)
        to_fix = to_fix[~loc]

        for func in [fix_multiplier, fix_10000, fix_multiplier_and_10000, fix_replace_w_pvm]:
            if to_fix.empty:
                break

            fix = func(to_fix)
            bias = fix / (to_fix.loc[:, col_price] * to_fix.loc[:, col_volume] * multiplier)

            loc = (bias > thres_lower) & (bias < thres_upper)
            fixed = fix[loc]
            to_fix = to_fix[~loc]

            after.loc[fixed.index, col_amount] = fixed

        return after
