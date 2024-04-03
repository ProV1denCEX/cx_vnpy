from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Callable

import pandas as pd
from pyTSL import Client, DoubleToDatetime

from Pandora.constant import SymbolSuffix
from vnpy.trader.database import get_database
from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval, Product, OptionType
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

    "上海证券交易所": Exchange.SSE,
    "深圳证券交易所": Exchange.SZSE,
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

OPTION_TYPE_MAP = {
    "认购": OptionType.CALL,
    "认沽": OptionType.PUT,
}


INDEX_SYMBOL_MAP = {
    "IO": "000300",
    "MO": "000852",
    "HO": "000016",
}


CHINA_TZ = ZoneInfo("Asia/Shanghai")

LISTING_SYMBOL = "listing_only"
ALL_SYMBOL = "all"


def get_option_symbol(symbol):
    if symbol.startswith("OP"):
        return symbol.replace("OP", "")

    else:
        return symbol


def get_contract(symbol: str):
    return ''.join((i for i in symbol if i.isalpha()))


def get_option_product_info(series, exchange: Exchange):
    if exchange == Exchange.CFFEX:
        symbol = series["StockID"]
        underlying = symbol.split("-")[0]
        portfolio = get_contract(underlying)

    elif exchange in {Exchange.SSE, Exchange.SZSE}:
        symbol = series["标的证券代码"]
        underlying = symbol[2:]
        portfolio = underlying + "_O"

    else:
        underlying = series["标的证券代码"]
        portfolio = get_contract(underlying) + "_o"

    return underlying, portfolio


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

    def query_trade_time(self, output: Callable = print):
        """
        Query history contract data.
        """
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        pass

    def query_contract_history(self, req: HistoryRequest, output: Callable = print) -> Optional[List[ContractData]]:
        """
        Query history contract data.
        """
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        contracts = []

        if req.product == Product.OPTION:
            bk_func = "GetBk"
            bks = "ALLOptionsBK()"

            start_str: str = req.start.strftime("%Y%m%d")
            end_str: str = req.end.strftime("%Y%m%d")

            cmd = (f"symbols := {bk_func}({bks}); "
                   "return select * from infotable 720 "
                   "of symbols "
                   f"where ['截止日'] >= {start_str} and ['截止日'] <= {end_str} "
                   "end;")

            result = self.client.exec(cmd)
            if not result.error():
                data = pd.DataFrame(result.value())

                if not data.empty:
                    data["首个交易日"] = pd.to_datetime(data["首个交易日"].astype(str))
                    data["最后交易日"] = pd.to_datetime(data["最后交易日"].astype(str))
                    data["截止日"] = pd.to_datetime(data["截止日"].astype(str))

                    for idx, d in data.iterrows():
                        exchange = EXCHANGE_CHINESE_MAP.get(d["上市地"])
                        underlying, portfolio = get_option_product_info(d, exchange)

                        contract: ContractData = ContractData(
                            symbol=get_option_symbol(d["StockID"]),
                            exchange=exchange,
                            name=d["StockName"],
                            product_id=portfolio,
                            product=req.product,
                            size=d["合约单位"],
                            pricetick=d["最小报价单位"],

                            list_date=d["首个交易日"],
                            expire_date=d["最后交易日"],
                            datetime=d["截止日"],

                            option_strike=d['行权价'],
                            option_underlying=underlying,
                            option_type=OPTION_TYPE_MAP[d['期权类型']],

                            gateway_name="TSL"
                        )

                        contract.option_listed = contract.list_date
                        contract.option_expiry = contract.expire_date
                        contract.option_index = str(contract.option_strike)  # same w ctp
                        contract.option_portfolio = contract.product_id

                        contracts.append(contract)

        elif req.product == Product.FUTURES:
            if req.symbol == LISTING_SYMBOL:
                bk_func = "GetBk"

            else:
                bk_func = "GetBkAll"

            bks = "'国内商品期货;股指期货;国债期货'"

            cmd = (f"symbols := {bk_func}({bks}); "
                   "return select * from infotable 703 "
                   "of symbols end;")

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

        elif req.product == Product.ETF:
            underlying_symbols = self.database.query(
                self.database.table_name["contract_options"],
                fields="distinct option_underlying, exchange",
                where='exchange in ("SSE", "SZSE")',
            )

            for i, d in underlying_symbols.iterrows():
                contract = ContractData(
                    symbol=d['option_underlying'],
                    exchange=Exchange[d['exchange']],
                    name=d['option_underlying'],
                    product_id=d['option_underlying'],
                    product=Product.ETF,
                    size=10000,
                    pricetick=0.0001,
                    list_date=datetime(2000, 1, 1),
                    expire_date=datetime(2100, 1, 1),

                    gateway_name="TSL"
                )

                contracts.append(contract)

        elif req.product == Product.INDEX:
            underlying_symbols = self.database.query(
                self.database.table_name["contract_options"],
                fields="distinct product_id, exchange",
                where='exchange = "CFFEX"',
            )

            for i, d in underlying_symbols.iterrows():
                symbol = INDEX_SYMBOL_MAP.get(d['product_id'], d['product_id'])
                contract = ContractData(
                    symbol=symbol,
                    exchange=Exchange.SSE,
                    name=d['product_id'],
                    product_id=symbol,
                    product=Product.INDEX,
                    size=1,
                    pricetick=0.01,
                    list_date=datetime(2000, 1, 1),
                    expire_date=datetime(2100, 1, 1),

                    gateway_name="TSL"
                )

                contracts.append(contract)

        else:
            raise NotImplementedError

        return contracts

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> Optional[List[BarData]]:
        """查询K线数据"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        if req.symbol in {LISTING_SYMBOL, ALL_SYMBOL}:
            bars = self.query_bar_history_by_date(req, output)

        else:
            bars = self.query_bar_history_by_contract(req, output)

        return bars

    def query_bar_history_by_date(self, req: HistoryRequest, output: Callable = print) -> Optional[List[BarData]]:
        if req.product == Product.FUTURES:
            bks = "'国内商品期货;股指期货;国债期货'"

            if req.symbol == LISTING_SYMBOL:
                bk_func = "GetBk"

            else:
                bk_func = "GetBkAll"

        elif req.product == Product.OPTION:
            bk_func = "GetBk"
            bks = "ALLOptionsBK()"

        else:
            raise NotImplementedError

        tsl_ticker = f"{bk_func}({bks})"
        tsl_interval: str = INTERVAL_MAP[req.interval]

        bars: List[BarData] = []

        start_str: str = req.start.strftime("%Y%m%d.%H%M%ST")
        end_str: str = req.end.strftime("%Y%m%d.%H%M%ST")

        cmd: str = (
            f"setsysparam(pn_cycle(),{tsl_interval}());"
            "setsysparam('cyclefilter', 1);"
            "return select * from markettable "
            f"datekey {start_str} to {end_str} "
            f"of {tsl_ticker} end;"
        )
        result = self.client.exec(cmd)

        if result.error():
            output(result)

        else:
            data = pd.DataFrame(result.value())
            if not data.empty:
                data['dt'] = data['date'].apply(DoubleToDatetime)

                shift: timedelta = SHIFT_MAP.get(req.interval, None)
                if shift:
                    data['dt'] -= shift

                contracts = self.database.load_contract_data(product=req.product, start=req.start.replace(hour=0, minute=0, second=0))

                contract_info = pd.DataFrame({
                    'symbol': [contract.symbol for contract in contracts],
                    'name': [contract.name for contract in contracts],
                    'exchange': [contract.exchange for contract in contracts],
                    'size': [contract.size for contract in contracts],
                })

                if req.product == Product.FUTURES:
                    mc_symbol = {contract.product_id + SymbolSuffix.MC for contract in contracts}
                    mnc_symbol = {contract.product_id + SymbolSuffix.MNC for contract in contracts}

                    contract_info = contract_info.loc[~contract_info['symbol'].isin(mc_symbol | mnc_symbol)]

                    data['StockID'] = data['StockID'].str.upper()
                    data = data.merge(contract_info, how='left', left_on='StockID', right_on='name')

                    data = self.fix_amount(data, multiplier='size')

                elif req.product == Product.OPTION:
                    data['StockID'] = data['StockID'].apply(get_option_symbol)
                    data = data.merge(contract_info, how='left', left_on='StockID', right_on='symbol')

                if pd.isna(data).any().any():
                    output("WARNING: na detected in tsl data query. check contract info!")
                    data = data.dropna()

                data = self.fix_zero_price(data)

                for _, d in data.iterrows():
                    bar: BarData = BarData(
                        symbol=d["symbol"],
                        exchange=d["exchange"],
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
                    if req.product in {Product.FUTURES, Product.OPTION}:
                        bar.open_interest = d["sectional_cjbs"]

                    bars.append(bar)

        return bars

    def query_bar_history_by_contract(self, req: HistoryRequest, output: Callable = print) -> Optional[List[BarData]]:
        if req.contract:
            contract = req.contract

        else:
            contracts = self.database.load_contract_data(symbol=req.symbol, product=req.product, start=req.start, end=req.end)
            if contracts:
                contract = contracts[0]

            else:
                raise ValueError("Contract data not found in db")

        symbol, exchange, ticker = contract.symbol, contract.exchange, contract.name

        timeout = 0
        if req.product == Product.OPTION:
            if exchange in {Exchange.SSE, Exchange.SZSE}:
                tsl_ticker = f"'OP{symbol}'"
                timeout = int(1000 * 5)

            else:
                tsl_ticker = f"'{symbol}'"

        elif req.product == Product.FUTURES:
            tsl_exchange: str = EXCHANGE_MAP.get(exchange, "")
            tsl_ticker = f"'{tsl_exchange}{ticker}'"

        else:
            tsl_exchange: str = EXCHANGE_MAP.get(exchange, "")
            tsl_ticker = f"'{tsl_exchange}{symbol}'"

        tsl_interval: str = INTERVAL_MAP[req.interval]

        bars: List[BarData] = []

        start_str: str = req.start.strftime("%Y%m%d.%H%M%ST")
        end_str: str = req.end.strftime("%Y%m%d.%H%M%ST")

        cmd: str = (
            f"setsysparam(pn_cycle(),{tsl_interval}());"
            "setsysparam('cyclefilter', 1);"
            "return select * from markettable "
            f"datekey {start_str} to {end_str} "
            f"of {tsl_ticker} end;"
        )
        result = self.client.exec(cmd, timeout=timeout)

        if result.error():
            output(result)

        else:
            data = pd.DataFrame(result.value())
            if not data.empty:
                data['dt'] = data['date'].apply(DoubleToDatetime)

                shift: timedelta = SHIFT_MAP.get(req.interval, None)
                if shift:
                    data['dt'] -= shift

                if req.product == Product.FUTURES:
                    data = self.fix_amount(data, multiplier=contract.size)  # Option's size is variable

                data = self.fix_zero_price(data)

                for _, d in data.iterrows():
                    bar: BarData = BarData(
                        symbol=symbol,
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
                    if req.product in {Product.FUTURES, Product.OPTION}:
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
    def fix_zero_price(data, **kwargs):
        col_price = kwargs.get('col_price', 'close')
        col_volume = kwargs.get('col_volume', 'vol')

        col_prices = kwargs.get('col_prices', ['open', 'high', 'low', 'close'])

        after = data.copy()

        loc = data[col_volume] == 0
        after.loc[loc, col_prices] = after.loc[loc, col_price]

        return after

    @staticmethod
    def fix_amount(data, **kwargs):
        thres_lower = kwargs.get('thres_lower', 0.8)
        thres_upper = kwargs.get('thres_lower', 1.2)

        col_price = kwargs.get('col_price', 'close')
        col_volume = kwargs.get('col_volume', 'vol')
        col_amount = kwargs.get('col_amount', 'amount')

        multiplier = kwargs.get('multiplier', 1)

        after = data.copy()

        def get_multiplier(data_):
            if isinstance(multiplier, str):
                return data_[multiplier]

            else:
                return multiplier

        def fix_multiplier(data_):
            return data_.loc[:, col_amount] * get_multiplier(data_)

        def fix_10000(data_):
            return data_.loc[:, col_amount] * 10000

        def fix_multiplier_and_10000(data_):
            return data_.loc[:, col_amount] * 10000 * get_multiplier(data_)

        def fix_replace_w_pvm(data_):
            return data_.loc[:, col_price] * data_.loc[:, col_volume] * get_multiplier(data_)

        to_fix = data.copy()
        bias = to_fix.loc[:, col_amount] / (
                to_fix.loc[:, col_price] * to_fix.loc[:, col_volume] * get_multiplier(to_fix))
        loc = (bias > thres_lower) & (bias < thres_upper)
        to_fix = to_fix[~loc]

        for func in [fix_multiplier, fix_10000, fix_multiplier_and_10000, fix_replace_w_pvm]:
            if to_fix.empty:
                break

            fix = func(to_fix)
            bias = fix / (to_fix.loc[:, col_price] * to_fix.loc[:, col_volume] * get_multiplier(to_fix))

            loc = (bias > thres_lower) & (bias < thres_upper)
            fixed = fix[loc]
            to_fix = to_fix[~loc]

            after.loc[fixed.index, col_amount] = fixed

        return after
