from copy import copy
from datetime import timedelta, time, datetime

from Pandora.constant import SymbolSuffix
from Pandora.helper import TDays
from vnpy.event import Event, EventEngine
from vnpy.trader.database import get_database
from Pandora.trader.object import LogData, TickData, SubscribeRequest, ContractData
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.event import EVENT_LOG, EVENT_CONTRACT, EVENT_TICK
from Pandora.constant import Product

APP_NAME = "MainContractManager"
GATEWAY_NAME = "MC"


class MainContractManager(BaseEngine):
    """Tick 数据引擎，负责Tick过滤、主连Tick实时生成"""

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self._name_mc_map = {}
        self._mc_symbols = {}

        self.mc_symbol_map = {}
        self.mc_symbol_gateway = {}

        self.symbol_coef = {}
        self.symbol_mc_map = {}

        self.main_contracts = []

        self.last_tick_time = {}
        self.thres_dt_local = timedelta(minutes=1)

        self._subscribe = self.main_engine.subscribe
        self.main_engine.subscribe = self.subscribe

        self.load_main_contracts()
        self.map_contracts()
        self.register_event()

        for i in self.main_contracts:
            self.put_event(EVENT_CONTRACT, i)

    def put_event(self, event_type: str, data) -> None:
        """"""
        event: Event = Event(event_type, data)
        self.event_engine.put(event)

    def load_main_contracts(self):
        # load mc contract obj
        database = get_database()
        today = datetime.combine(TDays.get_tday(fmt=None), time(0))
        contracts = database.load_contract_data(product=Product.FUTURES, start=today, end=today)
        self.main_contracts = [contract for contract in contracts if contract.symbol == contract.product_id + SymbolSuffix.MC]
        # mnc = [contract for contract in contracts if contract.symbol == contract.product_id + SymbolSuffix.MNC]

        if self.main_contracts:
            self._name_mc_map = {i.name: i for i in self.main_contracts}
            self._mc_symbols = {i.symbol for i in self.main_contracts}

            self.write_log(f"主连合约加载成功，主连数量: {len(self.main_contracts)}")

        else:
            self.write_log(f"主连合约加载失败，请检查!!!")

    def map_contracts(self):
        contracts = self.main_engine.get_all_contracts()
        for i in contracts:
            self.map_contract(i)

    def map_contract(self, contract: ContractData):
        # 过滤自己产生的contract
        if contract.symbol in self._mc_symbols:
            return

        # 注册数据
        if contract.name in self._name_mc_map:
            mc = self._name_mc_map[contract.name]

            self.mc_symbol_map[mc.symbol] = contract.symbol
            self.mc_symbol_gateway[mc.symbol] = contract.gateway_name

            self.symbol_coef[contract.symbol] = mc.min_volume
            self.symbol_mc_map[contract.symbol] = mc.symbol

    def register_event(self) -> None:
        """"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_contract_event)

    def process_contract_event(self, event: Event) -> None:
        """"""
        contract: ContractData = event.data
        self.map_contract(contract)

    def process_tick_event(self, event: Event) -> None:
        """"""
        tick: TickData = event.data

        if tick.symbol in self.symbol_coef:
            tick_mc = copy(tick)
            coef = self.symbol_coef[tick.symbol]

            tick_mc.symbol = self.symbol_mc_map[tick.symbol]
            tick_mc.turnover *= coef
            tick_mc.last_price *= coef
            tick_mc.limit_down *= coef
            tick_mc.limit_up *= coef
            tick_mc.open_price *= coef
            tick_mc.high_price *= coef
            tick_mc.low_price *= coef
            tick_mc.pre_close *= coef

            tick_mc.bid_price_1 *= coef
            tick_mc.bid_price_2 *= coef
            tick_mc.bid_price_3 *= coef
            tick_mc.bid_price_4 *= coef
            tick_mc.bid_price_5 *= coef

            tick_mc.ask_price_1 *= coef
            tick_mc.ask_price_2 *= coef
            tick_mc.ask_price_3 *= coef
            tick_mc.ask_price_4 *= coef
            tick_mc.ask_price_5 *= coef

            tick_mc.gateway_name = GATEWAY_NAME

            tick_mc.__post_init__()

            self.put_event(EVENT_TICK, tick_mc)
            self.put_event(EVENT_TICK + tick_mc.vt_symbol, tick_mc)

    def subscribe(self, req: SubscribeRequest, gateway_name: str) -> None:
        if req.symbol in self.mc_symbol_map:
            mc_symbol = req.symbol
            req.symbol = self.mc_symbol_map[mc_symbol]
            gateway_name = self.mc_symbol_gateway[mc_symbol]

            req.__post_init__()

        self._subscribe(req, gateway_name)

    def get_main_contracts(self):
        return self.main_contracts

    def write_log(self, msg: str) -> None:
        """"""
        log: LogData = LogData(msg=msg, gateway_name=APP_NAME)
        event: Event = Event(type=EVENT_LOG, data=log)
        self.event_engine.put(event)
