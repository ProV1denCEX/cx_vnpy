import importlib
import glob
import traceback
from collections import defaultdict
from pathlib import Path
from types import ModuleType
from typing import Dict, List, Set, Tuple, Type, Any, Callable, Optional
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from Pandora.helper import TDays
from vnpy.event import Event, EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
from Pandora.trader.object import (
    SubscribeRequest,
    HistoryRequest,
    LogData,
    TickData,
    BarData,
    ContractData
)
from vnpy.trader.event import (
    EVENT_TICK
)
from Pandora.constant import (
    Direction,
    Interval,
    Offset
)
from Pandora.trader.utility import load_json, save_json, extract_vt_symbol
from vnpy.trader.database import BaseDatabase, get_database, DB_TZ

from .base import (
    APP_NAME,
    EVENT_PORTFOLIO_LOG,
    EVENT_PORTFOLIO_STRATEGY,
    EngineType
)
from .template import StrategyTemplate


class SignalEngine(BaseEngine):
    """组合策略引擎"""

    engine_type: EngineType = EngineType.SIGNAL

    setting_filename: str = "portfolio_strategy_setting.json"
    data_filename: str = "portfolio_strategy_data.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.strategy_data: Dict[str, Dict] = {}

        self.classes: Dict[str, Type[StrategyTemplate]] = {}
        self.strategies: Dict[str, StrategyTemplate] = {}

        self.symbol_strategy_map: Dict[str, List[StrategyTemplate]] = defaultdict(list)
        self.orderid_strategy_map: Dict[str, StrategyTemplate] = {}

        self.init_executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=1)

        self.vt_tradeids: Set[str] = set()

        # 数据库和数据服务
        self.database: BaseDatabase = get_database()

        self.trade_date = TDays.get_tday(end_hour=20)

    def init_engine(self) -> None:
        """初始化引擎"""
        self.load_trade_dates()
        self.load_strategy_class()
        self.load_strategy_setting()
        self.load_strategy_data()
        self.register_event()
        self.write_log("组合策略引擎初始化成功")

    def load_trade_dates(self):
        self.trade_date = TDays.get_tday(end_hour=20)

    def close(self) -> None:
        """关闭"""
        self.stop_all_strategies()

    def register_event(self) -> None:
        """注册事件引擎"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)

    def process_tick_event(self, event: Event) -> None:
        """行情数据推送"""
        tick: TickData = event.data

        strategies: list = self.symbol_strategy_map[tick.vt_symbol]
        if not strategies:
            return

        for strategy in strategies:
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_tick, tick)

    def send_order(
        self,
        strategy: StrategyTemplate,
        vt_symbol: str,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        lock: bool,
        net: bool,
    ) -> list:
        """发送委托"""
        self.write_log(f"委托失败，信号模式下，引擎不会处理任何订单与交易.", strategy)

        return []

    def cancel_order(self, strategy: StrategyTemplate, vt_orderid: str) -> None:
        """委托撤单"""
        self.write_log(f"撤单失败，信号模式下，引擎不会处理任何订单与交易.", strategy)

    def get_engine_type(self) -> EngineType:
        """获取引擎类型"""
        return self.engine_type

    def get_contract(self, strategy: StrategyTemplate, vt_symbol: str) -> Optional[ContractData]:
        """获取合约价格跳动"""
        return self.main_engine.get_contract(vt_symbol)

    def get_pricetick(self, strategy: StrategyTemplate, vt_symbol: str) -> float:
        """获取合约价格跳动"""
        contract: Optional[ContractData] = self.main_engine.get_contract(vt_symbol)

        if contract:
            return contract.pricetick
        else:
            return None

    def get_size(self, strategy: StrategyTemplate, vt_symbol: str) -> int:
        """获取合约乘数"""
        contract: Optional[ContractData] = self.main_engine.get_contract(vt_symbol)

        if contract:
            return contract.size
        else:
            return None

    def load_bars(self, strategy: StrategyTemplate, days: int, interval: Interval) -> None:
        """加载历史数据"""
        vt_symbols: list = strategy.vt_symbols
        dts: Set[datetime] = set()
        history_data: Dict[Tuple, BarData] = {}

        # 通过接口、数据服务、数据库获取历史数据
        for vt_symbol in vt_symbols:
            data: List[BarData] = self.load_bar(vt_symbol, days, interval)

            for bar in data:
                dts.add(bar.datetime)
                history_data[(bar.datetime, vt_symbol)] = bar

        dts: list = list(dts)
        dts.sort()

        for dt in dts:
            bars: dict = {}

            for vt_symbol in vt_symbols:
                bar: Optional[BarData] = history_data.get((dt, vt_symbol), None)

                # 如果获取到合约指定时间的历史数据，缓存进bars字典
                if bar:
                    bars[vt_symbol] = bar

            self.call_strategy_func(strategy, strategy.on_bars, bars)

    def load_bar(self, vt_symbol: str, days: int, interval: Interval) -> List[BarData]:
        """加载单个合约历史数据"""
        symbol, exchange = extract_vt_symbol(vt_symbol)
        end: datetime = datetime.now(DB_TZ)
        start: datetime = end - timedelta(days)
        contract: Optional[ContractData] = self.main_engine.get_contract(vt_symbol)
        data: List[BarData]

        # 通过接口获取历史数据
        if contract and contract.history_data:
            req: HistoryRequest = HistoryRequest(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                start=start,
                end=end
            )
            data = self.main_engine.query_history(req, contract.gateway_name)

        # 通过数据库获取历史数据
        else:
            data = self.database.load_bar_data(
                symbol=symbol,
                exchange=exchange,
                product=contract.product,
                interval=interval,
                start=start,
                end=end,
            )

        return data

    def call_strategy_func(
        self, strategy: StrategyTemplate, func: Callable, params: Any = None
    ) -> None:
        """安全调用策略函数"""
        try:
            if params:
                func(params)
            else:
                func()
        except Exception:
            strategy.trading = False
            strategy.inited = False

            msg: str = f"触发异常已停止\n{traceback.format_exc()}"
            self.write_log(msg, strategy)

    def add_strategy(
        self, class_name: str, strategy_name: str, vt_symbols: list, setting: dict
    ) -> None:
        """添加策略实例"""
        if strategy_name in self.strategies:
            self.write_log(f"创建策略失败，存在重名{strategy_name}")
            return

        strategy_class: Optional[StrategyTemplate] = self.classes.get(class_name, None)
        if not strategy_class:
            self.write_log(f"创建策略失败，找不到策略类{class_name}")
            return

        strategy: StrategyTemplate = strategy_class(self, strategy_name, vt_symbols, setting)
        self.strategies[strategy_name] = strategy

        for vt_symbol in vt_symbols:
            strategies: list = self.symbol_strategy_map[vt_symbol]
            strategies.append(strategy)

        self.save_strategy_setting()
        self.put_strategy_event(strategy)

    def init_strategy(self, strategy_name: str) -> None:
        """初始化策略"""
        self.init_executor.submit(self._init_strategy, strategy_name)

    def _init_strategy(self, strategy_name: str) -> None:
        """初始化策略"""
        strategy: StrategyTemplate = self.strategies[strategy_name]

        if strategy.inited:
            self.write_log(f"{strategy_name}已经完成初始化，禁止重复操作")
            return

        self.write_log(f"{strategy_name}开始执行初始化")

        # 调用策略on_init函数
        self.call_strategy_func(strategy, strategy.on_init)

        # 恢复策略状态
        data: Optional[dict] = self.strategy_data.get(strategy_name, None)
        if data:
            for name in strategy.variables:
                value: Optional[Any] = data.get(name, None)
                if value is None:
                    continue

                # 对于持仓和目标数据字典，需要使用dict.update更新defaultdict
                if name in {"pos_data", "target_data"}:
                    strategy_data = getattr(strategy, name)
                    strategy_data.update(value)
                # 对于其他int/float/str/bool字段则可以直接赋值
                else:
                    setattr(strategy, name, value)

        # 订阅行情
        for vt_symbol in strategy.vt_symbols:
            contract: Optional[ContractData] = self.main_engine.get_contract(vt_symbol)
            if contract:
                req: SubscribeRequest = SubscribeRequest(
                    symbol=contract.symbol, exchange=contract.exchange)
                self.main_engine.subscribe(req, contract.gateway_name)
            else:
                self.write_log(f"行情订阅失败，找不到合约{vt_symbol}", strategy)

        # 推送策略事件通知初始化完成状态
        strategy.inited = True
        self.put_strategy_event(strategy)
        self.write_log(f"{strategy_name}初始化完成")

    def start_strategy(self, strategy_name: str) -> None:
        """启动策略"""
        strategy: StrategyTemplate = self.strategies[strategy_name]
        if not strategy.inited:
            self.write_log(f"策略{strategy.strategy_name}启动失败，请先初始化")
            return

        if strategy.trading:
            self.write_log(f"{strategy_name}已经启动，请勿重复操作")
            return

        # 调用策略on_start函数
        self.call_strategy_func(strategy, strategy.on_start)

        # 推送策略事件通知启动完成状态
        strategy.trading = True
        self.put_strategy_event(strategy)

    def stop_strategy(self, strategy_name: str) -> None:
        """停止策略"""
        strategy: StrategyTemplate = self.strategies[strategy_name]
        if not strategy.trading:
            return

        # 调用策略on_stop函数
        self.call_strategy_func(strategy, strategy.on_stop)

        # 将交易状态设为False
        strategy.trading = False

        # 撤销全部委托
        strategy.cancel_all()

        # 同步数据状态
        self.sync_strategy_data(strategy)

        # 推送策略事件通知停止完成状态
        self.put_strategy_event(strategy)

    def edit_strategy(self, strategy_name: str, setting: dict) -> None:
        """编辑策略参数"""
        strategy: StrategyTemplate = self.strategies[strategy_name]
        strategy.update_setting(setting)

        self.save_strategy_setting()
        self.put_strategy_event(strategy)

    def remove_strategy(self, strategy_name: str) -> bool:
        """移除策略实例"""
        strategy: StrategyTemplate = self.strategies[strategy_name]
        if strategy.trading:
            self.write_log(f"策略{strategy.strategy_name}移除失败，请先停止")
            return

        for vt_symbol in strategy.vt_symbols:
            strategies: list = self.symbol_strategy_map[vt_symbol]
            strategies.remove(strategy)

        for vt_orderid in strategy.active_orderids:
            if vt_orderid in self.orderid_strategy_map:
                self.orderid_strategy_map.pop(vt_orderid)

        self.strategies.pop(strategy_name)
        self.save_strategy_setting()

        self.strategy_data.pop(strategy_name, None)
        save_json(self.data_filename, self.strategy_data)

        return True

    def load_strategy_class(self) -> None:
        """加载策略类"""
        path1: Path = Path(__file__).parent.joinpath("strategies")
        self.load_strategy_class_from_folder(path1, "vnpy_portfoliostrategy.strategies")

        path2: Path = Path.cwd().joinpath("strategies")
        self.load_strategy_class_from_folder(path2, "strategies")

    def load_strategy_class_from_folder(self, path: Path, module_name: str = "") -> None:
        """通过指定文件夹加载策略类"""
        for suffix in ["py", "pyd", "so"]:
            pathname: str = str(path.joinpath(f"*.{suffix}"))
            for filepath in glob.glob(pathname):
                stem: str = Path(filepath).stem
                strategy_module_name: str = f"vnpy.app.{module_name}.{stem}"
                self.load_strategy_class_from_module(strategy_module_name)

    def load_strategy_class_from_module(self, module_name: str) -> None:
        """通过策略文件加载策略类"""
        try:
            module: ModuleType = importlib.import_module(module_name)

            for name in dir(module):
                value = getattr(module, name)
                if (isinstance(value, type) and issubclass(value, StrategyTemplate) and value is not StrategyTemplate):
                    self.classes[value.__name__] = value
        except:  # noqa
            msg: str = f"策略文件{module_name}加载失败，触发异常：\n{traceback.format_exc()}"
            self.write_log(msg)

    def load_strategy_data(self) -> None:
        """加载策略数据"""
        self.strategy_data = load_json(self.data_filename)

    def sync_strategy_data(self, strategy: StrategyTemplate) -> None:
        """保存策略数据到文件"""
        data: dict = strategy.get_variables()
        data.pop("inited")      # 不保存策略状态信息
        data.pop("trading")

        self.strategy_data[strategy.strategy_name] = data
        save_json(self.data_filename, self.strategy_data)

    def get_all_strategy_class_names(self) -> list:
        """获取所有加载策略类名"""
        return list(self.classes.keys())

    def get_strategy_class_parameters(self, class_name: str) -> dict:
        """获取策略类参数"""
        strategy_class: StrategyTemplate = self.classes[class_name]

        parameters: dict = {}
        for name in strategy_class.parameters:
            parameters[name] = getattr(strategy_class, name)

        return parameters

    def get_strategy_parameters(self, strategy_name) -> dict:
        """获取策略参数"""
        strategy: StrategyTemplate = self.strategies[strategy_name]
        return strategy.get_parameters()

    def init_all_strategies(self) -> None:
        """初始化所有策略"""
        for strategy_name in self.strategies.keys():
            self.init_strategy(strategy_name)

    def start_all_strategies(self) -> None:
        """启动所有策略"""
        for strategy_name in self.strategies.keys():
            self.start_strategy(strategy_name)

    def stop_all_strategies(self) -> None:
        """停止所有策略"""
        for strategy_name in self.strategies.keys():
            self.stop_strategy(strategy_name)

    def load_strategy_setting(self) -> None:
        """加载策略配置"""
        strategy_setting: dict = load_json(self.setting_filename)

        for strategy_name, strategy_config in strategy_setting.items():
            self.add_strategy(
                strategy_config["class_name"],
                strategy_name,
                strategy_config["vt_symbols"],
                strategy_config["setting"]
            )

    def save_strategy_setting(self) -> None:
        """保存策略配置"""
        strategy_setting: dict = {}

        for name, strategy in self.strategies.items():
            strategy_setting[name] = {
                "class_name": strategy.__class__.__name__,
                "vt_symbols": strategy.vt_symbols,
                "setting": strategy.get_parameters()
            }

        save_json(self.setting_filename, strategy_setting)

    def put_strategy_event(self, strategy: StrategyTemplate) -> None:
        """推送事件更新策略界面"""
        data: dict = strategy.get_data()
        event: Event = Event(EVENT_PORTFOLIO_STRATEGY, data)
        self.event_engine.put(event)

    def write_log(self, msg: str, strategy: StrategyTemplate = None) -> None:
        """输出日志"""
        if strategy:
            msg: str = f"{strategy.strategy_name}: {msg}"

        log: LogData = LogData(msg=msg, gateway_name=APP_NAME)
        event: Event = Event(type=EVENT_PORTFOLIO_LOG, data=log)
        self.event_engine.put(event)

    def send_email(self, msg: str, strategy: StrategyTemplate = None) -> None:
        """发送邮件"""
        if strategy:
            subject: str = f"{strategy.strategy_name}"
        else:
            subject: str = "组合策略引擎"

        self.main_engine.send_email(subject, msg)
