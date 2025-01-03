import sys
from threading import Thread
from queue import Queue, Empty
from copy import copy
from typing import Any, Dict, List, Optional

from vnpy.event import Event, EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
from Pandora.constant import Exchange
from Pandora.trader.object import (
    SubscribeRequest,
    TickData,
    BarData,
    ContractData
)
from vnpy.trader.event import EVENT_TICK, EVENT_CONTRACT, EVENT_TIMER
from Pandora.trader.utility import load_json, save_json, BarGenerator
from vnpy.trader.database import BaseDatabase, get_database
from vnpy.app.vnpy_spreadtrading.base import EVENT_SPREAD_DATA, SpreadData


APP_NAME = "DataRecorder"

EVENT_RECORDER_LOG = "eRecorderLog"
EVENT_RECORDER_UPDATE = "eRecorderUpdate"
EVENT_RECORDER_EXCEPTION = "eRecorderException"


class RecorderEngine(BaseEngine):
    """
    For running data recorder.
    """

    setting_filename: str = "data_recorder_setting.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.queue: Queue = Queue()
        self.thread: Thread = Thread(target=self.run)
        self.active: bool = False

        self.tick_recordings: Dict[str, Dict] = {}
        self.bar_recordings: Dict[str, Dict] = {}
        self.bar_generators: Dict[str, BarGenerator] = {}
        self.sub_bar_generators: Dict[str, List[BarGenerator]] = {}

        self.timer_count: int = 0
        self.timer_interval: int = 10

        self.ticks: List[TickData] = []
        self.bars: List[BarData] = []

        self.database: BaseDatabase = get_database()

        # self.load_setting()
        self.register_event()
        self.start()
        self.put_event()

    def load_setting(self) -> None:
        """"""
        setting: dict = load_json(self.setting_filename)
        self.tick_recordings = setting.get("tick", {})
        self.bar_recordings = setting.get("bar", {})

    def save_setting(self) -> None:
        """"""
        setting: dict = {
            "tick": self.tick_recordings,
            "bar": self.bar_recordings
        }
        save_json(self.setting_filename, setting)

    def run(self) -> None:
        """"""
        while self.active:
            try:
                task: Any = self.queue.get(timeout=1)
                task_type, data = task

                if task_type == "tick":
                    self.database.save_tick_data(data, stream=True)

                elif task_type == "bar":
                    data_2_save = {}
                    for bar in data:
                        product = self.bar_recordings[bar.vt_symbol].get('product')

                        if product in data_2_save:
                            data_2_save[product].append(bar)

                        else:
                            data_2_save[product] = [bar]

                    for product, bars in data_2_save.items():
                        self.database.save_bar_data(bars, stream=True, product=product)

            except Empty:
                continue

            except Exception:
                self.active = False

                info = sys.exc_info()
                event: Event = Event(EVENT_RECORDER_EXCEPTION, info)
                self.event_engine.put(event)

    def close(self) -> None:
        """"""
        self.active = False

        if self.thread.is_alive():
            self.thread.join()

    def start(self) -> None:
        """"""
        self.active = True
        self.thread.start()

    def add_bar_recording(self, vt_symbol: str) -> None:
        """"""
        if vt_symbol in self.bar_recordings:
            self.write_log(f"已在K线记录列表中：{vt_symbol}")
            return

        if Exchange.LOCAL.value not in vt_symbol:
            contract: Optional[ContractData] = self.main_engine.get_contract(vt_symbol)
            if not contract:
                self.write_log(f"找不到合约：{vt_symbol}")
                return

            self.bar_recordings[vt_symbol] = {
                "symbol": contract.symbol,
                "exchange": contract.exchange.value,
                "product": contract.product,
                "gateway_name": contract.gateway_name
            }

            self.subscribe(contract)
        else:
            self.bar_recordings[vt_symbol] = {}

        self.save_setting()
        self.put_event()

        self.write_log(f"添加K线记录成功：{vt_symbol}")

    def add_tick_recording(self, vt_symbol: str) -> None:
        """"""
        if vt_symbol in self.tick_recordings:
            self.write_log(f"已在Tick记录列表中：{vt_symbol}")
            return

        # For normal contract
        if Exchange.LOCAL.value not in vt_symbol:
            contract: Optional[ContractData] = self.main_engine.get_contract(vt_symbol)
            if not contract:
                self.write_log(f"找不到合约：{vt_symbol}")
                return

            self.tick_recordings[vt_symbol] = {
                "symbol": contract.symbol,
                "exchange": contract.exchange.value,
                "gateway_name": contract.gateway_name
            }

            self.subscribe(contract)
        # No need to subscribe for spread data
        else:
            self.tick_recordings[vt_symbol] = {}

        self.save_setting()
        self.put_event()

        self.write_log(f"添加Tick记录成功：{vt_symbol}")

    def remove_bar_recording(self, vt_symbol: str) -> None:
        """"""
        if vt_symbol not in self.bar_recordings:
            self.write_log(f"不在K线记录列表中：{vt_symbol}")
            return

        self.bar_recordings.pop(vt_symbol)
        self.save_setting()
        self.put_event()

        self.write_log(f"移除K线记录成功：{vt_symbol}")

    def remove_tick_recording(self, vt_symbol: str) -> None:
        """"""
        if vt_symbol not in self.tick_recordings:
            self.write_log(f"不在Tick记录列表中：{vt_symbol}")
            return

        self.tick_recordings.pop(vt_symbol)
        self.save_setting()
        self.put_event()

        self.write_log(f"移除Tick记录成功：{vt_symbol}")

    def register_event(self) -> None:
        """"""
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_contract_event)
        self.event_engine.register(EVENT_SPREAD_DATA, self.process_spread_event)

    def update_tick(self, tick: TickData) -> None:
        """"""
        if tick.vt_symbol in self.tick_recordings:
            self.record_tick(copy(tick))

        if tick.vt_symbol in self.bar_recordings:
            bg: BarGenerator = self.get_bar_generator(tick.vt_symbol)
            bg.update_tick(copy(tick))

    def process_timer_event(self, event: Event) -> None:
        """"""
        self.timer_count += 1
        if self.timer_count < self.timer_interval:
            return
        self.timer_count = 0

        if self.bars:
            self.queue.put(("bar", self.bars.copy()))
            self.bars.clear()

        if self.ticks:
            self.queue.put(("tick", self.ticks.copy()))
            self.ticks.clear()

    def process_tick_event(self, event: Event) -> None:
        """"""
        tick: TickData = event.data
        self.update_tick(tick)

    def process_contract_event(self, event: Event) -> None:
        """"""
        contract: ContractData = event.data
        vt_symbol: str = contract.vt_symbol

        if vt_symbol in self.tick_recordings or vt_symbol in self.bar_recordings:
            self.subscribe(contract)

    def process_spread_event(self, event: Event) -> None:
        """"""
        spread: SpreadData = event.data
        tick: TickData = spread.to_tick()

        # Filter not inited spread data
        if tick.datetime:
            self.update_tick(tick)

    def write_log(self, msg: str) -> None:
        """"""
        event: Event = Event(
            EVENT_RECORDER_LOG,
            msg
        )
        self.event_engine.put(event)

    def put_event(self) -> None:
        """"""
        tick_symbols: List[str] = list(self.tick_recordings.keys())
        tick_symbols.sort()

        bar_symbols: List[str] = list(self.bar_recordings.keys())
        bar_symbols.sort()

        data: dict = {
            "tick": tick_symbols,
            "bar": bar_symbols
        }

        event: Event = Event(
            EVENT_RECORDER_UPDATE,
            data
        )
        self.event_engine.put(event)

    def record_tick(self, tick: TickData) -> None:
        """"""
        self.ticks.append(tick)

    def record_bar(self, bar: BarData) -> None:
        """"""
        self.bars.append(bar)

    def on_1min_bar(self, bar):
        self.record_bar(bar)

        sub_bar_generators = self.sub_bar_generators.get(bar.vt_symbol)
        for bg in sub_bar_generators:
            bg.on_bar(bar)

    def get_bar_generator(self, vt_symbol: str) -> BarGenerator:
        """"""
        bg: Optional[BarGenerator] = self.bar_generators.get(vt_symbol, None)

        if not bg:
            bg = BarGenerator(self.on_1min_bar)
            self.bar_generators[vt_symbol] = bg

        sub_bar_generators = self.sub_bar_generators.get(vt_symbol, None)
        if not sub_bar_generators:
            sub_bar_generators = []
            for i in [2, 3, 5, 15]:
                bg = BarGenerator(None, window=i)
                bg.on_bar = bg.update_bar
                bg.on_window_bar = self.record_bar

                sub_bar_generators.append(bg)

            self.sub_bar_generators[vt_symbol] = sub_bar_generators

        return bg

    def subscribe(self, contract: ContractData) -> None:
        """"""
        req: SubscribeRequest = SubscribeRequest(
            symbol=contract.symbol,
            exchange=contract.exchange
        )
        self.main_engine.subscribe(req, contract.gateway_name)
