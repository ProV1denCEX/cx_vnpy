from typing import List, Dict
from datetime import time

import numpy as np

from vnpy.app.vnpy_portfoliostrategy.base import EngineType
from Pandora.trader.utility import ArrayManager
from Pandora.trader.object import TickData, BarData
from vnpy.trader.constant import Interval

from vnpy.app.vnpy_portfoliostrategy import StrategyTemplate, StrategyEngine
from vnpy.app.vnpy_portfoliostrategy.utility import PortfolioBarGenerator


class STMStrategy(StrategyTemplate):
    author = "XCX"

    capital = 1e8
    interval = Interval.MINUTE_15

    window = 500

    vol_window = 100
    vol_target = 0.45
    vol_exit = False

    qtl_long_window = window
    enter_long_qtl = 0.9
    exit_long_qtl = 0.5

    qtl_short_window = window
    enter_short_qtl = 0.1
    exit_short_qtl = 0.5

    parameters = [
        "capital",
        "window",

        "vol_window",
        "vol_target",
        "vol_exit",

        "qtl_long_window",
        "enter_long_qtl",
        "exit_long_qtl",

        "qtl_short_window",
        "enter_short_qtl",
        "exit_short_qtl"
    ]
    variables = [
    ]

    def __init__(
        self,
        strategy_engine: StrategyEngine,
        strategy_name: str,
        vt_symbols: List[str],
        setting: dict
    ) -> None:
        """构造函数"""
        super().__init__(strategy_engine, strategy_name, vt_symbols, setting)

        self.am_size = self.window
        self.prepare_trade()

        # 创建每个合约的ArrayManager
        self.ams: Dict[str, ArrayManager] = {}
        for vt_symbol in self.vt_symbols:
            self.ams[vt_symbol] = ArrayManager(self.am_size)

        self.pbg = PortfolioBarGenerator(None, Interval.to_window(self.interval), self.on_bars)
        self.pbg.on_bars = self.pbg.update_bars

        if self.get_engine_type() == EngineType.SIGNAL:
            self.pos_data = self.target_data

    def prepare_trade(self) -> None:
        self.qtl_long_window = self.qtl_short_window = self.window
        self.am_size = max(self.qtl_long_window, self.qtl_short_window) * 3 + max(self.window, self.vol_window) * 3 + 100

    def on_init(self) -> None:
        """策略初始化回调"""
        self.write_log("策略初始化")

        if self.interval == Interval.MINUTE_15:
            days = int(self.am_size / 23) * 2  # mult 2 to fit trade day num, may fix in the future

        else:
            raise NotImplementedError

        self.load_bars(days, self.interval)

    def on_start(self) -> None:
        """策略启动回调"""
        self.write_log("策略启动")

        self.update_portfolio()

    def on_stop(self) -> None:
        """策略停止回调"""
        engine_type = self.get_engine_type()
        if engine_type == EngineType.SIGNAL:
            self.save_strategy_portfolio()

        self.write_log("策略停止")

    def on_tick(self, tick: TickData) -> None:
        """行情推送回调"""
        self.pbg.update_tick(tick)

    def on_bars(self, bars: Dict[str, BarData]) -> None:
        """K线切片回调"""
        super().on_bars(bars)

        for vt_symbol, bar in bars.items():
            if (
                    bar.datetime.time() < time(9)  # 滤掉早盘集合竞价
                    or bar.datetime.time() >= time(23)  # 滤掉深夜交易
                    # or (time(21) > bar.datetime.time() >= time(20, 45))  # 滤掉夜盘集合竞价
            ):
                return

            am: ArrayManager = self.ams[vt_symbol]
            am.update_bar(bar)

        if not self.trading:
            return

        self.update_portfolio()

        engine_type = self.get_engine_type()
        if engine_type != EngineType.SIGNAL:
            self.rebalance_portfolio(bars)

        self.put_event()

    def update_portfolio(self):
        for vt_symbol, am in self.ams.items():
            if not am.inited:
                continue

            std = self.get_volatility(am)

            if self.vol_exit and std > self.vol_target:
                self.set_target(vt_symbol, 0)

                continue

            stm = am.stm(self.window, array=True)

            factor_value = stm[-1]
            prev_value = stm[-2]

            current_pos = self.get_pos(vt_symbol)

            if current_pos == 0:
                if factor_value > np.quantile(stm[-self.qtl_long_window:], self.enter_long_qtl) > prev_value:
                    target_size = self.get_target_size_by_std_minus(vt_symbol)
                    self.set_target(vt_symbol, target_size)

                elif factor_value < np.quantile(stm[-self.qtl_short_window:], self.enter_short_qtl) < prev_value:
                    target_size = self.get_target_size_by_std_minus(vt_symbol)

                    self.set_target(vt_symbol, -target_size)

            elif current_pos > 0:
                if factor_value < np.quantile(stm[-self.qtl_short_window:], self.enter_short_qtl) < prev_value:
                    target_size = self.get_target_size_by_std_minus(vt_symbol)

                    self.set_target(vt_symbol, -target_size)

                elif factor_value < np.quantile(stm[-self.qtl_long_window:], self.exit_long_qtl) < prev_value:
                    self.set_target(vt_symbol, 0)

            elif current_pos < 0:
                if factor_value > np.quantile(stm[-self.qtl_long_window:], self.enter_long_qtl) > prev_value:
                    target_size = self.get_target_size_by_std_minus(vt_symbol)

                    self.set_target(vt_symbol, target_size)

                elif factor_value > np.quantile(stm[-self.qtl_short_window:], self.exit_short_qtl) > prev_value:
                    self.set_target(vt_symbol, 0)

        engine_type = self.get_engine_type()
        if engine_type != EngineType.BACKTESTING:
            self.save_strategy_portfolio()

    # def calculate_price(self, vt_symbol: str, direction: Direction, reference: float) -> float:
    #     """计算调仓委托价格（支持按需重载实现）"""
    #     if direction == Direction.LONG:
    #         price: float = reference + self.get_pricetick(vt_symbol) * 0
    #     else:
    #         price: float = reference - self.get_pricetick(vt_symbol) * 0
    #
    #     return price
    def get_volatility(self, am: ArrayManager):
        roc = am.roc(1, True)
        std = np.std(np.log(1 + roc / 100)[-self.vol_window:]) * np.sqrt(252 * 23)

        return std

    def get_target_size_by_std_minus(self, vt_symbol: str, param=500, day_count=23, n=3, std_min=0.1, std_max=0.45) -> int:
        am = self.ams[vt_symbol]

        roc = am.roc(1, True)
        weight = np.std(np.log(1 + roc / 100)[-param:]) * np.sqrt(252 * day_count)

        weight = ((std_max - weight) / (std_max - std_min) * (n - 1) + 1) / n
        weight = min(max(weight, 1 / n), 1)

        weight *= 1 / len([i for i in self.ams.values() if i.inited])

        size = int(self.capital * weight / self.get_size(vt_symbol) / am.close[-1])

        return size
