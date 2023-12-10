from typing import List, Dict
from datetime import datetime, time

import numpy as np

from vnpy.trader.utility import ArrayManager
from vnpy.trader.object import TickData, BarData
from vnpy.trader.constant import Direction, Interval

from vnpy.app.vnpy_portfoliostrategy import StrategyTemplate, StrategyEngine
from vnpy.app.vnpy_portfoliostrategy.utility import PortfolioBarGenerator


class STMStrategy(StrategyTemplate):
    author = "XCX"

    capital = 1e8
    interval = Interval.MINUTE_15

    stm_window = 500

    qtl_long_window = stm_window
    enter_long_qtl = 0.9
    exit_long_qtl = 0.5

    qtl_short_window = stm_window
    enter_short_qtl = 0.1
    exit_short_qtl = 0.5

    parameters = [
        "capital",
        "stm_window",
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

        self.stm_data: Dict[str, float] = {}

        # 创建每个合约的ArrayManager
        self.ams: Dict[str, ArrayManager] = {}
        self.am_size = self.stm_window + max(self.qtl_short_window, self.qtl_long_window) + 100
        for vt_symbol in self.vt_symbols:
            self.ams[vt_symbol] = ArrayManager(self.am_size)

        self.pbg = PortfolioBarGenerator(None, Interval.to_window(self.interval), self.on_bars)
        self.pbg.on_bars = self.pbg.update_bars

    def on_init(self) -> None:
        """策略初始化回调"""
        self.write_log("策略初始化")

        if self.interval == Interval.MINUTE_15:
            days = int(self.am_size / 23) * 2  # mult 2 to fit trade day num, may fix in the future

        else:
            raise NotImplementedError

        self.load_bars(days)

    def on_start(self) -> None:
        """策略启动回调"""
        self.write_log("策略启动")

    def on_stop(self) -> None:
        """策略停止回调"""
        self.write_log("策略停止")

    def on_tick(self, tick: TickData) -> None:
        """行情推送回调"""
        self.pbg.update_tick(tick)

    def on_bars(self, bars: Dict[str, BarData]) -> None:
        """K线切片回调"""
        # 更新K线计算RSI数值
        for vt_symbol, bar in bars.items():
            if bar.datetime.time() < time(9) or bar.datetime.time() >= time(23):
                return

            am: ArrayManager = self.ams[vt_symbol]
            am.update_bar(bar)

        if not self.trading:
            return

        for vt_symbol, bar in bars.items():
            am: ArrayManager = self.ams[vt_symbol]
            if not am.inited:
                continue

            stm = am.stm(self.stm_window, array=True)

            self.stm_data[vt_symbol] = stm.iat[-1]

            current_pos = self.get_pos(vt_symbol)
            if current_pos == 0:
                if self.stm_data[vt_symbol] > stm.iloc[-self.qtl_long_window:].quantile(self.enter_long_qtl):
                    target_size = self.get_target_size_by_std_minus(vt_symbol)
                    self.set_target(vt_symbol, target_size)

                elif self.stm_data[vt_symbol] < stm.iloc[-self.qtl_short_window:].quantile(self.enter_short_qtl):
                    target_size = self.get_target_size_by_std_minus(vt_symbol)

                    self.set_target(vt_symbol, -target_size)

            elif current_pos > 0:
                if self.stm_data[vt_symbol] < stm.iloc[-self.qtl_short_window:].quantile(self.enter_short_qtl):
                    target_size = self.get_target_size_by_std_minus(vt_symbol)

                    self.set_target(vt_symbol, -target_size)

                elif self.stm_data[vt_symbol] < stm.iloc[-self.qtl_long_window:].quantile(self.exit_long_qtl):
                    self.set_target(vt_symbol, 0)

            elif current_pos < 0:
                if self.stm_data[vt_symbol] > stm.iloc[-self.qtl_long_window:].quantile(self.enter_long_qtl):
                    target_size = self.get_target_size_by_std_minus(vt_symbol)

                    self.set_target(vt_symbol, target_size)

                elif self.stm_data[vt_symbol] > stm.iloc[-self.qtl_short_window:].quantile(self.exit_short_qtl):
                    self.set_target(vt_symbol, 0)

        self.rebalance_portfolio(bars)

        self.put_event()

    # def calculate_price(self, vt_symbol: str, direction: Direction, reference: float) -> float:
    #     """计算调仓委托价格（支持按需重载实现）"""
    #     if direction == Direction.LONG:
    #         price: float = reference + self.get_pricetick(vt_symbol) * 0
    #     else:
    #         price: float = reference - self.get_pricetick(vt_symbol) * 0
    #
    #     return price

    def get_target_size_by_std_minus(self, vt_symbol: str, param=500, day_count=23, n=3, std_min=0.1, std_max=0.45) -> int:
        am = self.ams[vt_symbol]

        roc = am.roc(1, True)
        weight = np.std(np.log(1 + roc / 100)[-param:]) * np.sqrt(252 * day_count)

        weight = ((std_max - weight) / (std_max - std_min) * (n - 1) + 1) / n
        weight = min(max(weight, 1 / n), 1)

        weight *= 1 / len([i for i in self.ams.values() if i.inited])

        size = int(self.capital * weight / self.get_size(vt_symbol) / am.close[-1])

        return size
