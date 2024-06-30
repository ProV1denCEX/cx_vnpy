from typing import List, Dict
from datetime import datetime, time

import numpy as np
import pandas as pd

from vnpy.app.vnpy_portfoliostrategy.base import EngineType
from vnpy.trader.setting import SETTINGS
from vnpy.trader.utility import ArrayManager
from vnpy.trader.object import TickData, BarData
from vnpy.trader.constant import Direction, Interval

from vnpy.app.vnpy_portfoliostrategy import StrategyTemplate, StrategyEngine
from vnpy.app.vnpy_portfoliostrategy.utility import PortfolioBarGenerator, ATRExitHelper


class ArgminCrossMAStrategy(StrategyTemplate):
    author = "XCX"

    capital = 1e8
    interval = Interval.MINUTE_15

    window = 400

    atr_multiplier = 11

    vol_window = 100
    vol_target = 0.45

    bband_width = 2

    ls_imba = .75

    parameters = [
        "capital",
        "window",

        "atr_multiplier",

        "vol_window",
        "vol_target",

        "bband_width",

        "ls_imba"
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

        self.bband_window = self.atr_window = self.ma_window = self.window

        # 创建每个合约的ArrayManager
        self.ams: Dict[str, ArrayManager] = {}
        self.am_size = max(self.bband_window, self.ma_window) + max(self.window, self.atr_window, self.vol_window) * 3 + 100

        self.atr_helpers: Dict[str, ATRExitHelper] = {}
        for vt_symbol in self.vt_symbols:
            self.ams[vt_symbol] = ArrayManager(self.am_size)
            self.atr_helpers[vt_symbol] = ATRExitHelper(self.ams[vt_symbol])

        self.pbg = PortfolioBarGenerator(None, Interval.to_window(self.interval), self.on_bars)
        self.pbg.on_bars = self.pbg.update_bars

        if self.get_engine_type() == EngineType.SIGNAL:
            self.pos_data = self.target_data

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
        self.update_portfolio()

        self.write_log("策略停止")

    def on_tick(self, tick: TickData) -> None:
        """行情推送回调"""
        self.pbg.update_tick(tick)

    def on_bars(self, bars: Dict[str, BarData]) -> None:
        """K线切片回调"""
        super().on_bars(bars)

        for vt_symbol in self.vt_symbols:
            if vt_symbol in bars:
                bar = bars[vt_symbol]

                if (
                        bar.datetime.time() < time(9)  # 滤掉早盘集合竞价
                        or bar.datetime.time() >= time(23)  # 滤掉深夜交易
                        # or (time(21) > bar.datetime.time() >= time(20, 45))  # 滤掉夜盘集合竞价
                ):
                    return

                am: ArrayManager = self.ams[vt_symbol]
                am.update_bar(bar)

                self.atr_helpers[vt_symbol].on_bar(bar)

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

            atr_helper = self.atr_helpers[vt_symbol]
            current_pos = self.get_pos(vt_symbol)

            std = self.get_volatility(am)

            # if current_pos == 0:
            if std > self.vol_target:
                self.set_target(vt_symbol, 0)
                atr_helper.reset()

                continue

            direction = self.get_signal(am)
            if direction == Direction.LONG and current_pos <= 0:
                target_size = self.get_target_size_by_std_minus(vt_symbol, self.vol_window, std_max=self.vol_target)

                self.set_target(vt_symbol, target_size)

                atr_helper.on_target(self.last_prices[vt_symbol], Direction.LONG)

            elif direction == Direction.SHORT and current_pos >= 0:
                target_size = self.get_target_size_by_std_minus(vt_symbol, self.vol_window, std_max=self.vol_target)

                self.set_target(vt_symbol, -target_size)

                atr_helper.on_target(self.last_prices[vt_symbol], Direction.SHORT)

            else:
                if (
                        (current_pos > 0 and atr_helper.check_stoploss(self.atr_window, self.atr_multiplier))
                        or (current_pos < 0 and atr_helper.check_stoploss(self.atr_window, self.atr_multiplier * self.ls_imba))
                        # or atr_helper.check_stoploss(self.atr_window, self.atr_multiplier)
                ):
                    self.set_target(vt_symbol, 0)
                    atr_helper.reset()

        engine_type = self.get_engine_type()
        if engine_type != EngineType.BACKTESTING:
            self.save_strategy_portfolio()

    def get_factor(self, am: ArrayManager):
        # _, factor, _ = am.macd(self.window, int(self.window / 12 * 26), int(self.window / 12 * 9), array=True)
        factor = am.argmin(self.window, array=True)

        return factor

    def get_volatility(self, am: ArrayManager):
        roc = am.roc(1, True)
        std = np.std(np.log(1 + roc / 100)[-self.vol_window:]) * np.sqrt(252 * 23)

        return std

    def get_signal(self, am: ArrayManager):
        return self.trade_by_cross_ma(am)
        # return self.trade_by_bband(am)

# def calculate_price(self, vt_symbol: str, direction: Direction, reference: float) -> float:
    #     """计算调仓委托价格（支持按需重载实现）"""
    #     if direction == Direction.LONG:
    #         price: float = reference + self.get_pricetick(vt_symbol) * 0
    #     else:
    #         price: float = reference - self.get_pricetick(vt_symbol) * 0
    #
    #     return price

    def save_strategy_portfolio(
            self,
            account_name: str = SETTINGS["account.name"],
            investor_id: str = SETTINGS["account.investorid"],
    ):
        print("Dev Stage, No save")

    def trade_by_cross_ma(self, am: ArrayManager):
        factor = self.get_factor(am)

        ma = factor[-self.ma_window:].mean()

        if factor[-1] > ma > factor[-2]:
            direction = Direction.LONG

        elif factor[-1] < ma < factor[-2]:
            direction = Direction.SHORT

        else:
            direction = None

        return direction

    def trade_by_bband(self, am: ArrayManager):
        factor = self.get_factor(am)

        factor_value = factor[-1]
        prev_value = factor[-2]

        bband_mid = factor[-self.bband_window:].mean()
        bband_width = factor[-self.bband_window:].std()

        if factor_value > bband_mid + self.bband_width * bband_width > prev_value:
            direction = Direction.LONG

        elif factor_value < bband_mid - self.bband_width * bband_width * self.ls_imba < prev_value:
            direction = Direction.SHORT

        else:
            direction = None

        return direction

    def get_target_size_by_std_minus(self, vt_symbol: str, param=500, day_count=23, n=3, std_min=0.1, std_max=0.45) -> int:
        am = self.ams[vt_symbol]

        roc = am.roc(1, True)
        weight = np.std(np.log(1 + roc / 100)[-param:]) * np.sqrt(252 * day_count)

        weight = ((std_max - weight) / (std_max - std_min) * (n - 1) + 1) / n
        weight = min(max(weight, 1 / n), 1)

        weight *= 1 / len([i for i in self.ams.values() if i.inited])

        size = int(self.capital * weight / self.get_size(vt_symbol) / am.close[-1])

        return size
