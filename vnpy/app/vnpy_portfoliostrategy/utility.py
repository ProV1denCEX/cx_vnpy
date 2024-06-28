from datetime import datetime
from typing import Callable, Dict, Optional

from vnpy.trader.constant import Direction
from vnpy.trader.object import BarData, TickData, Interval
from vnpy.trader.utility import ArrayManager


class PortfolioBarGenerator:
    """组合K线生成器"""

    def __init__(
        self,
        on_bars: Callable,
        window: int = 0,
        on_window_bars: Callable = None,
        interval: Interval = Interval.MINUTE
    ) -> None:
        """构造函数"""
        self.on_bars: Callable = on_bars

        self.interval: Interval = interval
        self.interval_count: int = 0

        self.bars: Dict[str, BarData] = {}
        self.last_ticks: Dict[str, TickData] = {}

        self.hour_bars: Dict[str, BarData] = {}
        self.finished_hour_bars: Dict[str, BarData] = {}

        self.window: int = window
        self.window_bars: Dict[str, BarData] = {}
        self.on_window_bars: Callable = on_window_bars

        self.last_dt: datetime = None

    def update_tick(self, tick: TickData) -> None:
        """更新行情切片数据"""
        # 用localtime 保证严格正推，且仅推一次bar slice
        if self.last_dt and self.last_dt.minute != tick.localtime.minute:
            for bar in self.bars.values():
                bar.datetime = bar.datetime.replace(second=0, microsecond=0)

            self.on_bars(self.bars)
            self.bars = {}

        bar: Optional[BarData] = self.bars.get(tick.vt_symbol, None)
        if not bar:
            bar = BarData(
                symbol=tick.symbol,
                exchange=tick.exchange,
                interval=Interval.MINUTE,
                datetime=tick.datetime,  # TODO: 1. 检查生成的bar 会不会时间轴回溯；是否需要换成localtime
                gateway_name=tick.gateway_name,
                open_price=tick.last_price,
                high_price=tick.last_price,
                low_price=tick.last_price,
                close_price=tick.last_price,
                open_interest=tick.open_interest
            )
            self.bars[bar.vt_symbol] = bar
        else:
            bar.high_price = max(bar.high_price, tick.last_price)
            bar.low_price = min(bar.low_price, tick.last_price)
            bar.close_price = tick.last_price
            bar.open_interest = tick.open_interest
            bar.datetime = tick.datetime

        last_tick: Optional[TickData] = self.last_ticks.get(tick.vt_symbol, None)
        if last_tick:
            bar.volume += max(tick.volume - last_tick.volume, 0)
            bar.turnover += max(tick.turnover - last_tick.turnover, 0)

        self.last_ticks[tick.vt_symbol] = tick
        self.last_dt = tick.localtime

    def update_bars(self, bars: Dict[str, BarData]) -> None:
        """更新一分钟K线"""
        if self.interval == Interval.MINUTE:
            self.update_bar_minute_window(bars)
        else:
            self.update_bar_hour_window(bars)

    def update_bar_minute_window(self, bars: Dict[str, BarData]) -> None:
        """更新N分钟K线"""
        for vt_symbol, bar in bars.items():
            window_bar: Optional[BarData] = self.window_bars.get(vt_symbol, None)

            # 如果没有N分钟K线则创建
            if not window_bar:
                dt: datetime = bar.datetime.replace(second=0, microsecond=0)
                window_bar = BarData(
                    symbol=bar.symbol,
                    exchange=bar.exchange,
                    datetime=dt,
                    gateway_name=bar.gateway_name,
                    open_price=bar.open_price,
                    high_price=bar.high_price,
                    low_price=bar.low_price
                )
                self.window_bars[vt_symbol] = window_bar

            # 更新K线内最高价及最低价
            else:
                window_bar.high_price = max(
                    window_bar.high_price,
                    bar.high_price
                )
                window_bar.low_price = min(
                    window_bar.low_price,
                    bar.low_price
                )

            # 更新K线内收盘价、数量、成交额、持仓量
            window_bar.close_price = bar.close_price
            window_bar.volume += bar.volume
            window_bar.turnover += bar.turnover
            window_bar.open_interest = bar.open_interest

        # 检查K线是否合成完毕
        if not (bar.datetime.minute + 1) % self.window:
            self.on_window_bars(self.window_bars)
            self.window_bars = {}

    def update_bar_hour_window(self, bars: Dict[str, BarData]) -> None:
        """更新小时K线"""
        for vt_symbol, bar in bars.items():
            hour_bar: Optional[BarData] = self.hour_bars.get(vt_symbol, None)

            # 如果没有小时K线则创建
            if not hour_bar:
                dt: datetime = bar.datetime.replace(minute=0, second=0, microsecond=0)
                hour_bar = BarData(
                    symbol=bar.symbol,
                    exchange=bar.exchange,
                    datetime=dt,
                    gateway_name=bar.gateway_name,
                    open_price=bar.open_price,
                    high_price=bar.high_price,
                    low_price=bar.low_price,
                    close_price=bar.close_price,
                    volume=bar.volume,
                    turnover=bar.turnover,
                    open_interest=bar.open_interest
                )
                self.hour_bars[vt_symbol] = hour_bar

            else:
                # 如果收到59分的分钟K线，更新小时K线并推送
                if bar.datetime.minute == 59:
                    hour_bar.high_price = max(
                        hour_bar.high_price,
                        bar.high_price
                    )
                    hour_bar.low_price = min(
                        hour_bar.low_price,
                        bar.low_price
                    )

                    hour_bar.close_price = bar.close_price
                    hour_bar.volume += bar.volume
                    hour_bar.turnover += bar.turnover
                    hour_bar.open_interest = bar.open_interest

                    self.finished_hour_bars[vt_symbol] = hour_bar
                    self.hour_bars[vt_symbol] = None

                # 如果收到新的小时的分钟K线，直接推送当前的小时K线
                elif bar.datetime.hour != hour_bar.datetime.hour:
                    self.finished_hour_bars[vt_symbol] = hour_bar

                    dt: datetime = bar.datetime.replace(minute=0, second=0, microsecond=0)
                    hour_bar = BarData(
                        symbol=bar.symbol,
                        exchange=bar.exchange,
                        datetime=dt,
                        gateway_name=bar.gateway_name,
                        open_price=bar.open_price,
                        high_price=bar.high_price,
                        low_price=bar.low_price,
                        close_price=bar.close_price,
                        volume=bar.volume,
                        turnover=bar.turnover,
                        open_interest=bar.open_interest
                    )
                    self.hour_bars[vt_symbol] = hour_bar

                # 否则直接更新小时K线
                else:
                    hour_bar.high_price = max(
                        hour_bar.high_price,
                        bar.high_price
                    )
                    hour_bar.low_price = min(
                        hour_bar.low_price,
                        bar.low_price
                    )

                    hour_bar.close_price = bar.close_price
                    hour_bar.volume += bar.volume
                    hour_bar.turnover += bar.turnover
                    hour_bar.open_interest = bar.open_interest

        # 推送合成完毕的小时K线
        if self.finished_hour_bars:
            self.on_hour_bars(self.finished_hour_bars)
            self.finished_hour_bars = {}

    def on_hour_bars(self, bars: Dict[str, BarData]) -> None:
        """推送小时K线"""
        if self.window == 1:
            self.on_window_bars(bars)
        else:
            for vt_symbol, bar in bars.items():
                window_bar: Optional[BarData] = self.window_bars.get(vt_symbol, None)
                if not window_bar:
                    window_bar = BarData(
                        symbol=bar.symbol,
                        exchange=bar.exchange,
                        datetime=bar.datetime,
                        gateway_name=bar.gateway_name,
                        open_price=bar.open_price,
                        high_price=bar.high_price,
                        low_price=bar.low_price
                    )
                    self.window_bars[vt_symbol] = window_bar
                else:
                    window_bar.high_price = max(
                        window_bar.high_price,
                        bar.high_price
                    )
                    window_bar.low_price = min(
                        window_bar.low_price,
                        bar.low_price
                    )

                window_bar.close_price = bar.close_price
                window_bar.volume += bar.volume
                window_bar.turnover += bar.turnover
                window_bar.open_interest = bar.open_interest

            self.interval_count += 1
            if not self.interval_count % self.window:
                self.interval_count = 0
                self.on_window_bars(self.window_bars)
                self.window_bars = {}


class ATRExitHelper(object):
    def __init__(self, am: ArrayManager):
        self.am = am

        self.open_price = 0.0
        self.current_price = 0.0
        self.direction: Direction = None
        self.best_price = 0.0

    def reset(self):
        self.open_price = 0.0
        self.current_price = 0.0
        self.direction: Direction = None
        self.best_price = 0.0

    def on_bar(self, bar: BarData):
        if self.direction == Direction.LONG:
            self.current_price = bar.close_price
            self.best_price = max(self.best_price, bar.high_price)

        elif self.direction == Direction.SHORT:
            self.current_price = bar.close_price
            self.best_price = min(self.best_price, bar.low_price)

    def on_target(self, price: float, direction: Direction):
        self.open_price = price
        self.current_price = price
        self.direction = direction
        self.best_price = price

    def check_stoploss(self, atr_window, atr_multiplier):
        if not self.am.inited:
            return

        atr = self.am.atr(atr_window, False)

        if self.direction == Direction.LONG:
            return self.best_price - self.current_price > atr * atr_multiplier

        elif self.direction == Direction.SHORT:
            return self.current_price - self.best_price > atr * atr_multiplier
