"""
General constant enums used in the trading platform.
"""

from enum import Enum

from Pandora.constant import Product, Exchange, Interval

PANDORA_OBJ = {Product, Exchange, Interval}


class Direction(Enum):
    """
    Direction of order/trade/position.
    """
    LONG = "Long"
    SHORT = "Short"
    NET = "Net"


class Offset(Enum):
    """
    Offset of order/trade.
    """
    NONE = ""
    OPEN = "开"
    CLOSE = "平"
    CLOSETODAY = "平今"
    CLOSEYESTERDAY = "平昨"


class Status(Enum):
    """
    Order status.
    """
    SUBMITTING = "提交中"
    NOTTRADED = "未成交"
    PARTTRADED = "部分成交"
    ALLTRADED = "全部成交"
    CANCELLED = "已撤销"
    REJECTED = "拒单"


class OrderType(Enum):
    """
    Order type.
    """
    LIMIT = "限价"
    MARKET = "市价"
    STOP = "STOP"
    FAK = "FAK"
    FOK = "FOK"
    RFQ = "询价"


class OptionType(Enum):
    """
    Option type.
    """
    CALL = "看涨期权"
    PUT = "看跌期权"

    @staticmethod
    def from_str(type: str):
        if type.upper() in {"看涨期权", "CALL", "C"}:
            return OptionType.CALL

        if type.upper() in {"看跌期权", "PUT", "P"}:
            return OptionType.PUT

        raise ValueError(f"Invalid OptionType {type}")


class Currency(Enum):
    """
    Currency.
    """
    USD = "USD"
    HKD = "HKD"
    CNY = "CNY"
    CAD = "CAD"

