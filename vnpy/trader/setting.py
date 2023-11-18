"""
Global setting of the trading platform.
"""

from logging import CRITICAL
from typing import Dict, Any
from tzlocal import get_localzone_name

from .utility import load_json

SETTINGS: Dict[str, Any] = {
    "font.family": "微软雅黑",
    "font.size": 12,

    "log.active": True,
    "log.level": CRITICAL,
    "log.console": True,
    "log.file": True,

    "email.server": "smtp.qq.com",
    "email.port": 465,
    "email.username": "",
    "email.password": "",
    "email.sender": "",
    "email.receiver": "",

    "datafeed.name": "tinysoft",
    "datafeed.username": "cxfundqa",
    "datafeed.password": "cxfund888888",

    "database.timezone": get_localzone_name(),
    "database.name": "dolphindb",
    "database.database": "CTA_TESTTRADE",
    "database.host": "192.168.50.238",
    "database.port": 8848,
    "database.user": "admin",
    "database.password": "zaq1@WSXcde3",
    "database.table_name": {
        'tick': 'tick',
        'bar': 'bar',
        'tickoverview': 'tickoverview',
        'baroverview': 'baroverview',

        'contract_futures': 'contract_futures',
        'contract_options': 'contract_options',
    }
}

# Load global setting from json file.
SETTING_FILENAME: str = "vt_setting.json"
SETTINGS.update(load_json(SETTING_FILENAME))


def get_settings(prefix: str = "") -> Dict[str, Any]:
    prefix_length: int = len(prefix)
    return {k[prefix_length:]: v for k, v in SETTINGS.items() if k.startswith(prefix)}
