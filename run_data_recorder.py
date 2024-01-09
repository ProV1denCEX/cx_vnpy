import multiprocessing
import sys
from time import sleep
from datetime import datetime, time
from logging import INFO

from vnpy.event import EventEngine
from vnpy.trader.constant import Product
from vnpy.trader.database import get_database
from vnpy.trader.setting import SETTINGS
from vnpy.trader.engine import MainEngine

from vnpy.app.vnpy_ctp import CtpGateway
from vnpy.app.vnpy_datarecorder import DataRecorderApp
from vnpy.app.vnpy_datarecorder.engine import EVENT_RECORDER_LOG
from vnpy.app.vnpy_mcmanager.engine import MainContractManager
from vnpy.trader.utility import load_json

SETTINGS["log.active"] = True
SETTINGS["log.level"] = INFO
SETTINGS["log.console"] = True


CTP_SETTING = {
    "用户名": "057114",
    "密码": "zaq1@WSXcde3",
    "经纪商代码": "9999",
    "交易服务器": "tcp://180.168.146.187:10202",
    "行情服务器": "tcp://180.168.212.230:41214",
    "产品名称": "simnow_client_test",
    "授权编码": "0000000000000000"
}


# Chinese futures market trading period (day/night)
# 多给点时间供数据库写入
DAY_START = time(8, 45)
DAY_END = time(15, 30)

NIGHT_START = time(20, 45)
NIGHT_END = time(23, 45)


def check_trading_period():
    """"""
    current_time = datetime.now().time()

    trading = False
    if (
        (DAY_START <= current_time <= DAY_END)
        or (NIGHT_START <= current_time <= NIGHT_END)
    ):
        trading = True

    return trading


def run_child():
    """
    Running in the child process.
    """
    SETTINGS["log.file"] = True

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    gateway = main_engine.add_gateway(CtpGateway)
    recorder = main_engine.add_app(DataRecorderApp)
    main_engine.write_log("主引擎创建成功")

    log_engine = main_engine.get_engine("log")
    event_engine.register(EVENT_RECORDER_LOG, log_engine.process_log_event)
    main_engine.write_log("注册日志事件监听")

    # ctp_setting = load_json(f"connect_{gateway.gateway_name.lower()}.json")

    mc_manager = main_engine.add_engine(MainContractManager)

    main_engine.connect(CTP_SETTING, "CTP")
    main_engine.write_log("连接CTP接口")

    sleep(10)

    while True:
        contracts = main_engine.get_all_contracts()

        if len(contracts) > len(mc_manager.get_main_contracts()):
            for i in contracts:
                if i.product == Product.FUTURES:
                    recorder.add_tick_recording(i.vt_symbol)
                    recorder.add_bar_recording(i.vt_symbol)

            break

        sleep(5)

    db = get_database()
    db.save_contract_data(contracts)

    while True:
        sleep(10)

        trading = check_trading_period()
        if not trading:
            print("关闭子进程")
            main_engine.close()
            sys.exit(0)


def run_parent():
    """
    Running in the parent process.
    """
    print("启动CTA策略守护父进程")

    child_process = None

    while True:
        trading = check_trading_period()

        # Start child process in trading period
        if trading and child_process is None:
            print("启动子进程")
            child_process = multiprocessing.Process(target=run_child)
            child_process.start()
            print("子进程启动成功")

        # 非记录时间则退出子进程
        if not trading and child_process is not None:
            if not child_process.is_alive():
                child_process = None
                print("子进程关闭成功")

        sleep(5)


if __name__ == "__main__":
    # run_child()
    run_parent()
