"""
DolphinDB脚本，用于在DolphinDB中创建数据库和数据表。
"""

from vnpy.trader.setting import SETTINGS


DB_PATH = "dfs://" + SETTINGS["database.database"]


# 创建数据库
def create_database_script(database: str):
    db_path = "dfs://" + database
    script = f"""
    dataPath = "{db_path}"
    db = database(dataPath, VALUE, 2000.01M..2030.12M, engine=`TSDB)
    """

    return script


# 创建bar表
def create_bar_table_script(database: str, table_name: str = "bar"):
    db_path = "dfs://" + database
    script = f"""
    dataPath = "{db_path}"
    db = database(dataPath)

    bar_columns = ["symbol", "exchange", "datetime", "interval", "volume", "turnover", "open_interest", "open_price", "high_price", "low_price", "close_price"]
    bar_type = [SYMBOL, SYMBOL, NANOTIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE]
    bar = table(1:0, bar_columns, bar_type)

    db.createPartitionedTable(
        bar,
        "{table_name}",
        partitionColumns=["datetime"],
        sortColumns=["symbol", "exchange", "interval", "datetime"],
        keepDuplicates=LAST)
    """

    return script


# 创建tick表
def create_tick_table_script(database: str, table_name: str = "tick"):
    db_path = "dfs://" + database
    script = f"""
    dataPath = "{db_path}"
    db = database(dataPath)

    tick_columns = ["symbol", "exchange", "datetime", "name", "volume", "turnover", "open_interest", "last_price", "last_volume", "limit_up", "limit_down",
                    "open_price", "high_price", "low_price", "pre_close",
                    "bid_price_1", "bid_price_2", "bid_price_3", "bid_price_4", "bid_price_5",
                    "ask_price_1", "ask_price_2", "ask_price_3", "ask_price_4", "ask_price_5",
                    "bid_volume_1", "bid_volume_2", "bid_volume_3", "bid_volume_4", "bid_volume_5",
                    "ask_volume_1", "ask_volume_2", "ask_volume_3", "ask_volume_4", "ask_volume_5", "localtime"]
    tick_type = [SYMBOL, SYMBOL, NANOTIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
                 DOUBLE, DOUBLE, DOUBLE, DOUBLE,
                 DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
                 DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
                 DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
                 DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, NANOTIMESTAMP]
    tick = table(1:0, tick_columns, tick_type)

    db.createPartitionedTable(
        tick,
        "{table_name}",
        partitionColumns=["datetime"],
        sortColumns=["symbol", "exchange", "datetime"],
        keepDuplicates=LAST)
    """

    return script


# 创建bar_overview表
def create_baroverview_table_script(database: str, table_name: str = "baroverview"):
    db_path = "dfs://" + database
    script = f"""
    dataPath = "{db_path}"
    db = database(dataPath)

    overview_columns = ["symbol", "exchange", "interval", "count", "start", "end", "datetime"]
    overview_type = [SYMBOL, SYMBOL, SYMBOL, INT, NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP]
    baroverview = table(1:0, overview_columns, overview_type)
    db.createPartitionedTable(
        baroverview,
        "{table_name}",
        partitionColumns=["datetime"],
        sortColumns=["symbol", "exchange", "interval", "datetime"],
        keepDuplicates=LAST)
    """

    return script


# 创建tick_overview表
def create_tickoverview_table_script(database: str, table_name: str = "tickoverview"):
    db_path = "dfs://" + database
    script = f"""
    dataPath = "{db_path}"
    db = database(dataPath)
    overview_columns = ["symbol", "exchange", "count", "start", "end", "datetime"]
    overview_type = [SYMBOL, SYMBOL, INT, NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP]
    tickoverview = table(1:0, overview_columns, overview_type)
    db.createPartitionedTable(
        tickoverview,
        "{table_name}",
        partitionColumns=["datetime"],
        sortColumns=["symbol", "exchange", "datetime"],
        keepDuplicates=LAST)
    """

    return script


def create_contract_table_script(database: str, table_name: str = "contract"):
    db_path = "dfs://" + database
    script = f"""
    dataPath = "{db_path}"
    db = database(dataPath)
    overview_columns = ["symbol", "exchange", "datetime", "name", "product", "product_id", "size", "pricetick", "list_date", "expire_date", "min_volume"]
    overview_type = [SYMBOL, SYMBOL, NANOTIMESTAMP, SYMBOL, SYMBOL, SYMBOL, DOUBLE, DOUBLE, NANOTIMESTAMP, NANOTIMESTAMP, DOUBLE]
    tickoverview = table(1:0, overview_columns, overview_type)
    db.createPartitionedTable(
        tickoverview,
        "{table_name}",
        partitionColumns=["expire_date"],
        sortColumns=["symbol", "exchange", "product", "list_date"],
        keepDuplicates=LAST)
    """

    return script


def create_contract_future_table_script(database: str, table_name: str = "contract_futures"):
    db_path = "dfs://" + database
    script = f"""
    dataPath = "{db_path}"
    db = database(dataPath)
    overview_columns = ["symbol", "exchange", "datetime", "name", "product_id", "size", "pricetick", "list_date", "expire_date", "min_volume"]
    overview_type = [SYMBOL, SYMBOL, NANOTIMESTAMP, SYMBOL, SYMBOL, DOUBLE, DOUBLE, NANOTIMESTAMP, NANOTIMESTAMP, DOUBLE]
    tickoverview = table(1:0, overview_columns, overview_type)
    db.createPartitionedTable(
        tickoverview,
        "{table_name}",
        partitionColumns=["expire_date"],
        sortColumns=["symbol", "exchange", "list_date"],
        keepDuplicates=LAST)
    """

    return script


def create_contract_option_table_script(database: str, table_name: str = "contract_options"):
    db_path = "dfs://" + database
    script = f"""
    dataPath = "{db_path}"
    db = database(dataPath)
    overview_columns = ["symbol", "exchange", "datetime", "name", "product_id", "size", "pricetick", "list_date", "expire_date", "min_volume", "option_strike", "option_underlying", "option_type", "option_portfolio", "option_index"]
    overview_type = [SYMBOL, SYMBOL, NANOTIMESTAMP, SYMBOL, SYMBOL, DOUBLE, DOUBLE, NANOTIMESTAMP, NANOTIMESTAMP, DOUBLE, DOUBLE, SYMBOL, SYMBOL, SYMBOL, SYMBOL]
    tickoverview = table(1:0, overview_columns, overview_type)
    db.createPartitionedTable(
        tickoverview,
        "{table_name}",
        partitionColumns=["datetime"],
        sortColumns=["symbol", "exchange", "datetime"],
        keepDuplicates=LAST)
    """

    return script


def create_trade_time_table_script(database: str, table_name: str = "trade_time"):
    db_path = "dfs://" + database
    script = f"""
    dataPath = "{db_path}"
    db = database(dataPath)

    overview_columns = ["product_id", "type", "serial", "start", "end", "datetime"]
    overview_type = [SYMBOL, SYMBOL, INT, NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP]
    baroverview = table(1:0, overview_columns, overview_type)
    db.createPartitionedTable(
        baroverview,
        "{table_name}",
        partitionColumns=["datetime"],
        sortColumns=["product_id", "type", "datetime"],
        keepDuplicates=LAST)
    """

    return script


def create_factor_table_script(database: str, table_name: str = "factor"):
    db_path = "dfs://" + database
    script = f"""
    dataPath = "{db_path}"
    db = database(dataPath)

    bar_columns = ["symbol", "exchange", "datetime", "interval", "factor_id", "factor_name", "factor_value"]
    bar_type = [SYMBOL, SYMBOL, NANOTIMESTAMP, SYMBOL, SYMBOL, SYMBOL, DOUBLE]
    bar = table(1:0, bar_columns, bar_type)

    db.createPartitionedTable(
        bar,
        "{table_name}",
        partitionColumns=["datetime"],
        sortColumns=["symbol", "exchange", "interval", "factor_id", "datetime"],
        keepDuplicates=LAST)
    """

    return script


SCRIPTS_FUNC = {
    'database': create_database_script,
    'tick': create_tick_table_script,

    'bar': create_bar_table_script,
    'bar_options': create_bar_table_script,
    'bar_futures': create_bar_table_script,

    'contract': create_contract_table_script,
    'contract_futures': create_contract_future_table_script,
    'contract_options': create_contract_option_table_script,

    'factor': create_factor_table_script,
}
