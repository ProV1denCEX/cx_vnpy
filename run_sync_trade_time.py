import datetime as dt

import numpy as np
import pandas as pd
import dolphindb as ddb

from Pandora.constant import Product
from vnpy.trader.database import get_database
from vnpy.trader.setting import SETTINGS


def main():
    database = get_database()
    start = (dt.datetime.now() - dt.timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)

    contracts = database.load_contract_data(product=Product.FUTURES, start=start, end=None)
    contracts = [i.product_id for i in contracts]

    aaa = 1


if __name__ == '__main__':
    main()
