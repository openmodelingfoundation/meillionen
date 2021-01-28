#!/usr/bin/env python3

from simplecrop_cli import interface, run

import pandas as pd


if __name__ == '__main__':
    args = interface.to_cli()
    daily_path = args.get_source('daily')
    daily = pd.read_parquet(daily_path)
    yields = run('simplecrop', daily)
    yield_path = args.get_sink('yield')
    yields.to_parquet(yield_path)