from typing import Union, List, Dict, Any

import numpy as np
import pandas as pd

MAX_BACK_QUARTER = 20  # Max bound of company slices in time
MIN_BACK_QUARTER = 0  # Min bound of company slices in time

QUARTER_WINDOWS = [2, 4, 10]

# Column names for feature calculation (like revenue, debt etc)
QUARTER_COLUMNS = [
    "revenue",
    "netinc",
    "ncf",
    "assets",
    "ebitda",
    "debt",
    "fcf",
    "gp",
    "workingcapital",
    "cashneq",
    "rnd",
    "sgna",
    "ncfx",
    "divyield",
    "currentratio",
    "netinccmn",
]

DAILY_WINDOWS = [100, 200, 400, 800]
DAILY_AGG_COLUMNS = ["marketcap", "pe"]


def calc_series_stats(series: Union[List[float], np.array]) -> Dict[str, float]:
    series = np.array(series).astype('float')
    series = series[~np.isnan(series)]
    series = list(series)
    if len(series) == 0:
        series = np.array([np.nan])
    stats = {
        'mean': np.mean(series),
        'median': np.median(series),
        'max': np.max(series),
        'min': np.min(series),
        'std': np.std(series),
    }
    return stats


def compute_df_quarterly_ticker(
        df_quarterly_ticker: pd.DataFrame,
        ticker: str,
) -> List[Dict[str, Any]]:
    data_len = len(df_quarterly_ticker)
    max_bq = min(MAX_BACK_QUARTER, data_len - 1)
    min_bq = min(MIN_BACK_QUARTER, data_len - 1)
    assert min_bq <= max_bq

    result: List[Dict[str, Any]] = []
    for back_quarter in range(min_bq, max_bq):
        curr_data = df_quarterly_ticker[back_quarter:]
        if not len(curr_data):
            continue

        features = {
            'ticker': ticker,
            'date': curr_data['date'].values[0],
        }
        for col in QUARTER_COLUMNS:
            for window in QUARTER_WINDOWS:
                # Calculate window features
                # Series - list of col values (from newest to old)
                # Diffs - list of fare diffs (from old to new)
                #   direction does not matter for statistics like min, max, std, mean
                series = curr_data[col].values[:window].astype('float')
                diffs = np.diff(series[::-1])

                for s_name, s_value in zip(['series', 'diffs'], [series, diffs]):
                    name_prefix = 'quarter_{}_{}_{}'.format(s_name, window, col)
                    stats = calc_series_stats(s_value)
                    for k, v in stats.items():
                        features['{}_{}'.format(name_prefix, k)] = v

        result.append(features)

    return result


def compute_df_daily_ticker(
        df_quarterly_ticker: pd.DataFrame,
        df_daily_ticker: pd.DataFrame,
        ticker: str,
) -> List[Dict[str, Any]]:
    data_len = len(df_quarterly_ticker)
    max_bq = min(MAX_BACK_QUARTER, data_len - 1)
    min_bq = min(MIN_BACK_QUARTER, data_len - 1)
    assert min_bq <= max_bq

    result: List[Dict[str, Any]] = []
    for back_quarter in range(min_bq, max_bq):
        curr_data = df_quarterly_ticker[back_quarter:]
        if not len(curr_data):
            continue

        # Date to start counting daily features from
        curr_date = np.datetime64(curr_data['date'].values[0])
        curr_daily_data = df_daily_ticker[curr_daily_data['date'] < curr_date]
        if not len(curr_daily_data):
            continue

        features = {
            'ticker': ticker,
            'date': curr_date,
        }
        for col in DAILY_AGG_COLUMNS:
            for window in DAILY_WINDOWS:
                # Calculate window features
                # Series - list of col values (from newest to old)
                # Diffs - list of fare diffs (from old to new)
                #   direction does not matter for statistics like min, max, std, mean
                series = curr_daily_data[col].values[:window].astype('float')
                diffs = np.diff(series[::-1])

                for s_name, s_value in zip(['series', 'diffs'], [series, diffs]):
                    name_prefix = 'daily_{}_{}_{}'.format(s_name, window, col)
                    stats = calc_series_stats(s_value)
                    for k, v in stats.items():
                        features['{}_{}'.format(name_prefix, k)] = v

        result.append(features)

    return result
