import time
from functools import reduce
from typing import Dict, Any

import numpy as np
import pandas as pd
import requests

from ml_trader.utils import save_json, check_create_folder

DEFAULT_TYPE_LIST = [
    'quarterlyTotalCapitalization',
    'quarterlyTotalRevenue',
    'quarterlyNetIncome',
    'quarterlyFreeCashFlow',
    'quarterlyTotalAssets',
    'quarterlyEBITDA',
    'quarterlyNetDebt',
    'quarterlyGrossProfit',
    'quarterlyWorkingCapital',
    'quarterlyCashAndCashEquivalents',
    'quarterlyResearchAndDevelopment',
    'quarterlyCashDividendsPaid',
]


def _parse_raw_values(json_data: Dict[str, Any]):
    new_row = {}
    for key in json_data.keys():
        if type(json_data[key]) == dict and 'raw' in json_data[key]:
            new_row[key] = json_data[key]['raw']
            continue
        # if type(json_data[key]) in [list, dict] and len(json_data[key]) == 0:
        #     new_row[key] = None
        #     continue
        new_row[key] = json_data[key]

    return new_row


def _parse_quarterly_json(json_data: Dict[str, Any]):
    dfs = []
    for data in json_data['timeseries']['result']:
        name_set = set(data.keys()).intersection(set(DEFAULT_TYPE_LIST))
        if len(name_set) == 1:
            name = list(name_set)[0]
            new_data = [
                {'date': row['asOfDate'], name: row['reportedValue']['raw']}
                for row in data[name]
            ]
            dfs.append(pd.DataFrame(new_data))
    if len(dfs) == 0:
        return
    result = reduce(lambda l, r: pd.merge(l, r, on='date', how='left'), dfs)
    for key in set(DEFAULT_TYPE_LIST).difference(set(result.columns)):
        result[key] = None

    result['date'] = result['date'].astype(np.datetime64)
    result = result.sort_values('date', ascending=False)

    return result


def download_ticker_base(ticker: str, base_path: str) -> None:
    url = ('https://query{query_id}.finance.yahoo.com/v10/finance/quoteSummary/{ticker}'
           '?modules=summaryProfile,defaultKeyStatistics&corsDomain=finance.yahoo.com')
    url = url.format(query_id=2, ticker=ticker)

    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    }

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(r.status_code, ticker)
        return

    json_data = r.json()['quoteSummary']['result'][0]

    result = {}
    b1 = _parse_raw_values(json_data['summaryProfile'])
    b2 = _parse_raw_values(json_data['defaultKeyStatistics'])
    result.update(b1)
    result.update(b2)

    filepath = '{}/{}.json'.format(base_path, ticker)
    save_json(filepath, result)


def download_ticker_quarterly(ticker: str, base_path: str) -> None:
    base_url = ('https://query{query_id}.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries'
                '/{ticker}?lang=en-US&region=US&padTimeSeries=false&type={type_str}'
                '&merge=false&period1=493590046&period2={period2}&corsDomain=finance.yahoo.com')
    url = base_url.format(query_id=2,
                          ticker=ticker,
                          type_str=','.join(DEFAULT_TYPE_LIST),
                          period2=int(time.time()))

    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    }
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(r.status_code, ticker)
        return

    json_data = r.json()
    quarterly_df = _parse_quarterly_json(json_data)

    filepath = '{}/{}.csv'.format(base_path, ticker)
    check_create_folder(filepath)
    quarterly_df.to_csv(filepath, index=False)


def download_yahoo(ticker: str, base_path: str) -> None:
    download_ticker_base(ticker, base_path=base_path + '/yahoo/base')
    time.sleep(np.random.uniform(0, 0.5))
    download_ticker_quarterly(ticker, base_path=base_path + '/yahoo/quarterly')
    time.sleep(np.random.uniform(0, 0.5))
