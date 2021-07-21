import copy
import os
import time
from concurrent.futures import ProcessPoolExecutor
from typing import List

import numpy as np
import pandas as pd
import requests

from ml_trader.utils import load_config, check_create_folder, save_json, chunks

QUANDL_COMMODITY_CODES = (
    'LBMA/GOLD',
    'LBMA/SILVER',
    'JOHNMATT/PLAT',
    'JOHNMATT/PALL',
    'ODA/PALUM_USD',
    'ODA/PCOPP_USD',
    'ODA/PNICK_USD',
    'SHFE/RBV2013',
    'ODA/PBARL_USD',
    'TFGRAIN/CORN',
    'ODA/PRICENPQ_USD',
    'CHRIS/CME_DA1',
    'ODA/PBEEF_USD',
    'ODA/PPOULT_USD',
    'ODA/PPORK_USD',
    'ODA/PWOOLC_USD',
    'CHRIS/CME_CL1',
    'ODA/POILWTI_USD',
    'ODA/POILBRE_USD',
    'CHRIS/CME_NG1',
    'ODA/PCOALAU_USD',
    'ODA/PCOFFOTM_USD',
    'ODA/PCOCO_USD',
    'ODA/PSUGAUSA_USD',
    'ODA/PORANG_USD',
    'ODA/PBANSOP_USD',
    'ODA/POLVOIL_USD',
    'ODA/PLOGSK_USD',
    'ODA/PCOTTIND_USD'
)


def _format_quandl_url(path: str) -> str:
    config = load_config()
    api_key = config["quandl"]["api_key"]
    url = f'https://www.quandl.com/api/v3/{path}'
    return f'{url}&api_key={api_key}' if '?' in url else f'{url}?api_key={api_key}'


def download_base_zip(path: str, save_path: str) -> None:
    full_url = _format_quandl_url(path)
    r_info = requests.get(full_url)
    if r_info.status_code != 200:
        print(f'Error: {path}, {r_info.status_code}')
        return

    zip_link = r_info.json()['datatable_bulk_download']['file']['link']
    print(f'Started downloading ZPI from {zip_link}')
    r_file = requests.get(zip_link)
    if r_file.status_code != 200:
        print(f'Error: {zip_link}, {r_file.status_code}')
        return

    check_create_folder(save_path)
    with open(save_path, 'wb') as f:
        f.write(r_file.content)


def quandl_base_to_df(filepath: str, tickers: List[str]) -> pd.DataFrame:
    tickers_df = pd.read_csv(filepath)
    tickers_df = tickers_df[tickers_df['table'] == 'SF1']

    tmp = pd.DataFrame()
    tmp['ticker'] = tickers
    tmp['flag'] = True
    tickers_df = pd.merge(tickers_df, tmp, on='ticker', how='left')
    tickers_df['flag'] = tickers_df['flag'].fillna(False)
    tickers_df = tickers_df[tickers_df['flag']]
    del tickers_df['flag']

    return tickers_df.reset_index(drop=True)


def _batch_ticker_download(
        path: str,
        tickers: List[str],
        base_path: str,
        sleep_time: float = 2,
) -> None:
    print(f'Downloading {tickers}')

    full_url = _format_quandl_url(path.format(ticker=','.join(tickers)))
    r = requests.get(full_url)
    if r.status_code != 200:
        print(f'Error: {full_url}')
        return

    data = r.json()
    datatable_data = np.array(data['datatable']['data'])
    ticker_seq = np.array([x[0] for x in data['datatable']['data']])

    for ticker in tickers:
        ticker_data = copy.deepcopy(data)
        ticker_data['datatable']['data'] = datatable_data[ticker_seq == ticker].tolist() if len(ticker_seq) else []

        filepath = '{}/{}.json'.format(base_path, ticker)
        save_json(filepath, ticker_data)

    time.sleep(np.random.uniform(0, sleep_time))


def multiprocess_ticker_download(
        path: str,
        tickers: List[str],
        base_path: str,
        batch_size: int = 2,
        n_jobs: int = 4,
        skip_exists: bool = True,
) -> None:
    os.makedirs(base_path, exist_ok=True)

    tickers_to_download = tickers
    if skip_exists:
        exist_tickers = [x.split('.')[0] for x in os.listdir(base_path)]
        if exist_tickers:
            print(f'Skip {len(exist_tickers)} tickers')
        tickers_to_download = list(set(tickers).difference(set(exist_tickers)))

    with ProcessPoolExecutor(max_workers=n_jobs) as executor:
        for chunk in chunks(tickers_to_download, batch_size):
            executor.submit(
                _batch_ticker_download,
                path=path,
                tickers=chunk,
                base_path=base_path,
            )


def download_commodities(base_path: str) -> None:
    """
    Download commodities price history from
    https://blog.quandl.com/api-for-commodity-data
    """
    for code in QUANDL_COMMODITY_CODES:
        print(f'Downloading {code}')
        full_url = _format_quandl_url(f'datasets/{code}')
        r = requests.get(full_url)
        if r.status_code != 200:
            print(f'Error: {full_url}')
            return

        code_data = r.json()
        filepath = '{}/{}.json'.format(base_path, code.replace('/', '_'))
        save_json(filepath, code_data)
