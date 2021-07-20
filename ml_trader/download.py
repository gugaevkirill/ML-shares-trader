import os
import time

import numpy as np

from data_loaders.yahoo import download_ticker_base, download_ticker_quarterly

datasets_path = os.path.join(os.path.dirname(os.getcwd()), 'datasets')


def download_yahoo(ticker: str) -> None:
    download_ticker_base(ticker, base_path=datasets_path)
    time.sleep(np.random.uniform(0, 0.5))
    download_ticker_quarterly(ticker, base_path=datasets_path)
    time.sleep(np.random.uniform(0, 0.5))


download_yahoo('AAPL')