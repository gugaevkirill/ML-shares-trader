import os

from ml_trader.data_loaders.quandl import multiprocess_ticker_download
from ml_trader.utils import load_config

if __name__ == '__main__':
    datasets_path = os.path.join(os.path.dirname(os.getcwd()), 'datasets')
    config = load_config()

    # Yahoo (all)
    # download_yahoo('AAPL', base_path=datasets_path)

    # Quandl #1
    # zip_download(
    #     'datatables/SHARADAR/TICKERS?qopts.export=true',
    #     datasets_path + '/quandl/tickers.zip'
    # )
    # tickers.zip extracted to SHARADAR_TICKERS_6cc728d11002ab9cb99aa8654a6b9f4e.csv

    # Quandl #2
    # multiprocess_ticker_download(
    #     path='datatables/SHARADAR/SF1?ticker={ticker}',
    #     tickers=config["tickers_quandl"],
    #     base_path=datasets_path + '/quandl/core_fundamental',
    # )

    # Quandl #3
    multiprocess_ticker_download(
        path='datatables/SHARADAR/DAILY?ticker={ticker}',
        tickers=config["tickers_quandl"],
        base_path=datasets_path + '/quandl/daily',
    )
