import os

from ml_trader.data_loaders.quandl import (download_commodities,
                                           download_base_zip,
                                           multiprocess_ticker_download)
from ml_trader.data_loaders.yahoo import download_yahoo
from ml_trader.utils import load_config

if __name__ == '__main__':
    datasets_path = os.path.join(os.path.dirname(os.getcwd()), 'datasets')
    config = load_config()

    # Yahoo Example (base + quarterly)
    #   we do not use Yahoo in the project
    download_yahoo('AAPL', base_path=datasets_path)

    # Quandl #1 base
    # tickers.zip extracted to SHARADAR_TICKERS_6cc728d11002ab9cb99aa8654a6b9f4e.csv
    download_base_zip(
        'datatables/SHARADAR/TICKERS?qopts.export=true',
        datasets_path + '/quandl/tickers.zip'
    )

    # Quandl #2
    multiprocess_ticker_download(
        path='datatables/SHARADAR/SF1?ticker={ticker}',
        tickers=config["tickers"],
        base_path=datasets_path + '/quandl/quarterly',
    )

    # Quandl #3
    multiprocess_ticker_download(
        path='datatables/SHARADAR/DAILY?ticker={ticker}',
        tickers=config["tickers"],
        base_path=datasets_path + '/quandl/daily',
    )

    # Quandl #4 commodities
    download_commodities(datasets_path + '/quandl/commodity')
