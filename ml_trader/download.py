import os

from data_loaders.yahoo import download_yahoo

datasets_path = os.path.join(os.path.dirname(os.getcwd()), 'datasets')

download_yahoo('AAPL', base_path=datasets_path)
