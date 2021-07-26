from copy import deepcopy
from typing import Any, List, Union

import numpy as np
import pandas as pd
from tqdm import tqdm


class LogExpModel:
    def __init__(self, base_model):
        self.base_model = base_model

    def fit(self, X: pd.DataFrame, y: Union[pd.Series, np.ndarray, List[Any]]) -> None:
        mask = (y > 0).values
        self.base_model.fit(X[mask], np.log(list(y[mask])))

    def predict(self, X) -> np.ndarray:
        return np.exp(self.base_model.predict(X))


class EnsembleModel:
    """
    Trains ensemble of base_models using Bagging
    """

    def __init__(self, base_models: List, bagging_fraction: float = 0.8, models_cnt: int = 20):
        self.base_models = base_models
        self.bagging_fraction = bagging_fraction
        self.models_cnt = models_cnt
        self.models = []

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        for _ in tqdm(range(self.models_cnt)):
            idxs = np.random.randint(0, len(X), int(len(X) * self.bagging_fraction))
            curr_model = deepcopy(np.random.choice(self.base_models))
            curr_model.fit(X.iloc[idxs], y.iloc[idxs])
            self.models.append(curr_model)

    def predict(self, X):
        preds = []
        for k in range(self.models_cnt):
            try:
                model_pred = self.models[k].predict_proba(X)[:, 1]
            except AttributeError:
                model_pred = self.models[k].predict(X)

            preds.append(model_pred)

        return np.mean(preds, axis=0)
