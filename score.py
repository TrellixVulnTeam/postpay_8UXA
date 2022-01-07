import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin


class ProbabilityToScore(BaseEstimator, TransformerMixin):
    def __init__(self):

        self.pdo = 20  # Points to Double the Odds
        self.factor = self.pdo / np.log(2)
        self.offset = 600  # Base score

    def fit(self, X, y=None):
        # we need this step to fit the sklearn pipeline
        return self

    def transform(self, probabilities_list):
        # Calculate the score
        score_tmp = [
            (self.offset + self.factor * (np.log((1 - p) / p)))
            for p in probabilities_list
        ]

        # Limit the score to 300-850
        score = np.clip(score_tmp, 300, 850)

        return score
