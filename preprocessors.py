import datetime
import re

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class TemporalVariableTransformer(BaseEstimator, TransformerMixin):
    # Temporal elapsed time transformer

    def __init__(self, variables, reference_variable):

        if not isinstance(variables, list):
            raise ValueError("variables should be a list")

        self.variables = variables
        self.reference_variable = reference_variable

    def fit(self, X, y=None):
        # we need this step to fit the sklearn pipeline
        return self

    def transform(self, X):

        # so that we do not over-write the original dataframe
        X = X.copy()

        for feature in self.variables:
            X[feature] = X[self.reference_variable] - X[feature]

        return X


class TemporalVariableTransformerDays(BaseEstimator, TransformerMixin):
    # Temporal elapsed time transformer

    def __init__(self, variables, reference_variable, variables_first=False):

        if not isinstance(variables, list):
            raise ValueError("variables should be a list")

        self.variables = variables
        self.variables_first = variables_first

        self.new_col_list = [s + "_days" for s in variables]

        if reference_variable == "today":
            self.reference_variable = datetime.datetime.today()
            self.is_today = True
        else:
            self.reference_variable = reference_variable

    def fit(self, X, y=None):
        # we need this step to fit the sklearn pipeline
        return self

    def transform(self, X):

        # so that we do not over-write the original dataframe
        X = X.copy()

        for feature, new_col in zip(self.variables, self.new_col_list):
            if self.is_today:
                if self.variables_first:
                    X[new_col] = (X[feature] - self.reference_variable).dt.days
                else:
                    X[new_col] = (self.reference_variable - X[feature]).dt.days
            else:
                if self.variables_first:
                    X[new_col] = (X[feature] - X[self.reference_variable]).dt.days
                else:
                    X[new_col] = (X[self.reference_variable] - X[feature]).dt.days

        return X


class TemporalVariableTransformerYears(BaseEstimator, TransformerMixin):
    # Temporal elapsed time transformer

    def __init__(self, variables, reference_variable, variables_first=False):

        if not isinstance(variables, list):
            raise ValueError("variables should be a list")

        self.variables = variables
        self.variables_first = variables_first

        self.new_col_list = [s + "_years" for s in variables]

        if reference_variable == "today":
            self.reference_variable = datetime.datetime.today()
            self.is_today = True
        else:
            self.reference_variable = reference_variable

    def fit(self, X, y=None):
        # we need this step to fit the sklearn pipeline
        return self

    def transform(self, X):

        # so that we do not over-write the original dataframe
        X = X.copy()

        for feature, new_col in zip(self.variables, self.new_col_list):
            if self.is_today:
                if self.variables_first:
                    X[new_col] = X[feature].dt.year - self.reference_variable.year
                else:
                    X[new_col] = self.reference_variable.year - X[feature].dt.year
            else:
                if self.variables_first:
                    X[new_col] = X[feature].dt.year - X[self.reference_variable].dt.year
                else:
                    X[new_col] = X[self.reference_variable].dt.year - X[feature].dt.year

        return X


class VariableNameRegex(BaseEstimator, TransformerMixin):
    # Temporal elapsed time transformer

    def __init__(self):

        self.regex = re.compile(r"\[|\]|<", re.IGNORECASE)

    def fit(self, X, y=None):
        # we need this step to fit the sklearn pipeline
        return self

    def transform(self, X):
        # so that we do not over-write the original dataframe
        X = X.copy()

        X.columns = [
            self.regex.sub("_", col)
            if any(x in str(col) for x in set(("[", "]", "<")))
            else col
            for col in X.columns.values
        ]

        return X


# categorical missing value imputer
class Mapper(BaseEstimator, TransformerMixin):
    def __init__(self, variables, mappings):

        if not isinstance(variables, list):
            raise ValueError("variables should be a list")

        self.variables = variables
        self.mappings = mappings

    def fit(self, X, y=None):
        # we need the fit statement to accomodate the sklearn pipeline
        return self

    def transform(self, X):
        X = X.copy()
        for feature in self.variables:
            X[feature] = X[feature].map(self.mappings)

        return X
