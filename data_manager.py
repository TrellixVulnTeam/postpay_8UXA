# Python libraries
import datetime
import json

# 3rd party libraries
import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame

# import datetime


def to_numeric(df, columns_list):
    """
    Convert columns to numeric
    """
    df[columns_list] = df[columns_list].apply(pd.to_numeric)


def create_agg_var(
    df: DataFrame,
    groupby_col: str,
    orig_cols: list,
    new_col_names: list,
    agg_fnc: str,
    condition_column: str = None,
    condition_cat: str = None,
    date_filter_col: str = None,
    nr_days_filter: int = None,
):  # sourcery no-metrics skip: assign-if-exp, inline-immediately-returned-variable, lift-return-into-if, switch
    """
    Create aggregated variables per groupby_col
    """
    if date_filter_col and nr_days_filter:
        start_date_filter = datetime.datetime.today() - datetime.timedelta(
            days=nr_days_filter
        )
        df = df[pd.to_datetime(df[date_filter_col]) >= start_date_filter]

    if condition_column:
        df = df[[*orig_cols, groupby_col, condition_column]]
    else:
        df = df[[*orig_cols, groupby_col]]

    if agg_fnc == "sum":
        if condition_column is None:
            agg_df = (
                df.groupby(groupby_col)
                .sum()[orig_cols]
                .reset_index()
                .rename(columns=dict(zip(orig_cols, new_col_names)))
            )
        else:
            agg_df = (
                df[df[condition_column] == condition_cat]
                .groupby(groupby_col)
                .sum()[orig_cols]
                .reset_index()
                .rename(columns=dict(zip(orig_cols, new_col_names)))
            )
        return agg_df
    elif agg_fnc == "mean":
        if condition_column is None:
            agg_df = (
                df.groupby(groupby_col)
                .mean()[orig_cols]
                .reset_index()
                .rename(columns=dict(zip(orig_cols, new_col_names)))
            )
        else:
            agg_df = (
                df[df[condition_column] == condition_cat]
                .groupby(groupby_col)
                .mean()[orig_cols]
                .reset_index()
                .rename(columns=dict(zip(orig_cols, new_col_names)))
            )
        return agg_df
    elif agg_fnc == "std":
        if condition_column is None:
            agg_df = (
                df.groupby(groupby_col)
                .std()[orig_cols]
                .reset_index()
                .rename(columns=dict(zip(orig_cols, new_col_names)))
            )
        else:
            agg_df = (
                df[df[condition_column] == condition_cat]
                .groupby(groupby_col)
                .std()[orig_cols]
                .reset_index()
                .rename(columns=dict(zip(orig_cols, new_col_names)))
            )
        return agg_df
    elif agg_fnc == "min":
        if condition_column is None:
            agg_df = (
                df.groupby(groupby_col)
                .min()[orig_cols]
                .reset_index()
                .rename(columns=dict(zip(orig_cols, new_col_names)))
            )
        else:
            agg_df = (
                df[df[condition_column] == condition_cat]
                .groupby(groupby_col)
                .min()[orig_cols]
                .reset_index()
                .rename(columns=dict(zip(orig_cols, new_col_names)))
            )
        return agg_df
    elif agg_fnc == "max":
        if condition_column is None:
            agg_df = (
                df.groupby(groupby_col)
                .max()[orig_cols]
                .reset_index()
                .rename(columns=dict(zip(orig_cols, new_col_names)))
            )
        else:
            agg_df = (
                df[df[condition_column] == condition_cat]
                .groupby(groupby_col)
                .max()[orig_cols]
                .reset_index()
                .rename(columns=dict(zip(orig_cols, new_col_names)))
            )
        return agg_df
    elif agg_fnc == "count":
        if condition_column is None:
            agg_df = (
                df.groupby(groupby_col)
                .count()[orig_cols]
                .reset_index()
                .rename(columns=dict(zip(orig_cols, new_col_names)))
            )
        else:
            agg_df = (
                df[df[condition_column] == condition_cat]
                .groupby(groupby_col)
                .count()[orig_cols]
                .reset_index()
                .rename(columns=dict(zip(orig_cols, new_col_names)))
            )
        return agg_df
    elif agg_fnc == "nunique":
        if condition_column is None:
            agg_df = (
                df.groupby(groupby_col)
                .nunique()[orig_cols]
                .reset_index()
                .rename(columns=dict(zip(orig_cols, new_col_names)))
            )
        else:
            agg_df = (
                df[df[condition_column] == condition_cat]
                .groupby(groupby_col)
                .nunique()[orig_cols]
                .reset_index()
                .rename(columns=dict(zip(orig_cols, new_col_names)))
            )
        return agg_df


def two_date_cols_diff(
    df: DataFrame, new_col_name: str, first_col: str, second_col: str
):
    """
    Create new variable as the number of days between two date columns
    """
    df_new = df
    df_new[new_col_name] = (
        pd.to_datetime(df_new[second_col]) - pd.to_datetime(df_new[first_col])
    ).dt.days
    return df_new


def days_from_today(
    df: DataFrame,
    new_col_name: str,
    date_col: str,
    drop_orig_col: bool = False,
):
    """Create new variable as the number of days between a date and today"""
    df_new = df
    df_new[new_col_name] = (
        datetime.datetime.today() - pd.to_datetime(df_new[date_col])
    ).dt.days
    if drop_orig_col:
        df_new.drop(columns=date_col, inplace=True)
    return df_new


def last_row(df: DataFrame, groupby_col: str, by_last_col: str):
    # sourcery skip: inline-immediately-returned-variable
    last_row_df = (
        df.groupby(groupby_col, as_index=True).max()[[by_last_col]].reset_index()
    )
    return last_row_df


def create_unpaid_cols(
    df: DataFrame,
    unpaid_col_names: list,
    condition_col: str,
    col_to_check_paid: str,
    col_to_check_unpaid: str,
    col_to_use_paid: str,
    col_to_use_unpaid: str,
):  # sourcery no-metrics skip: extract-duplicate-method
    for c in unpaid_col_names:
        df[c] = np.nan

    for i in range(len(df)):
        if df[condition_col][i] == "paid":
            if df[col_to_check_paid][i] <= 0:
                df["unpaid_at_due"][i] = 0
                df["unpaid_at_5"][i] = 0
                df["unpaid_at_10"][i] = 0
                df["unpaid_at_20"][i] = 0
                df["unpaid_at_30"][i] = 0
                df["unpaid_at_60"][i] = 0
                df["unpaid_at_90"][i] = 0
            elif (df[col_to_check_paid][i] > 0) & (df[col_to_check_paid][i] <= 5):
                df["unpaid_at_due"][i] = df[col_to_use_paid][i]
                df["unpaid_at_5"][i] = 0
                df["unpaid_at_10"][i] = 0
                df["unpaid_at_20"][i] = 0
                df["unpaid_at_30"][i] = 0
                df["unpaid_at_60"][i] = 0
                df["unpaid_at_90"][i] = 0
            elif (df[col_to_check_paid][i] > 5) & (df[col_to_check_paid][i] <= 10):
                df["unpaid_at_due"][i] = df[col_to_use_paid][i]
                df["unpaid_at_5"][i] = df[col_to_use_paid][i]
                df["unpaid_at_10"][i] = 0
                df["unpaid_at_20"][i] = 0
                df["unpaid_at_30"][i] = 0
                df["unpaid_at_60"][i] = 0
                df["unpaid_at_90"][i] = 0
            elif (df[col_to_check_paid][i] > 10) & (df[col_to_check_paid][i] <= 20):
                df["unpaid_at_due"][i] = df[col_to_use_paid][i]
                df["unpaid_at_5"][i] = df[col_to_use_paid][i]
                df["unpaid_at_10"][i] = df[col_to_use_paid][i]
                df["unpaid_at_20"][i] = 0
                df["unpaid_at_30"][i] = 0
                df["unpaid_at_60"][i] = 0
                df["unpaid_at_90"][i] = 0
            elif (df[col_to_check_paid][i] > 20) & (df[col_to_check_paid][i] <= 30):
                df["unpaid_at_due"][i] = df[col_to_use_paid][i]
                df["unpaid_at_5"][i] = df[col_to_use_paid][i]
                df["unpaid_at_10"][i] = df[col_to_use_paid][i]
                df["unpaid_at_20"][i] = df[col_to_use_paid][i]
                df["unpaid_at_30"][i] = 0
                df["unpaid_at_60"][i] = 0
                df["unpaid_at_90"][i] = 0
            elif (df[col_to_check_paid][i] > 30) & (df[col_to_check_paid][i] <= 60):
                df["unpaid_at_due"][i] = df[col_to_use_paid][i]
                df["unpaid_at_5"][i] = df[col_to_use_paid][i]
                df["unpaid_at_10"][i] = df[col_to_use_paid][i]
                df["unpaid_at_20"][i] = df[col_to_use_paid][i]
                df["unpaid_at_30"][i] = df[col_to_use_paid][i]
                df["unpaid_at_60"][i] = 0
                df["unpaid_at_90"][i] = 0
            elif (df[col_to_check_paid][i] > 60) & (df[col_to_check_paid][i] <= 90):
                df["unpaid_at_due"][i] = df[col_to_use_paid][i]
                df["unpaid_at_5"][i] = df[col_to_use_paid][i]
                df["unpaid_at_10"][i] = df[col_to_use_paid][i]
                df["unpaid_at_20"][i] = df[col_to_use_paid][i]
                df["unpaid_at_30"][i] = df[col_to_use_paid][i]
                df["unpaid_at_60"][i] = df[col_to_use_paid][i]
                df["unpaid_at_90"][i] = 0
            elif df[col_to_check_paid][i] > 90:
                df["unpaid_at_due"][i] = df[col_to_use_paid][i]
                df["unpaid_at_5"][i] = df[col_to_use_paid][i]
                df["unpaid_at_10"][i] = df[col_to_use_paid][i]
                df["unpaid_at_20"][i] = df[col_to_use_paid][i]
                df["unpaid_at_30"][i] = df[col_to_use_paid][i]
                df["unpaid_at_60"][i] = df[col_to_use_paid][i]
                df["unpaid_at_90"][i] = df[col_to_use_paid][i]

        elif df[condition_col][i] == "unpaid":
            if (df[col_to_check_unpaid][i] >= 0) & (df[col_to_check_unpaid][i] < 5):
                df["unpaid_at_due"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_5"][i] = np.nan
                df["unpaid_at_10"][i] = np.nan
                df["unpaid_at_20"][i] = np.nan
                df["unpaid_at_30"][i] = np.nan
                df["unpaid_at_60"][i] = np.nan
                df["unpaid_at_90"][i] = np.nan
            elif (df[col_to_check_unpaid][i] >= 5) & (df[col_to_check_unpaid][i] < 10):
                df["unpaid_at_due"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_5"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_10"][i] = np.nan
                df["unpaid_at_20"][i] = np.nan
                df["unpaid_at_30"][i] = np.nan
                df["unpaid_at_60"][i] = np.nan
                df["unpaid_at_90"][i] = np.nan
            elif (df[col_to_check_unpaid][i] >= 10) & (df[col_to_check_unpaid][i] < 20):
                df["unpaid_at_due"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_5"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_10"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_20"][i] = np.nan
                df["unpaid_at_30"][i] = np.nan
                df["unpaid_at_60"][i] = np.nan
                df["unpaid_at_90"][i] = np.nan
            elif (df[col_to_check_unpaid][i] >= 20) & (df[col_to_check_unpaid][i] < 30):
                df["unpaid_at_due"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_5"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_10"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_20"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_30"][i] = np.nan
                df["unpaid_at_60"][i] = np.nan
                df["unpaid_at_90"][i] = np.nan
            elif (df[col_to_check_unpaid][i] >= 30) & (df[col_to_check_unpaid][i] < 60):
                df["unpaid_at_due"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_5"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_10"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_20"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_30"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_60"][i] = np.nan
                df["unpaid_at_90"][i] = np.nan
            elif (df[col_to_check_unpaid][i] >= 60) & (df[col_to_check_unpaid][i] < 90):
                df["unpaid_at_due"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_5"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_10"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_20"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_30"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_60"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_90"][i] = np.nan
            elif df[col_to_check_unpaid][i] >= 90:
                df["unpaid_at_due"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_5"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_10"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_20"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_30"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_60"][i] = df[col_to_use_unpaid][i]
                df["unpaid_at_90"][i] = df[col_to_use_unpaid][i]


def get_substring(string_value: str, strings_list: list):
    if string_value is None:
        return np.nan
    for s in strings_list:
        if s in string_value:
            return s


def load_metadata(config_name):
    """
    Insert the configuration file, and get a dictionary as an output
    "r" - is for 'read' mode
    """
    with open(config_name, "r") as fd:
        json_dict = json.load(
            fd
        )  # load() loads the file and turns it into a dictionary

    return json_dict


def partitioned_row_number(
    df: DataFrame, groupby_col_list: list, sort_by_col_list: list
):
    partitioned_row_number = df.groupby(groupby_col_list)[sort_by_col_list].rank(
        method="first", ascending=True
    )
    return partitioned_row_number
