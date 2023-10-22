import pandas as pd
import numpy as np
from numpy import nansum
from ipdb import set_trace


def weighted_average(df, column, weight_column="Weight"):
    data = df[column]
    masked_data = np.ma.masked_array(data, np.isnan(data))
    return np.ma.average(masked_data, weights=df[weight_column])


def weighted_median(values, weights):
    i = np.argsort(values)
    c = np.cumsum(weights[i])
    return values[i[np.searchsorted(c, 0.5 * c[-1])]]


def mask_na(series_data):
    return np.ma.masked_array(series_data, np.isnan(series_data))


def weighted_average_df(df, column_weight_name_pairs, groups):
    cross_columns = []
    weight_columns = []
    name_columns = []
    for column_weight_pair in column_weight_name_pairs:
        column, weight, name = column_weight_pair
        cross_column = f"_cross_{column}_{weight}"
        df[cross_column] = df[column] * df[weight]
        cross_columns.append(cross_column)
        weight_columns.append(weight)
        name_columns.append(name)
    df_grouped = df.groupby(groups)[cross_columns + weight_columns].agg(nansum)
    for cross_column, weight, name in zip(cross_columns, weight_columns, name_columns):
        df_grouped[name] = df_grouped[cross_column] / df_grouped[weight]

    return df_grouped[name_columns]
