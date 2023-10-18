import pandas as pd
import numpy as np


def weighted_average(df, column, weight_column = "Weight"):
    data = df[column]
    masked_data = np.ma.masked_array(data, np.isnan(data))
    return np.ma.average(masked_data, weights=df[weight_column])

def weighted_median(values, weights):
    i = np.argsort(values)
    c = np.cumsum(weights[i])
    return values[i[np.searchsorted(c, 0.5 * c[-1])]]

def mask_na(series_data):
    return np.ma.masked_array(series_data, np.isnan(series_data))

