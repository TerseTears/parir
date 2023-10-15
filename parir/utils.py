from functools import wraps
from time import time
from pathlib import Path
import pyreadr
import pandas as pd


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        print("func:%r" % (f.__name__))
        result = f(*args, **kw)
        te = time()
        print("took: %2.4f sec" % (te - ts))
        return result

    return wrap


@timing
def read_all_years(element, name, metadata):
    years_tuple = metadata["analysis_years"]
    years = list(range(years_tuple[0], years_tuple[1] + 1))

    all_df = []
    for year in years:
        df = pyreadr.read_r(Path(metadata["rdata_path"]) / f"Y{year}{name}.rda")[
            element
        ]
        df["Year"] = year
        all_df.append(df)
    return pd.concat(all_df)
