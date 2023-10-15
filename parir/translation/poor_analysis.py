from parir.utils import timing
import pandas as pd
import numpy as np

def weighted_average(df, column):
    data = df[column]
    masked_data = np.ma.masked_array(data, np.isnan(data))
    return np.ma.average(masked_data, weights=df.Weight)


@timing
def poor_stats(final_poor, metadata, extra_group = None):
    if extra_group is None:
        groups = ["Year", "FinalPoor"]
    else:
        groups = ["Year", extra_group, "FinalPoor"]
    columns = ["HEmployed", "HLiterate", "HEduYears", "HAge"]
    all_df = []
    all_df.append(
        final_poor.groupby(groups).apply(lambda x: pd.Series({"SampleSize": len(x)}))
    )
    for column in columns:
        df = final_poor.groupby(groups).apply(
            lambda x: pd.Series({column: weighted_average(x, column)})
        )
        all_df.append(df)
    return pd.concat(all_df, axis=1)
