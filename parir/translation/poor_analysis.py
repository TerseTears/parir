from parir.utils import timing
import pandas as pd
import numpy as np
from parir.processing import weighted_average, mask_na
from ipdb import set_trace


@timing
def poor_comparison(final_poor, extra_group=None):
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


def poor_stats(final_poor: pd.DataFrame, extra_group=None):
    if extra_group is None:
        groups = ["Year"]
    else:
        groups = ["Year", extra_group]

    final_poor["HouseStatus"] = "Other"
    final_poor.loc[
        final_poor["tenure"].isin(["OwnLandandBuilding", "Apartment"]), "HouseStatus"
    ] = "Owns"
    final_poor.loc[
        final_poor["tenure"].isin(["Rented", "Mortgage"]), "HouseStatus"
    ] = "Rent"

    final_poor = final_poor.assign(
        FGT1M=lambda x: (x.CMPovLine - x.Total_Consumption_Month_per) / x.CMPovLine,
        FGT2M=lambda x: (
            (x.CMPovLine - x.Total_Consumption_Month_per) / x.CMPovLine
        ).pow(2),
        Engel=lambda x: x.TOriginalFoodExpenditure / x.Total_Expenditure_Month,
        House_Share=lambda x: x.House_Exp / x.Total_Expenditure_Month,
        WeightSize=lambda x: x.Weight * x.Size,
        PovertyHCR=lambda x: x.FinalPoor,
    )
    columns = [
        "MeterPrice",
        "House_Share",
        "Bundle_Value",
        "Engel",
        "Total_Expenditure_Month",
    ]

    columns_with_size = [
        "FPLine",
        "FoodKCaloriesHH_Per",
        "Total_Expenditure_Month_per",
        "Total_Consumption_Month",
        "CMPovLine",
        "PovertyLine",
        "PovertyHCR",
    ]

    return final_poor.groupby(groups).apply(
        lambda x: pd.Series(
            {column: weighted_average(x, column) for column in columns}
            | {
                column: weighted_average(x, column, "WeightSize")
                for column in columns_with_size
            }
            | {
                "SampleSize": len(x),
                "PoorSampleSize": sum(x["FinalPoor"]),
                "PovertyGap": weighted_average(
                    x.loc[x["FinalPoor"] == 1], "FGT1M", "WeightSize"
                ),
                "PovertyDepth": weighted_average(
                    x.loc[x["FinalPoor"] == 1], "FGT2M", "WeightSize"
                ),
            }
        )
    )
