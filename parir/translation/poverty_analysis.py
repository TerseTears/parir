from parir.utils import timing
import pandas as pd
import numpy as np
from parir.processing import weighted_average, mask_na
import pyreadr
from pathlib import Path
from parir.utils import read_all_years, timing
from ipdb import set_trace


@timing
def merge_with_engel(MD_food, engelid):
    engelid["cluster3"] = engelid["cluster3"].astype("int64")
    engelid = engelid[
        [
            "cluster3",
            "Region",
            "CMPovLine",
            "PovertyLine",
            "Engel",
            "ModifiedEngel",
            "OER",
            "ModOER",
            "DSC",
            "ModDSC",
        ]
    ]
    MD_final = pd.merge(MD_food, engelid, on=["cluster3", "Region"], how="left")


    MD_final["cluster3"] = MD_final["cluster3"].astype("int64")

    MD_final["FinalPoor"] = 0
    MD_final["FinalPoor"] = MD_final["FinalPoor"].mask(
        MD_final["Total_Consumption_Month_per"] < MD_final["CMPovLine"], 1
    )

    return MD_final

def modified_index(df, column2replace, column1, column2, column3):
    df[column2replace] = (df[column1] + df[column2] + df[column3]) / 3
    df[column2replace] = df[column2replace].mask(
        df[column2].isna() & df[column3].isna(), df[column1]
    )
    df[column2replace] = df[column2replace].mask(
        ~df["EngelX"].isna() & df["EngelX2"].isna(), (df[column1] + df[column2]) / 2
    )
    return df


def engel_computation(food_poor):
    food_poor.loc[food_poor["Durable_Dep"].isna(), "Durable_Dep"] = 0
    food_poor = food_poor.assign(
        EngelH=lambda x: x.TOriginalFoodExpenditure / x.Total_Expenditure_Month
    )
    # Occasioanl Expenditure Ratio
    food_poor = food_poor.assign(
        OEX=lambda x: x.Medical_Exp
        + x.Durable_Dep
        + x.Durable_NoDep
        + x.Durable_Emergency
    )
    food_poor = food_poor.assign(OER_H=lambda x: x.OEX / x.Total_Expenditure_Month)

    # Durable Service Cost
    food_poor = food_poor.assign(
        DSC_H=lambda x: x.OwnedDurableItemsDepreciation / x.Total_Expenditure_Month
    )

    engelid = (
        food_poor[
            (
                lambda x: (x.TOriginalFoodExpenditure_Per > 0.8 * x.FPLine)
                & (x.TOriginalFoodExpenditure_Per < 1.2 * x.FPLine)
            )(food_poor)
        ]
        .groupby(["Year", "Region", "cluster3"], as_index=False)
        .apply(
            lambda x: pd.Series(
                {
                    "Engel": weighted_average(x, "EngelH"),
                    "OER": weighted_average(x, "OER_H"),
                    "DSC": weighted_average(x, "DSC_H"),
                    "FPLine": np.mean(x.FPLine),
                }
            )
        )
    )
    inflation_df = pd.read_csv("parir/data/inflation.csv").sort_values("Year")
    inflation_df = inflation_df.assign(F1=lambda x: (1 + x.D2FoodInf) / (1 + x.D2Inf))
    inflation_df["l.F1"] = inflation_df.F1.shift()
    inflation_df["F2"] = inflation_df["F1"] * inflation_df["l.F1"]

    engelid = pd.merge(engelid, inflation_df, on="Year", how="left").sort_values(
        ["Year", "cluster3"]
    )

    engelid_clusters = engelid.groupby("cluster3")
    engelid["l.Engel"] = engelid_clusters["Engel"].shift()
    engelid["l2.Engel"] = engelid_clusters["Engel"].shift(2)
    engelid["l.OER"] = engelid_clusters["OER"].shift()
    engelid["l2.OER"] = engelid_clusters["OER"].shift(2)
    engelid["l.DSC"] = engelid_clusters["DSC"].shift()
    engelid["l2.DSC"] = engelid_clusters["DSC"].shift(2)

    engelid["EngelX"] = engelid["l.Engel"] * engelid["F1"]
    engelid["EngelX2"] = engelid["l2.Engel"] * engelid["F2"]

    engelid = modified_index(engelid, "ModifiedEngel", "Engel", "EngelX", "EngelX2")
    engelid = modified_index(engelid, "ModOER", "OER", "l.OER", "l2.OER")
    engelid = modified_index(engelid, "ModDSC", "DSC", "l.DSC", "l2.DSC")
    engelid = engelid.assign(PovertyLine=lambda x: x.FPLine / x.ModifiedEngel)
    engelid = engelid.assign(PovertyLine0=lambda x: x.FPLine / x.Engel)
    engelid = engelid.assign(CMPovLine=lambda x: x.PovertyLine * (1 - x.OER + x.DSC))

    return engelid
