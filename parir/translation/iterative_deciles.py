from parir.utils import timing
import pandas as pd
import numpy as np
import itertools
from parir.processing import weighted_average, weighted_median
import pyreadr
from pathlib import Path
from parir.utils import read_all_years, timing
from ipdb import set_trace
import math


def tornqvist_index(data_df):
    data_df["MeterPrice"] = np.where(
        data_df["tenure"].isin(["Free", "Other", "AgainstService"]),
        np.nan,
        data_df["MeterPrice"],
    )
    data_df["House_Exp"] = np.where(
        data_df["tenure"].isin(["Free", "Other", "AgainstService"]),
        np.nan,
        data_df["House_Exp"],
    )

    data_df = data_df.assign(
        wj1=lambda x: x.FoodExpenditure / x.Total_Expenditure_Month,
        wj2=lambda x: x.House_Exp / x.Total_Expenditure_Month,
    )

    index_df = (
        data_df.groupby(["Region", "NewArea_Name"])
        .apply(
            lambda x: pd.Series(
                {
                    "N": len(x),
                    "wj1": weighted_median(x, "wj1"),
                    "wj2": weighted_median(x, "wj2"),
                    "pj1": weighted_median(x, "Bundle_Value"),
                    "pj2": weighted_median(x, "MeterPrice"),
                }
            )
        )
        .reset_index()
        .dropna()
    )
    set_trace()

    index_df = index_df.eval(
        """
    wj = wj1 + wj2
    wj1 = wj1 / wj
    wj2 = wj2 / wj
    """
    )
    x_tehran = index_df[index_df.NewArea_Name == "Sh_Tehran"].to_dict("records")[0]
    wk1 = x_tehran["wj1"]
    wk2 = x_tehran["wj2"]
    pk1 = x_tehran["pj1"]
    pk2 = x_tehran["pj2"]

    index_df["SimpleIndex"] = 0.5 * index_df["pj1"] / pk1 + 0.5 * index_df["pj2"] / pk2
    index_df["AnotherIndex"] = (
        index_df["wj1"] * index_df["pj1"] / pk1
        + index_df["wj2"] * index_df["pj2"] / pk2
    )
    # TornqvistIndex
    index_df["PriceIndex"] = index_df.apply(
        lambda x: math.exp(
            (wk1 + x["wj1"]) / 2
            * math.log(x["pj1"] / pk1)
            + (wk2 + x["wj2"]) / 2 * math.log(x["pj2"] / pk2)
        )
    , axis=1)
    index_df = index_df[["Region", "NewArea_Name", "PriceIndex"]]
    return index_df


def iterative_decile(merged_data, household_data):
    expenditure_cols = [
        "OriginalFoodExpenditure",
        "FoodOtherExpenditure",
        "Cigar_Exp",
        "Cloth_Exp",
        "Amusement_Exp",
        "Communication_Exp",
        "House_Exp",
        "Energy_Exp",
        "Furniture_Exp",
        "Hotel_Exp",
        "Restaurant_Exp",
        "Hygiene_Exp",
        "Transportation_Exp",
        "Other_Exp",
        "Add_to_NonDurable",
        "Medical_Exp",
        "Durable_Dep",
        "Durable_NoDep",
        "Durable_Emergency",
        "OwnedDurableItemsDepreciation",
    ]
    smerged_data = merged_data[
        expenditure_cols
        + [
            "HHID",
            "Region",
            "NewArea",
            "NewArea_Name",
            "FoodExpenditure",
            "TOriginalFoodExpenditure_Per",
            "Total_Expenditure_Month",
            "Total_Expenditure_Month_per",
            "Total_Consumption_Month",
            "Total_Consumption_Month_per",
            "TFoodKCaloriesHH_Per",
            "Calorie_Need_WorldBank",
            "Calorie_Need_NutritionInstitute",
            "Weight",
            "MeterPrice",
            "Size",
            "EqSizeOECD",
        ]
    ]
    smerged_data = smerged_data.merge(household_data[["HHID", "tenure"]], on="HHID")

    smerged_data = smerged_data.assign(
        Bundle_Value=lambda x: x.TOriginalFoodExpenditure_Per
        * x.Calorie_Need_WorldBank
        / x.TFoodKCaloriesHH_Per
    )

    # TODO arbitrary measure. tnx eynian.
    smerged_data = smerged_data[smerged_data.TFoodKCaloriesHH_Per >= 300]

    price_df = tornqvist_index(smerged_data)

    return price_df
