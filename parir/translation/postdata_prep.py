from parir.utils import timing
import pandas as pd
import numpy as np
from parir.processing import weighted_average, weighted_median
import pyreadr
from pathlib import Path
from parir.utils import read_all_years, timing
from ipdb import set_trace


def reweight_pop(inital_poor):
    reweights = pd.read_csv("parir/data/reweights.csv")
    inital_poor = pd.merge(inital_poor, reweights, on=["Year", "Region"], how="left")
    inital_poor["Weight"] = inital_poor["Weight"].mask(
        inital_poor["Year"] >= 90,
        (lambda x: x.Weight * x.PopWeight / (np.sum(x.Weight * x.Size) / 1000))(
            inital_poor
        ),
    )
    return inital_poor


def food_poor_computation(initial_poor, metadata):
    basket_baseyear = pd.read_excel(
        Path(metadata["rresult_path"]) / "FoodBasketStatsList.xlsx", "25"
    )
    initial_poor = reweight_pop(initial_poor)
    initial_poor["Selected_Group"] = np.where(
        initial_poor["Dcil_IP_Cons_PAdj"].isin([2, 3, 4, 5]), 1, 0
    )
    initial_poor = initial_poor[
        [
            "HHID",
            "Region",
            "Weight",
            "Size",
            "cluster3",
            "EqSizeCalory",
            "Selected_Group",
        ]
    ]

    food_data = read_all_years("BigFData", "BigFDataTotalNutrition", metadata)
    food_data = food_data[["HHID", "Year", "FoodCode", "FoodType", "Price", "FGrams"]]

    initial_poor["HHID"] = initial_poor["HHID"].astype("int64")
    food_data["HHID"] = food_data["HHID"].astype("int64")

    bfd2 = food_data.merge(
        initial_poor,
        on="HHID",
        how="left",
    )
    bfd2 = bfd2.mask(bfd2.isna(), 0)
    bfd2["Price"] = bfd2["Price"].mask(bfd2["Price"] < 0.1, np.nan)

    bfd2 = bfd2.assign(FGramsWeight=lambda x: x.Weight * x.Size * x.FGrams)
    set_trace()
    # bfd2 = bfd2.groupby(["HHID", "FoodType", "Year"], as_index=False).agg(lambda x: x.iloc[0])
    # bfd2 = bfd2.groupby(["HHID", "FoodType", "Year"], as_index=False).apply(
    #     lambda x: pd.Series(
    #         {
    #             #"Price": weighted_average(x, "Price", "FGramsWeight"),
    #             # "FGrams": np.sum(x.FGrams),
    #             "cluster3": x.cluster3.iloc[0],
    #             # "Region": x.Region.iloc[0],
    #             # "Weight": x.Weight.iloc[0],
    #             # "Size": x.Size.iloc[0],
    #             # "Selected_Group": x.Selected_Group.iloc[0],
    #         }
    #     )
    # )
    bfd2 = bfd2.assign(FGramsWeight=lambda x: x.Weight * x.Size * x.Fgrams)
    bfd3 = (
        bfd2.loc[bfd2["Selected_Group"] == 1 & ~bfd2["Price"].isna()]
        .groupby("FoodType", "Region", "cluster3", "Year", as_index=False)
        .apply(
            lambda x: pd.Series(
                {
                    "MedPrice": weighted_median(x["Price"], x["FGramsWeight"]),
                    "MeanPrice": weighted_average(x, "Price", "FGramsWeight"),
                }
            )
        )
    )
    basket_price = (
        bfd3.loc[~bfd3["MeanPrice"].isna()]
        .groupby(["FoodType", "Region", "cluster3", "Year"], as_index=False)["Price"]
        .min()
    )
    basket_cost = pd.merge(basket_baseyear, basket_price, on="FoodType", how="left")
    basket_cost = basket_cost.assign(
        Cost=lambda x: (x.StandardFGramspc / 1000) * x.Price
    )

    FPLineBasket = basket_cost.groupby("cluster3", as_index=False)["Cost"].sum()

    return basket_cost
