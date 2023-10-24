from parir.utils import timing
import pandas as pd
import numpy as np
import itertools
from parir.processing import weighted_average, mask_na
import pyreadr
from pathlib import Path
from parir.utils import read_all_years, timing
from ipdb import set_trace
import copy


def standard_basket(metadata, pct_deviation=25):
    # 95 is the base year
    metadata = copy.copy(metadata)
    metadata["analysis_years"] = (95, 95)
    initial_clustered = read_all_years("MD", "InitialPoorClustered", metadata)
    food_data = read_all_years("BigFData", "BigFDataTotalNutrition", metadata)
    food_data["HHID"] = food_data["HHID"].astype("int64")
    bfd2 = list(
        itertools.product(
            np.unique(initial_clustered.HHID), np.unique(food_data.FoodType)
        )
    )
    bfd2 = pd.DataFrame(bfd2, columns=["HHID", "FoodType"])
    bfd2 = bfd2.merge(food_data, how="left")

    bfd2 = bfd2.merge(
        initial_clustered[
            [
                "HHID",
                "Region",
                "Weight",
                "Size",
                "ProvinceCode",
                "ProvinceName",
                "FoodExpenditure",
                "FoodExpenditure_Per",
                "EqSizeCalory",
                "Dcil_IP_Cons_PAdj",
                "Dcil_Gen_Cons_PAdj",
                "Dcil_Gen_Cons_Nominal",
                "Pctl_Gen_Cons_Nominal",
                "Pctl_Gen_Cons_PAdj",
            ]
        ],
        on="HHID",
        how="left",
    )

    bfd2 = bfd2.mask(bfd2.isna(), 0)
    bfd2["Price"] = bfd2["Price"].mask(bfd2["Price"] < 0.1, np.nan)

    summary_operations = {
        "FGrams_0": ("FGrams", "sum"),
        "FoodKCalories": ("FoodKCalories", "sum"),
        "FoodProtein": ("FoodProtein", "sum"),
        "FoodVitaminA": ("FoodVitaminA", "sum"),
        "FoodRiboflavin": ("FoodRiboflavin", "sum"),
        "FoodFe": ("FoodFe", "sum"),
        "FoodCalcium": ("FoodCalcium", "sum"),
        "Region": ("Region", "first"),
        "Weight": ("Weight", "first"),
        "FoodExpenditure_Per": ("FoodExpenditure_Per", "first"),
        "FoodExpenditure": ("FoodExpenditure", "first"),
        "Size": ("Size", "first"),
        "EqSizeCalory": ("EqSizeCalory", "first"),
        "Dcil_IP_Cons_PAdj": ("Dcil_IP_Cons_PAdj", "first"),
        "ProvinceName": ("ProvinceName", "first"),
        "Dcil_Gen_Cons_Nominal": ("Dcil_Gen_Cons_Nominal", "first"),
        "Dcil_Gen_Cons_PAdj": ("Dcil_Gen_Cons_PAdj", "first"),
        "Pctl_Gen_Cons_Nominal": ("Pctl_Gen_Cons_Nominal", "first"),
        "Pctl_Gen_Cons_PAdj": ("Pctl_Gen_Cons_PAdj", "first"),
    }

    selected_decile = bfd2.groupby("HHID", as_index=False).agg(**summary_operations)

    if selected_decile is None:
        raise Exception("Nothing to aggregate")

    percent_operations = """
                          FGramspc = FGrams_0 / EqSizeCalory
                          FKCalspc = FoodKCalories / EqSizeCalory
                          FoodProteinpc = FoodProtein / EqSizeCalory
                          FoodVitaminApc = FoodVitaminA / EqSizeCalory
                          FoodRiboflavinpc = FoodRiboflavin / EqSizeCalory
                          FoodFepc = FoodFe / EqSizeCalory
                          FoodCalciumpc = FoodCalcium / EqSizeCalory
    """

    selected_decile = selected_decile.eval(percent_operations)

    selected_decile = selected_decile[
        [
            "HHID",
            "FGramspc",
            "FKCalspc",
            "FoodProteinpc",
            "FoodVitaminApc",
            "FoodRiboflavinpc",
            "FoodFepc",
            "FoodCalciumpc",
            "Region",
            "ProvinceName",
            "Weight",
            "Size",
            "EqSizeCalory",
            "FoodExpenditure_Per",
            "FoodExpenditure",
            "Dcil_IP_Cons_PAdj",
            "Dcil_Gen_Cons_Nominal",
            "Pctl_Gen_Cons_Nominal",
            "Dcil_Gen_Cons_PAdj",
            "Pctl_Gen_Cons_PAdj",
        ]
    ]
    calories = pd.read_csv("parir/data/calories.csv")
    calories_H = calories[calories.Threshold == f"H{pct_deviation}"]
    calories_H = pd.Series(calories_H.Value.values, index=calories_H.Need)
    calories_L = calories[calories.Threshold == f"L{pct_deviation}"]
    calories_L = pd.Series(calories_L.Value.values, index=calories_L.Need)

    if not isinstance(selected_decile, pd.DataFrame):
        raise Exception("Not a dataframe")

    selected_decile = selected_decile[
        selected_decile["FKCalspc"].between(
            calories_L["KCaloryNeed"], calories_H["KCaloryNeed"]
        )
        & selected_decile["FoodProteinpc"].between(
            calories_L["ProteinNeed"], calories_H["ProteinNeed"]
        )
        & selected_decile["FoodVitaminApc"].between(
            calories_L["VitaminANeed"], calories_H["VitaminANeed"]
        )
        & selected_decile["FoodRiboflavinpc"].between(
            calories_L["RiboflavinNeed"], calories_H["RiboflavinNeed"]
        )
        & selected_decile["FoodFepc"].between(
            calories_L["FeNeed"], calories_H["FeNeed"]
        )
        & selected_decile["FoodCalciumpc"].between(
            calories_L["CalciumNeed"], calories_H["CalciumNeed"]
        )
    ]

    selected_decile = selected_decile.merge(
        bfd2[
            [
                "HHID",
                "FoodType",
                "FoodCode",
                "FGrams",
                "Price",
                "Expenditure",
                "FoodKCalories",
                "FoodProtein",
                "FoodVitaminA",
                "FoodRiboflavin",
                "FoodFe",
                "FoodCalcium",
            ]
        ],
        on="HHID",
        how="left",
    )

    base_year_basket = selected_decile.groupby(["HHID", "FoodType"]).agg(
        **summary_operations
    )
    base_year_basket = (
        base_year_basket[base_year_basket.Region == "Urban"]
        .eval(percent_operations)
        .assign(WeightSize=lambda x: x.Weight * x.Size)
        .groupby("FoodType")
        .apply(
            lambda x: pd.Series(
                {
                    "FGramspc": weighted_average(x, "FGramspc", "WeightSize"),
                    "FKCalspc": weighted_average(x, "FKCalspc", "WeightSize"),
                    "FoodProteinpc": weighted_average(x, "FoodProteinpc", "WeightSize"),
                    "FoodRiboflavinpc": weighted_average(
                        x, "FoodRiboflavinpc", "WeightSize"
                    ),
                    "FoodVitaminApc": weighted_average(
                        x, "FoodVitaminApc", "WeightSize"
                    ),
                    "FoodFepc": weighted_average(x, "FoodFepc", "WeightSize"),
                    "FoodCalciumpc": weighted_average(x, "FoodCalciumpc", "WeightSize"),
                }
            )
        )
        .reset_index()
    )
    calories_groups = pd.read_csv("parir/data/calories_groups.csv")
    calories_adult = calories_groups[
        (calories_groups.Reference == "WorldBank") & (calories_groups.Group == "Adult")
    ].Value.values[0]

    basket_calory_sum = base_year_basket.FKCalspc.sum()

    base_year_basket = base_year_basket.eval(
        f"""
    StandardFGramspc = FGramspc * {calories_adult / basket_calory_sum}
    StandardKcal = FKCalspc * {calories_adult / basket_calory_sum}
    StandardProtein = FoodProteinpc * {calories_adult / basket_calory_sum}
    StandardVitaminA = FoodVitaminApc * {calories_adult / basket_calory_sum}
    StandardFe = FoodFepc * {calories_adult / basket_calory_sum}
    StandardRibiflavin = FoodRiboflavinpc * {calories_adult / basket_calory_sum}
    StandardCalcium = FoodCalciumpc * {calories_adult / basket_calory_sum}
        """
    )

    base_year_basket.to_excel(
        "results/FoodBasketStatsList.xlsx", f"{pct_deviation}", index=False
    )

    return base_year_basket
