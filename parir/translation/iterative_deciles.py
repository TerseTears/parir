from parir.utils import timing
import pandas as pd
import numpy as np
import itertools
from parir.processing import weighted_average, weighted_median
import pyreadr
from pathlib import Path
from parir.utils import read_all_years, timing
from ipdb import set_trace


def do_deciling(
    household_data, price_index=None, order_by="Consumption", size_name="_per"
):
    if price_index is not None:
        if "PriceIndex" in household_data.columns:
            household_data = household_data.drop(columns=["PriceIndex"])
        household_data = household_data.merge(
            price_index, on=["Region", "NewArea_Name"]
        )
        varname0 = f"Total_{order_by}_Month{size_name}"
        varname = f"{varname0}_PriceAdj"
        household_data[varname] = (
            household_data[varname0] / household_data["PriceIndex"]
        )
    else:
        varname = f"Total_{order_by}_Month{size_name}"

    household_data[order_by] = household_data[varname]
    household_data = household_data.sort_values(by=order_by)
    household_data["WeightSize"] = household_data["Weight"] * household_data["Size"]
    household_data["cumul_rel_weight"] = (
        household_data["WeightSize"].cumsum() / household_data["WeightSize"].sum()
    )
    xr25th = household_data.groupby(["Region", "NewArea_Name"])[order_by].agg(
        xr25th=lambda x: x.iloc[24]
    )
    household_data = household_data.merge(
        xr25th, left_on=["Region", "NewArea_Name"], right_index=True
    )
    household_data["First25"] = np.where(household_data["xr25th"], 1, 0)
    household_data["Decile"] = pd.cut(
        household_data["cumul_rel_weight"],
        bins=np.arange(0, 1.1, 0.1),
        labels=range(1, 11),
    )
    household_data["Percentile"] = pd.cut(
        household_data["cumul_rel_weight"],
        bins=np.arange(0, 1.01, 0.01),
        labels=range(1, 101),
    )
    return household_data[["HHID", "Decile", "Percentile", "First25"]]


# if(!is.null(PriceIndexDT)){
#   if("PriceIndex" %in% names(HHDT)) HHDT <- HHDT[,PriceIndex:=NULL]
#   HHDT <- merge(HHDT,PriceIndexDT,by=c("Region","NewArea_Name"))
#   PriceAdj <- "PriceAdj"
#   varname0 <- paste0("Total_",OrderingVar,"_Month",Size)
#   varname <- paste0("Total_",OrderingVar,"_Month",Size,"_PriceAdj")
#   HHDT <- HHDT[,(varname):=get(varname0)/PriceIndex]
# }else{
#   varname <- paste0("Total_",OrderingVar,"_Month",Size)
# }


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
        lambda x: np.exp(
            (wk1 + x["wj1"]) / 2 * np.log(x["pj1"] / pk1)
            + (wk2 + x["wj2"]) / 2 * np.log(x["pj2"] / pk2)
        ),
        axis=1,
    )
    index_df = index_df[["Region", "NewArea_Name", "PriceIndex"]]
    return index_df


def iterative_decile(merged_data, household_data, metadata):
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

    gdc = do_deciling(smerged_data, price_df, "Consumption", "_per")
    gdc = gdc.rename(columns={"Decile": "Decil_Gen_Cons_PAdj", "Precentile": "Pctl_Gen_Cons_PAdj"})
    gdx = do_deciling(smerged_data, price_df, "Expediture", "_per")
    gdx = gdx.rename(columns={"Decile": "Decil_Gen_Exp_PAdj", "Precentile": "Pctl_Gen_Exp_PAdj"})

    smerged_data = smerged_data.merge(gdc).merge(gdx)

    return smerged_data
