import hbsir
import pandas as pd
import numpy as np
from hbsir.metadata_reader import metadata, open_yaml

metadata.reload_file("tables")
metadata.reload_file("schema")
metadata.reload_file("commodities")

food_tbl = hbsir.load_table("food", [1397], recreate=True, on_missing="create")
food_eyn = food_tbl.view["eyn_food"]
nutritions_daily = [nutrition + "_daily_grams" for nutrition in
                     ["protein", "vitamin_A", "ribloflavin", "fe", "calcium"]]
for nutrition in nutritions_daily:
    food_eyn = food_eyn.eval(f"""{nutrition} = Amount * 1000 / Duration""")

daily_grams_total = food_eyn[["ID", "Year"] + nutritions_daily].groupby(["ID", "Year"]).sum()
food_eyn