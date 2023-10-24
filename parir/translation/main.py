%load_ext autoreload
%autoreload 3
from pathlib import Path
import pyreadr
import pandas as pd
from parir.utils import read_all_years
from parir.translation.basket_analysis import standard_basket
from parir.translation.postdata_prep import reweight_pop, food_poor_computation
from parir.translation.poverty_analysis import merge_with_engel, engel_computation
from parir.translation.poor_analysis import poor_stats, poor_comparison
import numpy as np
np.seterr(divide='ignore', invalid='ignore')

metadata = {
    'analysis_years': (85, 85),
    'rdata_path': Path("D:/IRHEIS/DataProcessed"),
    'rresult_path': Path("D:/IRHEIS/DataResults")
            }

# 166-pre
basket_std = standard_basket(metadata)

# 166
MD_initial = read_all_years("MD", "InitialPoorClustered", metadata)
MD_food_poor = food_poor_computation(MD_initial, basket_std, metadata)
# 167
engelid = engel_computation(MD_food_poor)
# 168
MD_final = merge_with_engel(MD_food_poor, engelid)
# 169-step9-PovertyStats.R
poor_stats(MD_final, "Region")
# 170-step10-PoorStats.R
poor_comparison(MD_final, "Region")

