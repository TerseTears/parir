%load_ext autoreload
%autoreload 3
from pathlib import Path
import pyreadr
from parir.utils import read_all_years
from parir.translation.poverty_analysis import merge_with_engel
from parir.translation.poor_analysis import poor_stats, poor_comparison
metadata = {
    'analysis_years': (85, 99),
    'rdata_path': Path("D:/IRHEIS/DataProcessed")
            }

# 168
MD = merge_with_engel(metadata)
# 169-step9-PovertyStats.R
poor_stats(MD, "Region")
# 170-step10-PoorStats.R
poor_comparison(MD, "Region")

