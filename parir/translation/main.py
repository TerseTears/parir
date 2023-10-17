%load_ext autoreload
%autoreload 3
from pathlib import Path
import pyreadr
from parir.utils import read_all_years
from parir.translation.poverty_analysis import merge_with_engel, engel_computation
from parir.translation.poor_analysis import poor_stats, poor_comparison
metadata = {
    'analysis_years': (85, 87),
    'rdata_path': Path("D:/IRHEIS/DataProcessed")
            }
# 167
# BigEngelTable = pyreadr.read_r(Path(metadata["rdata_path"] / "BigEngelTable.rda"))[
#     "BigEngelTable"
# ]
MD = read_all_years("MD", "FinalPoor", metadata)
engelid = engel_computation(MD)
# 168
MD = merge_with_engel(engelid, metadata)
# 169-step9-PovertyStats.R
poor_stats(MD, "Region")
# 170-step10-PoorStats.R
poor_comparison(MD, "Region")

