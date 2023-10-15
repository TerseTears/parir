%load_ext autoreload
%autoreload 3
from pathlib import Path
from parir.utils import read_all_years
from parir.translation.poor_analysis import poor_stats, poor_comparison
metadata = {
    'analysis_years': (85, 99),
    'rdata_path': Path("D:/IRHEIS/DataProcessed")
            }

# 169-step9-PovertyStats.R
MD = read_all_years("MD", "FinalPoor", metadata)
poor_stats(MD, "Region")

# 170-step10-PoorStats.R
poor_comparison(MD, "Region")

