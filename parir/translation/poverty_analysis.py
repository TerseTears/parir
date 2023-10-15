from parir.utils import timing
import pandas as pd
import numpy as np
from parir.processing import weighted_average, mask_na
import pyreadr
from pathlib import Path
from parir.utils import read_all_years, timing
from ipdb import set_trace


@timing
def merge_with_engel(metadata):
    BigEngelTable = pyreadr.read_r(Path(metadata["rdata_path"] / "BigEngelTable.rda"))[
        "BigEngelTable"
    ]
    BigEngelTable["cluster3"] = BigEngelTable["cluster3"].astype(int)
    BigEngelTable = BigEngelTable[
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

    MD = read_all_years("MD", "FinalPoor", metadata)
    MD["cluster3"] = MD["cluster3"].astype(int)
    set_trace()
    return pd.merge(MD, BigEngelTable, on=["cluster3", "Region"], how= "left")
