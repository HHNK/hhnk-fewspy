# %%
import pandas as pd

from hhnk_fewspy import api_functions
from tests_fewspy.config import FOLDER_DATA


# %%
def test_get_parameters():
    """Get FEWS parameters from api"""
    df = api_functions.get_table_as_df(table_name="parameters")

    assert len(df) == 96


if __name__ == "__main__":
    pass
