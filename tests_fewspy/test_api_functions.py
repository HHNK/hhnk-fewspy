# %%
import datetime

import pandas as pd
from config import FOLDER_DATA

from hhnk_fewspy import api_functions, connect_API


# %%
def test_get_parameters():
    """Get FEWS parameters from api"""
    df = api_functions.get_table_as_df(table_name="parameters")

    assert len(df) == 96


def test_runTask_through_API():
    """TODO deze faalt, mag niet via API starten."""
    pi = connect_API.connect_rest()
    api_response = pi.runTask(
        workflowId="Import_TMX_from_file",
        startTime=datetime.datetime.now(),
        endTime=datetime.datetime.now(),
        userId="",
    )

    assert api_response == {"id": "Readonly mode is enabled. Access is not allowed."}


if __name__ == "__main__":
    pass
