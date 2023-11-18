# %%

import datetime
import os
import typing


def dict_to_datetime(data: dict) -> datetime:
    """Convert a FEWS PI datetime dict to datetime object.
    Args:
        data (dict): FEWS PI datetime (e.g. {'date': '2022-05-01', 'time': '00:00:00'})
    Returns:
        datetime: Converted datetime object (in example datetime.datetime(2022, 5, 1, 0, 0))
    """
    time = data.get("time", "00:00:00")
    date_time = datetime.fromisoformat(f'{data["date"]}T{time}')
    return date_time
