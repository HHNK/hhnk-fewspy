# %%

import datetime

import pandas as pd

from hhnk_fewspy.api_functions import get_intervalstatistics

MONTH_MAP = {
    "jan": 1,
    "feb": 2,
    "mrt": 3,
    "apr": 4,
    "mei": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "okt": 10,
    "nov": 11,
    "dec": 12,
}


def statistics_to_df(r_json: dict) -> pd.DataFrame:
    """Transform the statistics json response from the API into a dataframe.
    Only tested for interval=CALENDAR_MONTH.

    Parameters
    ----------
    r_json : dict
        r.json() response from API

    Example
    -------
    >>> r = hhnk_fewspy.get_intervalstatistics(**kwargs)
    >>> statistics_to_df(r_json=r.json())

    Returns
    -------
    df : pd.DataFrame
        dataframe containing location IDs, statsitic column and start- end_date
    """
    rows = []
    for res in r_json["timeSeriesIntervalStatistics"]:
        istats = res["intervalstatistics"]
        row_loc = res["header"]

        for istat in istats:  # bijv '% beschikbaar'
            statistic = istat["statistic"]
            vals = istat["values"]

            for val in vals:
                for va in val:
                    row = row_loc.copy()
                    month_year = list(va)[0]

                    month, year = month_year.split("-")

                    month_num = MONTH_MAP[month]
                    startdate = f"{year}-{month_num:02d}-01 00:00:00"
                    # Adjust the enddate to be in 2025 if the month is Decembermonth_num == 12:
                    if month_num == 12:
                        year = int(year) + 1
                    enddate = f"{year}-{(month_num % 12) + 1:02d}-01 00:00:00"
                    now = datetime.datetime.now()
                    if datetime.datetime.strptime(enddate, "%Y-%m-%d %H:%M:%S") > now:
                        # FIXME this causes issues around the first of the month because endtime will be before starttime.
                        enddate = datetime.datetime.now() - datetime.timedelta(days=1, hours=2)
                        enddate = datetime.datetime.strftime(enddate, "%Y-%m-%d %H:%M:%S")
                        # FIXME this would be the fix, but the RVWapi doesnt like that.
                        # enddate = now.strftime("%Y-%m-%d %H:%M:%S")

                    ##row[statistic] = float(va[month_year])
                    try:
                        row[statistic] = float(va[month_year])
                    except ValueError:
                        row[statistic] = 0
                    row["start_date"] = startdate
                    row["end_date"] = enddate
                    rows.append(row)
    df = pd.DataFrame(rows)
    return df


# %%

if __name__ == "__main__":
    kwargs = {
        "interval": "CALENDAR_MONTH",
        "statistics": "% beschikbaar",
        "filterId": "WinCC_HHNK_WEB",
        "parameterIds": "WNS2369.h.pred",
        "locationIds": ["ZRG-L-0519_kelder", "ZRG-P-0500_kelder"],
        "startTime": datetime.datetime(year=2023, month=3, day=20),
        "endTime": datetime.datetime(year=2024, month=3, day=20),
    }

    r = get_intervalstatistics(**kwargs)
    r_json = r.json()
