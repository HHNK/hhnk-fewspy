# %%
import json
import os
import warnings

import pandas as pd
import requests

# TODO make this setting mutable
# FEWS_REST_URL = os.getenv('FEWS_REST_URL', "https://fews.hhnk.nl/FewsWebServices/rest/fewspiservice/v1/")

FEWS_REST_URL = "https://fews.hhnk.nl/FewsWebServices/rest/fewspiservice/v1/"
# FEWS_REST_URL = "http://localhost:8080/FewsWebServices/rest/fewspiservice/v1/"


class connect_API:
    @staticmethod
    def connect_rest():
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=SyntaxWarning)
            import hkvfewspy

        pi = hkvfewspy.PiRest(verify=False)
        pi.setUrl(FEWS_REST_URL)
        return pi


def call_FEWS_api(param="locations", documentFormat="PI_JSON", debug=False, **kwargs):
    """JSON with scenarios based on supplied filters
    !! format for timeseries should be XML. For others JSON is preferred !!
    """
    url = f"{FEWS_REST_URL}{param}/"

    payload = {
        "documentFormat": documentFormat,
        # "documentVersion": "1.25",
        "documentVersion": "1.34",
    }

    for key, value in kwargs.items():
        # if key == "locationIds": #FIXME was dit ergens nodig?
        #     #             pass #skip because hashtags are transformed into %23...
        #     url = f"{url}?locationIds={value}"
        # else:
        payload[key] = value
    r = requests.get(url=url, params=payload)
    if debug:
        print(r.url)
    r.raise_for_status()
    return r


def get_table_as_df(table_name: str) -> pd.DataFrame:
    """
    Get table as dataframe from API.
    Apply endpoint mapper to get the table.
    """

    endpoint_mapper = {
        "parameters": "timeSeriesParameters",
        "locations": "locations",
    }

    r = call_FEWS_api(param=table_name, documentFormat="PI_JSON")
    try:
        df = pd.DataFrame(r.json()[endpoint_mapper[table_name]])
    except KeyError as e:
        print(f"Available keys: {r.json().keys()}")
        raise e

    return df


def get_timeseries(tz="Europe/Amsterdam", debug=False, **kwargs) -> pd.DataFrame:
    """Get timeseries from FEWS API

    Example use:
    Tend=datetime.datetime.now()
    T0=Tend - datetime.timedelta(days=1)

    get_timeseries(parameterIds='Stuw.stand.meting', locationIds=KST-JL-2571, startTime=T0, endTime=Tend, convertDatum=True)
    """
    from hkvfewspy.utils.pi_helper import read_timeseries_response

    payload = {"documentFormat": "PI_XML"}
    for key, value in kwargs.items():
        # set time in correct format.
        if key in ["startTime", "endTime"]:
            value = value.strftime("%Y-%m-%dT%H:%M:%SZ")
        payload[key] = value

    r = call_FEWS_api(param="timeseries", debug=debug, **payload)

    df = read_timeseries_response(r.text, tz_client=tz, header="longform")
    return df


def get_location_headers():
    """Location header with available parameters"""
    df = get_timeseries(parameterIds=None, locationIds="KST-JL-2571", convertDatum=True, onlyHeaders=True)
    return df


def get_locations(col="locations"):
    r = call_FEWS_api(param="locations", documentFormat="PI_JSON")
    df = pd.DataFrame(r.json()["locations"])
    return df


def get_intervalstatistics(debug=False, **kwargs):
    """Kwarg example:
    kwargs = {
                "interval": "CALENDAR_MONTH",
                "statistics": "percentage_available",
                "filterId": "WinCC_HHNK_WEB",
                "parameterIds": "WNS2369.h.pred",
                "locationIds": ["ZRG-L-0519_kelder","ZRG-P-0500_kelder"],
                "startTime": datetime.datetime(year=2023, month=3, day=20),
                "endTime": datetime.datetime(year=2024, month=3, day=20),
            }
    """
    payload = {"documentFormat": "PI_JSON"}
    for key, value in kwargs.items():
        # set time in correct format.
        if key in ["startTime", "endTime"]:
            value = value.strftime("%Y-%m-%dT%H:%M:%SZ")
        payload[key] = value

    r = call_FEWS_api(param="timeseries/intervalstatistics", debug=debug, **payload)

    return r


def check_location_id(loc_id, df):
    """Use example:
    check_location_id(loc_id='MPN-AS-427')
    """
    if loc_id not in df["locationId"].values:
        suggested_loc = df[df["locationId"].str.contains(loc_id)]
        if suggested_loc.empty:
            print("LocationId {} not found. Requesting timeseries will result in an error.".format(loc_id))


# %%
