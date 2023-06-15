import requests
import pandas as pd
import hkvfewspy
from hkvfewspy.utils.pi_helper import read_timeseries_response
import json
FEWS_REST_URL = 'https://fews.hhnk.nl/FewsWebServices/rest/fewspiservice/v1/'


class connect_API:   
    @staticmethod
    def connect_rest():
        pi = hkvfewspy.PiRest(verify=False) 
        pi.setUrl(FEWS_REST_URL)
        return pi


def call_FEWS_api(param='locations', documentFormat='PI_JSON', debug=False, **kwargs):
    """return json containing scenarios based on supplied filters
    !! format for timeseries should be XML. For others JSON is preferred !!"""
    url = "{}{}/".format(FEWS_REST_URL, param)

    payload = {'documentFormat':documentFormat,
              'documentVersion': '1.25',
              }

    for key, value in kwargs.items():
        if key=='locationIds':
#             pass #skip because hashtags are transformed into %23...
            url = f'{url}?locationIds={value}'
        else:
            payload[key] = value
    r = requests.get(url=url, params=payload)
    if debug:
        print(r.url)
    r.raise_for_status()
    return r


def get_timeseries(tz='Europe/Amsterdam', debug=False, **kwargs):
    """Example use:
    Tend=datetime.datetime.now()
    T0=Tend - datetime.timedelta(days=1)

    get_timeseries(parameterIds='Stuw.stand.meting', locationIds=KST-JL-2571, startTime=T0, endTime=Tend, convertDatum=True)
        """
    payload = {'documentFormat':'PI_XML'}
    for key, value in kwargs.items():
        #set time in correct format.
        if key in ['startTime', 'endTime']: 
            value = value.strftime('%Y-%m-%dT%H:%M:%SZ')
        payload[key] = value

    r = call_FEWS_api(param='timeseries', debug=debug, **payload)

    df = read_timeseries_response(r.text, tz_client=tz, header="longform")
    return df


def get_location_headers():
    """Location header with available parameters"""
    df = get_timeseries(parameterIds=None, locationIds='KST-JL-2571', convertDatum=True, onlyHeaders=True)
    return df


def get_locations(col='locations'):
    r = call_FEWS_api(param='locations', documentFormat='PI_JSON')

    r.encoding = 'UTF-8'
    a=json.loads(r.text)
    b=pd.DataFrame(a)
    c=b[col].to_dict()
    return pd.DataFrame(c).T


def check_location_id(loc_id, df):
    """Example use: 
    check_location_id(loc_id='MPN-AS-427') """
    if loc_id not in df['locationId'].values:
        suggested_loc = df[df['locationId'].str.contains(loc_id)]
        if suggested_loc.empty:
            print('LocationId {} not found. Requesting timeseries will result in an error.'.format(loc_id))

