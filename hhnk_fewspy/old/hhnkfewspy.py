import datetime
import inspect
import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from lxml import objectify

import hhnk_fewspy.old.pixml

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=SyntaxWarning)
    import hkvfewspy
FEWS_REST_URL = "https://fews.hhnk.nl/FewsWebServices/rest/fewspiservice/v1/"
FEWS_SOAP_URL = "https://fews.hhnk.nl/FewsWebServices/fewspiservice?wsdl"
PAYLOAD = {"documentFormat": "PI_JSON", "documentVersion": "1.25"}


class connect_API:
    @staticmethod
    def connect_soap():
        pi = hkvfewspy.PiSoap()
        pi.setClient(wsdl=FEWS_SOAP_URL)
        return pi

    @staticmethod
    def connect_rest():
        pi = hkvfewspy.PiRest(verify=False)
        pi.setUrl(FEWS_REST_URL)
        return pi


def call_FEWS_api(param="locations", documentFormat="PI_JSON", debug=False, **kwargs):
    """return json containing scenarios based on supplied filters
    !! format for timeseries should be XML. For others JSON is preferred !!"""
    url = "{}{}/".format(FEWS_REST_URL, param)

    payload = {
        "documentFormat": documentFormat,
        "documentVersion": "1.25",
    }

    for key, value in kwargs.items():
        if key == "locationIds":
            #             pass #skip because hashtags are transformed into %23...
            url = f"{url}?locationIds={value}"
        else:
            payload[key] = value
    r = requests.get(url=url, params=payload)
    if debug:
        print(r.url)
    r.raise_for_status()
    return r


def get_timeseries(tz="Europe/Amsterdam", debug=False, **kwargs):
    """Example use:
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

    r.encoding = "UTF-8"
    a = json.loads(r.text)
    b = pd.DataFrame(a)
    c = b[col].to_dict()
    return pd.DataFrame(c).T


def check_location_id(loc_id, df):
    """Example use:
    check_location_id(loc_id='MPN-AS-427')"""
    if loc_id not in df["locationId"].values:
        suggested_loc = df[df["locationId"].str.contains(loc_id)]
        if suggested_loc.empty:
            print("LocationId {} not found. Requesting timeseries will result in an error.".format(loc_id))


def df_to_xml(df, ts_header, out_path):
    """Write input df to pixml format. ts_header should look like;
        ts_header={}
    ts_header['module_instance_id'] = 'DebietBerekening'
    ts_header['location_id']        = row['id']
    ts_header['qualifier_id']       = None
    ts_header['parameter_id']       = 'Q.berekend'
    ts_header['missing_val']        = -9999

    df should be a dataframe with 'datetime' as index and 'value' as only column (more options like flag are possible,
    consult hkvfewspy setPiTimeSeries module))
    """

    pi = connect_API.connect_rest()  # connection werkt niet (meer, tijdelijk?)
    # pi = hkvfewspy.PiSoap()

    pi_ts = pi.setPiTimeSeries()

    # set a header object
    pi_ts.write.header.moduleInstanceId(ts_header["module_instance_id"])
    pi_ts.write.header.locationId(ts_header["location_id"])
    if ts_header["qualifier_id"]:
        pi_ts.write.header.qualifierId(ts_header["qualifier_id"])
    pi_ts.write.header.parameterId(ts_header["parameter_id"])
    pi_ts.write.header.missVal(ts_header["missing_val"])

    # set an events object (pandas.Series or pandas.DataFrame)
    pi_ts.write.events(df)
    pi_ts_xml = pi_ts.to.pi_xml()

    with open(out_path, "w") as f:
        f.write(pi_ts_xml)


def binary_xml_to_df(timeseries_source, verbose=False) -> pd.DataFrame:
    """deprecated pixml werkt niet goed in py39, vervangen door xml_to_dict"""
    i = 1
    timeseries = {}
    reader = pixml.SeriesReader(timeseries_source)  # , start=start, end=end, step=step, ma=ma,

    for series in reader.read():
        # De tree per series is niet uniek maar nog gewoon de xml van alle series in de bin.
        # plus nog wat extra op het einde lijkt. Dit is beetje verwarrend,
        # vandaar dat indexering gebruikt wordt.
        locId = None
        paramId = None

        df = pd.DataFrame(series)
        elem = series.tree[i]
        for header in elem:
            for item in header:
                if item.tag.endswith("locationId"):
                    locId = item.text
                if item.tag.endswith("parameterId"):
                    paramId = item.text

        if (locId is None) or (paramId is None):
            print(f"did not find locid/parameter in i={i}, skipping...")
        else:
            # Masked array omzetten naar numpy array.
            series.ma.fill_value = np.nan
            arr = np.ma.filled(series.ma)

            df = pd.DataFrame(series, columns=["time", "value"])
            df["value"] = arr  # dit kan vast handiger, maar anders krijg ik de timeseries niet goed in df
            df.set_index("time", inplace=True)

            if verbose:
                print(f"locId: {locId}. paramId: {paramId}")

            timeseries[locId] = {}
            timeseries[locId][paramId] = {}
            timeseries[locId][paramId]["tree"] = elem
            timeseries[locId][paramId]["df"] = df.copy()
        i += 1
    return timeseries


class xmlSeries:
    def __init__(self, metadata, data, binary, columns=None):
        self.metadata = metadata
        self.binary = binary
        if not self.binary:
            self.df = self.make_df(data, columns)
        else:
            self.df = self.make_df_binary(data)

    def make_df(self, data, columns):
        df = pd.DataFrame(data, columns=columns)
        return df

    def make_df_binary(
        self,
        data,
    ):
        df = pd.DataFrame(data, columns=["value"])

        # Make datetime col
        assert self.metadata["timeStep"]["unit"] == "second"
        df.set_index(self.timeseries, inplace=True)
        return df

    @property
    def locid(self):
        return self.metadata["locationId"]

    @property
    def paramid(self):
        return self.metadata["parameterId"]

    @property
    def start(self):
        return self._get_datetime("startDate")

    @property
    def end(self):
        return self._get_datetime("endDate")

    @property
    def timesteps(self):
        return (self.end - self.start).total_seconds() / eval(self.metadata["timeStep"]["multiplier"]) + 1

    @property
    def timeseries(self):
        return pd.date_range(start=self.start, end=self.end, periods=self.timesteps)

    def _get_datetime(self, key):
        return datetime.datetime.strptime(self.metadata[key]["date"] + self.metadata[key]["time"], "%Y-%m-%d%H:%M:%S")

    def __repr__(self):
        funcs = "." + " .".join(
            [i for i in dir(self) if not i.startswith("__") and hasattr(inspect.getattr_static(self, i), "__call__")]
        )
        variables = "." + " .".join(
            [
                i
                for i in dir(self)
                if not i.startswith("__") and not hasattr(inspect.getattr_static(self, i), "__call__")
            ]
        )
        repr_str = f"""functions: {funcs}
variables: {variables}"""
        return f"""xmlSeries locationId={self.metadata["locationId"]}, parameterId={self.metadata["parameterId"]}
"""


def xml_to_dict(xml_path, binary=False):
    """#TODO add timezone (assume UTC now)"""
    if binary:
        binfile_path = Path(xml_path).with_suffix(".bin")
        bin_values = np.fromfile(
            binfile_path,
            dtype=np.float32,
            offset=0,
            count=-1,
        )

    # Read headers
    xml_data = objectify.parse(xml_path)  # Parse XML data
    root = xml_data.getroot()  # Root element

    series = {}

    # Get children, filter out timezone.
    rootchildren = [child for child in root.getchildren() if child.tag.endswith("series")]

    # Loop over children (individual timeseries in the xml)
    for i, child in enumerate(rootchildren):
        subchildren = child.getchildren()  # alle headers en en timevalues
        metadata = None
        data = []
        columns = None

        for subchild in subchildren:
            # Write header to dict
            if subchild.tag.endswith("header"):  # gewoonlijk eerste subchild is de header
                header_childs = subchild.getchildren()
                metadata = {}
                for item in header_childs:
                    key = item.tag.split("}")[-1]

                    item_keys = item.keys()

                    if len(item_keys) == 0:
                        metadata[key] = item.text
                    else:
                        metadata[key] = {}
                        for item_key, item_value in zip(item_keys, item.values()):
                            metadata[key][item_key] = item_value
            # Get data
            else:
                data.append(subchild.values())

            # If binary we assume equidistant series with equal length.
            if binary:
                bin_size = int(len(bin_values) / len(rootchildren))
                data = bin_values[i * bin_size : i * bin_size + bin_size]

        columns = subchild.keys()
        serie = xmlSeries(metadata, data=data, columns=columns, binary=binary)

        if serie.locid not in series.keys():
            series[serie.locid] = {}
        series[serie.locid][serie.paramid] = serie

    return series


def print_xml(timeseries):
    """helper function. Print header en tree gegevens van de timeseries."""
    for key in timeseries.keys():
        print(key)
        for key2 in timeseries[key]:
            for header in timeseries[key][key2]["tree"]:
                for line in header:
                    print(line.text)
        print("")
    ## WERKT NIET WANT GEEN RECHTEN ##
    # Write directly to FEWS
    # pi.putTimeSeriesForFilter(filterId='HHNK_OPP_WEB_SL',
    #                         piTimeSeriesXmlContent=pi_ts_xml)
    ## WERKT NIET WANT GEEN RECHTEN ##
