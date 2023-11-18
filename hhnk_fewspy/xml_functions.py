# %%
from pathlib import Path

import numpy as np
import pandas as pd
from lxml import objectify

from hhnk_fewspy.api_functions import connect_API
from hhnk_fewspy.xml_classes import XmlHeader, XmlSeries, XmlTimeSeries


# TODO xml classes gebruiken ipv connect_API
def df_to_xml(df, ts_header: XmlHeader, out_path=None):
    """Write input df to pixml format.

    df should be a dataframe with 'datetime' as index and 'value' as only column (more options like flag are possible,
    consult hkvfewspy setPiTimeSeries module))
    """

    pi = connect_API.connect_rest()  # connection werkt niet (meer, tijdelijk?)
    pi_ts = pi.setPiTimeSeries()

    # set a header object
    ts_header.write(pi_ts=pi_ts)

    # set an events object (pandas.Series or pandas.DataFrame)
    pi_ts.write.events(df)
    pi_ts_xml = pi_ts.to.pi_xml()

    if out_path is None:
        return pi_ts_xml
    with open(out_path, "w") as f:
        f.write(pi_ts_xml)


# TODO XmlTimeSeries ipv XmlSeries gebruiken?
def xml_to_dict(xml_path, binary: bool = False):
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
        serie = XmlSeries(metadata, data=data, columns=columns, binary=binary)

        if serie.locid not in series.keys():
            series[serie.locid] = {}
        series[serie.locid][serie.paramid] = serie

    return series


def xml_to_df(xml_path, binary: bool, parameter: str):
    """Turn dict of input binary to dataframe with every column another timeserie"""
    xmldict = xml_to_dict(xml_path=xml_path, binary=binary)

    print([xmldict[key].keys() for key in xmldict])

    cols = [key for key in xmldict if parameter in xmldict[key].keys()]
    time_df = pd.DataFrame(index=xmldict[cols[0]][parameter].df.index, columns=cols)

    for col in cols:
        missval = float(xmldict[col][parameter].metadata["missVal"])
        time_df[col] = xmldict[col][parameter].df.replace(missval, np.nan)
    return time_df


def print_xml(timeseries):
    """Helper function. Print header en tree gegevens van de timeseries."""
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


# %%

if __name__ == "__main__":
    from tests_fewspy.config import FOLDER_DATA

    xml_to_df(xml_path=FOLDER_DATA.bin_test_series.base, binary=True, parameter="")
