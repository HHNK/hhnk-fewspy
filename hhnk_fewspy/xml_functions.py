import pandas as pd
import numpy as np
from lxml import objectify
from pathlib import Path

from hhnk_fewspy.xml_classes import XmlHeader, XmlTimeSeries, XmlSeries
from hhnk_fewspy.api_functions import connect_API
    

#TODO xml classes gebruiken ipv connect_API
def df_to_xml(df, ts_header: XmlHeader, out_path=None):
    """Write input df to pixml format. 
        
        df should be a dataframe with 'datetime' as index and 'value' as only column (more options like flag are possible,
        consult hkvfewspy setPiTimeSeries module))
        """

    pi=connect_API.connect_rest() #connection werkt niet (meer, tijdelijk?)
    pi_ts = pi.setPiTimeSeries()

    # set a header object
    ts_header.write(pi_ts=pi_ts)

    # set an events object (pandas.Series or pandas.DataFrame)
    pi_ts.write.events(df)
    pi_ts_xml = pi_ts.to.pi_xml()

    if out_path is None:
        return pi_ts_xml
    with open(out_path, 'w') as f:
        f.write(pi_ts_xml)
            

#TODO XmlTimeSeries ipv XmlSeries gebruiken?
def xml_to_dict(xml_path, binary=False):
    """#TODO add timezone (assume UTC now)
    """
    if binary:
        binfile_path = Path(xml_path).with_suffix(".bin")
        bin_values = np.fromfile(
            binfile_path,
            dtype=np.float32,
            offset=0,
            count=-1,
        )


    #Read headers
    xml_data = objectify.parse(xml_path)  # Parse XML data
    root = xml_data.getroot()  # Root element

    series = {}

    #Get children, filter out timezone.
    rootchildren = [child for child in root.getchildren() if child.tag.endswith("series")]

    #Loop over children (individual timeseries in the xml)
    for i, child in enumerate(rootchildren):
        subchildren = child.getchildren() #alle headers en en timevalues
        metadata = None
        data = []
        columns = None
        
        for subchild in subchildren:

            #Write header to dict
            if subchild.tag.endswith("header"): #gewoonlijk eerste subchild is de header
                header_childs = subchild.getchildren()
                metadata = {}
                for item in header_childs:
                    key = item.tag.split("}")[-1]

                    item_keys = item.keys()
                    
                    if len(item_keys)==0:
                        metadata[key] = item.text
                    else:
                        metadata[key] = {}
                        for item_key, item_value in zip(item_keys, item.values()):
                            metadata[key][item_key] = item_value
            #Get data
            else:
                data.append(subchild.values())

            #If binary we assume equidistant series with equal length.
            if binary:
                bin_size = int(len(bin_values)/len(rootchildren))
                data=bin_values[i*bin_size:i*bin_size+bin_size]

        columns=subchild.keys()
        serie = XmlSeries(metadata, data=data, columns=columns, binary=binary)

        if serie.locid not in series.keys():
            series[serie.locid] = {}
        series[serie.locid][serie.paramid] = serie 

    return series


def print_xml(timeseries):
    """helper function. Print header en tree gegevens van de timeseries."""
    for key in timeseries.keys():
        print(key)
        for key2 in timeseries[key]:
            for header in timeseries[key][key2]['tree']:
                for line in header:
                    print(line.text)                
        print('')
    ## WERKT NIET WANT GEEN RECHTEN ##
    #Write directly to FEWS
    # pi.putTimeSeriesForFilter(filterId='HHNK_OPP_WEB_SL',
    #                         piTimeSeriesXmlContent=pi_ts_xml)
    ## WERKT NIET WANT GEEN RECHTEN ##


class DataFrameTimeseries:
    """Dataframe should contain datetime indices with each column as a separate locationid
    This class will turn a dataframe with timeseries into an xml. 
    """
    def __init__(self, df, out_path, 
                 header_settings = {"module_instance_id":"",
                                    "parameter_id":"",
                                    "qualifier_ids":[],
                                    "missing_val":-9999}):
        
        self.df = df
        self.header_settings = header_settings
        self.xml = XmlTimeSeries(out_path=out_path)


    def _make_eventstr_base(self, pd_timeindex_serie):
        """Create base event string with date and time and empty value
        the value can be added later with string formatting"""
        return f'\t\t<event date="{pd_timeindex_serie.strftime(f"%Y-%m-%d")}" time="{pd_timeindex_serie.strftime(f"%H:%M:%S")}" value="{"{}"}"/>\n'


    def _get_header(self, location_id):
        return XmlHeader(module_instance_id=self.header_settings["module_instance_id"],
                        location_id=location_id,
                        parameter_id=self.header_settings["parameter_id"],
                        qualifier_ids=self.header_settings["qualifier_ids"],
                        missing_val=self.header_settings["missing_val"],
                        )
    
    def get_series_from_df(self):
        """extract timeseries from df and add them to the xml timeseries."""
        self.eventstr_base = "".join(pd.Series(self.df.index).apply(self._make_eventstr_base))

        #Add each column as separate serie to df
        for key, pd_serie in self.df.items():
            ts_header = self._get_header(location_id = key)
            
            #Insert values into eventstring
            eventstr = self.eventstr_base.format(*pd_serie.values)

            #Add series to xml
            self.xml.add_serie(header=ts_header, eventstr=eventstr)
            

    def write(self):
        self.xml.write()

    
    def run(self):
        self.get_series_from_df()
        self.write()