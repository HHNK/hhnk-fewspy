import requests
import pandas as pd
import hkvfewspy
from hkvfewspy.utils.pi_helper import read_timeseries_response
import json
import numpy as np
import inspect
from lxml import objectify
import pandas as pd
from pathlib import Path
import datetime
FEWS_REST_URL = 'https://fews.hhnk.nl/FewsWebServices/rest/fewspiservice/v1/'
FEWS_SOAP_URL = 'https://fews.hhnk.nl/FewsWebServices/fewspiservice?wsdl'
PAYLOAD = {'documentFormat':'PI_JSON',
              'documentVersion': '1.25'}

class connect_API:
    # @staticmethod
    # def connect_soap():
    #     pi = hkvfewspy.PiSoap() 
    #     pi.setClient(wsdl=FEWS_SOAP_URL)
    #     return pi
    
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


class XmlHeader():
    """Init timeseries header. can be written to timeseries by supplying the pi_ts"""
    def __init__(self, module_instance_id,
                    location_id,
                    parameter_id,
                    qualifier_ids=[],
                    missing_val=-9999,
                    ):
        
        self.module_instance_id = module_instance_id
        self.location_id = location_id
        self.qualifier_ids = qualifier_ids
        self.parameter_id = parameter_id
        self.missing_val = missing_val

    def write(self, pi_ts):
        """write to hkvfewspy timeseries"""
        pi_ts.write.header.moduleInstanceId(self.module_instance_id)
        pi_ts.write.header.locationId(self.location_id)
        if self.qualifier_ids:
            pi_ts.write.header.qualifierId(self.qualifier_ids)
        pi_ts.write.header.parameterId(self.parameter_id)
        pi_ts.write.header.missVal(self.missing_val)
        return pi_ts
    

    @property
    def qualifier_str(self):
        if len(self.qualifier_ids)==0:
            return ""
        else:
            return "\n" + ("<qualifierId>{}</qualifierId>\n"*len(self.qualifier_ids)).format(self.qualifier_ids)


    def to_str(self, indent=2):
        tab='\t'

        return \
f"""{tab*indent}<header>
{tab*(indent+1)}<type>instantaneous</type>
{tab*(indent+1)}<moduleInstanceId>{self.module_instance_id}</moduleInstanceId>
{tab*(indent+1)}<locationId>{self.location_id}</locationId>
{tab*(indent+1)}<parameterId>{self.parameter_id}</parameterId>{self.qualifier_str}
{tab*(indent+1)}<missVal>{self.missing_val}</missVal>
{tab*indent}</header>"""
    
    def __repr__(self):
        return self.to_str(indent=0)
    

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
            

class xmlSeries():
    def __init__(self, metadata, data, binary, columns=None):
        self.metadata = metadata
        self.binary= binary
        if not self.binary:
            self.df = self.make_df(data, columns)
        else:
            self.df = self.make_df_binary(data)

    def make_df(self, data, columns):
        df = pd.DataFrame(data, columns=columns)
        return df
    
    def make_df_binary(self, data,):
        df = pd.DataFrame(data, columns=["value"])

        #Make datetime col
        assert self.metadata["timeStep"]["unit"] == "second"
        df.set_index(self.timeseries, inplace=True)
        return df

    @property
    def locid(self):
        return self.metadata['locationId']

    @property
    def paramid(self):
        return self.metadata['parameterId']

    @property
    def start(self):
        return self._get_datetime("startDate")

    @property
    def end(self):
        return self._get_datetime("endDate")
    
    @property
    def timesteps(self):
        return (self.end-self.start).total_seconds()/eval(self.metadata["timeStep"]["multiplier"]) +1

    @property
    def timeseries(self):
        return pd.date_range(start=self.start, end=self.end, periods=self.timesteps)

    def _get_datetime(self, key):
        return datetime.datetime.strptime(self.metadata[key]["date"]+self.metadata[key]["time"], 
                            "%Y-%m-%d%H:%M:%S")

    def __repr__(self):
        funcs = '.'+' .'.join([i for i in dir(self) if not i.startswith('__') and hasattr(inspect.getattr_static(self,i)
        , '__call__')])
        variables = '.'+' .'.join([i for i in dir(self) if not i.startswith('__') and not hasattr(inspect.getattr_static(self,i)
        , '__call__')])
        repr_str = f"""functions: {funcs}
variables: {variables}"""
        return f"""xmlSeries locationId={self.metadata['locationId']}, parameterId={self.metadata['parameterId']}
"""


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
            for header in timeseries[key][key2]['tree']:
                for line in header:
                    print(line.text)                
        print('')
    ## WERKT NIET WANT GEEN RECHTEN ##
    #Write directly to FEWS
    # pi.putTimeSeriesForFilter(filterId='HHNK_OPP_WEB_SL',
    #                         piTimeSeriesXmlContent=pi_ts_xml)
    ## WERKT NIET WANT GEEN RECHTEN ##



class XmlSerie():
    """
    <series>
		<header>
			<type>instantaneous</type>
			<moduleInstanceId>yesy</moduleInstanceId>
			<locationId>02110-01</locationId>
			<parameterId>h.streef.boven</parameterId>
			<missVal>-9999</missVal>
		</header>
        <event date="2023-01-02" time="00:00:00.000" value="nan"/>
    </series>
    """
    def __init__(self, header: XmlHeader, eventstr):
        self.header = header
        self.events = eventstr


    def make_event_str(self, pd_serie):
        return f'\t\t<event date="{pd_serie.index.strftime(f"%Y-%m-%d")}" time="{pd_serie.index.strftime(f"%H:%M:%S")}" value="{pd_serie.values}"/>'


    def get_events(self):
        return '\n'.join(self.events)
    

    def to_str(self):
        return \
f"""\t<series>
{self.header.to_str(indent=2)}
{self.events}\t</series>
"""
    

class XmlTimeseries:
    def __init__(self, out_path, tzone="0.0"):
        self.out_path = out_path

        self.head = '<?xml version="1.0" ?>\n<TimeSeries xmlns="http://www.wldelft.nl/fews/PI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd" version="1.22">\n'
        self.timezone = f"<timeZone>{tzone}</timeZone>\n"
        self.series = []
        self.tail = "</TimeSeries>"


    def add_serie(self, header, eventstr):
        self.series.append(XmlSerie(header=header, eventstr=eventstr))


    def write(self):
        with open(self.out_path, 'w') as f:
            f.write(self.head)
            f.write(self.timezone)
            for serie in self.series:
                f.write(serie.to_str())
            f.write(self.tail)


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
        self.xml = XmlTimeseries(out_path=out_path)


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