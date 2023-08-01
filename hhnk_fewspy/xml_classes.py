# %%
import pandas as pd
import inspect
import datetime
from lxml import objectify
import numpy as np
import hhnk_research_tools as hrt


# TODO toevoegen aan header (uit de event str halen):
            # <timeStep unit="second" multiplier="900"/>
			# <startDate date="2021-06-22" time="06:00:00"/>
            # <endDate date="2021-06-22" time="07:00:00"/>
#TODO XmlHeader.write uitfaseren
#TODO metadata XmlSeries overnemen in XmlTimeSeries
#TODO bij wegschrijven max x events en anders opsplitsen naar meerdere bestanden.

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
    

class XmlSerie():
    """
    Part of XmlTimeSeries
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


class XmlTimeSeries:
    """#TODO XmlFile, rename??"""
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


#TODO where is this used?
# Now used in xml_to_dict, but maybe we can use XmlTimeSeries
class XmlSeries():
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


class XmlFile(hrt.File):
    """Mother of all classes. 
    Can read xml to df and write back to file"""
    def __init__(self, xml_path):
        super().__init__(base=xml_path)


    @property
    def binfile_path(self):
        binfile_path = self.pl.with_suffix(".bin")
        if binfile_path.exists():
            return binfile_path
        else:
            return None


    # def to_df():


    def to_dict(self):

        #Check if there is a bin file
        if self.binfile_path is None:
            binary = False
        else:
            binary = True

        
        if binary:
            bin_values = np.fromfile(
                self.binfile_path,
                dtype=np.float32,
                offset=0,
                count=-1,
            )


        #Read headers
        xml_data = objectify.parse(self.base)  # Parse XML data
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


class DataFrameTimeseries():
    def __init__(self):
        """Dataframe should contain datetime indices with each column 
        as a separate locationid. This class will turn a dataframe 
        with timeseries into an xml. 
        """
        pass

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

    
    def run(self, 
            df, 
            out_path, 
            header_settings = {"module_instance_id":"",
                                "parameter_id":"",
                                "qualifier_ids":[],
                                "missing_val":-9999}
    ):
        self.df = df
        self.header_settings = header_settings
        self.xml = XmlTimeSeries(out_path=out_path)
    

        self.get_series_from_df()
        self.write()




# %%
if __name__ == "__main__":
    from pathlib import Path
    self = XmlFile(Path(__file__).parents[1] / "tests_fewspy/data/bin_test_series.xml")
    d=self.to_dict()
    e = d["union_6750-98"]
    f = e["H.meting"]

