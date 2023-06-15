import pandas as pd
import inspect
import datetime


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
