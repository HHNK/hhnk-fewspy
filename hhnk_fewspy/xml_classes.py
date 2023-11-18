# %%
import datetime
import inspect
from dataclasses import dataclass
from typing import Union

import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from lxml import objectify

# TODO toevoegen aan header (uit de event str halen):

# <timeStep unit="second" multiplier="900"/>
# <startDate date="2021-06-22" time="06:00:00"/>
# <endDate date="2021-06-22" time="07:00:00"/>
# TODO XmlHeader.write uitfaseren
# TODO metadata XmlTimeSeries overnemen in XmlTimeSeries
# TODO bij wegschrijven max x events en anders opsplitsen naar meerdere bestanden.

# TODO bezig met XmlFile.read_to_dict. De serie.df moet ergens in kunnen landen.

DATETIME_KEYS = ["start_date", "end_date"]
FLOAT_KEYS = ["miss_val", "lat", "lon", "x", "y", "z"]
EVENT_COLUMNS = ["datetime", "value", "flag"]


def camel_to_snake_case(camel_case: str) -> str:
    """Convert camelCase to snake_case."""
    return "".join(["_" + i.lower() if i.isupper() else i for i in camel_case]).lstrip("_")


@dataclass
class XmlDate:
    date: str
    time: str

    @classmethod
    def from_pi_header(self, data: dict):
        """Convert FEWS datetime dict to object

        data (dict): fews datetime (e.g. {'date': '2000-01-01', 'time': '00:00:00'})
        """
        date = data.get("date", "2000-01-01")
        time = data.get("time", "00:00:00")
        return XmlDate(date=date, time=time)

    @property
    def date_time(self):
        return datetime.datetime.fromisoformat(f"{self.date}T{self.time}")

    @property
    def xml_str(self):
        return f"""<startDate date="{self.date}" time="{self.time}"/>"""

    def __str__(self):
        return str(self.date_time)


@dataclass
class XmlHeader:
    """FEWS-PI header-style dataclass"""

    type: str
    location_id: str
    parameter_id: str
    time_step: dict
    miss_val: Union[int, float]
    units: str
    start_date: XmlDate
    end_date: XmlDate
    module_instance_id: str = None
    lat: float = None
    lon: float = None
    x: float = None
    y: float = None
    station_name: str = None
    z: float = None
    qualifier_ids: list[str] = None

    @classmethod
    def from_pi_header(cls, pi_header: dict) -> XmlHeader:
        """Parse Header from FEWS PI header dict.
        see: https://github.com/hdsr-mid/hdsr_fewspy/blob/main/hdsr_fewspy/converters/json_to_df_time_series.py
        Args:
            pi_header (dict): FEWS PI header as dictionary
        Returns:
            Header: FEWS-PI header-style dataclass
        """

        def _convert_kv(key: str, value):
            key = camel_to_snake_case(key)
            if key in DATETIME_KEYS:
                value = XmlDate.from_pi_header(value)
            if key in FLOAT_KEYS:
                value = float(value)
            return key, value

        args = (_convert_kv(k, v) for k, v in pi_header.items())
        header = XmlHeader(**{i[0]: i[1] for i in args if i[0] in cls.__dataclass_fields__.keys()})
        return header

    @property
    def id(self):
        """Unique timeseries id, can be used as df column name"""
        return f"{self.location_id}__{self.parameter_id}__{self._time_step_str}"

    @property
    def qualifier_str(self):
        """Qualifier str for header, every qualifier is printed on new line."""
        # TODO maybe add rstrip \n?
        if self.qualifier_ids is None:
            return ""
        return "\n" + ("<qualifierId>{}</qualifierId>\n" * len(self.qualifier_ids)).format(self.qualifier_ids)

    @property
    def _time_step_str(self):
        """Str representation of timestep for self.id"""
        return f"{self.time_step['multiplier']}{self.time_step['unit']}"

    def to_str(self, indent: int = 2):
        """Str representation of header

        indent (int): indentation for writing to file
        """
        tab = "\t"

        return f"""{tab*indent}<header>
{tab*(indent+1)}<type>instantaneous</type>
{tab*(indent+1)}<moduleInstanceId>{self.module_instance_id}</moduleInstanceId>
{tab*(indent+1)}<locationId>{self.location_id}</locationId>
{tab*(indent+1)}<parameterId>{self.parameter_id}</parameterId>{self.qualifier_str}
{tab*(indent+1)}<timeStep unit="{self.time_step['unit']}" multiplier="{self.time_step['multiplier']}"/>
{tab*(indent+1)}<missVal>{self.miss_val}</missVal>
{tab*indent}</header>"""

    def __repr__(self):
        return self.to_str(indent=0)


class XmlTimeSeries:
    """
    Unique timeserie in an xml.
    This is part of XmlFile and has its own headers and event string.

    example output:
    <series>
                <header>
                        <type>instantaneous</type>
                        <moduleInstanceId>yesy</moduleInstanceId>
                        <locationId>02110-01</locationId>
                        <parameterId>h.streef.boven</parameterId>
                        <missVal>-9999</missVal>
                </header>
        <event date="2023-01-02" time="00:00:00" value="nan"/>
        <event date="2023-01-02" time="00:15:00" value="4"/>

    </series>
    """

    def __init__(self, header: XmlHeader, data: np.array):
        self.header = header
        self.data = data
        # TODO fill events later? Must work two ways, when writing and reading
        self._events = None

        self._eventstr_base = None
        self._df = None

    @property
    def id(self):
        """Unique timeseries id"""
        return self.header.id

    @property
    def start(self) -> datetime.datetime:
        """Start datetime"""
        return self.header.start_date.date_time

    @property
    def end(self) -> datetime.datetime:
        """End datetime"""
        return self.header.end_date.date_time

    @property
    def timesteps(self) -> int:
        """Number of timesteps in series"""
        # TODO what to do with nonequi?
        if self.header.time_step["unit"] == "second":
            unit_factor = 1  # seconds dont need adjustment
        elif self.header.time_step["unit"] == "minute":
            unit_factor = 60
        elif self.header.time_step["unit"] == "hour":
            unit_factor = 3600
        else:
            raise NotImplementedError(f"Timestep unit [{self.header.time_step['unit']}]")
        return (self.end - self.start).total_seconds() / unit_factor / int(self.header.time_step["multiplier"]) + 1

    @property
    def timeseries_index(self) -> pd.DatetimeIndex:
        """Index column of all timesteps"""
        return pd.date_range(start=self.start, end=self.end, periods=self.timesteps)

    @staticmethod
    def _make_eventstr_base(pd_timeindex_serie):
        """Create base event string with date and time and empty value
        the value can be added later with string formatting
        """
        return f'\t\t<event date="{pd_timeindex_serie.strftime(f"%Y-%m-%d")}" \
time="{pd_timeindex_serie.strftime(f"%H:%M:%S")}" \
value="{"{}"}"/>\n'

    @property
    def eventstr_base(self):
        """Create string with every event on a new line.
        This string still needs to be formatted with the values.

        example:
        <event date="2021-06-22" time="06:00:00" value="{}"/>
        <event date="2021-06-22" time="06:15:00" value="{}"/>
        """
        # TODO get rid of apply
        if self._eventstr_base is None:
            self._eventstr_base = "".join(pd.Series(self.df.index).apply(self._make_eventstr_base))
        return self._eventstr_base

    @property
    def events(self):
        """Str representation of events filled with values

        example:
        <event date="2021-06-22" time="06:00:00" value="-4.80"/>
        <event date="2021-06-22" time="06:15:00" value="-4.81"/>
        """
        if self._events is None:
            self._events = self.eventstr_base.format(*self.df["value"].to_numpy())
        return self._events

    def to_str(self):
        """Str representation of serie. Has headers and events
        can be used for writing in a XmlFile
        """
        return f"""\t<series>
{self.header.to_str(indent=2)}
{self.events}\t</series>
"""

    @property
    def df(self):
        """Data to df"""
        if self._df is None:
            df = pd.DataFrame(self.data, columns=["value"])
            df.set_index(self.timeseries_index, inplace=True)
            self._df = df
        return self._df


class XmlFile(hrt.File):
    """Mother of all classes.
    XmlFile can both be a binary or non-binary file. It can contain multiple
    Can read xml to df and write back to file
    """

    def __init__(self, xml_path):
        super().__init__(base=xml_path)

    @property
    def binfile_path(self):
        """Path to binfile if it exists, otherwise returns None.
        Binfile should have same name as the xml and should be in same folder.
        """
        binfile_path = self.path.with_suffix(".bin")
        if binfile_path.exists():
            return binfile_path
        return None

    @property
    def is_binary(self):
        if self.binfile_path is None:
            return False
        return True

    # def to_df():

    def read_to_dict(self):
        """Create dictionary of input xml"""
        # Check if there is a bin file
        # %%
        if self.is_binary:
            bin_values = np.fromfile(self.binfile_path, dtype=np.float32, offset=0, count=-1)

        # Read data
        xml_data = objectify.parse(self.base)  # Parse XML data
        root = xml_data.getroot()  # Root element

        series = {}

        # Get children, filter out timezone.
        rootchildren = [child for child in root.getchildren() if child.tag.endswith("series")]

        # Loop over children (individual timeseries in the xml)
        for i, child in enumerate(rootchildren):
            subchildren = child.getchildren()  # alle headers en en timevalues
            metadata = None
            data = []

            for subchild in subchildren:
                # Write header to dict
                if subchild.tag.endswith("header"):  # gewoonlijk eerste subchild is de header
                    header_childs = subchild.getchildren()
                    metadata = {}
                    for item in header_childs:
                        key = item.tag.split("}")[-1]

                        # if value of key is a dict, these are the keys['date', 'time']
                        item_keys = item.keys()
                        if len(item_keys) == 0:
                            metadata[key] = item.text
                        else:
                            metadata[key] = {}
                            for item_key, item_value in zip(item_keys, item.values()):
                                metadata[key][item_key] = item_value
                # Get data
                else:
                    # TODO test if works on non binary?
                    data.append(subchild.values())

                # If binary we assume equidistant series with equal length.
                if self.is_binary:
                    bin_size = int(len(bin_values) / len(rootchildren))
                    data = bin_values[i * bin_size : i * bin_size + bin_size]

            # columns = subchild.keys()

            header = XmlHeader.from_pi_header(metadata)
            serie = XmlTimeSeries(header=header, data=data)

            if serie.header.location_id not in series:
                series[serie.header.location_id] = {}
            series[serie.header.location_id][serie.header.parameter_id] = serie
        # %%
        return series

    def add_serie(self, header, eventstr):
        """Add timeseries to xml"""
        self.series.append(XmlTimeSerie(header=header, eventstr=eventstr))

    def write(self, tzone="0.0"):
        """Write timeseries to an xml file."""
        head = '<?xml version="1.0" ?>\n<TimeSeries xmlns="http://www.wldelft.nl/fews/PI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd" version="1.22">\n'

        with open(self.path, "w") as f:
            f.write(head)
            f.write(f"<timeZone>{tzone}</timeZone>\n")
            for serie in self.series:
                f.write(serie.to_str())
            f.write("</TimeSeries>")


# %%
# return series


# TODO add from_xml_file and to_xml_file?
class DataFrameTimeseries:
    def __init__(self):
        """Dataframe should contain datetime indices with each column
        as a separate locationid. This class will turn a dataframe
        with timeseries into an xml.
        """
        pass

    def _make_eventstr_base(self, pd_timeindex_serie):
        """Create base event string with date and time and empty value
        the value can be added later with string formatting
        """
        return f'\t\t<event date="{pd_timeindex_serie.strftime(f"%Y-%m-%d")}" time="{pd_timeindex_serie.strftime(f"%H:%M:%S")}" value="{"{}"}"/>\n'

    def _get_header(self, location_id):
        return XmlHeader(
            module_instance_id=self.header_settings["module_instance_id"],
            location_id=location_id,
            parameter_id=self.header_settings["parameter_id"],
            qualifier_ids=self.header_settings["qualifier_ids"],
            missing_val=self.header_settings["missing_val"],
        )

    def get_series_from_df(self):
        """Extract timeseries from df and add them to the xml timeseries."""
        self.eventstr_base = "".join(pd.Series(self.df.index).apply(self._make_eventstr_base))

        # Add each column as separate serie to df
        for key, pd_serie in self.df.items():
            ts_header = self._get_header(location_id=key)

            # Insert values into eventstring
            eventstr = self.eventstr_base.format(*pd_serie.values)

            # Add series to xml
            self.xml.add_serie(header=ts_header, eventstr=eventstr)

    def write(self):
        self.xml.write()

    def run(
        self,
        df,
        out_path,
        header_settings={"module_instance_id": "", "parameter_id": "", "qualifier_ids": [], "missing_val": -9999},
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

    d = self.read_to_dict()
    e = d["union_6750-98"]
    f = e["H.meting"]

    # %%
    xmldict = d
    parameter = "H.meting"

    cols = [key for key in xmldict if parameter in xmldict[key].keys()]
    time_df = pd.DataFrame(index=xmldict[cols[0]][parameter].df.index, columns=cols)

    for col in cols:
        missval = float(xmldict[col][parameter].metadata["missVal"])
        time_df[col] = xmldict[col][parameter].df.replace(missval, np.nan)
    # return time_df
    time_df
# %%


"""
Structuur inladen van een XML


XmlFile heeft 1 of meerdere XmlTimeSeries

elke XmlTimeSeries heeft een header en de XmlFile heeft een eigen header en tail. 

Inlezen van bestand ->
Losse timeseries toevoegen aan de XmlFile

Per parameter kunnen we dit weergeven in een df. Wat doen we als er meerdere parameters in een df staan?



Structuur wegschrijven van een DataFrame.
Als we een berekening doen kunnen we een DataFrame hebben met per kolom een locatie_parameter combinatie.
Elke kolom is dan een XmlTimeSeries met eigen headers en events. 

deze moeten toegevoegd worden aan een XmlFile en dan kan het weggeschreven worden.



"""
