# %%
import datetime
import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from lxml import objectify

# TODO bij wegschrijven max x events en anders opsplitsen naar meerdere bestanden.


DATETIME_KEYS = ["start_date", "end_date"]
FLOAT_KEYS = ["miss_val", "lat", "lon", "x", "y", "z"]
EVENT_COLUMNS = ["datetime", "value", "flag"]

TAB = "\t"


def camel_to_snake_case(camel_case: str) -> str:
    """Convert camelCase to snake_case."""
    return "".join(["_" + i.lower() if i.isupper() else i for i in camel_case]).lstrip("_")


def snake_to_camel_case(snake_case: str) -> str:
    """Convert snake_case to camelCase."""
    words = snake_case.split("_")
    return words[0] + "".join(i.title() for i in words[1:])


@dataclass
class XmlDate:
    """Date representation of xml start or end date.

    Usage
    .date_time (datetime.datetime): datetime format
    .xml_str (str): str format used in excel.
    """

    key: str
    date: str
    time: str

    @classmethod
    def from_pi_header(cls, key, data: dict):
        """Convert FEWS datetime dict to object

        data (dict): fews datetime (e.g. {'date': '2000-01-01', 'time': '00:00:00'})
        """
        key = snake_to_camel_case(key)
        date = data.get("date", "2000-01-01")
        time = data.get("time", "00:00:00")
        return XmlDate(key=key, date=date, time=time)

    @property
    def date_time(self) -> datetime.datetime:
        return datetime.datetime.fromisoformat(f"{self.date}T{self.time}")

    @property
    def xml_str(self) -> str:
        return f"""<{self.key} date="{self.date}" time="{self.time}"/>"""

    def __str__(self):
        return str(self.date_time)


@dataclass
class XmlHeader:
    """FEWS-PI header-style dataclass"""

    parameter_id: str = None
    time_step: dict = None
    miss_val: Union[int, float] = None
    units: str = None
    start_date: XmlDate = None
    end_date: XmlDate = None
    type: str = "instantaneous"
    location_id: str = None
    module_instance_id: str = None
    lat: float = None
    lon: float = None
    x: float = None
    y: float = None
    station_name: str = None
    z: float = None
    qualifier_ids: list[str] = None

    @classmethod
    def from_pi_header_element(cls, subchild: objectify.ObjectifiedElement):
        """Parse Header from FEWS PI header dict.
        see: https://github.com/hdsr-mid/hdsr_fewspy/blob/main/hdsr_fewspy/converters/json_to_df_time_series.py
        Args:
            subchild (objectify.ObjectifiedElement): Header element read  with lxml
        Returns:
            Header: FEWS-PI header-style dataclass
        """

        # Create metadata dict from element
        header_childs = subchild.getchildren()
        metadata = {}
        for item in header_childs:
            key = item.tag.split("}")[-1]

            # if value of key is a dict, these are the keys. e.g.['date', 'time']
            item_keys = item.keys()
            if len(item_keys) == 0:
                metadata[key] = item.text
            else:
                metadata[key] = {}
                for item_key, item_value in zip(item_keys, item.values()):
                    metadata[key][item_key] = item_value

        # transform dict to input for XmlHeader
        def _convert_kv(key: str, value):
            key = camel_to_snake_case(key)
            if key in DATETIME_KEYS:
                value = XmlDate.from_pi_header(key, value)
            if key in FLOAT_KEYS:
                value = float(value)
            return key, value

        args = (_convert_kv(k, v) for k, v in metadata.items())
        header = XmlHeader(**{i[0]: i[1] for i in args if i[0] in cls.__dataclass_fields__.keys()})
        return header

    @property
    def id(self):
        """Unique timeseries id, can be used as df column name"""
        id_params = [self.location_id, self.parameter_id]

        if self.time_step is not None:
            id_params.append(self._time_step_str)

        return "__".join(id_params)

    @property
    def _time_step_str(self):
        """Str representation of timestep for self.id"""
        return f"{self.time_step['multiplier']}{self.time_step['unit']}"

    def to_str(self, indent: int = 2):
        """Str representation of header

        indent (int): indentation for writing to file
        """

        return_str = f"""{TAB*indent}<header>
{TAB*(indent+1)}<type>instantaneous</type>
{TAB*(indent+1)}<moduleInstanceId>{self.module_instance_id}</moduleInstanceId>
{TAB*(indent+1)}<locationId>{self.location_id}</locationId>
{TAB*(indent+1)}<parameterId>{self.parameter_id}</parameterId>"""
        if self.qualifier_ids is not None:
            if not isinstance(self.qualifier_ids, list):
                raise TypeError("self.qualifier_ids should be of type list.")

            return_str = "\n".join(
                [
                    return_str,
                    (f"{TAB*(indent+1)}<qualifierId>{{}}</qualifierId>\n" * len(self.qualifier_ids))
                    .format(*self.qualifier_ids)
                    .rstrip("\n"),
                ]
            )

        if self.time_step is not None:
            return_str = "\n".join(
                [
                    return_str,
                    f"""{TAB*(indent+1)}<timeStep unit="{self.time_step['unit']}" multiplier="{self.time_step['multiplier']}"/>""",
                ]
            )

        return_str = "\n".join(
            [
                return_str,
                f"""{TAB*(indent+1)}<missVal>{self.miss_val}</missVal>
{TAB*indent}</header>""",
            ]
        )
        return return_str

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

    def __init__(self, header: XmlHeader, data: np.array = None, is_binary=False):
        self.header = header
        self.data = data
        self.is_binary = is_binary

        self._events = None

        self._eventstr_base = None
        self._df = None

    @classmethod
    def from_df(cls, header: XmlHeader, df_serie: pd.Series):
        ts = XmlTimeSeries(header=header)
        # set dataframe.
        df_ts = pd.DataFrame(df_serie)
        df_ts.columns = ["value"]
        ts.df = df_ts
        return ts

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
        return int(
            (self.end - self.start).total_seconds() / unit_factor / int(self.header.time_step["multiplier"]) + 1
        )

    @property
    def timeseries_index(self) -> pd.DatetimeIndex:
        """Index column of all timesteps"""
        return pd.date_range(start=self.start, end=self.end, periods=self.timesteps)

    @staticmethod
    def _make_eventstr_base(pd_timeindex_serie, indent=2):
        """Create base event string with date and time and empty value
        the value can be added later with string formatting
        """
        return f'{TAB*indent}<event date="{pd_timeindex_serie.strftime(f"%Y-%m-%d")}" \
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
            self._eventstr_base = "".join(
                pd.Series(self.df.index).apply(lambda x: self._make_eventstr_base(x, indent=2))
            )
        return self._eventstr_base

    @eventstr_base.setter
    def eventstr_base(self, eventstr_base):
        self._eventstr_base = eventstr_base

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
{self.events}\t</series>"""

    def print(self):
        print(self.to_str())

    @property
    def df(self) -> pd.DataFrame:
        """Data to pd.DataFrame. The df has datetime index and one column 'value'"""
        if self._df is None:
            if self.is_binary:
                df = pd.DataFrame(self.data, columns=["value"])
                df.set_index(self.timeseries_index, inplace=True)
            else:
                df = pd.DataFrame(self.data)
                df.set_index(pd.to_datetime(df[0] + "T" + df[1]), inplace=True)
                df.drop([0, 1], axis=1, inplace=True)
                df.rename({2: "value"}, axis=1, inplace=True)
                df["value"] = df["value"].astype(float)
            self._df = df
        return self._df

    @df.setter
    def df(self, df):
        self._df = df


class XmlFile(hrt.File):
    """Mother of all classes.
    XmlFile can both be a binary or non-binary file.
    The file is a collection of TimeSeries that are stored in the .series attribute.

    Can read xml to df and write back to file
    """

    def __init__(self, xml_path):
        super().__init__(base=xml_path)

        self.head = '<?xml version="1.0" ?>\n<TimeSeries xmlns="http://www.wldelft.nl/fews/PI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd" version="1.22">\n'

        # collection of TimeSeries
        self.series = pd.Series()

        # Filled at runtime.
        self.header_base = None

    @classmethod
    def from_xml_file(cls, xml_path):
        """Read xml file and return XmlFile object"""

        xml_file = XmlFile(xml_path=xml_path)

        # Read data
        xml_data = objectify.parse(xml_file.base)  # Parse XML data
        root = xml_data.getroot()  # Root element

        # Get children, filter out timezone.
        rootchildren = [child for child in root.getchildren() if child.tag.endswith("series")]

        # Check if there is a bin file
        is_binary = xml_file.is_binary  # local variable for speed
        if is_binary:
            bin_values = np.fromfile(xml_file.binfile_path, dtype=np.float32, offset=0, count=-1)
            bin_size = int(len(bin_values) / len(rootchildren))

        # Loop over children (individual timeseries in the xml)
        for i, child in enumerate(rootchildren):
            subchildren = child.getchildren()  # alle headers en en timevalues
            header = None
            data = []

            for subchild in subchildren:
                # Write header to dict
                if subchild.tag.endswith("header"):  # gewoonlijk eerste subchild is de header
                    header = XmlHeader.from_pi_header_element(subchild)
                # Get data
                else:
                    # TODO test if works on non binary?
                    data.append(subchild.values())

                # If binary we assume equidistant series with equal length.
                if is_binary:
                    data = bin_values[i * bin_size : i * bin_size + bin_size]

            # columns = subchild.keys()
            if header is None:
                raise ValueError("Header should not be None at this point")

            serie = XmlTimeSeries(header=header, data=data, is_binary=is_binary)
            xml_file.add_time_series(serie=serie)

        return xml_file

    @classmethod
    def from_df(cls, df, module_instance_id, parameter_id, miss_val, qualifier_ids=None):
        """Create file from a df. The input header options are the same for all series.
        Write different combinations of these headers to different files.
        """
        xml_file = XmlFile(xml_path=None)

        # iter over columns and add each as time series.
        for series_name, s in df.items():
            series_header = XmlHeader(
                location_id=series_name.split("__")[0],
                module_instance_id=module_instance_id,
                parameter_id=parameter_id,
                qualifier_ids=qualifier_ids,
                miss_val=miss_val,
            )

            serie = XmlTimeSeries.from_df(header=series_header, df_serie=s)

            xml_file.add_time_series(serie=serie)
        return xml_file

    @property
    def binfile_path(self):
        # TODO only used in from_xml_file, should this be an attribute?
        """Path to binfile if it exists, otherwise returns None.
        Binfile should have same name as the xml and should be in same folder.
        """
        binfile_path = self.path.with_suffix(".bin")
        if binfile_path.exists():
            return binfile_path
        return None

    @property
    def is_binary(self):
        # TODO only used in from_xml_file, should this be an attribute?
        """Check if xml has a binary file"""
        if self.binfile_path is None:
            return False
        return True

    def add_time_series(self, serie: XmlTimeSeries) -> XmlTimeSeries.id:
        """Add timeseries .series.
        Returns the id of the series that was added
        """
        if serie.id not in self.series:
            self.series[serie.id] = serie
            return serie.id
        raise ValueError(f"{serie.id} serie id is already part of XmlFile")

    def print(self, tzone="0.0"):
        """Print timeseries as it would get written to xml."""
        print(self.head)
        print(f"<timeZone>{tzone}</timeZone>\n")
        for serie in self.series.to_numpy():
            print(serie.to_str())
        print("</TimeSeries>")

    def write(self, output_path: Union[str, Path, hrt.File] = None, tzone: str = "0.0"):
        """Write timeseries to an xml file."""
        if output_path is not None:
            output_path = hrt.File(output_path)

        if output_path.exists():
            raise FileExistsError(f"{output_path.base} already exists")

        with open(output_path.path, "w") as f:
            f.write(self.head)
            f.write(f"<timeZone>{tzone}</timeZone>\n")
            for serie in self.series.to_numpy():
                f.write(serie.to_str())
            f.write("</TimeSeries>")

    def to_df(self):
        """Get combined df of all timeseries in file"""
        return pd.concat([v.df.rename(columns={"value": k}) for k, v in self.series.items()], axis=1)


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
        return f'\t\t<event date="{pd_timeindex_serie.strftime("%Y-%m-%d")}" time="{pd_timeindex_serie.strftime("%H:%M:%S")}" value="{"{}"}"/>\n'

    def _get_header(self, location_id):
        return XmlHeader(
            module_instance_id=self.header_settings["module_instance_id"],
            location_id=location_id,
            parameter_id=self.header_settings["parameter_id"],
            qualifier_ids=self.header_settings["qualifier_ids"],
            miss_val=self.header_settings["missing_val"],
        )

    def get_series_from_df(self):
        """Extract timeseries from df and add them to the xml timeseries."""
        self.eventstr_base = "".join(pd.Series(self.df.index).apply(self._make_eventstr_base))

        # Add each column as separate serie to df
        for key, pd_serie in self.df.items():
            ts_header = self._get_header(location_id=key)

            # Insert values into eventstring
            eventstr = self.eventstr_base.format(*pd_serie.to_numpy())

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

    self = XmlFile.from_xml_file(Path(__file__).parents[1] / "tests_fewspy/data/bin_test_series.xml")

    # d = self.read_to_dict()
    # e = d["union_6750-98"]
    # f = e["H.meting"]
    # self.write(output_path=Path(__file__).parents[1] / "tests_fewspy/data/bin_test_series3.xml")

    df = self.to_df()
    # %%

    self2 = XmlFile.from_df(df=df, module_instance_id="hi", parameter_id="h", miss_val=-999)
    self2.write(output_path=Path(__file__).parents[1] / "tests_fewspy/data/bin_test_series4.xml")


# # %%
# import cProfile

# cProfile.run('XmlFile.from_xml_file("streefpeil_2023.xml")')
