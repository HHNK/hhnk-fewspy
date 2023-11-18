# %%
from hhnk_fewspy.xml_classes import XmlHeader


def test_xml_header():
    """Test if we can create a header from a dict"""
    xml_header = XmlHeader(
        **{
            "type": "instantaneous",
            "locationId": "union_5803-15",
            "parameterId": "H.meting",
            "timeStep": {"unit": "second", "multiplier": "900"},
            "startDate": {"date": "2021-06-22", "time": "06:00:00"},
            "endDate": {"date": "2021-06-22", "time": "07:00:00"},
            "missVal": "-999.0",
            "stationName": "union_5803-15",
            "units": "m",
        }
    )

    assert xml_header.module_instance_id is None
    assert xml_header.location_id == "union_5803-15"


if __name__ == "__main__":
    test_xml_header()
