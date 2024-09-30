# %%
from hhnk_fewspy.xml_classes import XmlFile, XmlHeader


def test_xml_header():
    """Test if we can create a header from a dict"""
    xml_header = XmlHeader(
        **{
            "type": "instantaneous",
            "location_id": "union_5803-15",
            "parameter_id": "H.meting",
            "miss_val": "-999.0",
        }
    )

    assert xml_header.module_instance_id is None
    assert xml_header.location_id == "union_5803-15"


def test_xml_file_bin():
    """Test if we can read binary xml file"""
    xml_file = XmlFile.from_xml_file(r"data/bin_test_series.xml")

    df = xml_file.to_df()
    assert int(df.sum().sum()) == -337126


def test_xml_file():
    """Test if we can read normal xml file"""
    xml_file = XmlFile.from_xml_file(r"data/normal_test_series.xml")

    df = xml_file.to_df()
    assert int(df.sum().sum()) == -11


# %%
if __name__ == "__main__":
    test_xml_header()

# %%
