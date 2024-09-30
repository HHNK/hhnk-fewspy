# %%
import hhnk_fewspy.xml_classes as xml_classes
import hhnk_fewspy.xml_functions as xml_functions
from tests_fewspy.config import FOLDER_DATA


def test_xml_to_dict():
    xml_functions.xml_to_dict(xml_path=FOLDER_DATA.bin_test_series.base, binary=True)


def test_xml():
    xml_functions.xml_to_df(xml_path=FOLDER_DATA.bin_test_series.base, binary=True, parameter="")


if __name__ == "__main__":
    test_xml()
