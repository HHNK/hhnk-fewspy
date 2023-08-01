# %%  
from pathlib import Path
import hhnk_research_tools as hrt


class Folders(hrt.Folder):
    def __init__(self, base):
        super().__init__(base)

        self.add_file("bin_test_series", "bin_test_series.xml")


DATA_DIR = Path(__file__).parent.absolute() / "data"
FOLDER_DATA = Folders(DATA_DIR)
