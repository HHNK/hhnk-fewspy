"""General tools used by HHNK"""

import glob
import os
from pathlib import Path


def log_arguments(log_file, arguments):
    """Log arguments to file

    Parameters
    ----------
    log_path : Union[str, Path, File]
        filepath to the logfile
    arguments : sys.argv
        sys.argv arguments to log
    """
    # Create parent if it doenst exist
    Path(str(log_file)).parent.mkdir(exist_ok=True)

    # Log args
    with open(str(log_file), "w") as f:
        f.write("Arguments:\n")
        for i, arg in enumerate(arguments):
            f.write(f"{i}:  {arg}\n")


def clean_logs(log_dir, keepcount=24, lognames=["settings_", "log_"]):
    """Settings and logs are written to new file every time. Only keep x of them"""  # noqa: D401

    for logname in lognames:
        for i in glob.glob(os.path.join(str(log_dir), f"{logname}*"))[:-keepcount]:
            os.remove(i)


def replace_datashare(d) -> Path:
    """Sawis user has problems with datashare. Replacing with d$ helps."""
    return Path(d.replace("Datashare", "d$"))
