"""General tools used by HHNK"""
import glob
import os

import hhnk_research_tools as hrt


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
    hrt.File(log_file).parent.create()

    # Log args
    with open(str(log_file), "w") as f:
        f.write("Arguments:\n")
        for i, arg in enumerate(arguments):
            f.write(f"{i}:  {arg}\n")


def clean_logs(log_dir, keepcount=24):
    """Settings and logs are written to new file every time. Only keep x of them"""  # noqa: D401
    for logname in ["settings_", "log_"]:
        for i in glob.glob(str(log_dir.path / f"{logname}*"))[:-keepcount]:
            os.remove(i)


def replace_datashare(d):
    """Sawis user has problems with datashare. Replacing with d$ helps."""
    return d.replace("Datashare", "d$")
