# %%
import os
import warnings
from pathlib import Path

# https://github.com/geopandas/geopandas/issues/1166
# python -m ipykernel install --user --name fewspy_env


def update_hkvfewspy():
    """Fix for writing multiple qualifiers to xml"""
    import importlib
    import shutil

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=SyntaxWarning)
        import hkvfewspy

    src = os.path.join(os.path.dirname(__file__), r"hkvfewspy/pi_helper.py")
    dst = os.path.join(Path(hkvfewspy.__file__).parent, "utils", "pi_helper.py")

    shutil.copy(src, dst)

    importlib.reload(hkvfewspy)
