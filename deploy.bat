REM install local folder to external deps
python setup.py bdist_wheel 
pip uninstall hhnk_fewspy -y
@REM pip install --target %appdata%\3Di\QGIS3\profiles\default\python\plugins\hhnk_threedi_plugin\external-dependencies --upgrade E:\github\%username%\hhnk-threedi-tools\dist\hhnk_threedi_tools-2023.2-py3-none-any.whl

