import os
import pandas as pd
from datetime import datetime, timedelta
from GetExcel import GetExcel
from CiSheet import GetCISheet

if __name__ == "__main__":
    
    main_path = "//doc/pdi/CENTER/1.CI/CI CONTROL DISTRIBUTION"
    sup_main_path = "//doc/pdi/CENTER/1.CI"

    current_year = datetime.now()
    year_folder = [file for file in os.listdir(main_path) if len(file) == 4]
    acept_file = [str(current_year.year)]
    year_folder_merge = [main_path+"/"+file for file in os.listdir(main_path) if len(file) == 4 and file in acept_file]
    
    
    for folder in year_folder_merge:
        xls_path = [folder+"/"+xls for xls in os.listdir(folder)]
        obj_getexcel = GetExcel(path=xls_path, year=folder[-4::])

    obj_getcisheet = GetCISheet(sup_main_path=sup_main_path)
    obj_getcisheet.CleanCISheet(folder_name=f"CI SHEET({current_year.year})", test=False)
    # print(obj_getcisheet.dic_path.keys())
    
# To activate virtual environment do it via cmd not powershell
# - via cmd ---> cd/venv/Scripts/activate.bat
# "\\doc\PDI\CENTER\1.CI\CI SHEET(2022)"