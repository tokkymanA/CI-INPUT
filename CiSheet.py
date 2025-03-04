import pandas as pd
import os
import glob
from datetime import datetime
import numpy as np
from ErrorLog import ErrorLogFile
from OracleConnect import ConnectOracle
import platform
import time

class GetCISheet:
    def __init__(self, sup_main_path):
        """
        Get CI SHEET from specific path
        """
        self.time_now = datetime.now()
        self.ci_sheet_folder = [f"CI SHEET({int(self.time_now.year) - i[0]})" for i in enumerate(range(0, 4))]
        self.dic_path = dict.fromkeys(self.ci_sheet_folder, None)
        self.error_log = ErrorLogFile()
        
        for file in self.ci_sheet_folder:
            string_path = f"{sup_main_path}/{file}"
            self.dic_path[file] = np.array(glob.glob(f"{string_path}/*.xls"))

    def checkDuplicate(self, frame:pd.DataFrame, test):

        # old version

        # if not test:
        #     frame.drop_duplicates()
        #     ref_data = ConnectOracle(frame=frame, condition='CheckModel').CheckModel()
        #     updated_row = frame[(frame["APPMON"] == datetime.now().strftime("%Y%m")) & (~frame["CINO"].isin(ref_data["cino"]))]

        #     if len(updated_row):
        #         ConnectOracle(frame=updated_row, condition='InsertModel')
        #         print(f"""{self.time_now.strftime("%d/%m/%Y %H:%M:%S")}: ModelUpdated {len(updated_row)} Row""")
                
        #     else:
        #         print(f"""{self.time_now.strftime("%d/%m/%Y %H:%M:%S")}: No Row ModelUpdated!""")

        frame.drop_duplicates(inplace=True)
        column_order = ["CINO", "CI_STS", "MODEL", "LINE", "SERIAL", "APPLY_DATE", "CENTER_IDCODE",
                        "INCHARGE_IDCODE", "CNAME", "UDATE", "CDATE", "INCHARGE_NAME"]
        
        frame.drop(columns=["OPERATOR_NAME"], inplace=True)
        frame.rename(columns={"APPMON":"APPLY_DATE"}, inplace=True)
        frame['APPLY_DATE'] = pd.to_datetime(frame['APPLY_DATE'], format='%Y%m%d').dt.strftime("%d/%m/%y")
        frame["LINE"] = "No"
        frame["SERIAL"] = "No"
        frame["CENTER_IDCODE"] = "No"
        frame["INCHARGE_IDCODE"] = "No"
        frame["INCHARGE_NAME"] = "No"
        ref_data = ConnectOracle(frame=frame, condition='CheckModelCI').CheckModelCI()
        updated_row = frame[(frame["APPLY_DATE"] == datetime.now().strftime("%d/%m/%y")) & (~frame["CINO"].isin(ref_data["cino"]))]
        print(frame)
        print(frame['APPLY_DATE'].agg(['min']).item())
       
        if len(updated_row) >= 1 and not test:
            print(f"""{self.time_now.strftime("%d/%m/%Y %H:%M:%S")}: ModelUpdated {len(updated_row)} Row""")
            ConnectOracle(frame=updated_row[column_order], condition='InsertModelCI')
            ConnectOracle(frame=updated_row[column_order], condition='UpdateModelCI')
            ConnectOracle(frame=frame[column_order], condition='UpdateInchargeCI')
        
        if len(ref_data) == 0 and test:
            print(f"""{self.time_now.strftime("%d/%m/%Y %H:%M:%S")}: Initiate Data {len(frame)} Row""")
            ConnectOracle(frame=frame[column_order], condition='InsertModelCI')
            ConnectOracle(frame=frame[column_order], condition='UpdateModelCI')
            ConnectOracle(frame=frame[column_order], condition='UpdateInchargeCI')

        else:
            print(f"""{self.time_now.strftime("%d/%m/%Y %H:%M:%S")}: ModelUpdated {len(updated_row)} Row""")
        

    def CleanCISheet(self, folder_name, test=False):
        """
        Prepare CI SHEET to pandas data frame format
        """
        if not test:
            ci_number = list()
            date_of_issue = list()
            incharge_name = list()
            apply_model = list()
            c_date = list()
            u_date = list()

            for file in self.dic_path[folder_name]:
                print(file)
                xls = pd.read_excel(file)
                xls.columns = xls.columns.str.strip()
                condition = xls["Unnamed: 1"] == "(CHANGE  POINT/ NOTES)"
                index_to_remove = xls[condition].index[0]
                xls_filter = xls.loc[:index_to_remove-1]
                get_ci_number = xls_filter.iloc[0]["Unnamed: 1"]
                get_issue_date_index = xls_filter.columns.get_loc("DATE OF ISSUE :")
                get_apply_month_index = xls.columns[xls.iloc[7].eq("MONTH")]
                get_apply_month_index_valid = xls[get_apply_month_index.item()][8:].first_valid_index()
                get_apply_month = xls[get_apply_month_index.item()][get_apply_month_index_valid]
                get_incharge = xls_filter.iloc[4, get_issue_date_index + 3]
                get_apply_model_column = xls_filter.columns[get_issue_date_index - 1]
                get_apply_model_series = xls_filter[get_apply_model_column][8::]
                get_apply_model = get_apply_model_series[get_apply_model_series.notna()]
                get_apply_model = str(",".join(get_apply_model)).replace(" ", "")

                ci_number.append(get_ci_number.replace(" ", ""))
                date_of_issue.append(get_apply_month)
                incharge_name.append(get_incharge)
                apply_model.append(get_apply_model)

                dt_object_c = datetime.strptime(time.ctime(os.path.getctime(file)), '%a %b %d %H:%M:%S %Y')
                dt_object_u = datetime.strptime(time.ctime(os.path.getmtime(file)), '%a %b %d %H:%M:%S %Y')

                c_date.append(dt_object_c.strftime("%d/%m/%Y"))
                u_date.append(dt_object_u.strftime("%d/%m/%Y"))
                
            ci_sts = ["01"] * len(ci_number)
            frame = pd.DataFrame(data={"CINO":ci_number, "CI_STS":ci_sts, "MODEL":apply_model, "APPMON":date_of_issue, "OPERATOR_NAME":incharge_name, "UDATE":u_date, "CDATE":c_date})
            frame["CNAME"] = platform.node()
            frame["MODEL"] = frame["MODEL"].str.split(',')
            frame_explode = frame.explode("MODEL")
            frame_explode.reset_index(drop=True, inplace=True)
            # frame_explode.to_csv(f"_{folder_name}.csv", index=False)
            self.checkDuplicate(frame=frame_explode, test=test)

        else:
            frame_explode = pd.read_csv("_CI SHEET(2025).csv") #manual read csv file
            self.checkDuplicate(frame=frame_explode, test=test)
            # ConnectOracle(frame=frame_explode, condition='InsertModel')
        

        

        