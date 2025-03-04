import os
import pandas as pd
from datetime import datetime
import platform
import numpy as np
from OracleConnect import ConnectOracle
from ErrorLog import ErrorLogFile

class GetExcel:
    """
    Get .xls files from specific path
    """
    def __init__(self, path:list, year):
        self.path_list = path
        self.year=year
        self.error_log = ErrorLogFile()
        self.time_now = datetime.now()
        self.cleanFrame(path=self.path_list)

    def matchCI(self, frame:pd.DataFrame):
        """
        Make a CI_TYPE column

        MasterKeyWorld ---> 01:PC LABEL, 02:EEPROM DATA, 03 REF CHARGE, 04:OIL CHARGE, 05: EA, 06:MFG LABEL
        UsedKeyWorld ---> 01: [LABEL], 02: [EEPROM], 03: ["REFRIGERANT", "GAS"], 04: ["OIL"], 05: ["UNKNOW"], 06: [MANUFACTURE]
        """
        ci_type = [""] * len(frame)
        index_concern = frame[(frame["CONCERN DEPT."] == "AAE") | (frame["CONCERN DEPT."] == "TDE") | (frame["CONCERN DEPT."] == "AAE  TDE")].index.tolist()
        context = frame.iloc[index_concern]["CHANGING POINT SUMMARY"].str.upper().tolist()
        zip_context_index = zip(index_concern, context)

        for num_index, text in zip_context_index:
            if "LABEL" in text and "MANUFACTURER'S" not in text:
                ci_type[num_index] = "01"

            if "EEPROM" in text:
                ci_type[num_index] = "02"
            
            if "GAS" in text or "REFRIGERANT" in text:
                ci_type[num_index] = "03"

            if "OIL" in text and "REFRIGERANT" not in text:
                ci_type[num_index] = "04"
            
            if "MANUFACTURER'S" in text and "LABEL" in text:
                ci_type[num_index] = "06"
            
            else:
                ci_type[num_index] = 'No'

        
        frame["CI_TYPE"] = ci_type
        return frame
    
    def checkDuplicate(self, frame:pd.DataFrame):
        """
        Check dupicate value in data frame insert the latest duplicate data into log table

        1) In this method the variable 'frame' should be concat entire year
        2) Check the CINO in data frame if there are any duplicate
        3) In the same time this method query the data for only current month store in 'ref_data'
        4) Subtract the similar data from variable 'frame' and 'ref_data' then store in 'updated_row'
        5) If length of 'updated_row' variable is not zero then update the data into database
        """
        
        ci_duplicate = frame[frame["CINO"].duplicated()]["CINO"].tolist()
        latest_updaet = frame.drop_duplicates(subset=["CINO"], keep="last")
        oldest_update = frame.drop_duplicates(subset=["CINO"], keep="first")
        ref_data = ConnectOracle(frame=latest_updaet, condition="CheckData").CheckData()
        updated_row = frame[(frame["APPMON"] == datetime.now().strftime("%Y%m")) & (~frame["CINO"].isin(ref_data))]
        
        if len(ci_duplicate) != 0:
            # print("FROM checkDupicate")
            # print(latest_updaet[latest_updaet["CINO"].isin(ci_duplicate)])
            # print('\n')
            # print(oldest_update[oldest_update["CINO"].isin(ci_duplicate)])
            ConnectOracle(frame=latest_updaet[latest_updaet["CINO"].isin(ci_duplicate)], condition="InsertLog")
            ConnectOracle(frame=latest_updaet[latest_updaet["CINO"].isin(ci_duplicate)], condition="UpdateRemarkAppmon")

        # note first time insert data into database use ---> return oldest_update or else use ---> return updated_row
        return updated_row 

        
    def cleanFrame(self, path):
        """
        Accept list of files path and for loop it one-one

        1) Find the 'REVISION RECORD' index and remomve it from xls.
        2) Remove any row that contain only Null, NaN, NaT
        3) Concat data frame to prepare to insert into database
        """
        columns_order = ["CI. No.", "DCS No.", "CONCERN DEPT.", "CHANGING POINT SUMMARY", "APPMON", "DISTRIBUTION DATE",
                "REMARK", "CI_TYPE", "CI_ENABLE", "CNAME", "UDATE", "CDATE"]
        
        columns_order_rename = ["CINO", "DCSNO", "CONCERN_DEPT", "DESCR", "APPMON", "DISTRIBUTION_DATE",
                                "REMARK", "CI_TYPE", "CI_ENABLE", "CNAME", "UDATE", "CDATE"]
        
        database_columns = dict(zip(columns_order, columns_order_rename))

        year_frame = None
        for file in enumerate(path):
            frame = pd.read_excel(file[1], skiprows=2)
            length = len(frame)
            appmon = file[1][-9::].replace(".xls", "")
            time_now = datetime.now().strftime("%d/%m/%Y")
            # print(f"{file[1]}")
            # "%d/%m/%Y %H:%M:%S"
            
            if frame["DISTRIBUTION DATE"].isna().all():
                print(f"No Distribution Date on this file{file[1]}!")

            else:
                try:

                    revission_index = frame[frame["No."] == "REVISION RECORD"].index.item()
                    drop_row = frame.iloc[revission_index:length].index
                    frame.drop(index=drop_row, inplace=True)
                    blank_row = frame[frame.isnull().all(axis=1)].index
                    frame.drop(index=blank_row, inplace=True)
                    frame.fillna({"REMARK":"No"}, inplace=True)
                    frame["APPMON"] = pd.to_datetime(appmon, format="%m-%y")
                    frame["APPMON"] = frame["APPMON"].dt.strftime("%Y%m")
                    frame["DISTRIBUTION DATE"] = pd.to_datetime(frame["DISTRIBUTION DATE"], format='mixed', dayfirst=True)
                    frame["DISTRIBUTION DATE"] = frame["DISTRIBUTION DATE"].dt.strftime("%d/%m/%Y")
                    frame["UDATE"] = time_now
                    frame["CDATE"] = time_now
                    frame["CI_ENABLE"] = "N"
                    frame["CNAME"] = platform.node()
                    frame.drop(columns=["No."], inplace=True, axis=1)

                    if type(year_frame) != type(frame):
                        year_frame = frame
                    
                    if type(year_frame) == type(frame):
                        if file[0] != 0:
                            year_frame = pd.concat([year_frame, frame], axis=0, ignore_index=True)
            
                except Exception as err:
                    
                    self.error_log.log_error(f"Error from Class:GetExcel Method:cleanFrame: {err}\nFileName: {file[1]}\n\n")
                    print("Error from Class:GetExcel Method:cleanFrame Detailed: No Revision Record")
                    self.error_log.log_error(f"Error from Class:GetExcel Method:cleanFrame: {err}\nFileName: {file[1]}\n\n")
                    print("Error from Class:GetExcel Method:cleanFrame")
                    print(f"File: {file[1]}\n")
                    print(err)


        if year_frame is not None:
            year_frame_new = self.matchCI(frame=year_frame)
            year_frame_new["CONCERN DEPT."] = year_frame_new["CONCERN DEPT."].str.split().str[-1]
            year_frame_new_order = year_frame_new.reindex(columns_order, axis=1)
            year_frame_new_order.rename(columns=database_columns, inplace=True)
            # year_frame_new_order.to_csv(f"{self.year}.csv", index=False)
            year_frame_new_order = self.checkDuplicate(year_frame_new_order)

            if (year_frame_new_order.empty == False):
                ConnectOracle(frame=year_frame_new_order.sort_index(ascending=False), condition='InsertData')
                print(f"""{self.time_now.strftime("%d/%m/%Y %H:%M:%S")}: Updated {len(year_frame_new_order)} Row""")
            else:
                print(f"""{self.time_now.strftime("%d/%m/%Y %H:%M:%S")}: No Row Updated!""")