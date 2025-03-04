import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from ErrorLog import ErrorLogFile
from datetime import datetime

class ConnectOracle:
    def __init__(self, frame, condition="InsertData"):

        self.frame  = frame
        self.username = "se"
        self.passworld = "isse"
        self.hostname = "ditdbdev"
        self.port = "1521"
        self.service_name = "ditlndev"
        
        try:
            self.min_date = self.frame['APPLY_DATE'].agg(['min']).item()
            self.max_date = self.frame['APPLY_DATE'].agg(['max']).item()
            self.min_date_mt005_format = self.min_date[3:]
            self.max_date_mt005_format = self.min_date[3:]
            
        except Exception as err:
            pass

        self.time_now = datetime.now()
        self.error_log = ErrorLogFile()

        if condition == "CheckData":
            self.name_database = 'dcs_ci_mt'

        if condition == 'CheckModel':
            self.name_database = 'dcs_ci_model'

        if condition == 'CheckModelCI':
            self.name_database = 'dcs_ci_dt'
        
        if condition == "InsertData":
            self.name_database = 'dcs_ci_mt'
            self.InsertData()
        
        if condition == "InsertLog":
            self.name_database = 'dcs_ci_mt_log'
            self.InsertDataLog()
        
        if condition == "UpdateRemarkAppmon":
            self.name_database = 'dcs_ci_mt'
            self.UpdateRemarkAppmon()

        if condition == "InsertModel":
            self.name_database = 'dcs_ci_model'
            self.InsertModel()

        if condition == 'InsertModelCI':
            self.name_database = 'dcs_ci_dt'
            self.InsertModelCI()

        if condition == "UpdateModelCI":
            self.name_database = 'dcs_ci_dt'
            self.UpdateModelCI()
        
        if condition == 'UpdateInchargeCI':
            self.name_database = 'dsc_ci_dt'
            self.UpdateInchargeCI()


    def CheckData(self):
        try:
            engine_check = create_engine(f"oracle+cx_oracle://{self.username}:{self.passworld}@{self.hostname}:{self.port}/{self.service_name}", isolation_level="AUTOCOMMIT")
            query=f"""
                    SELECT /*+ NO_RESULT_CACHE */ * FROM {self.name_database} WHERE APPMON = '{datetime.now().strftime('%Y%m')}'
                   """
            
            df = pd.read_sql(query, con=engine_check)
            return df['cino'].tolist()
            

        except Exception as err:
            self.error_log.log_error(f"Error from Class:ConnectOracle Method:CheckData: {err}\n\n")
            print(err)
        
        finally:
            engine_check.dispose()
    
    def CheckModel(self):
        try:
            engine_check_model = create_engine(f"oracle+cx_oracle://{self.username}:{self.passworld}@{self.hostname}:{self.port}/{self.service_name}", isolation_level="AUTOCOMMIT")
            query = f"""
                    SELECT /*+ NO_RESULT_CACHE */ * FROM {self.name_database} WHERE APPMON = '{datetime.now().strftime('%Y%m')}'
                    """
            df = pd.read_sql(query, con=engine_check_model)
            return df
            
        except Exception as err:
            self.error_log.log_error(f"Error from Class:ConnectOracle Method:CheckModel: {err}\n\n")
            print(err)
        
        finally:
            engine_check_model.dispose()

    def CheckModelCI(self):
        try:
            engine_check_model_ci = create_engine(f"oracle+cx_oracle://{self.username}:{self.passworld}@{self.hostname}:{self.port}/{self.service_name}", isolation_level="AUTOCOMMIT")
            query = f"""
                    SELECT /*+ NO_RESULT_CACHE */ * FROM {self.name_database} WHERE APPLY_DATE = '{datetime.now().strftime('%Y%m')}'
                    """
            df = pd.read_sql(query, con=engine_check_model_ci)
            return df
        
        except Exception as err:
            self.error_log.log_error(f"Error from Class:ConnectOracle Method:CheckModelCI: {err}\n\n")
            print(err)
        
        finally:
            engine_check_model_ci.dispose()
        
    def InsertData(self):
        try:
            engine = create_engine(f"oracle://{self.username}:{self.passworld}@{self.hostname}:{self.port}/{self.service_name}", isolation_level="AUTOCOMMIT")
            self.frame.to_sql(self.name_database, con=engine, if_exists='append', index=False, method=None, schema="SE")
            print(f"Insert Successfull: {self.name_database}")
            
        except Exception as err:
            self.error_log.log_error(f"Error from Class:ConnectOracle Method:InsertData: {err}\n\n")
            print("Error from Class:ConnectOracle Method:InsertData\n")
            print(str(err)[:100])
            

        finally:
            engine.dispose()
    
    def InsertDataLog(self):

        try:
            dsn = cx_Oracle.makedsn(self.hostname, self.port, self.service_name)
            connection = cx_Oracle.connect(user=self.username, password=self.passworld, dsn=dsn)
            cursor = connection.cursor()
            merge_query_log = f"""
                                MERGE INTO {self.name_database} target
                                USING (
                                    SELECT :1 AS CINO,
                                        :2 AS DCSNO,
                                        :3 AS CONCERN_DEPT,
                                        :4 AS DESCR,
                                        :5 AS APPMON,
                                        :6 AS DISTRIBUTION_DATE,
                                        :7 AS REMARK,
                                        :8 AS CI_TYPE,
                                        :9 AS CI_ENABLE,
                                        :10 AS CNAME,
                                        :11 AS UDATE,
                                        :12 AS CDATE
                                    FROM dual
                                ) source
                                ON (target.CINO = source.CINO)
                                WHEN MATCHED THEN
                                    UPDATE SET target.DCSNO = source.DCSNO,
                                            target.CONCERN_DEPT = source.CONCERN_DEPT,
                                            target.DESCR = source.DESCR,
                                            target.APPMON = source.APPMON,
                                            target.DISTRIBUTION_DATE = source.DISTRIBUTION_DATE,
                                            target.REMARK = source.REMARK,
                                            target.CI_TYPE = source.CI_TYPE,
                                            target.CI_ENABLE = source.CI_ENABLE,
                                            target.CNAME = source.CNAME,
                                            target.UDATE = source.UDATE
                                WHEN NOT MATCHED THEN
                                    INSERT (CINO, DCSNO, CONCERN_DEPT, DESCR, APPMON, DISTRIBUTION_DATE, REMARK, CI_TYPE, CI_ENABLE, CNAME, UDATE, CDATE)
                                    VALUES (source.CINO, source.DCSNO, source.CONCERN_DEPT, source.DESCR, source.APPMON, source.DISTRIBUTION_DATE, source.REMARK, 
                                    source.CI_TYPE, source.CI_ENABLE, source.CNAME, source.UDATE, source.CDATE)
                                """
            for row in self.frame.itertuples(index=False):
                cursor.execute(merge_query_log, row)
            connection.commit()
            print(f"""{self.time_now.strftime("%d/%m/%Y %H:%M:%S")}: Insert Log Compleate!""")
            
        except Exception as err:
            self.error_log.log_error(f"Error from Class:ConnectOracle Method:InsertDataLog: {err}\n\n")
            print("Error from Class:ConnectOracle Method:InsertDataLog\n")
            print(err)

        finally:
            cursor.close()
            connection.close()


    def UpdateRemarkAppmon(self):
        try:
            dsn = cx_Oracle.makedsn(self.hostname, self.port, self.service_name)
            connection = cx_Oracle.connect(user=self.username, password=self.passworld, dsn=dsn)
            cursor = connection.cursor()
            update_remark_query = f"""
                                    MERGE INTO {self.name_database} target
                                    USING (
                                        SELECT :1 AS CINO,
                                        :2 AS DCSNO,
                                        :3 AS CONCERN_DEPT,
                                        :4 AS DESCR,
                                        :5 AS APPMON,
                                        :6 AS DISTRIBUTION_DATE,
                                        :7 AS REMARK,
                                        :8 AS CI_TYPE,
                                        :9 AS CI_ENABLE,
                                        :10 AS CNAME,
                                        :11 AS UDATE,
                                        :12 AS CDATE
                                    FROM dual
                                    ) source
                                    ON (target.cino = source.cino) 
                                    WHEN MATCHED THEN
                                        UPDATE SET target.remark = target.appmon ||chr(10)|| target.remark,
                                                   target.appmon = source.appmon,
                                                   target.udate = '{datetime.now().strftime("%d/%m/%Y")}'
                                        WHERE target.appmon != source.appmon
                                                   
                                    """
            for row in self.frame.itertuples(index=False):
                cursor.execute(update_remark_query, row)
            connection.commit()
            print(f"""{self.time_now.strftime("%d/%m/%Y %H:%M:%S")}: UpdateRemark Compleate!""")

        except Exception as err:
            self.error_log.log_error(f"Error from Class:ConnectOracle Method:UpdateRemark: {err}\n\n")
            print("Error from Class:ConnectOracle Method:UpdateRemark\n")
            print(err)
            
        finally:
            cursor.close()
            connection.close()

    def InsertModel(self):
        try:
            dsn = cx_Oracle.makedsn(self.hostname, self.port, self.service_name)
            connection = cx_Oracle.connect(user=self.username, password=self.passworld, dsn=dsn)
            cursor = connection.cursor()
            insert_model_query = f"""
                                INSERT INTO DCS_CI_MODEL (CINO, CI_STS, MODEL, APPMON, OPERATOR_NAME, UDATE, CDATE, CNAME)
                                VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
                                 """
            for row in self.frame.itertuples(index=False):
                cursor.execute(insert_model_query, row)
            connection.commit()
            print(f"""{self.time_now.strftime("%d/%m/%Y %H:%M:%S")}: UpdateModel Compleate!""")

        except Exception as err:
            self.error_log.log_error(f"Error from Class:ConnectOracle Method:UpdateCISheet: {err}\n\n")
            print("Error from Class:ConnectOracle Method:UpdateCISheet\n")
            print(err)
        finally:
            connection.close()

    def InsertModelCI(self):
        try:
            dsn = cx_Oracle.makedsn(self.hostname, self.port, self.service_name)
            connection = cx_Oracle.connect(user=self.username, password=self.passworld, dsn=dsn)
            cursor = connection.cursor()
            insert_model_ci_query = f"""
                                INSERT INTO DCS_CI_DT (CINO, CI_STS, MODEL, LINE, SERIAL, APPLY_DATE, CENTER_IDCODE, INCHARGE_IDCODE, CNAME,
                                UDATE, CDATE, INCHARGE_NAME)
                                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)
                                 """
            for row in self.frame.itertuples(index=False):
                cursor.execute(insert_model_ci_query, row)
            connection.commit()
            print(f"""{self.time_now.strftime("%d/%m/%Y %H:%M:%S")}: InsertModelCI Compleate!""")

        except Exception as err:
            print(row)
            self.error_log.log_error(f"Error from Class:ConnectOracle Method:InsertModelCI: {err}\n\n")
            print("Error from Class:ConnectOracle Method:UpdateCISheet\n")
            print(err)

        finally:
            cursor.close()
            connection.close()

    def UpdateModelCI(self):
        try:
            dsn = cx_Oracle.makedsn(self.hostname, self.port, self.service_name)
            connection = cx_Oracle.connect(user=self.username, password=self.passworld, dsn=dsn)
            cursor = connection.cursor()
            select_query = f"""
                                WITH ranked_rows AS (
                                    SELECT
                                        TRIM(MODEL) AS MODEL,
                                        LINE,
                                        SERIAL,
                                        SRDATE,
                                        ROW_NUMBER() OVER (
                                            PARTITION BY TRIM(MODEL)
                                            ORDER BY SRDATE ASC
                                        ) AS rn
                                    FROM
                                        mt005 WHERE TO_CHAR(SRDATE,'mm/yy') BETWEEN '{self.min_date_mt005_format}' AND '{self.max_date_mt005_format}'
                                ),
                                filtered_rows AS (
                                    SELECT
                                        MODEL,
                                        LINE,
                                        SERIAL,
                                        SRDATE
                                    FROM
                                        ranked_rows
                                    WHERE
                                        rn = 1
                                )
                                SELECT
                                    b.CINO,
                                    b.CI_STS,
                                    TRIM(b.MODEL) AS MODEL,
                                    a.LINE,
                                    a.SERIAL,
                                    c.DCSNO
                                FROM
                                    dcs_ci_dt b
                                JOIN
                                    filtered_rows a
                                ON
                                    TRIM(b.MODEL) = a.MODEL
                                JOIN
                                    dcs_ci_mt c
                                ON
                                    b.CINO = c.CINO
                                ORDER BY
                                    b.CINO
                            """
            
            merge_query = f"""
                            MERGE INTO dcs_ci_dt dt
                            USING (
                                WITH ranked_rows AS (
                                   SELECT
                                        TRIM(MODEL) AS MODEL,
                                        LINE,
                                        SERIAL,
                                        SRDATE,
                                        ROW_NUMBER() OVER (
                                            PARTITION BY TRIM(MODEL)
                                            ORDER BY SRDATE ASC
                                        ) AS rn
                                    FROM
                                        mt005 WHERE TO_CHAR(SRDATE,'mm/yy') BETWEEN '{self.min_date_mt005_format}' AND '{self.max_date_mt005_format}'
                                ),
                                filtered_rows AS (
                                    SELECT
                                        MODEL,
                                        LINE,
                                        SERIAL
                                    FROM
                                        ranked_rows
                                    WHERE
                                        rn = 1
                                )
                                SELECT
                                    b.CINO,
                                    b.MODEL,
                                    a.LINE,
                                    a.SERIAL
                                FROM
                                    dcs_ci_dt b
                                JOIN
                                    filtered_rows a
                                ON
                                    TRIM(b.MODEL) = a.MODEL
                            ) src
                            ON (dt.CINO = src.CINO AND dt.MODEL = src.MODEL)
                            WHEN MATCHED THEN
                                UPDATE SET
                                    dt.LINE = src.LINE,
                                    dt.SERIAL = src.SERIAL
                            """
            # Match the model and line 3
            cursor.execute(select_query)
            cursor.execute(merge_query)
            connection.commit()
            print(f"""{self.time_now.strftime("%d/%m/%Y %H:%M:%S")}: UpdateModelCI Compleate!""")

        except Exception as err:
            self.error_log.log_error(f"Error from Class:ConnectOracle Method:UpdateModelCI: {err}\n\n")
            print("Error from Class:ConnectOracle Method:UpdateModelCI\n")
            print(err)

        finally:
            cursor.close()
            connection.close()

    def UpdateInchargeCI(self):
        try:
            dsn = cx_Oracle.makedsn(self.hostname, self.port, self.service_name)
            connection = cx_Oracle.connect(user=self.username, password=self.passworld, dsn=dsn)
            cursor = connection.cursor()
            select_query = f"""
                                MERGE INTO dcs_ci_dt dt
                                USING (
                                    WITH ranked_rows AS (
                                        SELECT 
                                            TRIM(MODEL) AS MODEL,
                                            LINE,
                                            SERIAL,
                                            SRDATE,
                                            ROW_NUMBER() OVER (
                                                PARTITION BY TRIM(MODEL) 
                                                ORDER BY SRDATE ASC     
                                            ) AS rn
                                        FROM 
                                            mt005 WHERE TO_CHAR(SRDATE,'mm/yy') BETWEEN '{self.min_date_mt005_format}' AND '{self.max_date_mt005_format}'
                                    ),
                                    filtered_rows AS (
                                        SELECT 
                                            MODEL,
                                            LINE,
                                            SERIAL
                                        FROM 
                                            ranked_rows
                                        WHERE 
                                            rn = 1 -- Select only the first row per MODEL
                                    ),
                                    joined_data AS (
                                        SELECT 
                                            b.CINO,
                                            b.MODEL,
                                            a.LINE,
                                            a.SERIAL,
                                            mt.CI_TYPE,
                                            NVL(ci.INCHARGE_IDCODE, 'No') AS INCHARGE_IDCODE,
                                            NVL(e.NAME, 'No') AS INCHARGE_NAME
                                        FROM 
                                            dcs_ci_dt b
                                        JOIN 
                                            filtered_rows a
                                        ON 
                                            TRIM(b.MODEL) = a.MODEL
                                        JOIN 
                                            dcs_ci_mt mt
                                        ON 
                                            b.CINO = mt.CINO 
                                        LEFT JOIN 
                                            dcs_ci_incharge ci
                                        ON 
                                            a.LINE = ci.LINE AND mt.CI_TYPE = ci.CI_TYPE 
                                        LEFT JOIN 
                                            emmt e
                                        ON 
                                            ci.INCHARGE_IDCODE = e.CODE 

                                        WHERE b.APPLY_DATE BETWEEN '{self.min_date}' AND '{self.max_date}'
                                    )
                                    SELECT 
                                        CINO,
                                        MODEL,
                                        LINE,
                                        SERIAL,
                                        INCHARGE_IDCODE,
                                        INCHARGE_NAME
                                    FROM 
                                        joined_data
                                ) src
                                ON (dt.CINO = src.CINO AND dt.MODEL = src.MODEL)
                                WHEN MATCHED THEN
                                    UPDATE SET 
                                        dt.INCHARGE_IDCODE = src.INCHARGE_IDCODE,
                                        dt.INCHARGE_NAME = src.INCHARGE_NAME                           

                                                            """
            # TEST 3
            print(select_query)
            cursor.execute(select_query)
            connection.commit()
            print(f"""{self.time_now.strftime("%d/%m/%Y %H:%M:%S")}: UpdateInchargeCI Compleate!""")

        except Exception as err:
            self.error_log.log_error(f"Error from Class:ConnectOracle Method:UpdateInchargeCI: {err}\n\n")
            print("Error from Class:ConnectOracle Method:UpdateInchargeCI\n")
            print(err)
    
        finally:
            cursor.close()
            connection.close()
       


        