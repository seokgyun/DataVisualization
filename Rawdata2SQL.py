# 070723
# APPLIED FAST_EXECUTEMANY 
import os 
import numpy as np
import pandas as pd    
import matplotlib.pyplot as plt
import openpyxl
import xml.etree.ElementTree as ET
import pyodbc
import glob
import datetime
# send the pre-processed data to SQL
# connect to SQL server
def connect2SQL():
    #print("Connection attempt to SQL")
    conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=ELW12DDB01;' #ELW12DDB01 US-KR5ENN0\SQLEXPRESS
                        'Database=AI_PROCESS_DEV_D;' #AI_PROCESS_DEV_D practice
                        'Trusted_Connection=yes;')
    cursor = conn.cursor()
    #print("Connected to SQL successfully")
    return conn, cursor

## insert data into parameter_D
def Insert2PARAMETER_D(conn, cursor, df_param):
    print("Insertion attempt to PARAMETER_D")
    for row in df_param.itertuples(index=False):
        try:
            cursor.execute('''
                INSERT INTO PARAMETER_D (PARAMETER_NM)
                VALUES (?)
                ''',
                row.PARAMETER_NM
            )
            conn.commit()
        except pyodbc.IntegrityError as e:
            print("IntegrityError: {}".format(e))
            conn.rollback()
    
    print("Insertion attempt to PARAMETER_D is complete")

## insert data into component_D
def Insert2COMPONENT_D(conn, cursor, df_component):
    print("Insertion attempt to component_D")
    for row in df_component.itertuples(index=False):
        #print(row)
        try:
            cursor.execute('''
                INSERT INTO component_D ( component_NM)
                VALUES ( ?)
                ''',
                row.component_NM
            )
            conn.commit()
        except pyodbc.IntegrityError as e:
            print("IntegrityError: {}".format(e))
            conn.rollback()
    print("Insertion attempt to component_D is complete")

## insert data into batch_run_D
def Insert2BATCH_RUN_D(conn, cursor, df_batchrun):
    print("Insertion attempt to BATCH_RUN_D")
    for row in df_batchrun.itertuples(index=False):
        try:
            cursor.execute('''
                INSERT INTO BATCH_RUN_D (BATCH_RUN_ID, BATCH_RUN_DATE, DESCRIPTION, PURPOSE, BACKGROUND, CONCLUSIONS, NEXT_STEPS, USER_NM, PROJECT_NM, FILE_NM)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                row.BATCH_RUN_ID,
                row.BATCH_RUN_DATE,
                row.DESCRIPTION,
                row.PURPOSE,
                row.BACKGROUND,
                row.CONCLUSIONS,
                row.NEXT_STEPS,
                row.USER_NM,
                row.PROJECT_NM,
                row.FILE_NM
            )
        except pyodbc.IntegrityError as e:
            print("IntegrityError: {}".format(e))
            conn.rollback()
    conn.commit()
    print("Insertion attempt to BATCH_RUN_D is complete")

def Insert2RESULT_F(conn, cursor, df_result):
    print("Insertion attempt to RESULT_F")
    idx = 0
    for row in df_result.itertuples(index=False):
        idx+=1
        if idx%1000 ==0:
            print("reading RESULT_F... row={}".format(idx))

        try:
            cursor.execute("""
                INSERT INTO RESULT_F (BATCH_RUN_KEY, PARAMETER_KEY, COMPONENT_KEY, DATA_SOURCE, DATA_SEQUENCE, MASTER_VAL, STRING_VAL, NUMERIC_VAL, DATETIME_VAL, DATA_TYPE, UNIT_CD, RELATIVE_TIME, RELATIVE_TIME_S, RELATIVE_TIME_HR)
                VALUES ((SELECT BATCH_RUN_KEY FROM BATCH_RUN_D WHERE BATCH_RUN_ID = '{}'), 
                (SELECT PARAMETER_KEY FROM PARAMETER_D WHERE PARAMETER_NM = '{}'),
                (SELECT isnull(COMPONENT_KEY,'-999') FROM COMPONENT_D WHERE COMPONENT_NM = '{}'),
                '{}', {}, {}, {}, {}, '{}', '{}', '{}', '{}', {}, {})
                """.format(
                row.BATCH_RUN_ID,
                row.PARAMETER_NM,
                row.COMPONENT_NM,
                row.DATA_SOURCE,
                row.DATA_SEQUENCE,
                row.MASTER_VAL,
                row.STRING_VAL,
                row.NUMERIC_VAL,
                row.DATETIME_VAL,
                row.DATA_TYPE,
                row.UNIT_CD,
                row.RELATIVE_TIME,
                row.RELATIVE_TIME_S,
                row.RELATIVE_TIME_HR)
            )
        except pyodbc.IntegrityError as e:
            print("IntegrityError: {}".format(e))
            conn.rollback()
    conn.commit()
    print("Insertion attempt to RESULT_F is complete")

def Insert2RESULT_F_EXECUTEMANY(conn, cursor, df_result):
    conn, cursor= connect2SQL()
    print("Insertion attempt to RESULT_F (EXECUTEMANY)")
    params_list = []

    for row in df_result.itertuples(index=False):
        params = (
            row.BATCH_RUN_ID,
            row.PARAMETER_NM,
            row.COMPONENT_NM,
            row.DATA_SOURCE,
            row.DATA_SEQUENCE,
            row.FACT_ROW_SEQ,
            row.MASTER_VAL,
            row.STRING_VAL,
            row.NUMERIC_VAL,
            row.DATETIME_VAL,
            row.DATA_TYPE,
            row.UNIT_CD,
            row.RELATIVE_TIME,
            row.RELATIVE_TIME_S,
            row.RELATIVE_TIME_HR
        )
        params_list.append(params)

    try:
        query = """
            INSERT INTO RESULT_F_new (BATCH_RUN_KEY, PARAMETER_KEY, COMPONENT_KEY, DATA_SOURCE, DATA_SEQUENCE, FACT_ROW_SEQ, MASTER_VAL, STRING_VAL, NUMERIC_VAL, DATETIME_VAL, DATA_TYPE, UNIT_CD, RELATIVE_TIME, RELATIVE_TIME_S, RELATIVE_TIME_HR)
            VALUES ((SELECT BATCH_RUN_KEY FROM BATCH_RUN_D WHERE BATCH_RUN_ID = ?), 
            (SELECT PARAMETER_KEY FROM PARAMETER_D WHERE PARAMETER_NM = ?),
            (SELECT COMPONENT_KEY FROM COMPONENT_D WHERE COMPONENT_NM = ?),
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.fast_executemany = True
        cursor.executemany(query, params_list)
        conn.commit()

        print("Insertion attempt to RESULT_F is complete")

    except pyodbc.IntegrityError as e:
        print("IntegrityError: {}".format(e))
        conn.rollback()
def Insert2RESULT_F_EXECUTEMANY(conn, cursor, df_result):
    conn, cursor= connect2SQL()
    print("Insertion attempt to RESULT_F")
    idx = 0
    params_list = []

    for row in df_result.itertuples(index=False):
        idx += 1
        #if idx % 1000 == 0:
        #   print("reading RESULT_F... row={}".format(idx))
        params = (
            row.BATCH_RUN_ID,
            row.PARAMETER_NM,
            row.COMPONENT_NM,
            row.DATA_SOURCE,
            row.DATA_SEQUENCE,
            row.FACT_ROW_SEQ,
            row.MASTER_VAL,
            row.STRING_VAL,
            row.NUMERIC_VAL,
            row.DATETIME_VAL,
            row.DATA_TYPE,
            row.UNIT_CD,
            row.RELATIVE_TIME,
            row.RELATIVE_TIME_S,
            row.RELATIVE_TIME_HR
        )
        params_list.append(params)

    try:
        query = """
            INSERT INTO RESULT_F_new (BATCH_RUN_KEY, PARAMETER_KEY, COMPONENT_KEY, DATA_SOURCE, DATA_SEQUENCE, FACT_ROW_SEQ, MASTER_VAL, STRING_VAL, NUMERIC_VAL, DATETIME_VAL, DATA_TYPE, UNIT_CD, RELATIVE_TIME, RELATIVE_TIME_S, RELATIVE_TIME_HR)
            VALUES ((SELECT BATCH_RUN_KEY FROM BATCH_RUN_D WHERE BATCH_RUN_ID = ?), 
            (SELECT PARAMETER_KEY FROM PARAMETER_D WHERE PARAMETER_NM = ?),
            (SELECT COMPONENT_KEY FROM COMPONENT_D WHERE COMPONENT_NM = ?),
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.fast_executemany = False
        cursor.executemany(query, params_list)
        conn.commit()

        print("Insertion attempt to RESULT_F is complete")

    except pyodbc.IntegrityError as e:
        print("IntegrityError: {}".format(e))
        conn.rollback()

def Insert2RESULT_F_FAST_EXECUTEMANY(conn, cursor, df_result):
    conn, cursor= connect2SQL()
    print("Insertion attempt to RESULT_F (FAST_EXECUTEMANY)")

    # READ BATCH_RUN_D and merge it with df_result
    query = """
            SELECT BATCH_RUN_KEY, BATCH_RUN_ID from AI_PROCESS_DEV_D.dbo.BATCH_RUN_D 
            """
    cursor.execute(query)
    result=cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    rows = [list(row) for row in result]
    mapping_batchrun = pd.DataFrame(rows, columns=columns)
    #print(mapping_batchrun.head())
    df_result_new = pd.merge(mapping_batchrun,df_result,on='BATCH_RUN_ID')

    # READ PARAMETER_D and merge it with df_result
    query = """
            SELECT PARAMETER_KEY, PARAMETER_NM from AI_PROCESS_DEV_D.dbo.PARAMETER_D 
            """
    cursor.execute(query)
    result=cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    rows = [list(row) for row in result]
    mapping_parameter = pd.DataFrame(rows, columns=columns)
    #print(mapping_batchrun.head())
    df_result_new  = pd.merge(mapping_parameter,df_result_new ,on='PARAMETER_NM')

    # READ COMPONENT_D and merge it with df_result
    query = """
            SELECT COMPONENT_KEY, COMPONENT_NM from AI_PROCESS_DEV_D.dbo.COMPONENT_D 
            """
    cursor.execute(query)
    result=cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    rows = [list(row) for row in result]
    mapping_component = pd.DataFrame(rows, columns=columns)
    #print(mapping_batchrun.head())
    df_result_new  = pd.merge(mapping_component,df_result_new ,on='COMPONENT_NM')

    params_list = []

    for row in df_result_new.itertuples(index=False):
        # Convert datetime value to desired format
        datetime_val = datetime.datetime.strptime(row.DATETIME_VAL, '%m/%d/%Y %I:%M:%S %p')
        formatted_datetime = datetime_val.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        params = (
            row.BATCH_RUN_KEY,
            row.PARAMETER_KEY,
            row.COMPONENT_KEY,
            row.DATA_SOURCE,
            row.DATA_SEQUENCE,
            row.FACT_ROW_SEQ,
            row.MASTER_VAL,
            row.STRING_VAL,
            row.NUMERIC_VAL,
            formatted_datetime,
            row.DATA_TYPE,
            row.UNIT_CD,
            row.RELATIVE_TIME,
            row.RELATIVE_TIME_S,
            row.RELATIVE_TIME_HR
        )
        params_list.append(params)

    try:
        query = """
            INSERT INTO RESULT_F_new (BATCH_RUN_KEY, PARAMETER_KEY, COMPONENT_KEY, DATA_SOURCE, DATA_SEQUENCE, FACT_ROW_SEQ, MASTER_VAL, STRING_VAL, NUMERIC_VAL, DATETIME_VAL, DATA_TYPE, UNIT_CD, RELATIVE_TIME, RELATIVE_TIME_S, RELATIVE_TIME_HR)
            VALUES (?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.fast_executemany = True
        cursor.executemany(query, params_list)
        conn.commit()

        print("Insertion attempt to RESULT_F is complete")

    except pyodbc.IntegrityError as e:
        print("IntegrityError: {}".format(e))
        conn.rollback()

def READ_SYNONYM_TABLE():
    # connect to pyexasol
    pw='c99Qkdtnseo1!'
    import pyexasol
    connection = pyexasol.connect(dsn='dwh-exasol.pd.research.corteva.com/91E262C9BC54480B6077204C444AFEE87B7EE21002FEB25CF1275F9FCD98F657:8563',
                        user='iqr474',
                        password=pw,
                        compression=True)
    sql = "SELECT * FROM IDW_VW.CP_COMPONENT_SYNONYM_D_VW"

    # read database from exasol
    result = connection.execute(sql)
    df_synonym = pd.DataFrame(result.fetchall(), columns=result.col_names)
    connection.close()

    # make the master_list
    columns_to_keep = ['COMPONENT_KEY', 'SYNONYM_VAL', 'PREFERRED_FLG']
    df_master_list = df_synonym[columns_to_keep]
    df_master_list = df_master_list.loc[df_synonym['PREFERRED_FLG']==True]
    df_master_list = df_master_list.drop('PREFERRED_FLG', axis = 1)
    df_master_list.rename(columns={'SYNONYM_VAL':'MASTER_VAL'})

    # make the synonym_list
    columns_to_keep = ['COMPONENT_KEY', 'SYNONYM_VAL']
    df_synonym_list = df_synonym[columns_to_keep]
    df_synonym_list = pd.merge(df_synonym_list, df_master_list, on = 'COMPONENT_KEY')
    df_synonym_list = df_synonym_list.rename(columns={'COMPONENT_KEY':'COMPONENT_KEY','SYNONYM_VAL_x':'SYNONYM_VAL','SYNONYM_VAL_y':'MASTER_VAL'})
    df_synonym_list = df_synonym_list.drop('COMPONENT_KEY',axis = 1)
    #df_synonym_list.head()

    return df_synonym_list

def BATCH_RUN(df,file_path):
    try: 
        value_BATCH_RUN_DATE = df.at[2, 'Abs. Time (UTC-04 : 00)']
    except KeyError:
        value_BATCH_RUN_DATE = df.at[2, 'Abs. Time (UTC-05 : 00)']

    value_DESCRIPTION=''
    value_PURPOSE=''
    value_BACKGROUND=''
    value_CONCLUSIONS=''
    value_NEXT_STEPS=''

    file_segment = file_path.split('\\')
    idx_experiment=file_segment.index('Experiments')
    value_USER_NM = file_segment[idx_experiment+1]
    value_PROJECT_NM = file_segment[idx_experiment+2]
    value_BATCH_RUN_ID = file_segment[idx_experiment+4].split('.xlsx')[0]
    value_FILE_NM=file_path

    # Sync with synonym table and put the primary compound name as the project name
    synonym = READ_SYNONYM_TABLE()
    for row in range(synonym.shape[0]):
        if synonym['SYNONYM_VAL'][row] in file_segment[idx_experiment+2]:
            value_PROJECT_NM = synonym['MASTER_VAL'][row]
            break
    
    new_rows=[]
    new_rows.append([value_BATCH_RUN_ID, value_BATCH_RUN_DATE, value_DESCRIPTION, value_PURPOSE, value_BACKGROUND, value_CONCLUSIONS, value_NEXT_STEPS, value_USER_NM, value_PROJECT_NM, value_FILE_NM])

    # Create Batch_Run_D Table
    batchrun_column=['BATCH_RUN_ID','BATCH_RUN_DATE','DESCRIPTION','PURPOSE','BACKGROUND','CONCLUSIONS','NEXT_STEPS','USER_NM', 'PROJECT_NM','FILE_NM']
    df_batchrun=pd.DataFrame(columns=batchrun_column)
    new_df = pd.DataFrame(new_rows, columns=df_batchrun.columns)
    df_batchrun = pd.concat([df_batchrun, new_df], ignore_index=True)

    # Insert to SQL
    conn, cursor= connect2SQL()
    Insert2BATCH_RUN_D(conn, cursor, df_batchrun)
    return df_batchrun


def PARAMETER_D(df):
    # 1. import the current parameter table from sql server
    # 2. compare it with the column names in this experiment. 
    # 3. if new parameter is introduced, insert the new parameter into the sql server
    # IMPORT PARAMETER TABLE FROM SQL
    conn, cursor= connect2SQL()
    query = 'SELECT PARAMETER_NM FROM PARAMETER_D order by PARAMETER_KEY'
    df_param = pd.read_sql(query,conn)

    # change the turbidity column name: iC Vision Experiment\E-178359-028\Turbidity IA -> Turbidity IA 
    for parameter in list(df.columns):
        if 'Turbidity' in parameter:
            df=df.rename(columns={parameter:'Turbidity'})
    
    # check if there are new element in parameter
    new_row = []
    for parameter in list(df.columns):
        if parameter not in list(df_param["PARAMETER_NM"]):
            if ('TotalMass' in parameter) or ('MassFlow' in parameter) or ('TotalVolume' in parameter) or ('Temperature' in parameter):
                pass
            else:
                print("new parameter: {}".format(parameter))
                new_row = [parameter]
                new_df = pd.DataFrame(new_row, columns = df_param.columns)
                # insert the new parameter
                Insert2PARAMETER_D(conn, cursor, new_df)
                # Update df_param
                df_param = pd.concat([df_param, new_df],ignore_index=True)
    conn.close()
    return df_param, df
        
def COMPONENT_D(df):
    # Get the list of component
    component_list = []
    for parameter in list(df.columns):
        if ('TotalMass' in parameter) or ('MassFlow' in parameter) or ('TotalVolume' in parameter) or ('Temperature' in parameter):
            component_list.append(parameter.split('.')[0])
    component_list = list(set(component_list))
    print("component in this experiment: {}".format(component_list))

    # IMPORT component TABLE FROM SQL
    conn, cursor= connect2SQL()
    query = 'SELECT component_NM FROM component_D order by component_KEY'
    df_component =pd.read_sql(query,conn)

    # check if there are new element in component
    for component in component_list:
        if component not in list(df_component["component_NM"]):
            new_row = [component]
            new_df = pd.DataFrame(new_row, columns = df_component.columns)
            # insert the new parameter into sql server
            Insert2COMPONENT_D(conn, cursor, new_df)
            # Update df_component
            df_component = pd.concat([df_component, new_df],ignore_index=True)
    conn.close()
    return df_component, component_list

def RESULT_F(df, df_batchrun, data_source):
    row_interval = 15
    index = 0

    # Create RESULT_F Table
    result_column = ['BATCH_RUN_ID','PARAMETER_NM','COMPONENT_NM','DATA_SOURCE','DATA_SEQUENCE','FACT_ROW_SEQ','MASTER_VAL','STRING_VAL','NUMERIC_VAL','DATETIME_VAL','DATA_TYPE','UNIT_CD','RELATIVE_TIME','RELATIVE_TIME_S','RELATIVE_TIME_HR'] # 14 cols
    df_result=pd.DataFrame(columns=result_column)

    for row in range(1, df.shape[0], row_interval):
        # time values
        try: 
            datetime_val = df.at[row, 'Abs. Time (UTC-04 : 00)']
        except KeyError:
            datetime_val = df.at[row, 'Abs. Time (UTC-05 : 00)']
        relative_time = df.at[row, 'Rel. Time']
        relative_time_s = df.at[row, 'Rel. Time (in s)']
        relative_time_hr = relative_time_s/3600.0

        new_rows = []
        current_row = df.iloc[row] # pd.Series
        for col in range(4, df.shape[1]):
            colname = current_row.index[col] # column name
            
            # batch_run_ID
            value_BATCH_RUN_ID = df_batchrun["BATCH_RUN_ID"][0]

            # parameter_NM and COMPONENT_NM
            if ('TotalMass' in colname) or ('MassFlow' in colname) or ('TotalVolume' in colname) or ('Temperature' in colname):
                value_COMPONENT_NM = colname.split('.')[0]
                value_PARAMETER_NM = 'COMPONENT.'+colname.split('.')[1]
            else:
                value_COMPONENT_NM = 'NotApplied'
                value_PARAMETER_NM = colname

            # data sequence
            index += 1
            value_DATA_SEQUENCE = index
            value_DATA_SOURCE = data_source
            value_FACT_ROW_SEQ = row

            # Values
            if pd.isna(current_row[col]):
                value_NUMERIC_VAL = -999.999
            else:
                value_NUMERIC_VAL = current_row[col]
            value_MASTER_VAL = 'Null'
            value_STRING_VAL = 'Null'
  
            value_DATA_TYPE = str(type(current_row[col])).replace('\'','')
            value_UNIT_CD = df.at[0, colname]            
            new_rows.append([value_BATCH_RUN_ID, value_PARAMETER_NM, value_COMPONENT_NM, value_DATA_SOURCE, value_DATA_SEQUENCE, value_FACT_ROW_SEQ, value_MASTER_VAL, value_STRING_VAL, value_NUMERIC_VAL, datetime_val, value_DATA_TYPE, value_UNIT_CD, relative_time, relative_time_s, relative_time_hr])

        new_df = pd.DataFrame(new_rows, columns=df_result.columns)
        df_result = pd.concat([df_result, new_df], ignore_index=True)
    return df_result

def check_turbidity(folder_path,df):
    ''' check if turbidity column is present in the i-control excel file or not.
    if yes, pass this code. if not, search for the turbidity data from the xml files under the current folder, and add it to df'''

    import xml.etree.ElementTree as ET
    from datetime import datetime, timedelta

    for column in df.columns:
        if "Turbidity" in column:
            TurbidityInExcel = True
        else:
            TurbidityInExcel = False 

    if TurbidityInExcel:
        print("Turbidity info is not in the excel file")
        print("read xml files under the current folder")
        xml_files = glob.glob(folder_path + "/*.xml")

        for xml_file in xml_files:
            print("read file: {}".format(xml_file))
            print("search if the xml file has the turbidity info")
            tree = ET.parse(xml_file)
            root = tree.getroot()
            Trends = root.find('Trends')
            for Trend in Trends:
                if  'Turbidity' in Trend.attrib['Name']:
                    print("Found the turbidity data: "+Trend.attrib['Name'])
                    turbidity_raw = Trend
                    exit_loop = True
                    TurbidityInXML = True
                    break
                else:
                    exit_loop = False
            if exit_loop:
                print("xml read complete")
                break
            else:
                print("No turbidity data in the xml file")
                TurbidityInXML = False

        if TurbidityInXML:
            # make a turbidity list and add data there
            turbidity = [np.nan for i in range(len(df))]
            turbidity[0] = Trend.attrib['Unit']
            for row in range(len(turbidity_raw)):
                # convert the time string into total seconds
                time_string = turbidity_raw[row].attrib['T'] # '%H:%M:%S'
                # Parse the time string into a hour, min, sec
                hours, minutes, seconds = map(int, time_string.split(':'))
                # Calculate the total number of seconds
                total_seconds = timedelta(hours=hours, minutes=minutes, seconds=seconds).total_seconds()
                # Convert the total seconds to an integer
                time_seconds = int(total_seconds)

                index = int(time_seconds/2)+1 # data stored every 2 seconds
                turbidity[index] = float(turbidity_raw[row].text)

            # add a new column to df
            df["Turbidty_from_xml"] = turbidity
            print("turbidity insertion into df is complete")
    return df  

def BATCH_COMPONENT_ASSOC(df_batchrun,component_list):
    
    df_batch_component_assoc=pd.DataFrame()
    for component in component_list:
        # make the batch_component_assoc table
        new_row = {'BATCH_RUN_ID': df_batchrun["BATCH_RUN_ID"], 'COMPONENT_NM': component}
        new_df = pd.DataFrame(new_row, columns = ['BATCH_RUN_ID','COMPONENT_NM'])
        df_batch_component_assoc = pd.concat([df_batch_component_assoc, new_df],ignore_index=True)

    print(df_batch_component_assoc)

    # IMPORT component TABLE FROM SQL
    conn, cursor= connect2SQL()
    print("Insertion attempt to BATCH_COMPONENT_ASSOC_D")
    for row in df_batch_component_assoc.itertuples(index=False):
        print(row)
        try:
            cursor.execute("""
                INSERT INTO BATCH_COMPONENT_ASSOC_D (BATCH_RUN_KEY, COMPONENT_KEY)
                VALUES ((SELECT BATCH_RUN_KEY FROM BATCH_RUN_D WHERE BATCH_RUN_ID='{}'),
                (SELECT COMPONENT_KEY FROM COMPONENT_D WHERE COMPONENT_NM='{}'))
                """.format(
                row.BATCH_RUN_ID,
                row.COMPONENT_NM
                )
            )
            
            conn.commit()
        except pyodbc.IntegrityError as e:
            print("IntegrityError: {}".format(e))
            conn.rollback()
    print("Insertion attempt to BATCH_COMPONENT_ASSOC_D is complete")    
    conn.close()


# MAIN
IMPORT        = True
PREPROCESSING = True
PUSH2SQL      = True
PXRD          = True

start = 1
end = 2
for number in range(start, end):
    # xlsx file search under a folder path
    
    folder_path = r'\\elw16picdc01\Experiments\paul.larsen\XR-521 2020\ENBK-176325-00'+str(number) #\\elw16picdc01\Experiments\paul.larsen\XR-521 2021\E-176325-070; \\elw16picdc01\Experiments\johanna.strul\XDE-521\E-178359-023
    print("search excel files under the folder: {}".format(folder_path))
    # Use the glob function to search for .xlsx files in the folder
    xlsx_files = glob.glob(folder_path + "/*.xlsx")

    for file_path in xlsx_files:
        if IMPORT:
            # import data
            print("import start")
            print("file path: {}".format(file_path))
            df = pd.read_excel(file_path)
            data_source = 'i-Control' # need to modify later
            
            # check the presence of turbidity in i-control excel file
            df = check_turbidity(folder_path,df)
            print("import complete")

        if PREPROCESSING:
            print("PREPROCESSING start")
            df_component, component_list = COMPONENT_D(df) # update COMPONENT_D
            df_param, df = PARAMETER_D(df) # update PARAMETER_D
            df_batchrun= BATCH_RUN(df,file_path) # update BATCH_RUN_D
            BATCH_COMPONENT_ASSOC(df_batchrun,component_list)
            df_result =RESULT_F(df, df_batchrun, data_source)
            print("PREPROCESSING complete")

        if PUSH2SQL:
            conn, cursor= connect2SQL()
            Insert2RESULT_F_EXECUTEMANY(conn, cursor, df_result)
            conn.close()
            print("Disconnected from SQL")

        #if PXRD:

        