import streamlit as st
import pandas as pd
import numpy as np
import re
import xlrd
from datetime import time, datetime as dt,timedelta
from config import header_conversion,connection


# ------------------quality_header---------------------------------------#

def head_converter(df, header_conversion):
    columns = df.columns
    for column in columns:
        if column in header_conversion:
            df.rename(columns={column: header_conversion[str(column)]}, inplace=True)
    return df

        


# ## ---------------------------------------------------------------------------------------------------------------------------------##


def excel_files_validation(function, date, option):
    '''
The function validation1 is defined, which takes three arguments:

fun: a function to be called if certain conditions are met
selected_date: a date string in the format 'YYYY-MM-DD'
option: a string representing the name of a database table
The function opens a connection to a PostgreSQL database using the psycopg2 library. The connection parameters (e.g. host, port, database name, username, and password) are hardcoded into the function.

The function creates a cursor object using the connection object.
The function constructs a SQL query string s using f-strings, which selects the count of all rows in the database table specified by the option argument where the upload_date column is equal to the selected_date argument.
The function executes the SQL query using the cursor's execute method and fetches the result using the cursor's fetchone method, storing the result in the variable count.
The function constructs a second SQL query string s1 using f-strings, which selects the count of all rows in the database table specified by option with the suffix _error where the upload_date column is equal to the selected_date argument.
The function executes the second SQL query using the cursor's execute method and fetches the result using the cursor's fetchone method, storing the result in the variable count1.
The function checks if both count and count1 are equal to 0. If they are, it calls the function fun with the selected_date, option, and connection arguments.
If count and count1 are not both equal to 0, the function constructs two more SQL queries using f-strings:
s, which deletes all rows from the database table specified by option where the upload_date column is equal to the selected_date argument.
s1, which truncates (i.e. empties) the database table specified by option with the suffix _error.
The function executes the s query using the cursor's execute method, commits the changes to the database using the connection object's commit method, then executes the s1 query using the cursor's execute method and again commits the changes to the database.
Finally, the function calls the fun function with the selected_date, option, and connection arguments.'''
    # s = f'''select count(*) from {option} where date='{date}';'''
    # cursor.execute(s)
    # count_table = cursor.fetchone()# this count is for table count.
    s1 = f'''select count(*) from {option}_error;'''
    cursor.execute(s1)
    count_error_table = cursor.fetchone()[0]# this is count of error table
    if count_error_table == 0:
        function( date,option, connection)
    else:
        # s = f"delete from {option} where upload_date='{selected_date}';"
        s1 = f"truncate table {option}_error;"
        # cursor.execute(s)
        # connection.commit()
        cursor.execute(s1)
        connection.commit()
        # option+'()'
        function( date,option, connection)
# #-----------------------------------Global variables------------------------------------------#

cursor = connection.cursor() #connection for python to db for query

def datetime_converter(date):#this function is for convert serial number  it into timestamp
            return str(xlrd.xldate_as_datetime(date, 0).isoformat())

def date_convert(date):#this function is for convert serial number into date 
    return str(xlrd.xldate_as_datetime(date, 0).date())

def time_convert(date):#this function is for convert serial numeber into time 
    return str(xlrd.xldate_as_datetime(date, 0).time())

# def convert_to_time_string(time_float):#this fucntion is using in FTR file for getting time column format 
#     time_tuple = xlrd.xldate_as_tuple(time_float, 0)
#     time_obj = time(*time_tuple[3:])
#     time_str = time_obj.strftime('%I:%M:%S %p')
    # return time_str

def convert_to_time_string(time_float):
    if not np.isnan(time_float):
        time_tuple = xlrd.xldate_as_tuple(time_float, 0)
        time_obj = time(*time_tuple[3:])
        time_str = time_obj.strftime('%I:%M:%S %p')
        return time_str
    else:
        return ''


# def convert_date_string(date_str):# this is using in datawisepkt file for converting date column in required format

#     date_str = date_str.replace('th', '')
#     date_obj = pd.to_datetime(date_str.strip(), format='%d - %b- %Y')
#     date_str = date_obj.strftime('%Y-%m-%d')
#     return date_str

def convert_date_string(date):
    if isinstance(date, pd.Timestamp):
        date_str = str(date)
    else:
        date_str = date

    if 'th' in date_str:
        date_str = date_str.replace('th', '')
        date_obj = pd.to_datetime(date_str.strip(), format='%d - %b- %Y')
        date_str = date_obj.strftime('%Y-%m-%d')
    else:
        date_obj = pd.to_datetime(date_str, format='%Y-%m-%d')
        date_str = date_obj.strftime('%Y-%m-%d')
    return date_str

def fetch_data_function(query):##this is for status table usage fetchone for the count of normal and error table 
    cursor.execute(query)
    data=cursor.fetchone()[0]
    return data

# def update_error_table(option, column_name,value): 
#                         '''Here this update error function is for to upload if is there any error occurs while uploading the data into database.
#                           '''
#                         cursor.execute("ROLLBACK")
#                         query = f'''INSERT INTO {option}_error (%s) VALUES %s; ''' % (column_name, value)
#                         cursor.execute(query)
#                         connection.commit()

# def update_table(option, column_name, value):
#                         '''This function is for update the table in staging Database if the data is in correct format.

#                         '''
#                         cursor.execute("ROLLBACK")
#                         sql = f'''INSERT INTO {option} (%s) VALUES %s; ''' % (
#                             column_name, value)
#                         cursor.execute(sql)
#                         connection.commit()
# def reupload_table(option,date):
#                         '''This function is for update the table in staging Database if the data is in correct format.

#                         '''
#                         cursor.execute("ROLLBACK")
#                         sql =  f"delete from {option} where date='{date}';"
#                         cursor.execute(sql)
#                         connection.commit()
def sql_executor(sql):         
    print(sql)
    cursor.execute(sql)
    connection.commit()

def update_table(option, column_name, value):
    '''This function is for updating the table in the staging database if the data is in the correct format.'''
    try: 
        # cursor.execute("SAVEPOINT my_savepoint")
        sql = f'''INSERT INTO {option} (%s) VALUES %s; ''' % (column_name, value)
        sql_executor(sql)
        # If everything is successful, you can release the savepoint
        # cursor.execute("RELEASE my_savepoint")
    except Exception as e:
        st.write(e)
        cursor.execute("ROLLBACK")
        connection.commit()
        sql = f'''INSERT INTO {option}_error (%s) VALUES %s; ''' % (column_name, value)
        sql_executor(sql)


       
def reupload_table(option,date):
                        '''This function is for update the table in staging Database if the data is in correct format.

                        '''
                        try:
                            sql =  f"delete from {option} where date='{date}';"
                            cursor.execute(sql)
                            connection.commit()
                        except Exception as e:
                            st.write(e)
                            cursor.execute("ROLLBACK")
                            connection.commit()


# ----------------"Quality"--------------------------
quality_freezed_columns=['Agent name', 'Agent Name2', 'Agent ID', 'Customer_Number', 'Audit Date', 'Call Duration', 'Claim Type', 'Caller type ', 'Claim', 'Type of Query', 'Query_Type_Level', 'Sub_Type_Query_Level', 'Fatal/Non Fatal', 'Overall Score', 'With Fatal Score', 'Without Fatal Score', 'ACPT', 'Level I', 'Level II', 'Campaign', 'Call Date', 'QA name', 'TL name', 'Location', 'Observation', 'AOI', 'Fatal Reason', 'Did the agent follow Opening Verbiage | Introduce self & FHPL', 'Did the agent follow Opening Verbiage | Introduce self & FHPL.1', 'Was Greeting given within 3 of receiving the call', 'Was Greeting  given within 3 of receiving the call', 'Captured data required to help customer - NAme,Phone,Email (Not mandate), Dependent NAme', 'Captured data required to help customer - NAme,Phone,Email (Not mandate), Dependent NAme.1', 'Conduct a quick scan of the application in the CRM - Refer previous case history ', 'Conduct a quick scan of the  application in the CRM - Refer previous case history', 'Asked question "How may I help you today"', 'Asked question "How may I help you today".1', 'Is agent Listening attentively to everything what the customer said without interrupting', 'Is agent Listening attentively to everything what the customer said without interrupting ', 'Probe with relevant & precise questions to reach to the exact customer issue without overdoing it.', 'Probe with relevant & precise questions to reach to the exact customer issue without overdoing it..1', 'Agent paraphrased to understand requirement/query (wherever required)', 'Agent paraphrased to understand requirement/query (wherever required).1', 'Agent display willingness to help, using "Please & Thank you" when required', 'Agent display willingness to help, using "Please & Thank you" when required.1', 'Did agent adhere to the hold protocol and "Dead air" should be within threshold (>= 15 Sec)', 'Did agent adhere to the hold protocol and "Dead air" should be within threshold (>= 15 Sec).1', 'Agent Used simple, honest language. Avoid jargons.', 'Agent Used simple, honest language. Avoid jargons..1', 'Be optimistic & Enthusiastic - Agent did not sound robotic , sounded confident with accurate rate of speech', 'Be optimistic & Enthusiastic - Agent did  not sound robotic , sounded confident with accurate rate of speech', 'Believe/trust our customers and build a rapport with the customer', 'Believe/trust our customers and build a rapport with the customer.1', 'Agent used grammatically correct sentences , switched to regioNAl language when needed', 'Agent used grammatically correct sentences , switched to regioNAl language when needed.1', 'Give a heartfelt apology or genuine empathy for what has happened (wherever required).Congratulate for New born baby, new wedded spouse | In event of death/accident "Sorry to hear about this"', 'Give a heartfelt apology or genuine empathy for what has happened (wherever required).Congratulate for New born baby, new wedded spouse | In event of death/accident "Sorry to hear about this".1', 'Clarity of speech while delivering the information to the customer', 'Clarity of speech while delivering the information to the customer ', "Commitment to own the customer's issue and provide the appropriate solution.\xa0", "Commitment to own the customer's issue and provide the appropriate solution.\xa0.1", 'Customers query was addressed with positive approach and confidence', 'Customers query was addressed with positive approach and confidence.1', 'All Query of the customer were addressed - Provide correct TAT/OTRS number where needed ', 'All Query of the customer were addressed - Provide correct  TAT/OTRS number where needed', 'Agent set expectation & explain next steps to ensure customer is not confused', 'Agent set expectation & explain next steps to ensure customer is not confused.1', 'Did agent utilize all recourses - CRM/Knowledge base/Supervisor - All tools NAvigated effectively', 'Did agent utilize all recourses - CRM/Knowledge base/Supervisor - All tools NAvigated effectively.1', 'Proactively look for the most appropriate solution to prevent future calls.', 'Proactively look for the most appropriate solution to prevent future calls..1', 'Provided self-help options, Tool free# & Email ID wherever required', 'Provided self-help options, Tool free# & Email ID wherever required.1', 'Identified interNAl/exterNAl escalation triggered and handled it efficiently (Involve supervisor where needed)', 'Identified interNAl/exterNAl escalation triggered and handled it efficiently (Involve supervisor where needed).1', 'Agent tagged and documented the call', 'Agent tagged and documented the call ', 'Summarize the resolution provided on the call', 'Summarize the resolution provided on the call ', 'Validated all issue were addressed "Is there anything else I can assist you with"', 'Validated all issue were addressed "Is there anything else I can assist you with".1', 'Educated the customer about Survey IVR for all FTR calls', 'Educated the customer about Survey IVR for all FTR calls.1', 'Used the closing verbiage and thank the customer for his time', 'Used the closing verbiage and thank the customer for his time.1', 'Rude behavior observed / abusing customer', 'Rude behavior observed / abusing customer.1', 'Incorrect/Incomplete info provided on the call - Incorrect Resolution', 'Incorrect/Incomplete info provided on the call - Incorrect Resolution.1', 'Forced Closure/Call disconnection/Instigated call abandoned due to long hold & dead air', 'Forced Closure/Call disconnection/Instigated call abandoned due to long hold & dead air.1', 'Over persoNAlization (Flirting / Sharing PersoNAl Information with Customer)', 'Over persoNAlization (Flirting / Sharing PersoNAl Information with Customer).1', '(Security check )  policy holder DOB, Registered MB,Email, Verify All Dependent NAmes', '(Security check )  policy holder DOB, Registered MB,Email, Verify All Dependent NAmes.1', 'Tagging Guidelines followed', 'Tagging Guidelines followed.1', 'DT Followed on all calls', 'DT Followed on all calls.1', 'FTR Transferred to Survey IVR', 'FTR Transferred to Survey IVR.1', 'Escalation process followed', 'Escalation process followed.1', 'Scorable', 'Scored', 'Fatal', 'Week', 'Tenure', 'Month']


def quality(date, option, connection):
    '''
The input parameters are:

selected_date: a string that represents the date for which the data will be uploaded to the database.
option: a string that represents the name of the table in the database where the data will be uploaded.
connection: a connection object that is used to connect to a PostgreSQL database.
The function then prompts the user to upload a file using st.file_uploader from the Streamlit library. 
If a file is uploaded, the function reads the file using pd.read_excel and applies some transformations to the data. Specifically,
 it replaces spaces and forward slashes in column names with underscores, converts certain columns to datetime objects, 
 replaces single quotes in the AOI and Observation columns with backticks, and fills any missing values with empty strings.
The function then sets up a cursor to interact with the database using the psycopg2 library.
If the user clicks an 'Upload' button, the function iterates through the rows of the data and constructs an SQL query to insert each row into the specified database table. 
If there is an error during insertion, the function catches the exception, rolls back the changes, inserts the row into an _error table instead, and continues with the next row.
Finally, the function calls a status_table function to display a summary of the data that was uploaded. 
The output of the function is not explicitly stated in the code, but it likely does not return anything.'''
    uploaded_file = st.file_uploader("Choose CSV or Excel", type=['xlsx'])
    if uploaded_file is not None:
        # Can be used wherever a "file-like" object is accepted:
        file_name = uploaded_file.name
        file_check = file_name[-4:]
        if file_check == 'xlsx':
            df1 = pd.read_excel(uploaded_file, engine='openpyxl',
                                sheet_name='Audit Dump', header=[5])
        else:
            st.write('It is a Wrong type file')
        df2 = df1[quality_freezed_columns].copy()
        st.write(df2.head())
        head_converter(df2,header_conversion)

        df2.columns = df2.columns.str.replace(" ", "_")
        df2.columns = df2.columns.str.replace("/", "_")
        df2 = df2.applymap(lambda x: x.replace('\xa0', '') if isinstance(x, str) else x)
        df2['Call_Duration'] = pd.to_datetime(df2['Call_Duration'], format='%H:%M:%S')
        df2['Call_Duration'] = df2['Call_Duration'].dt.strftime('%H:%M:%S')
        df2['Audit_Date'] = pd.to_datetime(df2['Audit_Date'], format='%Y-%m-%d')
        df2['Audit_Date'] = df2['Audit_Date'].dt.strftime('%Y-%m-%d')
        df2['Call_Date'] = pd.to_datetime(df2['Call_Date'], format='%Y-%m-%d')
        df2['Call_Date'] = df2['Call_Date'].dt.strftime('%Y-%m-%d')
        df2=df2.rename(columns={'Audit_Date':'Date'})
        df2['AOI'] = df2['AOI'].str.replace("'", '`')
        df2['Observation'] = df2['Observation'].str.replace("'", '`')
        df2 = df2.fillna('')
        obj=df2['Date'].unique()#obj is storing unique dates from exising database dates 
        file_upload_db = obj.tolist()
        date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db] #here we are taking date column from existed database 
        avg_timestamp = sum(date_obj.timestamp() for date_obj in date_objs) / len(date_objs)
        avg_date = dt.fromtimestamp(avg_timestamp).strftime('%Y-%m-%d')
        start_date = (dt.strptime(avg_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')#here it will shown 30 days before you selected from new uploading file.
        end_date = (dt.strptime(avg_date, '%Y-%m-%d') + timedelta(days=30)).strftime('%Y-%m-%d')#here it will shown 30 days after you selected from new uploading file.
        st.write('Validation start_date',start_date)
        st.write('Validation end_date',end_date)
        fetch_date=f'''select distinct(date) from {option} where date between '{start_date}' and '{end_date}'; '''# here we are fetching the dates from exisited database.
        cursor.execute(fetch_date)
        exist_db_date=cursor.fetchall()
        db_results=exist_db_date
        db_list=exist_db_date
        db_list = [dt[0] for dt in db_results]
        for date in file_upload_db:
            if date not in db_list:
                st.write('Data is not available in the database for date:', date)
                if st.button(f'Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df2[df2['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name = ",".join(column)
                        update_table(option, column_name, value)
                        
                    status_table(connection, option, date)
                else:
                    break 
            elif date in db_list:
                st.write('Data already exists in the database for date:', date)
                if st.button(f'Re-Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    reupload_table(option,date)
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df2[df2['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name = ",".join(column)
                        
                        update_table(option, column_name, value)
                        
                    status_table(connection, option, date)
            else:
                break



# -------------------"forecast"--------------------


def forecast(date, option, connection):
    """
    This is a Python function called "forecast" which performs the following tasks:

Takes a file input from the user using the stlibrary's file_uploader method.
Checks the file extension of the uploaded file and based on the extension it reads the data into a pandas DataFrame. Currently, the function only reads excel files of extension ".xlsx" and writes an error message for any other file type.
The function then sets the "Intervals" column as the index of the DataFrame and drops the 'Overall' column.
The DataFrame is then transposed and 25 rows are selected.
The function then creates a connection to a PostgreSQL database using the psycopg2 library.
The transposed DataFrame is then inserted into the "forecast" table in the database using an SQL INSERT statement for each row of the DataFrame.
Input: A file (CSV or Excel) selected by the user.
Output: None. (Inserts data into a PostgreSQL database)
 """
    uploaded_file = st.file_uploader("Choose CSV or Excel", type=['xlsx'])
    if uploaded_file is not None:
        # Can be used wherever a "file-like" object is accepted:
        file_name = uploaded_file.name
        file_check = file_name[-4:]
        if file_check == 'xlsx':
            df1 = pd.read_excel(
                uploaded_file, sheet_name='Interval Wise Volume Projection', header=[2],index_col='Intervals')[:25]
        else:
            st.write('It is a Wrong type file')
        df = df1
        df.drop('Overall', axis=1, inplace=True)
        df=df.T
        df['date']=df.index
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        st.write(df.head())
        obj=df['date'].unique()#obj is storing unique dates from new file upload  database dates 
        file_upload_db = obj.tolist()
        date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db]

        # date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db] #here we are taking date column from new file upload database 
        avg_timestamp = sum(date_obj.timestamp() for date_obj in date_objs) / len(date_objs)
        avg_date = dt.fromtimestamp(avg_timestamp).strftime('%Y-%m-%d')
        start_date = (dt.strptime(avg_date, '%Y-%m-%d') - timedelta(days=130)).strftime('%Y-%m-%d')#here it will shown 10 days before you selected from new uploading file.
        end_date = (dt.strptime(avg_date, '%Y-%m-%d') + timedelta(days=30)).strftime('%Y-%m-%d')#here it will shown 10 days after you selected from new uploading file.
        st.write('Validation start_date:',start_date)
        st.write('Validation end_date:',end_date)
        fetch_date=f'''select distinct(date) from {option} where date between '{start_date}' and '{end_date}'; '''# here we are fetching the dates from exisited database.
        cursor.execute(fetch_date)
        exist_db_date=cursor.fetchall()
        db_results=exist_db_date

        db_list = [dt[0] for dt in db_results]
        for date in file_upload_db:
            if date not in db_list:
                st.write('Data is not available in the database for date:', date)
                if st.button(f'Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name=''
                        column_name=','.join(['"' + str(elem) + '"' for elem in column])
                        
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
                else:
                    break 
            elif date in db_list:
                st.write('Data already exists in the database for date:', date)
                if st.button(f'Re-Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    reupload_table(option,date)
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name=''
                        column_name=','.join(['"' + str(elem) + '"' for elem in column])
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
            else:
                break

       
# ------------------"Attrition"----------------------
attrition_tracker_freezed_columns=['Sr No', 'EmployeeID', 'EmployeeName','LOB', 'Supervisor', 'Attrited From ', 'Status', 'PID', 'DOJ', 'LWD', 'Month', 'Attrition Type', 'Reasons', 'Ielevate Updated', 'Category ', 'Resons']

def attrition_tracker(date, option, connection):
    """
    Input: A CSV or Excel file containing data.
Output: None. The function uploads data to the PostgreSQL database and displays the head of the dataframe.
Accepts a file input through file uploader.
It checks the file type (if it is .xlsx)
Reads data from the uploaded file, if it is .xlsx
Drops any column with "Unnamed" in the name.
Converts 'DOJ', 'LWD', and 'Month' columns to date-time type and changes the format to 'YYYY-MM-DD'.
Replaces space in the column names with '_' and replaces single quotes in data with a backtick.
Fills NaN values with an empty string.
Connects to a PostgreSQL database and inserts the data into the table 'attrition_tracker'.
"""

    uploaded_file = st.file_uploader("Choose CSV or Excel", type=['xlsx'])
    if uploaded_file is not None:
        # Can be used wherever a "file-like" object is accepted:
        file_name = uploaded_file.name
        file_check = file_name[-4:]
        if file_check == 'xlsx':
            df1 = pd.read_excel(uploaded_file, sheet_name='Attrition', header=[1])
        else:
            st.write('It is a Wrong type file')
        
        for col in df1.columns:
            if 'Unnamed' in col:
                df1.drop(col, axis=1, inplace=True)
        df = df1[attrition_tracker_freezed_columns].copy()
        df['DOJ'] = pd.to_datetime(df['DOJ'], format='%Y-%m-%d')
        df['DOJ'] = df['DOJ'].dt.strftime('%Y-%m-%d')
        df['LWD'] = pd.to_datetime(df['LWD'], format='%Y-%m-%d')
        df['LWD'] = df['LWD'].dt.strftime('%Y-%m-%d')
        df['Month'] = pd.to_datetime(df['Month'], format='%Y-%m-%d')
        df['Month'] = df['Month'].dt.strftime('%Y-%m-%d')
        df=df.rename(columns={'LWD':'Date'})
        df.columns = df.columns.str.replace(' ', '_')
        df = df.replace("'", "`", regex=True)
        df = df.fillna('')
        st.write(df.head())
        obj=df['Date'].unique()
        file_upload_db = obj.tolist()                
        file_upload_db = [date for date in file_upload_db if date != '']

        # file_upload_db = [str(date) for date in file_upload_db] # Convert dates to strings
        date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db]
        avg_timestamp = sum(date_obj.timestamp() for date_obj in date_objs) / len(date_objs)
        avg_date = dt.fromtimestamp(avg_timestamp).strftime('%Y-%m-%d')
        start_date = (dt.strptime(avg_date, '%Y-%m-%d') - timedelta(days=60)).strftime('%Y-%m-%d')
        end_date = (dt.strptime(avg_date, '%Y-%m-%d') + timedelta(days=60)).strftime('%Y-%m-%d')
        st.write('Validation Start_Date:',start_date)
        st.write('Validaition End_Date:',end_date)
        fetch_date=f'''select distinct(date) from {option} '''#where date between '{start_date}' and '{end_date}'; '''
        cursor.execute(fetch_date)
        exist_db_date=cursor.fetchall()
        db_results=exist_db_date
        # db_list=[str(date[0]) for date in exist_db_date] # Convert tuples to strings
        db_list = [dt[0] for dt in db_results]
        for date in file_upload_db:
            if date not in db_list:
                st.write('Data is not available in the database for date:', date)
                if st.button(f'Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name = ",".join(column)
                       
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
                else:
                    break 
            elif date in db_list:
                st.write('Data already exists in the database for date:', date)
                if st.button(f'Re-Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    reupload_table(option,date)
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name = ",".join(column)
                        
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
            else:
                break
        
# -----------------"Agent Roster"---------------------------
agent_roster_freezed_columns=['Sl No', 'EmployeeID', 'EmployeeName', 'Gender', 'Team lead', 'Location', 'Current Status', 'DOJ', 'Batch#', 'Floor Status', 'FTE', 'Shift', 'Roster Date', 'Shifts Timings', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun', 'WO', 'PH', 'Leaves', 'Total Planned ']

def agent_roster(date, option, connection):
    
    uploaded_file = st.file_uploader("Choose CSV or Excel", type=['xlsx'])
    if uploaded_file is not None:
        # Can be used wherever a "file-like" object is accepted:
        file_name = uploaded_file.name
        file_check = file_name[-4:]
        if file_check == 'xlsx':
            df1 = pd.read_excel(uploaded_file, header=[0, 1], sheet_name="Agents Roster")
        else:
            st.write('It is a Wrong type file')
        df2 = df1
        # print(list(df1.columns))
        l = list(df2.columns)
        df2.columns = [l[i][1] if i < 14 else l[i][0]
                      if i < 21 else l[i][1] for i in range(len(l))]
        df=df2[agent_roster_freezed_columns].copy()
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace('#', '_')
        df['DOJ'] = pd.to_datetime(df['DOJ'], format='%Y-%m-%d')
        df['DOJ'] = df['DOJ'].dt.strftime('%Y-%m-%d')
        df['Roster_Date']=pd.to_datetime(df['Roster_Date'], format='%Y-%m-%d')
        df['Roster_Date'] = df['Roster_Date'].dt.strftime('%Y-%m-%d')
        df=df.rename(columns={'Roster_Date':'Date'})
        st.write(df.head())
        df = df.fillna(0)
        obj=df['Date'].unique()
        file_upload_db = obj.tolist()
        valid_dates = [date for date in file_upload_db if isinstance(date, str)]
        date_objs = [dt.strptime(date, '%Y-%m-%d') for date in valid_dates]
        avg_timestamp = sum(date_obj.timestamp() for date_obj in date_objs) / len(date_objs)
        avg_date = dt.fromtimestamp(avg_timestamp).strftime('%Y-%m-%d')
        start_date = (dt.strptime(avg_date, '%Y-%m-%d') - timedelta(days=20)).strftime('%Y-%m-%d')
        end_date = (dt.strptime(avg_date, '%Y-%m-%d') + timedelta(days=20)).strftime('%Y-%m-%d')
        st.write('Validation Start_Date',start_date)
        st.write('Validaiton End_Date',end_date)
        fetch_date=f'''select distinct(date) from {option} where date between '{start_date}' and '{end_date}'; '''
        cursor.execute(fetch_date)
        exist_db_date=cursor.fetchall()
        db_results=exist_db_date
        db_list = [dt[0] for dt in db_results]
        for date in file_upload_db:
            if date not in db_list:
                st.write('Data is not available in the database for date:', date)
                if st.button(f'Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name = ",".join(column)
                       
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
                else:
                    break 
            elif date in db_list:
                st.write('Data already exists in the database for date:', date)
                if st.button(f'Re-Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    reupload_table(option,date)
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)                        
                        column_name=''
                        column_name = ",".join(column)
                        
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
            else:
                break


# -------------------"Nesting_Tracker"----------------------------------------

nesting_freezed_columns=['Date', 'Batch No', 'Trainer Name', 'Nesting Start Date ', 'Nesting end Date ', 'Count of Nestee', 'Name', 'Count of Audit Done by TL', 'Count of Coaching Done by TL', 'Count of Audit Done by Trainer', 'Count of Coaching Done by Trainer', 
'Count of Audit Done by QA', 'Count of Coaching Done by QA']
def nesting_track(date, option, connection):
    '''The function starts by checking if a file has been uploaded by the user using st.file_uploader() function from the Streamlit library.
If a file has been uploaded, the script checks if the file format is .xlsx using string manipulation.
If the file format is correct, the script reads the Excel file into a pandas DataFrame using pd.read_excel().
The DataFrame is cleaned and reformatted by renaming columns, converting dates to a standardized format, and filling any missing values with zero.
The average date is calculated from the list of unique dates in the DataFrame, and a start and end date range is determined based on the average date.
A SQL query is constructed to retrieve the dates that exist in the specified database table within the start and end date range.
The SQL query is executed using the database connection, and the resulting dates are stored in a list.
A loop iterates over the list of uploaded dates, and checks if each date already exists in the database using the list of database dates.
If the date does not exist in the database, the script prints a message to the console and provides an option for the user to upload the data to the database.
If the user clicks the upload button, the data for that date is filtered from the DataFrame, and each row of the filtered DataFrame is inserted into the database using the update_table() function.
If there is an error during the upload, the data is inserted into an error table using the update_error_table() function.
A status table is updated with information on the upload status using the status_table() function.
If the date already exists in the database, the script prints a message to the console and provides an option for the user to re-upload the data to the database.
If the user clicks the re-upload button, the existing data for that date is deleted from the database using the reupload_table() function, and the new data is inserted into the database using the update_table() function.
A status table is updated with information on the re-upload status using the status_table() function.'''

    uploaded_file = st.file_uploader('csv or excel', type=['xlsx'])
    if uploaded_file is not None:
        # Can be used wherever a "file-like" object is accepted:
        file_name = uploaded_file.name
        file_check = file_name[-4:]
        if file_check == 'xlsx':
            df1 = pd.read_excel(uploaded_file, sheet_name='Audit Count ')#reading an excel file 
        else:
            st.write('It is a Wrong type file')
        df = df1[nesting_freezed_columns].copy()
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        df.columns = df.columns.str.replace(' ', '_')
        df = df.fillna(0)
        st.write(df.head())
        obj=df['Date'].unique()
        file_upload_db = obj.tolist()
        date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db]
        avg_timestamp = sum(date_obj.timestamp() for date_obj in date_objs) / len(date_objs)
        avg_date = dt.fromtimestamp(avg_timestamp).strftime('%Y-%m-%d')
        start_date = (dt.strptime(avg_date, '%Y-%m-%d') - timedelta(days=20)).strftime('%Y-%m-%d')
        end_date = (dt.strptime(avg_date, '%Y-%m-%d') + timedelta(days=20)).strftime('%Y-%m-%d')
        st.write('Validaiton start_date:',start_date)
        st.write('validation End_date:',end_date)
        fetch_date=f'''select distinct(date) from {option} where date between '{start_date}' and '{end_date}'; '''
        cursor.execute(fetch_date)
        exist_db_date=cursor.fetchall()
        db_results=exist_db_date
        # db_list = [dt[0].strftime("%Y-%m-%d") for dt in db_results]
        db_list=[dt[0] for dt in db_results]
        # db_list = [dt.strptime(dt[0], "%Y-%m-%d").strftime("%Y-%m-%d") for dt in db_results]

        for date in file_upload_db:
            if date not in db_list:
                st.write('Data is not available in the database for date:', date)
                if st.button(f'Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name = ",".join(column)
                        
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
                else:
                    break 
            elif date in db_list:
                st.write('Data already exists in the database for date:', date)
                if st.button(f'Re-Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    reupload_table(option,date)
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name = ",".join(column)
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
            else:
                break
    
        

       
# ---------------"Audit_complaince"-------------------
import operator as op

audit_freezed_columns=['Sr no', 'Emp ID', 'Name', 'ReportDate', 'Tenure', 'Tl', 'Location', 'Status', 'Categorization', 'QA', 'AHT', 'QA.1', 'MTD Actual  BAUaudits', 'Week 1 Target', 'Week 1 Actual', 'Pending ', 'Reason', 'Week 2 Target', 'Week 2 Actual', 'Pending .1', 'Reason.1', 'Week 3 Target', 'Week 3 Actual', 'Pending .2', 'Reason.2', 'Week 4  Target', 'Week 4 actual', 'Pending .3', 'Reason.3', 'Overall count', 'Coaching count']


def audit_complaince(date, option, connection):

    uploaded_file = st.file_uploader("Choose CSV or Excel", type=['xlsx'])
    if uploaded_file is not None:
        # Can be used wherever a "file-like" object is accepted:
        file_name = uploaded_file.name
        file_check = file_name[-4:]
        if file_check == 'xlsx':
            df1 = pd.read_excel(
                uploaded_file, sheet_name='Allocation', engine='openpyxl')
        else:
            st.write('It is a Wrong type file')
        # df = df1
        df = df1[audit_freezed_columns].copy()
        #---------------data cleaning-----------------------------------##
        for i in range(len(df)):
            try:
                aht_value = pd.to_datetime(df.at[i, 'AHT'], format='%H:%M:%S.%f')
                if pd.notnull(aht_value):
                    df.at[i, 'AHT'] = aht_value.strftime('%H:%M:%S')
                else:
                    df.at[i, 'AHT'] = ''  # or any other appropriate value for missing or undefined datetime
            except ValueError:
                df.at[i, 'AHT'] = pd.to_datetime(df.at[i, 'AHT'], format='%H:%M:%S').strftime('%H:%M:%S')

        df['ReportDate']=pd.to_datetime(df['ReportDate'], format='%Y-%m-%d')
        df['ReportDate'] = df['ReportDate'].dt.strftime('%Y-%m-%d')
        df=df.rename(columns={'ReportDate':'Date'})
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace('.', '_')
        
        # df.columns = df.columns.str.upper()
        df = df.fillna(0)
        st.write(df.head())
        obj=df['Date'].unique()#obj is storing unique dates from new file upload  database dates 
        file_upload_db = obj.tolist()
        valid_dates = [date for date in file_upload_db if isinstance(date, str)]
        date_objs = [dt.strptime(date, '%Y-%m-%d') for date in valid_dates]

        # date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db] #here we are taking date column from new file upload database 
        avg_timestamp = sum(date_obj.timestamp() for date_obj in date_objs) / len(date_objs)
        avg_date = dt.fromtimestamp(avg_timestamp).strftime('%Y-%m-%d')
        start_date = (dt.strptime(avg_date, '%Y-%m-%d') - timedelta(days=10)).strftime('%Y-%m-%d')#here it will shown 10 days before you selected from new uploading file.
        end_date = (dt.strptime(avg_date, '%Y-%m-%d') + timedelta(days=10)).strftime('%Y-%m-%d')#here it will shown 10 days after you selected from new uploading file.
        st.write('Validation Start_date:',start_date)
        st.write('Validation End_date:',end_date)
        fetch_date=f'''select distinct(Date) from {option} where Date between '{start_date}' and '{end_date}'; '''# here we are fetching the dates from exisited database.
        cursor.execute(fetch_date)
        exist_db_date=cursor.fetchall()
        db_results=exist_db_date
        db_list = [dt[0] for dt in db_results]
        # st.write(db_list)
        # st.write(file_upload_db)
        for date in file_upload_db:
            if date not in db_list:
                st.write('Data is not available in the database for date:', date)
                if st.button(f'Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name=''
                        column_name=",".join(column)
                        
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
                else:
                    break 
            elif date in db_list:
                st.write('Data already exists in the database for date:', date)
                if st.button(f'Re-Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    reupload_table(option,date)
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name=''
                        column_name=",".join(column)
                        
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
            else:
                break
# --------------------"New_Hire_batch"-----------------------------------

# Define a function to format dates conditionally
def format_date(date):
    if pd.notna(date) and date != '-' and not pd.isna(date):
        return pd.to_datetime(date).strftime('%Y-%m-%d')
    else:
        return None  # Return None for 'NaT' values
new_batch_freezeed_columns=['SR', 'Employee ID', 'Agent Name ', 'Location ', 'Process', 'Trainer Name', 'Sr Manager _OPS', 'Sr Manager - T&Q', 'HR Joining Date', 'Training Start Date ', 'End Date ', 'Certification Date ', 'Floor Handover Date', 'Nautics Assessment Scre ', 'Nautics Observation', 'Final Tollgate Score', 'Certification Score', 'Designation', 'Primary Language', 'Secondary language', 'Batch Start Date', 'Classroom Certification ', 'Nesting  Start Date', 'Classroom Re - Certification ', 'Final Floor Release Date', 'Status-internal purpose', 'Last Working Day', 'Type of Attrition ', 'Reason - Attrition']
def new_hire_batch(date, option, connection):
    '''Create a file uploader using streamlit.
If a file is uploaded, read the data from the file and perform data cleaning operations.
Fetch unique dates from the uploaded file and compare them to the dates available in the database.
Calculate the average date from the uploaded file and set a date range to fetch data from the database.
If the data for a particular date is missing in the database, prompt the user to upload the data for that date. Create a new table in the database and insert the missing data into it.
If the data for a particular date is already present in the database, prompt the user to re-upload the data for that date. Update the existing data.
Create a status table indicating the status of each date for which data has been uploaded or re-uploaded.
'''
    uploaded_file = st.file_uploader("Choose CSV or Excel", type=['xlsx'])
    if uploaded_file is not None:
        # Can be used wherever a "file-like" object is accepted:
        file_name = uploaded_file.name
        file_check = file_name[-4:]
        if file_check == 'xlsx':
            df2 = pd.read_excel(
                uploaded_file, sheet_name='Raw Data', header=[1])
        else:
            st.write('It is a Wrong type file')
        df=df2[new_batch_freezeed_columns].copy()
        
        
        
                 
##------------------------------------data cleaning------------------------------------##
        # df['HR Joining Date'] = df['HR Joining Date'].dt.strftime('%Y-%m-%d')
        # df['Training Start Date '] = df['Training Start Date '].dt.strftime(
        #     '%Y-%m-%d')
        # df['End Date '] = df['End Date '].dt.strftime('%Y-%m-%d')
        # df['Certification Date '] = df['Certification Date '].dt.strftime(
        #     '%Y-%m-%d')
        # df['Floor Handover Date'] = df['Floor Handover Date'].dt.strftime(
        #     '%Y-%m-%d')
        # df['Batch Start Date'] = df['Batch Start Date'].dt.strftime('%Y-%m-%d')
        # df['Classroom Certification '] = df['Classroom Certification '].dt.strftime(
        #     '%Y-%m-%d')
        # df['Nesting  Start Date'] = df['Nesting  Start Date'].dt.strftime(
        #     '%Y-%m-%d')
        # df['Classroom Re - Certification '] = df['Classroom Re - Certification '].dt.strftime(
        #     '%Y-%m-%d')
        # df['Final Floor Release Date'] = df['Final Floor Release Date'].dt.strftime(
        #     '%Y-%m-%d')
        # Apply the formatting function to all date columns
        date_columns = ['HR Joining Date', 'Training Start Date ','Final Floor Release Date','Classroom Re - Certification ','Nesting  Start Date','Classroom Certification ','Batch Start Date','Floor Handover Date','Certification Date ','End Date ','Last Working Day']  # Add more column names as needed
        df[date_columns] = df[date_columns].applymap(format_date)
        
        df1 = df[df.columns[:29]]

        cols = ['day_' + str(i) for i in range(1, 16)]
        for col in cols:
            df1[col] = ''
        for i in range(29, len(df.columns)):
            col = 'day_'+str(i-28)
            df1[col] = df[df.columns[i]]
        df1['Date']=df1['Batch Start Date']
        # df1['Last Working Day'] = df1['Last Working Day'].fillna('N/A')
        df1.columns = df1.columns.str.replace(' ', '_')
        df1.columns = df1.columns.str.replace('-', 'to')
        df1.columns = df1.columns.str.replace('&', 'and')
        df1 = df1.fillna('')
        st.write(df1.head())
        obj=df1['Date'].unique()#obj is storing unique dates from new file upload  database dates 
        file_upload_db = obj.tolist()
        date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db]

        # date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db] #here we are taking date column from new file upload database 
        avg_timestamp = sum(date_obj.timestamp() for date_obj in date_objs) / len(date_objs)
        avg_date = dt.fromtimestamp(avg_timestamp).strftime('%Y-%m-%d')
        start_date = (dt.strptime(avg_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')#here it will shown 10 days before you selected from new uploading file.
        end_date = (dt.strptime(avg_date, '%Y-%m-%d') + timedelta(days=30)).strftime('%Y-%m-%d')#here it will shown 10 days after you selected from new uploading file.
        st.write('Validaiton Start_date:',start_date)
        st.write('Validation End_date:',end_date)
        fetch_date=f'''select distinct(date) from {option} where date between '{start_date}' and '{end_date}'; '''# here we are fetching the dates from exisited database.
        cursor.execute(fetch_date)
        exist_db_date=cursor.fetchall()
        db_results=exist_db_date
        db_list = [dt[0] for dt in db_results]
        for date in file_upload_db:
            if date not in db_list:
                st.write('Data is not available in the database for date:', date)
                if st.button(f'Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df1[df1['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name=''
                        column_name=",".join(column)
                       
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
                else:
                    break 
            elif date in db_list:
                st.write('Data already exists in the database for date:', date)
                if st.button(f'Re-Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    reupload_table(option,date)
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df1[df1['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name=''
                        column_name=",".join(column)
                        
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
            else:
                break

# ---------------------------'Master_data_pkt'------------------------------
master_freezed_columns=['Sr.No', 'Emp Code ', 'Employee Name ', 'PKT conducted date', 'Assessment Scores', 'Status ']

def master_datapkt(date, option, connection):
    '''The function begins by using a file uploader to upload a CSV or Excel file. If a file is uploaded and it is of type Excel, 
    it reads the data from the 'Master File Dec FHPL1' sheet into a Pandas DataFrame. 
    If the file is not an Excel file, it displays a message that it is the wrong type of file.
After reading the data, the function applies a function convert_date_string to the 'PKT conducted date' column to convert it to a standard date format. 
The function then cleans the column names by replacing spaces with underscores and fills any missing values with an empty string.
The function then calculates the average date from the uploaded file and selects a date range that includes 10 days before and after the average date. 
It then queries the database to fetch the dates that already exist in the database within the date range.
The function then iterates over the dates from the uploaded file and checks if the date is already present in the database or not. 
If the date is not present, it displays a message that data is not available in the database for that date and provides a button to upload the data. 
If the button is clicked, it filters the DataFrame to include only the rows for that date, iterates over the rows, and inserts them into the database. 
If there is any error during the insertion, it calls a function update_error_table to log the error in the database. It then updates the status table to indicate that the data has been uploaded.
If the date is already present in the database, it displays a message that data already exists in the database for that date and provides a button to re-upload the data. 
If the button is clicked, it first deletes the data for that date from the database by calling a function reupload_table, then filters the DataFrame to include only the rows for that date,
 iterates over the rows, and inserts them into the database. If there is any error during the insertion, it calls the update_error_table function to log the error in the database. 
 It then updates the status table to indicate that the data has been uploaded.
The function returns nothing. It only displays messages and provides buttons for the user to upload or re-upload data.
    '''
    uploaded_file = st.file_uploader("Choose CSV or Excel", type=['xls'])
    if uploaded_file is not None:
        # Can be used wherever a "file-like" object is accepted:
        file_name = uploaded_file.name
        file_check = file_name[-3:]
        if file_check == 'xls':
            df1 = pd.read_excel(
                uploaded_file, sheet_name='Master File Dec FHPL1')
        else:
            st.write('It is a Wrong type file')
        df = df1[master_freezed_columns].copy()
        print(list(df.columns))
        # df['PKT conducted date'] = df['PKT conducted date'].dt.strftime('%Y-%m-%d')
        df['PKT conducted date'] = df['PKT conducted date'].apply(format_date)

        df['date'] = df['PKT conducted date'] #.apply(convert_date_string_format2)
        df['Assessment Scores'] = df['Assessment Scores'].apply(lambda x: int(x * 100) if x <= 1 else x).astype(int)

        # df['date']=df['date'].dt.strftime('%Y-%m-%d')
        st.write(df.head())
        ##---------------data cleaning----------------------------------------------------##
        df.columns = df.columns.str.replace(' ', '_')
        df = df.fillna('')
        # st.write(df.head())
        obj=df['date'].unique()#obj is storing unique dates from new file upload  database dates 
        file_upload_db = obj.tolist()
        date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db]

        # date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db] #here we are taking date column from new file upload database 
        avg_timestamp = sum(date_obj.timestamp() for date_obj in date_objs) / len(date_objs)
        avg_date = dt.fromtimestamp(avg_timestamp).strftime('%Y-%m-%d')
        start_date = (dt.strptime(avg_date, '%Y-%m-%d') - timedelta(days=10)).strftime('%Y-%m-%d')#here it will shown 10 days before you selected from new uploading file.
        end_date = (dt.strptime(avg_date, '%Y-%m-%d') + timedelta(days=10)).strftime('%Y-%m-%d')#here it will shown 10 days after you selected from new uploading file.
        st.write('Validation Start_date:',start_date)
        st.write('Validation End_date', end_date)
        fetch_date=f'''select distinct(date) from {option} where date between '{start_date}' and '{end_date}'; '''# here we are fetching the dates from exisited database.
        cursor.execute(fetch_date)
        exist_db_date=cursor.fetchall()
        db_results=exist_db_date
        # st.write(file_upload_db)
        

        db_list = [dt[0] for dt in db_results]
        # st.write(db_list)
        for date in file_upload_db:
            if date not in db_list:
                st.write('Data is not available in the database for date:', date)
                if st.button(f'Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name=''
                        column_name=','.join(['"' + str(elem) + '"' for elem in column])
                        
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
                else:
                    break 
            elif date in db_list:
                st.write('Data already exists in the database for date:', date)
                if st.button(f'Re-Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    reupload_table(option,date)
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name=''
                        column_name=','.join(['"' + str(elem) + '"' for elem in column])
                        
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
            else:
                break

        
# -----------------login_logout---------------------------------------------------------
login_freezed_columns=['User name', 'User ID', 'Extension', 'User group', 'First login date', 'Last logout date', 'Campaign ID', 'Server IP', 
'Computer IP']

def login_logout(date, option, connection):
    '''Inputs:

    date: a date string in the format '%Y-%m-%d'
    option: a string representing the name of the table in the database to upload data to
    connection: a database connection object
    The function uses the file_uploader() function from the Streamlit library to allow the user to upload a file.
If a file is uploaded and it is of the correct type ('xlsb'), the function reads the file into a pandas DataFrame using the read_excel() function with the pyxlsb engine. 
Otherwise, the function displays an error message.
The DataFrame is cleaned by filling in missing values with 0 and converting the 'First login date' and 'Last logout date' columns to datetime objects using a custom datetime_converter() function.
The cleaned DataFrame is then processed further by formatting the 'First login date' and 'Last logout date' columns as strings, and creating a new 'Date' column by extracting the date from the 'First login date' column.
The column names are cleaned up by replacing spaces with underscores, and any remaining missing values are filled with empty strings.
The unique dates in the 'Date' column of the DataFrame are extracted and converted to a list.
The function calculates the average date from the list of unique dates, and uses it to define a 10-day range of dates to search for in the existing database.
The function constructs a SQL query to fetch the distinct dates from the existing database that fall within the 10-day range.
The query is executed using the database connection object, and the results are retrieved as a list of date strings.
The function compares the list of dates from the uploaded file to the list of dates from the database, and for each date in the file that is not in the database, 
it checks if the user wants to upload the data for that date.
If the user chooses to upload the data, the function filters the DataFrame to include only the rows for the selected date, 
and iterates over those rows to insert them into the database using the update_table() function.
If there is an error during the insert operation, the row is inserted into an error table using the update_error_table() function.
Finally, the function calls the status_table() function to update a status table in the database for the selected date. 
If the date already exists in the database, the user has the option to re-upload the data, which skips the status update step.
    '''

    uploaded_file = st.file_uploader("Choose CSV or Excel", type=['xlsb'])
    if uploaded_file is not None:
        # Can be used wherever a "file-like" object is accepted:
        file_name = uploaded_file.name
        file_check = file_name[-4:]
        if file_check == 'xlsb':
            df1 = pd.read_excel(uploaded_file, engine='pyxlsb', header=[2])
        else:
            st.write('It is a Wrong type file')
        df=df1[login_freezed_columns].copy()
        ##----------------------------------data cleaning-------------------------------##
        df = df.fillna(0)
        
        df['First login date'] = pd.to_datetime(
            df['First login date'].apply(datetime_converter), errors='coerce')
        
        df['Last logout date'] = pd.to_datetime(
            df['Last logout date'].apply(datetime_converter), errors='coerce')
        df['First login date'] = df['First login date'].dt.strftime(
            '%Y-%m-%d %H:%M:%S')
        df['Last logout date'] = df['Last logout date'].dt.strftime(
            '%Y-%m-%d %H:%M:%S')
        df['First login date'] = pd.to_datetime(df['First login date'],format='%Y-%m-%d %H:%M:%S')
        # df['First login date']=df['First login date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['Date'] = df['First login date'].dt.strftime('%Y-%m-%d')
        df['First login date'] = df['First login date'].dt.strftime('%Y-%m-%d %H:%M:%S')

        
        st.write(df.head())

        df.columns = df.columns.str.replace(' ', '_')
        df=df.fillna('')
        obj=df['Date'].unique()#obj is storing unique dates from new file upload  database dates 
        file_upload_db = obj.tolist()
        date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db]

        # date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db] #here we are taking date column from new file upload database 
        avg_timestamp = sum(date_obj.timestamp() for date_obj in date_objs) / len(date_objs)
        avg_date = dt.fromtimestamp(avg_timestamp).strftime('%Y-%m-%d')
        start_date = (dt.strptime(avg_date, '%Y-%m-%d') - timedelta(days=10)).strftime('%Y-%m-%d')#here it will shown 10 days before you selected from new uploading file.
        end_date = (dt.strptime(avg_date, '%Y-%m-%d') + timedelta(days=10)).strftime('%Y-%m-%d')#here it will shown 10 days after you selected from new uploading file.
        st.write('Validation Start_date:',start_date)
        st.write('Validaiton End_date:',end_date)
        fetch_date=f'''select distinct(Date) from {option} where Date between '{start_date}' and '{end_date}'; '''# here we are fetching the dates from exisited database.
        cursor.execute(fetch_date)
        exist_db_date=cursor.fetchall()
        db_results=exist_db_date

        db_list = [dt[0] for dt in db_results]
        for date in file_upload_db:
            if date not in db_list:
                st.write('Data is not available in the database for date:', date)
                if st.button(f'Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name=''
                        column_name=",".join(column)
                        
                        update_table(option, column_name, value)
                        
                    status_table(connection, option, date)
                else:
                    break 
            elif date in db_list:
                st.write('Data already exists in the database for date:', date)
                if st.button(f'Re-Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    reupload_table(option,date)
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name=''
                        column_name=",".join(column)
                        
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
            else:
                break

#---------------------------Tni_efficacy------------------------------------------------
tni_efficacy_freezed_columns=['Emp ID', 'Name', 'Status ', 'Training Date ', 'Report shared date', 'Training start time', 'Training end time', 'Same day Assessment score', 'Post 7Days PKT . Scores', 'Population Audited ', 'No Of calls Audited', 'Pre  Cq score', 'Post cq Score']
def tni_efficacy(date, option, connection):
    uploaded_file = st.file_uploader("Choose CSV or Excel", type=['xlsx'])
    if uploaded_file is not None:
        # Can be used wherever a "file-like" object is accepted:
        file_name = uploaded_file.name
        file_check = file_name[-4:]
        if file_check == 'xlsx':
            df1 = pd.read_excel(uploaded_file)
        else:
            st.write('It is a Wrong type file')
        df=df1[tni_efficacy_freezed_columns].copy()
        ##----------------------------------data cleaning-------------------------------##
        # st.write(df.head())

        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace('.', '_')
        df=df.fillna('')
        df = df.fillna(0)
        df['Report_shared_date'] = pd.to_datetime(df['Report_shared_date'],errors='coerce')
        df['Report_shared_date'] = df['Report_shared_date'].dt.strftime('%Y-%m-%d')
        df['Training_Date_'] = df['Training_Date_'].astype(str)
        df['Training_start_time'] = df['Training_start_time'].astype(str)
        df['Training_end_time'] = df['Training_end_time'].astype(str)
        # Apply the if-else condition to handle character values
        df=df.rename(columns={'Report_shared_date':'Date'})
        print(list(df.columns))
        st.write(df.head())
        obj=df['Date'].unique()#obj is storing unique dates from new file upload  database dates 
        file_upload_db = obj.tolist()
        date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db]
        date_objs_date_only = [date_obj.date() for date_obj in date_objs]
        st.write(date_objs_date_only)

        # date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db] #here we are taking date column from new file upload database 
        avg_timestamp = sum(date_obj.timestamp() for date_obj in date_objs) / len(date_objs)
        avg_date = dt.fromtimestamp(avg_timestamp).strftime('%Y-%m-%d')
        start_date = (dt.strptime(avg_date, '%Y-%m-%d') - timedelta(days=10)).strftime('%Y-%m-%d')#here it will shown 10 days before you selected from new uploading file.
        end_date = (dt.strptime(avg_date, '%Y-%m-%d') + timedelta(days=10)).strftime('%Y-%m-%d')#here it will shown 10 days after you selected from new uploading file.
        st.write('Validation Start_date:',start_date)
        st.write('Validaiton End_date:',end_date)
        fetch_date=f'''select distinct(Date) from {option} where Date between '{start_date}' and '{end_date}'; '''# here we are fetching the dates from exisited database.
        cursor.execute(fetch_date)
        exist_db_date=cursor.fetchall()
        db_results=exist_db_date

        db_list = [dt[0] for dt in db_results]
        st.write(db_list)
        for date in file_upload_db:
            if date not in db_list:
                st.write('Data is not available in the database for date:', date)
                if st.button(f'Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name=''
                        column_name=",".join(column)
                        
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
                else:
                    break 
            elif date in db_list:
                st.write('Data already exists in the database for date:', date)
                if st.button(f'Re-Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    reupload_table(option,date)
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name=''
                        column_name=",".join(column)
                        
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
            else:
                break
#--------------------------------BQ refresher---------------------------

def convert_time(time_obj):
    return time_obj.strftime('%H:%M:%S')
# bq_refresher_freezed_columns=['Emp_ID', 'Agent_Name', 'Date_of_refresher', 'Training_start_time', 'Training_end_time', 'Assessment_scores', 'Pre_Performance_CQ_Score', 'Pre_Performance_AHT', 'Post_Performance_CQ_Score', 'Post_Performance_AHT', 'AHT']
bq_refresher_freezed_columns=['Emp ID','Agent Name','Date of refresher','Training start time','Training end time','Assessment scores','Pre-Performance CQ Score','Pre-Performance AHT','Post-Performance CQ Score','Post-Performance AHT','AHT']
def bq_refresher(date, option, connection):
    uploaded_file = st.file_uploader("Choose CSV or Excel", type=['xlsx'])
    if uploaded_file is not None:
        # Can be used wherever a "file-like" object is accepted:
        file_name = uploaded_file.name
        file_check = file_name[-4:]
        if file_check == 'xlsx':
            df1 = pd.read_excel(uploaded_file,header=[0,1])
        else:
            st.write('It is a Wrong type file')
        df1.columns = [
                    'Emp ID',
                    'Agent Name',
                    'Date of refresher',
                    'Training start time',
                    'Training end time',
                    'Assessment scores',
                    'Pre-Performance CQ Score',
                    'Pre-Performance AHT',
                    'Post-Performance CQ Score',
                    'Post-Performance AHT',
                    'AHT'
                ]
        df1.columns.tolist()
        df=df1[bq_refresher_freezed_columns].copy()
        ##----------------------------------data cleaning-------------------------------##
        df = df.fillna(0)

        st.write(df.head())
        

        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace('-', '_')
        df['Training_start_time'] = df['Training_start_time'].apply(convert_time)
        df['Training_end_time'] = df['Training_end_time'].apply(convert_time)
        df['Pre_Performance_AHT'] = df['Pre_Performance_AHT'].apply(convert_time)
        df['Post_Performance_AHT'] = df['Post_Performance_AHT'].apply(convert_time)
        df['Date_of_refresher'] = pd.to_datetime(df['Date_of_refresher'],errors='coerce',format='%Y-%m-%d')
        df['Date_of_refresher'] = df['Date_of_refresher'].dt.strftime('%Y-%m-%d')
        df=df.fillna('')
        df['Date']=df['Date_of_refresher']
        # df=df.rename(columns={'Date_of_refresher':'Date'})
        obj=df['Date'].unique()#obj is storing unique dates from new file upload  database dates 
        file_upload_db = obj.tolist()
        date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db]

        # date_objs = [dt.strptime(date, '%Y-%m-%d') for date in file_upload_db] #here we are taking date column from new file upload database 
        avg_timestamp = sum(date_obj.timestamp() for date_obj in date_objs) / len(date_objs)
        avg_date = dt.fromtimestamp(avg_timestamp).strftime('%Y-%m-%d')
        start_date = (dt.strptime(avg_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')#here it will shown 10 days before you selected from new uploading file.
        end_date = (dt.strptime(avg_date, '%Y-%m-%d') + timedelta(days=30)).strftime('%Y-%m-%d')#here it will shown 10 days after you selected from new uploading file.
        st.write('Validation Start_date:',start_date)
        st.write('Validaiton End_date:',end_date)
        fetch_date=f'''select distinct(Date) from {option} where Date between '{start_date}' and '{end_date}'; '''# here we are fetching the dates from exisited database.
        cursor.execute(fetch_date)
        exist_db_date=cursor.fetchall()
        db_results=exist_db_date


        db_list = [dt[0].strftime('%Y-%m-%d') for dt in db_results]
        for date in file_upload_db:
            if date not in db_list:
                st.write('Data is not available in the database for date:', date)
                if st.button(f'Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name=''
                        column_name=",".join(column)
                        
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
                else:
                    break 
            elif date in db_list:
                st.write('Data already exists in the database for date:', date)
                if st.button(f'Re-Upload_{date}', key=date):
                    # session_state.button_clicked = True
                    reupload_table(option,date)
                    # Filter the DataFrame to only include rows for the missing date            
                    upload_df = df[df['Date'] == date]
                    # Iterate over the rows in the filtered DataFrame and insert them into the database            
                    for i, row in upload_df.iterrows():
                        y = row.keys()
                        column = list(y)
                        x = row.values                
                        value = tuple(x)
                        column_name=''
                        column_name=",".join(column)
                       
                        update_table(option, column_name, value)
                    status_table(connection, option, date)
            else:
                break


#-------------------------------Active_Hc_count-------------------------------------
# emp_details_freezed_columns=['PID', 'LOB', 'SL_NO', 'EMPLOYEEID', 'EMPLOYEENAME', 'SIP', 'GENDER', 'TEAM_LEAD', 'LOCATION', 'DOJ', 'CURRENT_STATUS', 'DESIGNATION', 'EMAIL', 'ADDRESS', 'PINCODE', 'ROLE_FIT', 'GRID', 'CONVINCE_OTHERS_ABOUT_IDEAS_AND_PLANS', 'DIPLOMATICALLY_HANDLE_SITUATIONS_AND_OVERCOME_DISAGREEMENTS', 'ENGAGE_WITH_PEOPLE_TO_IDENTIFY_THEIR_NEEDS_AND_CONCERNS', 'EVALUATE_SITUATIONS_AND_MAKE_DECISIONS_INDEPENDENTLY', 'EXERCISE_CAUTION_IN_MAKING_COMMITMENTS', 'PROVIDE_SUPPORT_SERVICE_FOR_CUSTOMERS_EMPLOYEES_STAKEHOLDERS', 'STRIVE_TO_MEET_TARGETS', 'UNDERSTAND_COMPLEX_PROCESSES__CONCEPTS_OR_SYSTEMS', 'WORK_WITH_STRONG_CUSTOMER_ORIENTATION', 'COLLECT_SPECIFIC_DETAILS_OR_INFORMATION_FROM_STAKEHOLDERS', 'FIND_SOLUTIONS_TO_COMPLEX_SITUATIONS__PROBLEMS', 'LEARN_NEW_SKILLS_ON_THE_JOB', 'MAKE_LARGE_NUMBER_OF_PHONE_CALLS_ON_A_DAILY_BASIS', 'PERFORM_REPITITIVE_TASKS_FOR_LONG_DURATIONS', 'PERFORM_WORK_STRICTLY_IN_ACCORDANCE_WITH_RULES_AND_PROCEDURES', 'MANAGER', 'SR__MANAGER']
emp_details_freezed_columns=['PID', 'LOB', 'Sl NO', 'EmployeeID', 'EmployeeName', 'SIP', 'Gender', 'Team lead', 'Location', 'DOJ', 'Current Status', 'Designation', 'Email', 'Address', 'Pincode', 'Role fit', 'Grid', 'Convince others about ideas and plans', 'Diplomatically handle situations and overcome disagreements', 'Engage with people to identify their needs and concerns', 'Evaluate situations and make decisions independently', 'Exercise caution in making commitments', 'Provide support service for customers/employees/stakeholders', 'Strive to meet Targets', 'Understand complex processes, concepts or systems', 'Work with strong customer orientation', 'Collect specific details or information from stakeholders', 'Find solutions to complex situations/ problems', 'Learn new skills on the job', 'Make large number of phone calls on a daily basis', 'Perform repititive tasks for long durations', 'Perform work strictly in accordance with rules and procedures','Manager','Sr .Manager']

def emp_detail(connection):
    uploaded_file = st.file_uploader("Choose CSV or Excel", type=['xlsx'])
    if uploaded_file is not None:
        # Can be used wherever a "file-like" object is accepted:
        file_name = uploaded_file.name
        file_check = file_name[-4:]
        if file_check == 'xlsx':
            dtype_dict = {
            'Manager': str,
            'Team lead':str,
            'SIP':str,
            'EmployeeID':str,
            'Sr .Manager':str
            # Add more columns and data types as needed
                }
            df1 = pd.read_excel(uploaded_file,sheet_name='Active',dtype=dtype_dict)
        else:
            st.write('It is a Wrong type file')
        # print(list(df1.columns))
        df=df1[emp_details_freezed_columns].copy()
        df.columns = map(str.upper, df.columns)
        df.columns=df.columns.str.replace(' ', '_' )
        df.columns=df.columns.str.replace('/', '_' )
        df.columns=df.columns.str.replace(',', '_')
        df.columns=df.columns.str.replace('.', '_')
        # print(list(df1.columns))
        # df=df1[emp_details_freezed_columns].copy()
        df['DOJ'] = pd.to_datetime(df['DOJ'])
        df['DOJ'] = df['DOJ'].dt.strftime('%Y-%m-%d')
        df['EMPLOYEENAME'] = df['EMPLOYEENAME'].apply(str.upper)
        df['EMPLOYEENAME'] = df['EMPLOYEENAME'].apply(lambda x: re.sub(r'[^\x00-\x7F]+', '', str(x)))
        df['EMPLOYEEID'] = df['EMPLOYEEID'].apply(lambda x: re.sub(r'[^\x00-\x7F]+', '', str(x)))
        df['PID'] = df['PID'].apply(lambda x: re.sub(r'[^\x00-\x7F]+', '', str(x)))
        df=df.fillna(0)
        st.write(df.head())
        if st.button('Upload'):
            cursor = connection.cursor()
            
            # Upload new file data into the database
            try:
                # Truncate the existing data in the database
                old_sql=f"TRUNCATE TABLE EMP_DETAILS;"
                cursor.execute(old_sql)
            
                for index, row in df.iterrows():
                    values = tuple(row.values.tolist())
                    sql = f"INSERT INTO EMP_DETAILS VALUES {values};"
                    cursor.execute(sql)
                connection.commit()
                st.write("File data uploaded successfully!")
            except Exception as e:
                st.write('Error:', e)
                cursor.execute("ROLLBACK")
                # sql = f'''INSERT INTO {option}_error (%s) VALUES %s; ''' % (column_name, value)
                # cursor.execute(sql)
            finally:
                cursor.close()



        


# def status_table(connection, option, selected_date):
#     # here query storing the count of selected option file based on date.
#     query = f"select count(*) from {option} where date='{selected_date}'"
#     result = fetch_data_function(query)
#     # here query1 is storing the error table data based on selected option of file.
#     query1 = f"select count(*) from {option}_ERROR"
#     result1 = fetch_data_function(query1)
#     # below updating inforamtiion into status table with the given options.
#     query = f'''insert into status (table_name,inserted_rows,error_rows,db_name) values('{option}',{result},{result1},'test');'''
#     cursor.execute(query)
#     connection.commit()
#     # here 's' is fetch the count of error table if data exist in error table 
#     s = f'''select count(*) from {option}_error;'''
#     count = fetch_data_function(s)
#     if count == 0:
#         st.write('sucessfully uploaded')
#         s = f'''select * from status order by upload_date desc;'''# here if data uplooaded sucessfully the status table will pop up in the UI that infotmation storing in 's'.
#         cursor.execute(s)
#         column_names = [desc[0] for desc in cursor.description]
#         df = pd.DataFrame(cursor.fetchall(), columns=column_names)
#         st.write(df)
#     else:
#         st.write('Not sucessfully uploaded')
#         s = f'''select * from {option}_error;'''# here 's' is fetching the error table data 
#         cursor.execute(s)
#         column_names = [desc[0] for desc in cursor.description]
#         df = pd.DataFrame(cursor.fetchall(), columns=column_names)
#         st.write(df)


'''----------------new status table created after creashed the database -----------------------'''

def status_table(connection, option, selected_date):
    # Query to get the count of selected option file based on date
    query = f"select count(*) from {option} where date='{selected_date}'"
    result = fetch_data_function(query)
    
    # Query to get the count of error table data based on selected option of file
    query1 = f"select count(*) from {option}_ERROR"
    result1 = fetch_data_function(query1)
    
    # Format the date and time as a string
    upload_datetime = dt.now().strftime('%Y-%m-%d %H:%M:%S')
    upload_datetime_sql = f"CONVERT(DATETIME, '{upload_datetime}', 120)"
    # Insert information into the status table with the given options
    query = f'''
    INSERT INTO status (table_name, inserted_rows, error_rows, db_name, upload_date)
    VALUES ('{option}', {result}, {result1}, 'landing_db', '{upload_datetime}');
    '''
    cursor.execute(query)
    connection.commit()
    
    # Query to fetch the count of error table if data exists in the error table
    s = f'''SELECT count(*) FROM {option}_error;'''
    count = fetch_data_function(s)
    
    if count == 0:
        st.write('Successfully uploaded')
        s = f'''SELECT table_name, inserted_rows, error_rows, db_name, upload_date FROM status ORDER BY upload_date DESC;'''
        cursor.execute(s)
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(cursor.fetchall(), columns=column_names)
        # Add a success column with a checkmark symbol
        df['Success'] = '✔'
        st.write(df)
    else:
        st.write('Not successfully uploaded')
        s = f'''SELECT * FROM {option}_error;'''
        cursor.execute(s)
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(cursor.fetchall(), columns=column_names)
        st.write(df)
