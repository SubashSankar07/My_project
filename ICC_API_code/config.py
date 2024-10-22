import mysql.connector
import json
import requests
from dynaconf import Dynaconf


settings = Dynaconf(
    settings_files=['.secrets.toml']
)
shost=settings.db_host
sdatabase=settings.db_database
suser=settings.db_user
spassword=settings.db_password

connection = mysql.connector.connect(host=shost,database=sdatabase,user=suser,password=spassword)
cursor = connection.cursor(buffered=True)

tables = {'agent_info_raw': 'agent','exportcall_raw': 'export'}

json_inserted_rows_data = {}
startdate='2023-01-01'
enddate=startdate
export_url=settings.export_url
agent_url=settings.agent_url
freezed_column_list = [
    'project_id', 'project_name', 'lead_entry_date', 'source', 'customer_name', 'call_date',
    'status', 'user', 'full_name', 'campaign_id', 'list_id', 'phone_number', 'cust_talk_sec',
    'lead_id', 'call_type', 'list_name', 'main_disposition', 'customer_hold_time', 'wrap_up_time',
    'queue_seconds', 'customer_number', 'uic_id', 'ingroup_name', 'registered_number_of_the_customer',
    'caller_name', 'uhid', 'corporate_name', 'claim_type', 'claim_no', 'claim_status', 'caller_type',
    'query_typelevel_1', 'query_type_level_2', 'subquery_type_level_2', 'additional_comments', 'callbacks_required',
    'call_transferred_to_survey_ivr', 'ingroup_list', 'resolution', 'mobilenumber', 'states', 'emailid',
    'reasonfornotselectedifno', 'rightparty', 'campaignname', 'currentlocation', 'phonenumber', 'altnumber',
    'selected', 'customername', 'folionumber', 'pannumber', 'casetype', 'typesubtype', 'paymentstatus',
    'paymentstatuskfin', 'documentstatus', 'assignedteam', 'remarks'
]

def fetch_data_function(query):
    cursor.execute(query)
    data=cursor.fetchone()[0]
    return data

def execute_function(query):
    cursor.execute(query)
    connection.commit()

def status_table_inserting(tablename,API_rows_Number):  
    table_row_count_query=f"select count(*) from {tablename} where date='{startdate}';"
    table_row_count=fetch_data_function(table_row_count_query)
    status_table_insert_query=f'''insert into status (table_name,json_rows,inserted_rows,db_name) values('{tablename}',{API_rows_Number[0]},{table_row_count},'test');'''
    execute_function(status_table_insert_query)

def time_error_function(value):
    time = []
    for each_value in value:
        try:
            hours, minutes, seconds = map(int, each_value.split(':'))
            total_seconds = (hours * 3600) + (minutes * 60) + seconds
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            time.append('{:02d}:{:02d}:{:02d}'.format(hours, minutes, seconds))
        except (AttributeError, ValueError):
            time.append(each_value)
    return time

def extracting_api_data(url,date):
        required_date = str(date.date())
        payload = json.dumps({"from_date": required_date, "to_date": required_date})
        response= requests.post(url, payload)
        API_response = response.json()['data']
        API_information=response.json()['message']
        API_rows_Number=[int(i) for i in API_information.split() if i.isdigit()]
        return API_response,API_rows_Number,date

port = settings.port
smtp_server = settings.smtp_server
login = settings.login
password = settings.password
sender_email = settings.sender_email
receiver_email = settings.receiver_email
