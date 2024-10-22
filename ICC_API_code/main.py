import pandas as pd
import pandas as pd
import operator as op
from datetime import date
import smtplib 
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import startdate,enddate,export_url,agent_url,freezed_column_list,fetch_data_function,execute_function,extracting_api_data,time_error_function,status_table_inserting,cursor,json_inserted_rows_data,tables,port,sender_email,smtp_server,password,login,receiver_email


def validation():
    for table_name,api_fun in tablename.items():
        count_query = f"select count(*) from {table_name} where date='{startdate}';"
        data_count = fetch_data_function(count_query)
        if data_count == 0:
            api_fun(table_name)
        else:
            delete_query = f"delete from {table_name} where date='{startdate}';"
            execute_function(delete_query)
            api_fun(table_name)

def agent_api_to_sdb(table_name):
    for date in pd.date_range(startdate,enddate):
        API_response,API_rows_Number,date = extracting_api_data(agent_url,date)
        day_counter=0
        for each_agent in API_response :
            day_counter=day_counter+1
            if each_agent["breaks"]:
                agent_info_dict = {k.lower():v for k, v in each_agent["breaks"].items()}          
                each_agent.update({"breaks":'0'})
                each_agent.update(agent_info_dict)
                value=tuple(each_agent.values())
                final_column=",".join(tuple(each_agent.keys()))
            try:
                agnet_data_insert_query="insert into agent_info_raw (%s) values %s;" %(final_column,value)
                execute_function(agnet_data_insert_query)
                date_update_query="update agent_info_raw set date = '%s' where date is null;" %(date.date())
                execute_function(date_update_query)
            except Exception as e: 
                time=time_error_function(value) 
                agnet_data_insert_query="insert into agent_info_raw (%s) values %s;" %(final_column,tuple(time))
                execute_function(agnet_data_insert_query)
                date_update_query="update agent_info_raw set date = '%s' where date is null;"%(date.date())
                execute_function(date_update_query)
                execute_function("ROLLBACK")
        print(date, day_counter)
        status_table_inserting(table_name,API_rows_Number)

def export_api_to_sdb(table_name):
    for date in pd.date_range(startdate,enddate):
        API_response,API_rows_Number,date = extracting_api_data(export_url,date)
        rows_counter = 0
        for index,each_row in enumerate(API_response):
            rows_counter = rows_counter + 1
            export_call_dict={k.lower():v for k, v in each_row.items()}
            columns = [i for i in export_call_dict.keys() if op.countOf(freezed_column_list,i)>0 ]
            final_columns = ",".join(columns) 
            value=tuple([str(r) for r in (list( map(export_call_dict.get, columns))) ])
            try:
                insert_query='''insert into exportcall_raw (%s) values %s;'''  %(final_columns,value)
                execute_function(insert_query)
                date_update_query="update exportcall_raw set date = '%s' where date is null;"%(date.date())
                execute_function(date_update_query)
            except Exception as e:
                print(e)
                execute_function("ROLLBACK")
                print(date, rows_counter)
        print(date, rows_counter)
        status_table_inserting(table_name,API_rows_Number)

tablename = {'exportcall_raw': export_api_to_sdb, 'agent_info_raw': agent_api_to_sdb}

def final_database():
    for table_name, prefix in tables.items():
        json_data_query = f"SELECT json_rows FROM status WHERE table_name='{table_name}' AND DATE_FORMAT(timestamp, '%Y-%m-%d')='{date.today()}';"
        json_rows=fetch_data_function(json_data_query)
        inserted_data_query = f"SELECT inserted_rows FROM status WHERE table_name='{table_name}' AND DATE_FORMAT(timestamp, '%Y-%m-%d')='{date.today()}';"
        inserted_rows = fetch_data_function(inserted_data_query)
        json_inserted_rows_data[f'{prefix}_json_rows_count'] = json_rows
        json_inserted_rows_data[f'{prefix}_insert_rows_count'] = inserted_rows
    cursor.close() 
    return json_inserted_rows_data

def email_notification():
    rows_data=final_database()
    message = MIMEMultipart("alternative")
    message["Subject"] = "ICC daily data insert API to SDB"
    message["From"] = sender_email
    message["To"] = receiver_email
    # write the text/plain part
    text = f"""\
    Dear Team,
    Exportcall_report API rows-{rows_data['export_json_rows_count']}
    Exportcall_report inserted rows-{rows_data['export_insert_rows_count']}
    Agent_info API rows-{rows_data['agent_json_rows_count']}
    Agent_info inserted rows-{rows_data['agent_insert_rows_count']}
    """
    # write the HTML part
    html = f"""\
    <html>
      <body>
        <p>Dear Team,<br>
        <p>Exportcall_report API rows-{rows_data['export_json_rows_count']}</p>
        <p>Exportcall_report inserted rows-{rows_data['export_insert_rows_count']}</p>
        <p>Agent_info API rows-{rows_data['agent_json_rows_count']}</p>
        <p>Agent_info inserted rows-{rows_data['agent_insert_rows_count']}</p>
      </body>
    </html>
    """
    # convert both parts to MIMEText objects and add them to the MIMEMultipart message
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")
    message.attach(part1)
    message.attach(part2)
    # send your email
    s = smtplib.SMTP(smtp_server, port)
    s.starttls()
    s.login(login,password)
    s.sendmail(sender_email, receiver_email, message.as_string())
    s.quit()

validation()
email_notification()