from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime,timedelta,date
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders
import mysql.connector
import requests
import pandas as pd
import json
import os
import pendulum
import operator as op
import re


connection = mysql.connector.connect(host='host.docker.internal',database='test',user='root', password='mypassword')
startdate=date.today() - timedelta(days=1)
enddate=date.today() - timedelta(days=1)
export_url = 'http://14.140.2.27:9086/rightside/webservices/custom_exportcall_report'
agent_url='http://14.140.2.27:9086/rightside/webservices/custom_apr_report'
column_list=['project_id','project_name','lead_entry_date','source','customer_name','call_date','status','user','full_name','campaign_id','list_id','phone_number','cust_talk_sec','lead_id','call_type','list_name','main_disposition','customer_hold_time','wrap_up_time','queue_seconds','customer_number','uic_id','ingroup_name','registered_number_of_the_customer','caller_name','uhid','corporate_name','claim_type','claim_no','claim_status','caller_type','query_typelevel_1','query_type_level_2','subquery_type_level_2','additional_comments','callbacks_required','call_transferred_to_survey_ivr','ingroup_list','resolution','mobilenumber','states','emailid','reasonfornotselectedifno','rightparty','campaignname','currentlocation','phonenumber','altnumber','selected','customername','folionumber','pannumber','casetype','typesubtype','paymentstatus','paymentstatuskfin','documentstatus','assignedteam','remarks',]



default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'retries': 1,
   'retry_delay':timedelta(minutes=10)
}

def validation():
    cursor=connection.cursor()
    s=f"select count(*) from exportcall_raw where date='{startdate}';"
    cursor.execute(s) 
    count= cursor.fetchone()
    if count==0:
        export_api_to_sdb()
    else:
        s=f"delete from exportcall_raw where date='{startdate}';"
        cursor.execute(s)
        connection.commit()
        export_api_to_sdb()
        
def validation1():
    cursor=connection.cursor()
    s=f"select count(*) from agent_info_raw where date='{startdate}';"
    cursor.execute(s) 
    count= cursor.fetchone()
    if count==0:
        agent_api_to_sdb()
    else:
        s=f"delete from agent_info_raw where date='{startdate}';"
        cursor.execute(s)
        connection.commit()
        agent_api_to_sdb()

#### export_report to staging db
def export_api_to_sdb():
    cursor = connection.cursor()
    for x in pd.date_range(startdate,enddate):
        required_dte = str(x.date())
        payload = json.dumps({"from_date": required_dte, "to_date": required_dte})
        resp = requests.post(export_url, payload)
        res = resp.json()['data']
        msg=resp.json()['message']
        message1=[int(i) for i in msg.split() if i.isdigit()]
        day_counter = 0
        for i, each_row in enumerate(res): 
            day_counter = day_counter + 1
            new_dict={k.lower():v for k, v in each_row.items()}
            # a=new_dict.values()
            c=new_dict.keys()
            result = [i for i in c if op.countOf(column_list,i)>0 ]
            a= list( map(new_dict.get, result) )
            st = ''
            for _, item in enumerate(result):
                if _ == 0:
                    st = st + item
                    continue
                else:
                    st = st+","+ item
            b=[str(r) for r in a ]
            value=tuple(b)
            try:
                s='''insert into exportcall_raw (%s) values %s;'''  %(st,value)
                cursor.execute(s)
                cursor.execute("update exportcall_raw set date = %s where date is null;", [x.date()])
                connection.commit() 
            except Exception as e:
                print(e)
                print(s)
                # day_counter = day_counter + 1
                cursor.execute("ROLLBACK")
                connection.commit()
                print(x.date(), day_counter)
        print(x.date(), day_counter)
        count_export=f"select count(*) from exportcall_raw where date='{startdate}';"
        cursor.execute(count_export) 
        result=cursor.fetchone()[0]
        print(result)
        sql=f'''insert into status (table_name,json_rows,inserted_rows,db_name) values('exportcall_raw',{message1[0]},{result},'test');'''
        cursor.execute(sql)
        connection.commit()
    

   ### agent API to stagingg db
#### agent to staging db
def agent_api_to_sdb():
    cursor = connection.cursor()
    for x in pd.date_range(startdate,enddate):
        required_dte = str(x.date())
        payload = json.dumps({"from_date": required_dte, "to_date": required_dte})
        resp = requests.post(agent_url, payload)
        res = resp.json()['data']
        msg=resp.json()['message']
        message2=[int(i) for i in msg.split() if i.isdigit()]
        day_counter = 0
        new_dict = {}
        for each_agent in res:
            day_counter=day_counter+1
            if each_agent["breaks"]:
                new_dict = {k.lower():v for k, v in each_agent["breaks"].items()}          
                each_agent.update({"breaks":'0'})
                each_agent.update(new_dict)
                column=tuple(each_agent.keys())
                value=tuple(each_agent.values())
                st = ''
                for _, item in enumerate(column):
                    if _ == 0:
                        st = st + item
                        continue
                    else:
                        st = st+","+ item
            try:
                s="insert into agent_info_raw (%s) values %s;" %(st,value)
                cursor.execute(s)
                cursor.execute("update agent_info_raw set date = %s where date is null;", [x.date()])
                connection.commit()
            except Exception as e:
                data=value
                time=[]
                for r in data:
                    try: 
                        hours, minutes, seconds = map(int, r.split(':'))
                    except AttributeError: 
                        time.append(r)
                        continue
                    except ValueError:
                        time.append(r)
                        continue
                    total_seconds = (hours * 3600) + (minutes * 60) + seconds
                    if seconds >= 60:
                        minutes += 1
                        seconds -= 60
                    seconds=(hours * 3600) + (minutes * 60) + seconds
                    hours, remainder = divmod(seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    times='{:02d}:{:02d}:{:02d}'.format(hours, minutes, seconds)
                    time.append(times)
                s="insert into agent_info_raw (%s) values %s;" %(st,tuple(time))
                cursor.execute(s)
                cursor.execute("update agent_info_raw set date = %s where date is null;", [x.date()])
                connection.commit()
                cursor.execute("ROLLBACK")
                print('error_data',day_counter)
        print(x.date(), day_counter)
        count_export=f"select count(*) from agent_info_raw where date='{startdate}';"
        cursor.execute(count_export) 
        result=cursor.fetchone()[0]
        print(result)
        sql=f'''insert into status (table_name,json_rows,inserted_rows,db_name) values('agent_info_raw',{message2[0]},{result},'test');'''
        cursor.execute(sql)
        connection.commit()
        
def final_database():
    cursor = connection.cursor(buffered=True)
    agent_json=f"select json_rows from status where table_name='agent_info_raw' and DATE_FORMAT(timestamp, '%Y-%m-%d')='{date.today()}';"
    cursor.execute(agent_json) 
    agent_json_rows=cursor.fetchone()[0]
    print(agent_json_rows)
    agent_inserted=f"select inserted_rows from status where table_name='agent_info_raw' and DATE_FORMAT(timestamp, '%Y-%m-%d')='{date.today()}';"
    cursor.execute(agent_inserted) 
    agent_inserted_rows=cursor.fetchone()[0]
    print(agent_inserted_rows)
    export_json=f"select json_rows from status where table_name='exportcall_raw' and DATE_FORMAT(timestamp, '%Y-%m-%d')='{date.today()}';"
    cursor.execute(export_json) 
    export_json_rows=cursor.fetchone()[0]
    print(export_json_rows)
    export_inserted=f"select inserted_rows from status where table_name='exportcall_raw' and DATE_FORMAT(timestamp, '%Y-%m-%d')='{date.today()}';"
    cursor.execute(export_inserted) 
    export_inserted_rows=cursor.fetchone()[0]
    print(export_inserted_rows)
    connection.close() 
    return{'agent_json':agent_json_rows,'agent_insert':agent_inserted_rows,'export_json':export_json_rows,'export_insert':export_inserted_rows}
    
def email_notification(agent_json,agent_insert,export_json,export_insert):
    port = 587
    smtp_server = "smtp.gmail.com"
    login = "prchs44@gmail.com" # paste your login generated by Mailtrap
    password = "byyjjhdevzgdzlue" # paste your password generated by Mailtrap
    sender_email = "prchs44@gmail.com"
    receiver_email = "polamarasetti.rajachandrasekhar@3i-infotech.com"
    message = MIMEMultipart("alternative")
    message["Subject"] = "ICC daily data insert API to SDB"
    message["From"] = sender_email
    message["To"] = receiver_email
    # write the text/plain part
    text = f"""\
    Dear Team,
    Exportcall_report API rows-{export_json}
    Exportcall_report inserted rows-{export_insert}
    Agent_info API rows-{agent_json}
    Agent_info inserted rows-{agent_insert}
    """
    # write the HTML part
    html = f"""\
    <html>
      <body>
        <p>Dear Team,<br>
        <p>Exportcall_report API rows-{export_json}</p>
        <p>Exportcall_report inserted rows-{export_insert}</p>
        <p>Agent_info API rows-{agent_json}</p>
        <p>Agent_info inserted rows-{agent_insert}</p>
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
    
    
with DAG(default_args=default_args,
         dag_id='raja',
         schedule_interval='0 1 * * * ',
         start_date=pendulum.datetime(year=2023, month=1, day=19,tz="utc"),
         end_date=None,
         catchup=False
         )as dag:
    export_api_to_sdb_ = PythonOperator(
    task_id="API_to_exportcall_raw",
    python_callable=validation,
    )
    
    final_database_=PythonOperator(
    task_id="main_database",
    python_callable=final_database
    )
        
    agent_api_to_sdb_=PythonOperator(
    task_id="API_to_agent_raw",
    python_callable=validation1,
    )

    email_notifications=PythonOperator(
    task_id="email_sender",
    python_callable=email_notification,
    op_kwargs=final_database_.output
    )

agent_api_to_sdb_>> final_database_>>email_notifications
export_api_to_sdb_>> final_database_>>email_notifications