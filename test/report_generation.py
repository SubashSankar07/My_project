# Import libraries
import pytz
import smtplib, ssl
import psycopg2 
import pandas as pd
import datetime 
import pendulum
import openpyxl
from airflow import DAG
from airflow.operators.python import PythonOperator
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication


# Convert IST to UTC
def IST2UTC(date_timestamp):
    ist_timezone = pytz.timezone("Asia/Kolkata")
    ist_datetime = ist_timezone.localize(date_timestamp)
    utc_datetime = ist_datetime.astimezone(pytz.utc)
    utc_timestamp = utc_datetime.strftime("%Y-%m-%d %H:%M:%S")
    return utc_timestamp

# Convert UTC to IST
def UTC2IST(date_timestamp):
    utc_timezone = pytz.utc
    ist_timezone = pytz.timezone("Asia/Kolkata")
    ist_datetime = utc_timezone.localize(date_timestamp).astimezone(ist_timezone)
    ist_datetime_str = ist_datetime.strftime("%Y-%m-%d %H:%M:%S")
    return ist_datetime_str

# Function for Generating Report and Trigger Mail
def main():
    current_date = datetime.datetime.now().date()
    timestamp = datetime.time(0, 0, 0, 0)
    today_with_timestamp = datetime.datetime.combine(current_date, timestamp)

    utc_timestamp = IST2UTC(today_with_timestamp)
    # Connecting to DB
    connection = psycopg2.connect(database="superset",user="superset_user",password="Superset-2023",host="3.108.122.116",port= '5432')
    connection1 = psycopg2.connect(database="reporting_db",user="reporting_user",password="Reporting-2023",host="3.108.122.116",port= '5432')
    

    cursor = connection.cursor()
    cursor1 = connection1.cursor()

    query = f"""select first_name, last_name, username, last_login from ab_user where last_login > '{utc_timestamp}' order by last_login ASC;"""
    cursor.execute(query)
    results = cursor.fetchall()
    # Report Creation
    df = pd.DataFrame(results, columns=["First Name", "Last Name", "Employee ID", "Last Login"])
    df['Last Login'] = df['Last Login'].apply(UTC2IST)

    query1 = f'''delete from daily_login_report where date='{current_date}';'''
    cursor1.execute(query1)
    for i, row in df.iterrows():
        value = row.values
        try:
            query2 = '''INSERT INTO daily_login_report VALUES (%s,%s,%s,%s);'''
            cursor1.execute(cursor1.mogrify(query2, value))
            connection1.commit()
        except Exception as e:
            cursor1.execute("ROLLBACK")
            connection1.commit()
            print(e)  
    
    df['Date'] = current_date

    # Save as an xlsx file
    df.to_excel(f"Daily_User_Login_Report.xlsx", index=False)
    ### EMAIL TRIGGERING
    # Email configuration for gmail
    smtp_server = 'smtp.gmail.com' 
    context = ssl.create_default_context()

    # For SMTP server address
    smtp_port = 587  #  SMTP server port
    sender_email = 'icctestuser2023@gmail.com'
    sender_password = 'iulk fjgk zwlj pjsa'

    # recipients='ABIRAMI.L@3i-infotechbpo.com,austin.rodrigues@3i-digitalbps.com,MERVYN.DAVID@3i-digitalbps.com,\
    #     shalini.gupta@3i-infotechbpo.com,arup.chowdhury@3i-digitalbps.com,partha.sil@3i-infotech.com'
    
    recipients='partha.sil@3i-infotech.com'
    
    # Create the email message
    subject = 'ICC Daily Users Login Report'
    message ="""Hi Team, 
                Please find the attached login details report for your reference.
                
    With Regards,
    Partha Sil
    Junior Data Scientist
    3i-infotech Pvt. Ltd."""

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipients
    msg['Subject'] = subject

    msg.attach(MIMEText(message, 'plain'))

    # attach the XLSX file to the email
    with open(f'Daily_User_Login_Report.xlsx', 'rb') as f:
        attach = MIMEApplication(f.read(), _subtype='xlsx')
        attach.add_header('Content-Disposition', 'attachment', filename=f'Daily_User_Login_Report_{current_date}.xlsx')
        msg.attach(attach)

    # Connect to the SMTP server
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls(context=context)  # Use TLS encryption
        server.login(sender_email, sender_password)

        # Send the email
        server.sendmail(sender_email, recipients.split(','), msg.as_string())

        print("Email sent successfully")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

'''
The DAG is scheduled to run daily at 23:59(IST) 
 It has a start date of September 28, 2023, and does not have an end date specified. 
 The catchup parameter is set to False, meaning that Airflow will only schedule and execute tasks for future runs and not catch up on missed runs.
''' 
# Define the default_args dictionary with your desired start_date and other parameters.
default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'retries':1 ,
}  

with DAG(default_args=default_args,
        dag_id='DAILY_LOGIN_REPORT_GENERATION',
        start_date=pendulum.datetime(year=2023, month=11, day=18,tz='Asia/Kolkata'),
        schedule_interval='59 23 * * *',
        end_date=None,
        catchup=False
        )as dag:
        
    daily_user_login_report = PythonOperator(
        task_id="report_generation_trigger_mail",
        python_callable=main,
        dag=dag,
    )







