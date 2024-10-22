
# below two lists are data file names and types of files.
dataset_list=['Quality','Forecast','audit_complaince','attrition_tracker','nesting_tracker','new_hire_batch','datawisepkt','Agent_roster','login_logout','Active_Hc_count','BQ_Refresher','Tni_Efficacy']
dataset_type_list=['xlsx', '.csv','xlsb','xls']


# below header_conversion is for changing column names into new names as per given.
header_conversion = {
    'Did the agent follow Opening Verbiage | Introduce self & FHPL': 'Greetings1_binary',
    'Did the agent follow Opening Verbiage | Introduce self & FHPL.1': 'Greetings1_values',
    'Was Greeting given within 3 of receiving the call': 'Greeting2_binary',
    'Was Greeting  given within 3 of receiving the call': 'Greeting2_values',
    'Captured data required to help customer - NAme,Phone,Email (Not mandate), Dependent NAme': 'Authentication1_binary',
    'Captured data required to help customer - NAme,Phone,Email (Not mandate), Dependent NAme.1': 'Authentication1_values',
    'Conduct a quick scan of the application in the CRM - Refer previous case history ': 'Authentication2_binary',
    'Conduct a quick scan of the  application in the CRM - Refer previous case history': 'Authentication2_values',
    'Asked question "How may I help you today"': 'Issue_Identification1_binary',
    'Asked question "How may I help you today".1': 'Issue_Identification1_values',
    'Is agent Listening attentively to everything what the customer said without interrupting': 'Issue_Identification2_binary',
    'Is agent Listening attentively to everything what the customer said without interrupting ': 'Issue_Identification2_values',
    'Probe with relevant & precise questions to reach to the exact customer issue without overdoing it.': 'Probing1_binary',
    'Probe with relevant & precise questions to reach to the exact customer issue without overdoing it..1': 'Probing1_values',
    'Agent paraphrased to understand requirement/query (wherever required)': 'Probing2_binary',
    'Agent paraphrased to understand requirement/query (wherever required).1': 'Probing2_values',
    'Agent display willingness to help, using "Please & Thank you" when required': 'Call_Etiquette1_binary',
    'Agent display willingness to help, using "Please & Thank you" when required.1': 'Call_Etiquette1_values',
    'Did agent adhere to the hold protocol and "Dead air" should be within threshold (>= 15 Sec)': 'Call_Etiquette2_binary',
    'Did agent adhere to the hold protocol and "Dead air" should be within threshold (>= 15 Sec).1': 'Call_Etiquette2_values',
    'Agent Used simple, honest language. Avoid jargons.': 'Call_Etiquette3_binary',
    'Agent Used simple, honest language. Avoid jargons..1': 'Call_Etiquette3_values',
    'Be optimistic & Enthusiastic - Agent did not sound robotic , sounded confident with accurate rate of speech': 'Call_Etiquette4_binary',
    'Be optimistic & Enthusiastic - Agent did  not sound robotic , sounded confident with accurate rate of speech': 'Call_Etiquette4_values',
    'Believe/trust our customers and build a rapport with the customer': 'Call_Etiquette5_binary',
    'Believe/trust our customers and build a rapport with the customer.1': 'Call_Etiquette5_values',
    'Agent used grammatically correct sentences , switched to regioNAl language when needed': 'Call_Etiquette6_binary',
    'Agent used grammatically correct sentences , switched to regioNAl language when needed.1': 'Call_Etiquette6_values',
    'Give a heartfelt apology or genuine empathy for what has happened (wherever required).Congratulate for New born baby, new wedded spouse | In event of death/accident "Sorry to hear about this"': 'Empathy_and_Sympathy1_binary',
    'Give a heartfelt apology or genuine empathy for what has happened (wherever required).Congratulate for New born baby, new wedded spouse | In event of death/accident "Sorry to hear about this".1': 'Empathy_and_Sympathy1_values',
    'Clarity of speech while delivering the information to the customer': 'Empathy_and_Sympathy1_binary1',
    'Clarity of speech while delivering the information to the customer ': 'Empathy_and_Sympathy1_values1',
    "Commitment to own the customer's issue and provide the appropriate solution.\xa0": "Ownership1_binary",
    "Commitment to own the customer's issue and provide the appropriate solution.\xa0.1": 'Ownership1_values',
    'Customers query was addressed with positive approach and confidence': 'Resolution1_binary',
    'Customers query was addressed with positive approach and confidence.1': 'Resolution1_values',
    'All Query of the customer were addressed - Provide correct TAT/OTRS number where needed ': 'Resolution2_binary',
    'All Query of the customer were addressed - Provide correct  TAT/OTRS number where needed': 'Resolution2_values',
    'Agent set expectation & explain next steps to ensure customer is not confused': 'Resolution3_binary',
    'Agent set expectation & explain next steps to ensure customer is not confused.1': 'Resolution3_values',
    'Did agent utilize all recourses - CRM/Knowledge base/Supervisor - All tools NAvigated effectively': 'Resolution4_binary',
    'Did agent utilize all recourses - CRM/Knowledge base/Supervisor - All tools NAvigated effectively.1': 'Resolution4_values',
    'Proactively look for the most appropriate solution to prevent future calls.': 'Resolution5_binary',
    'Proactively look for the most appropriate solution to prevent future calls..1': 'Resolution5_values',
    'Provided self-help options, Tool free# & Email ID wherever required': 'Resolution6_binary',
    'Provided self-help options, Tool free# & Email ID wherever required.1': 'Resolution6_values',
    'Identified interNAl/exterNAl escalation triggered and handled it efficiently (Involve supervisor where needed)': 'Resolution7_binary',
    'Identified interNAl/exterNAl escalation triggered and handled it efficiently (Involve supervisor where needed).1': 'Resolution7_values',
    'Agent tagged and documented the call': 'Tagging_and_Documentation_binary',
    'Agent tagged and documented the call ': 'Tagging_and_Documentation_values',
    'Summarize the resolution provided on the call': 'Summarization1_binary',
    'Summarize the resolution provided on the call ': 'Summarization1_values',
    'Validated all issue were addressed "Is there anything else I can assist you with"': 'Summarization2_binary',
    'Validated all issue were addressed "Is there anything else I can assist you with".1': 'Summarization2_values',
    'Educated the customer about Survey IVR for all FTR calls': 'Summarization3_binary',
    'Educated the customer about Survey IVR for all FTR calls.1': 'Summarization3_values',
    'Used the closing verbiage and thank the customer for his time': 'Closing _binary',
    'Used the closing verbiage and thank the customer for his time.1': 'Closing_values',
    'Rude behavior observed / abusing customer': 'Zero_Tolerance1_binary',
    'Rude behavior observed / abusing customer.1': 'Zero_Tolerance1_values',
    'Incorrect/Incomplete info provided on the call - Incorrect Resolution': 'Zero_Tolerance2_binary',
    'Incorrect/Incomplete info provided on the call - Incorrect Resolution.1': 'Zero_Tolerance2_values',
    'Forced Closure/Call disconnection/Instigated call abandoned due to long hold & dead air': 'Zero_Tolerance3_binary',
    'Forced Closure/Call disconnection/Instigated call abandoned due to long hold & dead air.1': 'Zero_Tolerance3_values',
    'Over persoNAlization (Flirting / Sharing PersoNAl Information with Customer)': 'Zero_Tolerance4_binary',
    'Over persoNAlization (Flirting / Sharing PersoNAl Information with Customer).1': 'Zero_Tolerance4_values',
    '(Security check )  policy holder DOB, Registered MB,Email, Verify All Dependent NAmes': 'Zero_Tolerance5_binary',
    '(Security check )  policy holder DOB, Registered MB,Email, Verify All Dependent NAmes.1': 'Zero_Tolerance5_values',
    'Tagging Guidelines followed': 'Compliance1_binary',
    'Tagging Guidelines followed.1': 'Compliance1_values',
    'DT Followed on all calls': 'Compliance2_binary',
    'DT Followed on all calls.1': 'Compliance2_values',
    'FTR Transferred to Survey IVR': 'Compliance3_binary',
    'FTR Transferred to Survey IVR.1': 'Compliance3_values',
    'Escalation process followed': 'Compliance4_binary',
    'Escalation process followed.1': 'Compliance4_values'
}

from dynaconf import Dynaconf
import psycopg2
#importing library dynacof to connect the secrets toml file. 
# import psycopg2 library for connection to database  
settings = Dynaconf(
    settings_files=['.secrets.toml']
)
# calling funtions from secrets toml file 
secret_host = settings.host
secret_port = settings.port
secret_database = settings.database
secret_user = settings.user
secret_password = settings.password

##new code for stable connection ----############
def establish_connection():
    return psycopg2.connect(
        host=secret_host,
        port=secret_port,
        database=secret_database,
        user=secret_user,
        password=secret_password
    )

def execute_query(connection, query, parameters=None):
    retry_count = 0
    while retry_count < 3:  # Set your desired max retry count
        try:
            if connection.closed:
                # Re-establish the connection if it's closed
                connection = establish_connection()
            with connection, connection.cursor() as cursor:
                cursor.execute(query, parameters)
                # Commit the transaction if necessary
                connection.commit()
            break
        except psycopg2.OperationalError as e:
            print(f"Error executing query: {e}")
            print(f"Retrying in 5 seconds...")
            connection.rollback()
            retry_count += 1
            if retry_count == 3:
                raise  # If max retries reached, raise the exception


#connection details for DB 
connection = psycopg2.connect(host=secret_host, port=secret_port,database=secret_database, user=secret_user, password=secret_password)

