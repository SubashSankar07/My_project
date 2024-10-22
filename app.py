import streamlit as st
from PIL import Image
import pandas as pd
import psycopg2
# import xlrd
from datetime import datetime as dt
from datetime import datetime

from config import dataset_list,dataset_type_list,connection,establish_connection,execute_query
from psycopg2 import pool, OperationalError
from file_uploader import fetch_data_function,quality,excel_files_validation,emp_detail,tni_efficacy,bq_refresher,nesting_track,status_table,audit_complaince,agent_roster,new_hire_batch,forecast,master_datapkt,login_logout,attrition_tracker
#-------------------------authentication---------------------#



#--------------------------page icon-------------------------#
image_fevicon=Image.open('fevicon.png')# image for fevicon in the page

st.set_page_config (page_title="3IFutureTech", page_icon=image_fevicon, layout="wide") # Heading of the page given image for web page fevicon.


#
#--------Header Scection-------#
image_sidebar = Image.open("3i-logo.png")# here is the image for logo in sidebar
st.sidebar.image(image_sidebar, width=200)#here calling the above image_sidebar to represent in webpage.
st.title("Insights Command Center")#here given page header.
#-------------------------------------------------------
# trails for date column 
#------------------------------------------------------
import datetime
selected_date = st.date_input("Please Select Date", [])
if selected_date ==():
    st.warning("Please select date")
else:
    try:
        start_date, end_date = selected_date
        start_date = datetime.datetime.strptime(str(start_date), '%Y-%m-%d')
        end_date = datetime.datetime.strptime(str(end_date), '%Y-%m-%d')
        current_date = start_date
        date_str = "" 
        while current_date <= end_date:
            date_str += current_date.strftime('%Y-%m-%d') + ", "
            current_date += datetime.timedelta(days=1)
        date_str = date_str[:-2] # Remove the final comma and space
        st.write(date_str) 
    except ValueError:
        st.write('please select end date')




dataset_dropdown,dataset_type_dropdown=st.columns(2) # Here giving partition of two dropdowns in a single column,for dataset and dataset_type


with dataset_dropdown:
    # here dataset options means excel file names that will occur in frontend UI dropdown 
    dataset_options = st.selectbox(
        "Select:",["Select data"]+dataset_list)
    if dataset_options in dataset_list:
        st.write('You Selected')
    else:
        st.write('Please select one dataset')
with dataset_type_dropdown:
    # here dataset type means file type of that respective selected file .
    dataset_type = st.selectbox('Select:',['Select File Type']+ dataset_type_list)
    if dataset_type in dataset_type_list:
        st.write('You Selected')
    else:
        st.write('Please select one file')


#---------------------------------------------Functions_calling-----------------------------------------#
#here given a dictionary for set of files list that are going to upload.
file_format_ident = {
    "audit_complaince": audit_complaince, 
    "Quality":quality,
    "attrition_tracker":attrition_tracker,
    "nesting_tracker":nesting_track,
    "Forecast":forecast,
    "new_hire_batch":new_hire_batch,
    "datawisepkt":master_datapkt,
    "Agent_roster":agent_roster,
    "Active_Hc_count":emp_detail,
    "login_logout":login_logout,
    "BQ_Refresher":bq_refresher,
    "Tni_Efficacy":tni_efficacy

}

# here given this for if the options are not selected then just pass ,else based on the dataset_option call that funciton to execute.



# if dataset_options == 'Select data' or dataset_type == 'Select File Type':
#     pass
# elif dataset_options == 'Active_Hc_count':
#     emp_detail(connection)
# else:
#     if selected_date != ():
#         cursor = connection.cursor()
#         try:
#             date_list = date_str.split(", ")
#             for date_str_2 in date_list:
#                 query = f'''select count(*) from {dataset_options} where date='{date_str_2}';'''
#                 count = fetch_data_function(query)
#                 st.write(f'here is information about the {date_str_2}: ', count)
#             connection.commit()  # Commit the transaction if everything is successful
#         except Exception as e:
#             connection.rollback()  # Rollback the transaction if an error occurs
#             st.write("Error:", str(e))
#         finally:
#             cursor.close()
#     else:
#         st.write('Please select a date')

#     if count==0:
#         excel_files_validation(file_format_ident[dataset_options],date_str, dataset_options)
#     elif count>=0:
#         excel_files_validation(file_format_ident[dataset_options],date_str, dataset_options)



# Example usage
connection = establish_connection()

if dataset_options == 'Select data' or dataset_type == 'Select File Type':
    pass
elif dataset_options == 'Active_Hc_count':
    emp_detail(connection)
else:
    if selected_date != ():
        cursor = connection.cursor()
        try:
            date_list = date_str.split(", ")
            for date_str_2 in date_list:
                query = f'''select count(*) from {dataset_options} where date='{date_str_2}';'''
                execute_query(connection, query)
                count = fetch_data_function(query)
                st.write(f'here is information about the {date_str_2}: ', count)
            connection.commit()  # Commit the transaction if everything is successful
        except Exception as e:
            connection.rollback()  # Rollback the transaction if an error occurs
            st.write("Error:", str(e))
        finally:
            cursor.close()
    else:
        st.write('Please select a date')

    if count == 0:
        excel_files_validation(file_format_ident[dataset_options], date_str, dataset_options)
    elif count >= 0:
        excel_files_validation(file_format_ident[dataset_options], date_str, dataset_options)

# Close the connection outside the loop or reuse it in subsequent operations
connection.close()


            
        


            
        