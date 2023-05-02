# ------------------------------------------- SECTION 1 --------------------------------------------------
# # Step 1 Import Libraries
import os
import configparser
import snowflake.connector
import pandas as pd
import streamlit as st

# # Page config must be set
st.set_page_config(
    layout="wide",
    page_title="Snowflake File Upload Interface"
)

#[SNOWFLAKEPOC]
#user = "********"
#password = "********"
#account = "po27287.central-india.azure"
#warehouse = "COMPUTE_WH"

# # Step 2 Create your connection parameters
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["SNOWFLAKEPOC"], client_session_keep_alive=True
    )

conn = init_connection()

env_selected = st.sidebar.selectbox("Select value",['DEV','QUA','PROD'])

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()
run_query("USE ROLE ACCOUNTADMIN;")
rows = run_query("SELECT EMPID,SALARY from sf_demo.sf_demo.emp_salary;")
pd_rows = pd.DataFrame(rows)
#st.write(pd_rows.columns)
pd_rows.columns = ['EMPID','SALARY']
#st.write(pd_rows.columns)                 
pd_rows = pd_rows.groupby(["EMPID"]).sum()

st.table(pd_rows)
st.write("**User selected**",env_selected)