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
    page_title="Streamlit Summit - Global ESG View point "
)


# # Step 2 Create your connection parameters
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["SNOWFLAKEPOC"], client_session_keep_alive=True
    )

conn = init_connection()

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

run_query("USE ROLE SYSADMIN;")

#esg_raw_data  = run_query("select csrhub_id,company_name,isin,ticker,industry_desc,dj30tag,community,employees,environment,governance,rating_date,rating_status from csrhub.public.faststarttrial ;")
esg_raw_df = pd.read_sql("SELECT * FROM csrhub.public.faststarttrial;", conn)

pd_esg_raw_df = pd.DataFrame(esg_raw_df)
pd_group_score_df = pd_esg_raw_df[['INDUSTRY_DESC','COMMUNITY','EMPLOYEES','ENVIRONMENT','GOVERNANCE']]
#pd_rows = pd_rows.groupby(["EMPID"]).sum()
pd_group_score_df = pd_group_score_df.groupby(['INDUSTRY_DESC'])['COMMUNITY','EMPLOYEES'].mean()

st.dataframe(pd_group_score_df)