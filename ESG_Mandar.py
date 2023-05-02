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
    return conn

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

run_query("USE ROLE SYSADMIN;")

@st.cache_data
def get_raw_esg_data(conn):
    esg_raw_df = pd.read_sql("SELECT * FROM csrhub.public.faststarttrial;", conn)
    return esg_raw_df


def get_agg_esg_data(conn):
    esg_raw_df = get_raw_esg_data(conn)
    pd_esg_raw_df = pd.DataFrame(esg_raw_df)
    pd_group_score_df = pd_esg_raw_df[['INDUSTRY_DESC','COMMUNITY','EMPLOYEES','ENVIRONMENT','GOVERNANCE']]
    pd_group_score_df = pd_group_score_df.groupby('INDUSTRY_DESC',as_index = False)[['COMMUNITY','EMPLOYEES','ENVIRONMENT','GOVERNANCE']].mean()
    return pd_group_score_df


try:
    conn = init_connection()
    pd_group_score_df = get_agg_esg_data(conn)
    st.dataframe(pd_group_score_df)
finally:
    st.write("Presentation Over")