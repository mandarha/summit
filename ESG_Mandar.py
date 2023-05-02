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

@st.cache_data
def get_raw_esg_data(_conn):
    pd.read_sql("USE ROLE SYSADMIN",_conn)
    esg_raw_df = pd.read_sql("SELECT * FROM csrhub.public.faststarttrial;", _conn)
    return esg_raw_df


def get_agg_esg_data(conn):
    esg_raw_df = get_raw_esg_data(conn)
    pd_esg_raw_df = pd.DataFrame(esg_raw_df)
    pd_group_score_df = pd_esg_raw_df[['INDUSTRY_DESC','COMMUNITY','EMPLOYEES','ENVIRONMENT','GOVERNANCE']]
    pd_group_score_df = pd_group_score_df.groupby('INDUSTRY_DESC',as_index = False)[['COMMUNITY','EMPLOYEES','ENVIRONMENT','GOVERNANCE']].mean()
    pd_group_score_df.columns = ['Industry','Avg Community Score','Avg Employee Score','Avg Environment Score','Avg Governance Score']
    return pd_group_score_df


try:
    st.sidebar.markdown("**:violet[This app displays ESG insights for various \
                        industries and possibility to drill down at individual company level for \
                        multiple factors like employees,governance,community and environment]**")
    conn = init_connection()
    pd_group_score_df = get_agg_esg_data(conn)
    st.dataframe(pd_group_score_df)
finally:
    st.write("**:blue[----------------------------------------Presentation Over----------------------------------------]**")