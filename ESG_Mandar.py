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
def get_raw_esg_data(_conn,selected_date):
    pd.read_sql("USE ROLE SYSADMIN",_conn)
    esg_raw_df = pd.read_sql("SELECT distinct* FROM csrhub.public.faststarttrial where rating_date < '{}';".format(selected_date), _conn)
    return esg_raw_df

@st.cache_resource
def get_agg_esg_data(_conn,selected_date):
    esg_raw_df = get_raw_esg_data(_conn,selected_date)
    pd_esg_raw_df = pd.DataFrame(esg_raw_df)
    pd_group_score_df = pd_esg_raw_df[['INDUSTRY_DESC','COMMUNITY','EMPLOYEES','ENVIRONMENT','GOVERNANCE']]
    pd_group_score_df = pd_group_score_df.groupby('INDUSTRY_DESC',as_index = False)[['COMMUNITY','EMPLOYEES','ENVIRONMENT','GOVERNANCE']].mean()
    pd_group_score_df.columns = ['Industry','Avg Community Score','Avg Employee Score','Avg Environment Score','Avg Governance Score']
    return pd_group_score_df


try:
    st.sidebar.header("**:green[ESG Insights ]:evergreen_tree:**")
    st.sidebar.header("**:green[----------------------------------------]**")
    st.sidebar.markdown("**:blue[This app displays ESG insights for various \
                        industries.\
                        Please scroll down to visualize further details at specific Industry level\
                        and then at individual Company level]**")
    st.sidebar.header("**:green[----------------------------------------]**")
    st.sidebar.markdown("**_:red[Disclaimer : This application is based on data provided by CSR Hub on Snowflake Marketplace]_**")
    conn = init_connection()
    selected_date = st.sidebar.date_input("Select date for which you need ESG Scorecard")
    pd_group_score_df = get_agg_esg_data(conn,selected_date)
    st.write("**:grey[Various ESG Scores Graphical Representation]**")
    st.bar_chart(data=pd_group_score_df, x='Industry', y=['Avg Community Score','Avg Employee Score','Avg Environment Score','Avg Governance Score'], width=0, height=0, use_container_width=True)
    st.write("**:blue[Industry Wise ESG Score card :dart:]**")
    st.dataframe(pd_group_score_df)
    list_of_industries =  pd_group_score_df.Industry.unique()
    st.sidebar.header("**:green[----------------------------------------]**")
    selected_industry = st.sidebar.selectbox("Select Industry you want to analyze further",list_of_industries)
    pd_selected_df = get_raw_esg_data(conn,selected_date)
    pd_selected_df_1 = pd_selected_df
    pd_selected_df = pd_selected_df[pd_selected_df['INDUSTRY_DESC'] == selected_industry]
    list_of_companies = pd_selected_df.COMPANY_NAME.unique()
    pd_selected_df = pd_selected_df.groupby(['CSRHUB_ID','COMPANY_NAME','INDUSTRY_DESC','RATING_STATUS'])[['RATING_DATE','COMMUNITY','EMPLOYEES','ENVIRONMENT','GOVERNANCE']].max()
    st.write("")
    st.write("")
    st.write("**:blue[ESG Score Details of Companies under industry ",selected_industry,"dated ",selected_date, " ]**")
    st.write("")
    st.write("")
    st.dataframe(pd_selected_df)
    selected_company = st.sidebar.selectbox("Select Company that you wish to see all ESG scores",list_of_companies)
    pd_select_comp_df = pd_selected_df_1[pd_selected_df_1['COMPANY_NAME'] == selected_company]
    pd_select_comp_df = pd_select_comp_df[['COMPANY_NAME','RATING_DATE','ISIN','COMMUNITY','EMPLOYEES','ENVIRONMENT','GOVERNANCE']]
    pd_select_comp_df = pd_select_comp_df.sort_values("RATING_DATE",ascending = False)
    st.write("")
    st.write("")
    st.write("**:blue[ESG Score Details for Company :  ",selected_company," ]**")
    st.write("")
    st.write("")
    st.dataframe(pd_select_comp_df)
    

    
finally:
    st.write("**:grey[------------------------------------------------------------Presentation Over------------------------------------------------------------]**")