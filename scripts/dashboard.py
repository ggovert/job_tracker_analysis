import streamlit as st
import duckdb
import pandas as pd

st.set_page_config(page_title="Hacker News Job Market Analysis", layout="wide")

# 1. Setup DuckDB Connection to S3
@st.cache_resource
def get_connection():
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
        SET s3_endpoint='rustfs:9000';
        SET s3_access_key_id='rustfsadmin';
        SET s3_secret_access_key='rustfsadmin123';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con

con = get_connection()

st.title("📊 Hacker News Job Market Insights")
st.markdown("Exploring the **Gold Layer** (One Big Table) for tech trends.")

# 2. Sidebar Filters
st.sidebar.header("Filters")
# Query unique roles and locations for dropdowns
roles = con.execute("SELECT DISTINCT role FROM read_parquet('s3://gold/full_job_market_analysis/*/*.parquet', hive_partitioning=true) WHERE role IS NOT NULL").df()
selected_role = st.sidebar.multiselect("Filter by Role", options=roles['role'].tolist())

# 3. Main Data Query (The OBT)
query = "SELECT * FROM read_parquet('s3://gold/full_job_market_analysis/*/*.parquet', hive_partitioning=true)"

if selected_role:
    # Convert list to a SQL-friendly string: ('Role1', 'Role2')
    role_list = ", ".join([f"'{r}'" for r in selected_role])
    query += f" WHERE role IN ({role_list})"

# Sort by date so the charts look better
query += " ORDER BY run_date DESC"

df = con.execute(query).df()

# 4. Visualizations
col1, col2 = st.columns(2)

with col1:
    st.subheader("Top Technologies")
    tech_counts = df['tech'].value_counts().head(10)
    st.bar_chart(tech_counts)

with col2:
    st.subheader("Seniority Distribution")
    seniority_counts = df['seniority'].value_counts()
    st.pie_chart(seniority_counts)

st.subheader("Raw Job Explorer")
st.dataframe(df[['run_date', 'company', 'location', 'role', 'tech', 'description']].head(50))