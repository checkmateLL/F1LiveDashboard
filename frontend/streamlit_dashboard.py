import streamlit as st
import pandas as pd
import sqlite3
from backend.config import SQLITE_DB_PATH

st.title("F1 Data Dashboard")

# Connect to the SQLite database.
conn = sqlite3.connect(SQLITE_DB_PATH)
conn.row_factory = sqlite3.Row

# Query available years.
df_years = pd.read_sql_query("SELECT DISTINCT year FROM events ORDER BY year DESC", conn)
st.subheader("Available Years")
st.dataframe(df_years)

selected_year = st.selectbox("Select Year", df_years['year'].tolist())

# Query events for the selected year.
df_events = pd.read_sql_query(
    "SELECT round_number, event_name, event_date FROM events WHERE year = ? ORDER BY round_number",
    conn, params=(selected_year,)
)
st.subheader("Events")
st.dataframe(df_events)

conn.close()