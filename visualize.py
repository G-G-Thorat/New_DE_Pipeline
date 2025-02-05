import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px

# Function to load data from the SQLite database
def load_data(db_name, table_name):
    """Load stock data from an SQLite database."""
    conn = sqlite3.connect(db_name)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df

# Initialize Streamlit dashboard
st.set_page_config(page_title="Stock Data Dashboard", layout="wide")
st.title("📈 Real-Time Stock Market Dashboard")

# Load Data
db_name = "stock_data.db"
table_name = "stocks"
df = load_data(db_name, table_name)

# Sidebar for user input
st.sidebar.header("Filter Data")

# Since there's no ticker column, display all data
st.subheader("Stock Data")
st.dataframe(df)

# Price Trend Line Chart
fig_price = px.line(df, x="Datetime", y="Close", title="Closing Price Trend")
st.plotly_chart(fig_price, use_container_width=True)

# Volume Traded Bar Chart
fig_volume = px.bar(df, x="Datetime", y="Volume", title="Trading Volume")
st.plotly_chart(fig_volume, use_container_width=True)

# High-Low Range Area Chart
fig_range = px.area(df, x="Datetime", y=["High", "Low"], title="Daily High-Low Price Range")
st.plotly_chart(fig_range, use_container_width=True)

st.write("**Note:** Data updates periodically based on the ingestion pipeline.")
