import streamlit as st
import pandas as pd
import plotly.express as px
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError

# Establishing connection to the PostgreSQL database
ENGINE_URL = "postgresql://postgres:admin@localhost:5432/phone_pe1"
engine = sqlalchemy.create_engine(ENGINE_URL)

def fetch_data(table, year):
    query = f'SELECT * FROM "{table}" WHERE "Year" = \'{year}\';'
    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except SQLAlchemyError as e:
        st.error(f"Error fetching data: {str(e)}")
        return pd.DataFrame()

def display_chart(df, x, y, title, kind='bar'):
    if df.empty:
        st.write("No data available for the selected year.")
        return

    try:
        if kind == 'bar':
            fig = px.bar(df, x=x, y=y, title=title)
        elif kind == 'pie':
            fig = px.pie(df, names=x, values=y, title=title)
        elif kind == 'time_series':
            fig = px.line(df, x=x, y=y, title=title)
        elif kind == 'heatmap':
            fig = px.density_heatmap(df, x=x, y=y, title=title, marginal_x="rug", marginal_y="histogram")
        elif kind == 'stacked_bar':
            fig = px.bar(df, x=x, y=y, title=title, color=x)
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error creating chart: {str(e)}")

def main():
    st.title("PhonePe Pulse Data Visualization Dashboard")
    year = st.sidebar.selectbox("Select Year", ["2018", "2019", "2020", "2021", "2022", "2023"])
    data_type = st.sidebar.radio("Data Type", ["Transactions", "Insurance", "Users", "Aggregate Insurance", "Aggregate Transactions", "Aggregate Users"])

    data_table_map = {
        "Transactions": ("map_transaction", "name", "amount", "Transaction Amount by District", 'time_series'),
        "Insurance": ("map_Insurance", "label", "metric", "Insurance Metrics by District", 'heatmap'),
        "Users": ("map_User", "State", "RegisteredUsers", "User Registrations by District", 'bar'),
        "Aggregate Insurance": ("Agg_Insurance", "State", "Insurance_amount", "Aggregate Insurance by State", 'heatmap'),
        "Aggregate Transactions": ("Agg_Trans", "Transaction_type", "Transaction_amount", "Aggregate Transactions by Type", 'stacked_bar'),
        "Aggregate Users": ("Agg_User", "Brand", "User_count", "User Registrations by Brand", 'pie')
    }

    if data_type in data_table_map:
        table, x, y, title, kind = data_table_map[data_type]
        df = fetch_data(table, year)
        display_chart(df, x, y, title, kind)
    else:
        st.write("Invalid data type selected.")

if __name__ == "__main__":
    main()
