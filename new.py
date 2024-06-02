import streamlit as st
import pandas as pd
import plotly.express as px
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError

# Establishing connection to the PostgreSQL database
ENGINE_URL = "postgresql://postgres:admin@localhost:5432/phone_pe1"
engine = sqlalchemy.create_engine(ENGINE_URL)

def fetch_data(table, year):
    query = f"SELECT * FROM \"{table}\" WHERE \"Year\" = '{year}';"
    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except SQLAlchemyError as e:
        st.error(f"Error fetching data: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame on error

def display_chart(df, x, y, title, kind='bar'):
    if not df.empty:
        if kind == 'bar':
            fig = px.bar(df, x=x, y=y, title=title)
        elif kind == 'pie':
            fig = px.pie(df, names=x, values=y, title=title)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("No data available for the selected year.")

def main():
    st.title("PhonePe Pulse Data Visualization Dashboard")
    year = st.sidebar.selectbox("Select Year", options=["2018", "2019", "2020", "2021", "2022", "2023"])
    data_type = st.sidebar.radio("Data Type", ["Transactions", "Insurance", "Users", "Aggregate Insurance", "Aggregate Transactions", "Aggregate Users"])

    # Data Retrieval and Visualization
    if data_type == "Transactions":
        df = fetch_data("map_transaction", year)
        display_chart(df, 'name', 'amount', "Transaction Amount by District")
    elif data_type == "Insurance":
        df = fetch_data("map_Insurance", year)
        display_chart(df, 'label', 'metric', "Insurance Metrics by District", kind='pie')
    elif data_type == "Users":
        df = fetch_data("map_User", year)
        display_chart(df, 'State', 'RegisteredUsers', "User Registrations by District")
    elif data_type == "Aggregate Insurance":
        df = fetch_data("Agg_Insurance", year)
        display_chart(df, 'State', 'Insurance_amount', "Aggregate Insurance by State")
    elif data_type == "Aggregate Transactions":
        df = fetch_data("Agg_Trans", year)
        display_chart(df, 'Transaction_type', 'Transaction_amount', "Aggregate Transactions by Type")
    elif data_type == "Aggregate Users":
        df = fetch_data("Agg_User", year)
        display_chart(df, 'Brand', 'User_count', "User Registrations by Brand", kind='pie')

if __name__ == "__main__":
    main()
