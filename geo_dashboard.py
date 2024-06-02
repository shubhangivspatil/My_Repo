
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import pydeck as pdk
import requests
import json

# Set the Streamlit page configuration as the first Streamlit command
st.set_page_config(layout="wide")

# Database Connection
DATABASE_URL = "postgresql://postgres:admin@localhost:5432/phone_pe1"
engine = create_engine(DATABASE_URL)

# Caching the data fetching functions
@st.cache_data
def fetch_agg_trans(year, quarter):
    query = f"""
    SELECT "State", "Transaction_type", "Transaction_count", "Transaction_amount"
    FROM public."Agg_Trans"
    WHERE "Year" = '{year}' AND "Quater" = {quarter}
    """
    return pd.read_sql(query, engine)

@st.cache_data
def fetch_agg_insurance(year, quarter):
    query = f"""
    SELECT "State", "Year", "Quater", "Insurance_type", "Insurance_count", "Insurance_amount"
    FROM public."Agg_Insurance"
    WHERE "Year" = '{year}' AND "Quater" = {quarter}
    """
    return pd.read_sql(query, engine)

@st.cache_data
def fetch_map_data(year):
    query = f"""
    SELECT "lat", "lng", "metric", "label", "Year"
    FROM public."map_Insurance"
    WHERE "Year" = '{year}'
    """
    return pd.read_sql(query, engine)
@st.cache_data
def get_geojson():
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    
    if response.status_code == 200:
        try:
            return response.json()
        except json.JSONDecodeError as e:
            st.error(f"Error parsing GeoJSON data: {e}")
            return None
    else:
        st.error(f"Error fetching GeoJSON data: {response.status_code}")
        return None

# Static function for 3D map visualization
def visualize_3d_map(map_data, title, color):
    try:
        st.write(f"Map Data for {title}:")
        st.write(map_data)
        layer = pdk.Layer(
            'ColumnLayer',
            data=map_data,
            get_position='[lng, lat]',
            get_elevation='metric',
            elevation_scale=500,
            radius=10000,
            get_fill_color=color,
            pickable=True,
            extruded=True
        )

        view_state = pdk.ViewState(
            latitude=22.0,
            longitude=77.0,
            zoom=4,
            pitch=50
        )

        r = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={"text": "{label}\n{metric}"}
        )

        st.pydeck_chart(r)
    except Exception as e:
        st.error(f"Error creating {title} map: {e}")

# Load GeoJSON data
india_geojson = get_geojson()

st.title("PhonePe Pulse | The Beat of Progress")
# Sidebar for Year and Quarter Selection
selected_year = st.sidebar.selectbox("Select Year", [2018, 2019, 2020, 2021, 2022])
selected_quarter = st.sidebar.selectbox("Select Quarter", ["Q1", "Q2", "Q3", "Q4"])

# Convert quarter string to integer
quarter_map = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
quarter = quarter_map[selected_quarter]

# Fetch data based on the selected option
trans_data = fetch_agg_trans(selected_year, quarter)
insurance_data = fetch_agg_insurance(selected_year, quarter)
map_data = fetch_map_data(selected_year)

# Tabs for different data views
tab1, tab2, tab3 = st.tabs(["Transactions", "Insurance Data", "Insights"])

with tab1:
    if not trans_data.empty:
        transaction_types = trans_data['Transaction_type'].unique()
        selected_transaction_type = st.sidebar.multiselect('Select Transaction Types', transaction_types, default=transaction_types)
        filtered_trans_data = trans_data[trans_data['Transaction_type'].isin(selected_transaction_type)]
        total_transactions = filtered_trans_data['Transaction_amount'].sum()
        st.sidebar.write(f"Total Transactions: {total_transactions}")

        st.write("## Transaction Data")
        st.write(filtered_trans_data)

        # Display summary of transaction types and amounts
        st.write("## Transaction Summary")
        for t_type in transaction_types:
            t_data = filtered_trans_data[filtered_trans_data['Transaction_type'] == t_type]
            total_amount = t_data['Transaction_amount'].sum()
            st.metric(label=t_type, value=f"₹ {total_amount:,.2f}")

        # Map visualization for transactions
        st.write("## Transactions 3D Map Visualization")
        if not map_data.empty:
            visualize_3d_map(map_data, "Transactions Data Across India", [255, 165, 0])  # Orange color for transactions
        else:
            st.write("No geospatial data available.")
    else:
        st.write("No transaction data available.")

with tab2:
    if not insurance_data.empty:
        st.write("## Insurance Data")
        st.write(insurance_data)

        # Map Visualization
        if not map_data.empty:
            st.write("## Insurance 3D Map Visualization")
            visualize_3d_map(map_data, "Insurance Data Across India", [0, 255, 255])  # Cyan color for insurance
        else:
            st.write("No geospatial data available.")
    else:
        st.write("No insurance data available.")

with tab3:
    st.subheader("Data Insights")
    if not trans_data.empty:
        highest_transaction_state = trans_data.loc[trans_data['Transaction_amount'].idxmax(), 'State']
        highest_transaction_amount = trans_data['Transaction_amount'].max()
        st.write(f"The highest transaction amount recorded was in {highest_transaction_state} with ₹ {highest_transaction_amount:,.2f}")

    if not insurance_data.empty:
        most_insurance_policies = insurance_data.loc[insurance_data['Insurance_count'].idxmax(), 'State']
        st.write(f"The state with the most insurance policies issued is {most_insurance_policies}.")