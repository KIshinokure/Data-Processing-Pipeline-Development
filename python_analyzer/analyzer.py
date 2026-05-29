import pyarrow.flight as flight
import polars as pl
import streamlit as st
import time
import pandas as pd

def get_flight_client():
    return flight.FlightClient("grpc://go-collector:8081")

def fetch_data(client):
    try:
        ticket = flight.Ticket(b"give_me_data")
        reader = client.do_get(ticket)
        arrow_table = reader.read_all()
        return pl.from_arrow(arrow_table)
    except Exception as e:
        return None

st.title("Industrial Monitoring Dashboard")

placeholder = st.empty()
client = get_flight_client()

while True:
    df = fetch_data(client)
    
    with placeholder.container():
        if df is not None and not df.is_empty():
            st.write("Live Data:")
            pandas_df = df.to_pandas()
            st.dataframe(pandas_df)
            st.bar_chart(data=pandas_df, x="machine_id", y="avg_temp")
        else:
            st.warning("Waiting for data...")
            
    time.sleep(2)
