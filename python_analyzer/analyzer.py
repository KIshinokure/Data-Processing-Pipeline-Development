import streamlit as st
import time
import json
from collections import deque
from kafka import KafkaConsumer
import threading

st.set_page_config(layout="wide")
st.title("Live Industrial Monitoring")

if "sliding_window" not in st.session_state:
    st.session_state.sliding_window = deque(maxlen=60)
    st.session_state.latest_count = 0
    st.session_state.latest_temp = 0.0

def kafka_listener():
    consumer = KafkaConsumer(
        'telemetry',
        bootstrap_servers=['kafka:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    for message in consumer:
        data = message.value
        st.session_state.sliding_window.append(data['avg_temp'])
        st.session_state.latest_count = data['count']
        st.session_state.latest_temp = data['avg_temp']

if "kafka_thread" not in st.session_state:
    thread = threading.Thread(target=kafka_listener, daemon=True)
    thread.start()
    st.session_state.kafka_thread = True

placeholder = st.empty()

while True:
    with placeholder.container():
        window_data = list(st.session_state.sliding_window)
        moving_avg = sum(window_data)/len(window_data) if window_data else 0.0
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Текущее окно (5 сек)", f"{st.session_state.latest_temp:.2f} °C")
        col2.metric("Скользящее окно (5 мин)", f"{moving_avg:.2f} °C")
        col3.metric("Записей в окне", st.session_state.latest_count)
        
        if window_data:
            st.line_chart(window_data)
        else:
            st.warning("Ожидание данных из Kafka...")
            
    time.sleep(1)
