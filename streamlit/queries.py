import streamlit as st


def get_connection():
    return st.connection("snowflake")


@st.cache_data(ttl=300)
def get_leaderboard():
    return get_connection().query("SELECT * FROM strategy_performance_summary;")
