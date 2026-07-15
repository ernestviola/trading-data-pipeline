import streamlit as st
from queries import get_leaderboard

conn = st.connection("snowflake")
data = get_leaderboard()
st.title("Strategy Performance Leaderboard")
st.write("Comparison of all strategies run by ticker")
st.dataframe(data)
