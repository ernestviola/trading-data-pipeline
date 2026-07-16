import streamlit as st
from queries import get_freshness

data = get_freshness()
stale = data[~data["IS_FRESH"]]

st.title("Data Quality Status")
st.write("Freshness of the latest data for each strategy/ticker pair")

if stale.empty:
    st.success("✅ Everything is up to date")
else:
    st.warning(f"⚠️ {len(stale)} strategy/ticker pair(s) are stale")

st.dataframe(data)
