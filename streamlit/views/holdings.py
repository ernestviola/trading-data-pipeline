import streamlit as st
from queries import get_holdings

st.title("Holdings")
st.write("Most current holdings by strategy and ticker")

data = get_holdings()

data = data.drop(columns=["PRICE_DATE", "RN"])

melted = data.melt(
    id_vars=["TICKER", "STRATEGY_USED"],
    var_name="metric",
    value_name="value",
)

pivoted = melted.pivot_table(
    index="metric",
    columns=["TICKER", "STRATEGY_USED"],
    values="value",
)

if not pivoted.empty:
    st.dataframe(pivoted)
