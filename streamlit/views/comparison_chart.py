import streamlit as st
from queries import get_tickers, get_comparison_chart

st.title("Strategy Comparison")
st.write("Comparison of strategies per ticker based on a $10,000 initial account.")
st.write("Total value includes the combined cash amount and shares market value.")

available_tickers = get_tickers()
ticker = st.selectbox("Ticker", options=available_tickers["TICKER"].tolist())

if ticker:
    data = get_comparison_chart(ticker)
    st.line_chart(
        data,
        x="PRICE_DATE",
        x_label="Date",
        y="TOTAL_VALUE",
        y_label="Total Value",
        color="STRATEGY_USED",
    )
