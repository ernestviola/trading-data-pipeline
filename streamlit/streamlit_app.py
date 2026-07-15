import streamlit as st

pg = st.navigation(
    [
        st.Page("views/comparison_chart.py", title="Comparison Chart"),
        st.Page("views/leaderboard.py", title="Leaderboard"),
        st.Page("views/holdings.py", title="Holdings"),
    ]
)

pg.run()
