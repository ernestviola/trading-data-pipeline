import streamlit as st
from queries import get_freshness

freshness = get_freshness()
stale = freshness[~freshness["IS_FRESH"]]

if stale.empty:
    st.success("All strategy/ticker pairs are up to date")
else:
    st.warning(f"{len(stale)} strategy/ticker pair(s) are stale")
    st.page_link("views/qc_status.py", label="View data quality status")

pg = st.navigation(
    [
        st.Page("views/qc_status.py", title="Quality Control"),
        st.Page("views/comparison_chart.py", title="Comparison Chart"),
        st.Page("views/leaderboard.py", title="Leaderboard"),
        st.Page("views/holdings.py", title="Holdings"),
    ]
)

pg.run()
