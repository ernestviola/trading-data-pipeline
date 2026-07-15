import streamlit as st


def get_connection():
    return st.connection("snowflake")


def get_leaderboard():
    return get_connection().query("SELECT * FROM strategy_performance_summary;")


def get_tickers():
    return get_connection().query(
        "SELECT DISTINCT ticker from portfolio_value order by ticker asc;"
    )


def get_holdings():
    return get_connection().query(
        """
            select * from (
                select *, row_number() over (partition by strategy_used, ticker order by price_date desc) as rn
                from portfolio_value
            )
            where rn = 1;
        """
    )


@st.cache_data(ttl=300)
def get_comparison_chart(symbol=""):
    return get_connection().query(
        """
            SELECT a.strategy_used, a.price_date, a.market_value + c.cash_after as total_value from portfolio_value a
            JOIN cash_position c
            ON a.price_date >= c.start_date AND (a.price_date < c.end_date OR c.end_date is NULL)
            AND a.strategy_used = c.strategy_used
            AND a.ticker = c.ticker
            WHERE a.ticker = ?
            ORDER BY a.strategy_used,price_date ASC
        """,
        params=(symbol,),
    )
