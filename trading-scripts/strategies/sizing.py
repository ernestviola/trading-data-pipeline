def size_trades(
    trades_df,
    cash_on_hand,
    base_position_size,
    strength_threshold,
    max_multiplier,
    shares_held,
):
    """
    trades_df must be sorted chronologically per ticker before calling this.
    Expects columns: side ('buy'/'sell'), signal_strength, price.
    signal_strength is a strategy-agnostic, non-negative measure of signal
    strength computed upstream by the calling strategy (e.g. abs(z_score)
    for mean-reversion, abs(normalized MACD histogram) for momentum) — this
    function doesn't know or care which strategy produced it.
    Adds: quantity, cash_after, shares_held_after.
    """
    quantities = []
    cash_trace = []
    shares_trace = []

    for row in trades_df.itertuples():
        # signal_strength is already a strategy-agnostic magnitude computed
        # upstream by the calling strategy

        # we divide by strength_threshold in order to normalize the strength
        # say threshold was 2 then we don't want the multiplier to start at
        # 2 we want it to start at 1
        dollar_size = base_position_size * min(
            row.signal_strength / strength_threshold, max_multiplier
        )

        # straight forward calc of the qty. Spend / price
        desired_qty = dollar_size / row.price

        if row.side == "buy":
            # affordable qty is the max amount we could purchase at current price
            affordable_qty = cash_on_hand / row.price

            # take the min of the 2 we can't buy more than we can afford and we don't want to buy more than our desired
            qty = min(desired_qty, affordable_qty)

            # update our cash on hand and our number of shares
            cash_on_hand -= qty * row.price
            shares_held += qty
        elif row.side == "sell":
            # cant sell more than we currently hold so take the min
            qty = min(desired_qty, shares_held)

            # update cash on hand and our number of shares
            cash_on_hand += qty * row.price
            shares_held -= qty
        else:
            qty = 0  # shouldn't happen, hold rows already filtered out

        quantities.append(qty)
        cash_trace.append(cash_on_hand)
        shares_trace.append(shares_held)

    trades_df["quantity"] = quantities
    trades_df["cash_after"] = cash_trace
    trades_df["shares_held_after"] = shares_trace
    return trades_df
