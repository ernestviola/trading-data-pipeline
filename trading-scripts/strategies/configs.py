from dataclasses import dataclass


@dataclass
class MeanReversionConfig:
    """Signal-generation knobs for the mean-reversion strategy.

    Window is the number of days the rolling window is active for.

    buy_strength_threshold/sell_strength_threshold are the z_score limits
    for when a buy or sell is allowed to happen, respectively. Split into
    two instead of one shared value since buy and sell sensitivity aren't
    necessarily symmetric (e.g. a stricter entry threshold than exit, or
    vice versa) - each also normalizes signal_strength for sizing.py on its
    own side (buy fills use buy_strength_threshold, sell fills use
    sell_strength_threshold).
    """

    window: int = 20
    buy_strength_threshold: float = 1.5
    sell_strength_threshold: float = 1.5


@dataclass
class MACDConfig:
    """
    Signal-generation knobs for the MACD momentum strategy.

    fast_period/slow_period/signal_period are the standard MACD spans (in
    trading days) for the fast EMA, slow EMA, and the EMA-of-MACD signal
    line respectively — 12/26/9 is the conventional default.

    buy_strength_threshold/sell_strength_threshold normalize the PPO-style
    histogram (see momentum.py) into sizing.py's multiplier range, mirroring
    MeanReversionConfig's split. Unlike mean-reversion, these don't affect
    *whether* a trade happens (that's the crossover event) - only how a buy
    fill is sized versus a sell fill for the same normalized histogram
    magnitude. These defaults are starting guesses, not empirically
    calibrated - expect to tune them once real backtest output exists.
    """

    fast_period: int = 12
    slow_period: int = 26
    signal_period: int = 9
    buy_strength_threshold: float = 0.5
    sell_strength_threshold: float = 0.5
