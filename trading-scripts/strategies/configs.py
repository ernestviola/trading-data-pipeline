from dataclasses import dataclass


@dataclass
class MeanReversionConfig:
    """Signal-generation knobs for the mean-reversion strategy.

    Window is the number of days the rolling window is active for.

    Strength_threshold is the z_score limit for when a buy or sell is allowed to happen.

    """

    window: int = 20
    strength_threshold: float = 1.5


@dataclass
class MACDConfig:
    """
    Signal-generation knobs for the MACD momentum strategy.

    fast_period/slow_period/signal_period are the standard MACD spans (in
    trading days) for the fast EMA, slow EMA, and the EMA-of-MACD signal
    line respectively — 12/26/9 is the conventional default.

    strength_threshold normalizes the PPO-style histogram (see momentum.py)
    into sizing.py's multiplier range, mirroring z_threshold's role for
    mean-reversion. This default is a starting guess, not empirically
    calibrated — expect to tune it once real backtest output exists.
    """

    fast_period: int = 12
    slow_period: int = 26
    signal_period: int = 9
    strength_threshold: float = 0.5
