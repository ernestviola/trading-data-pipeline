from strategies.mean_reversion import mean_reversion
from strategies.momentum import momentum
from strategies.configs import MeanReversionConfig, MACDConfig

# Each entry pairs a strategy function with its own typed config class.
# step_2()/callers never need to know a strategy's parameter shape - they
# just build the right config and pass it through untouched.
STRATEGIES = {
    "mean_reversion": (mean_reversion, MeanReversionConfig),
    "macd_momentum": (momentum, MACDConfig),
}
