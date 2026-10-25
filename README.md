# BTC / ETH Pair Trading

A market-neutral pair-trading backtest on hourly **BTC and ETH** data (Jan to Jul 2022), built with
[backtrader](https://www.backtrader.com/).

## Idea

BTC and ETH move together most of the time. When one looks statistically cheap *relative to the other*, go long the
cheap leg and short the rich leg, then exit once the relationship normalizes.

Each CSV carries a precomputed `signal` column for its leg: a probability in [0, 1] of how extreme that coin's move is,
conditional on the other coin.

| Condition | Action |
|---|---|
| BTC signal < 1% **and** ETH signal > 99% | long BTC, short ETH |
| BTC signal > 99% **and** ETH signal < 1% | short BTC, long ETH |
| Signals move back inside 30% / 70% | close both legs |

Each leg is sized to half of a $90k notional, with 0.04% commission per side.

## Result

```
Closed trades : 84   (42 pair trades)
Won / lost    : 43 / 41
Net PnL       : -1,020.54  on 100,000   (-1.0%)
```

**The strategy did not make money after fees.** The win rate was about 50% and some large losing legs came during
the June 2022 sell-off. Gross edge was small compared with commission and the tail of the spread. Possible next
steps: volatility-scaled sizing, a stop on spread divergence, and testing whether the relationship holds out-of-sample
before trading it.

## Run

```bash
pip install -r requirements.txt
python strategy.py
```

Set `plot = True` in `strategy.py` (and install `backtrader-plotting`) for an interactive Bokeh chart.
