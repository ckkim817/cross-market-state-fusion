# Polymarket RL Trading

An experiment in training reinforcement learning agents to trade prediction markets using real-time multi-source data fusion.

## What This Is

A PPO (Proximal Policy Optimization) agent that paper trades Polymarket's 15-minute binary crypto markets. The agent observes live data from Binance futures and Polymarket's orderbook, then learns to predict short-term price direction.

**Current status**: Paper trading only. The agent trains and makes decisions on live market data, but doesn't execute real orders.

## What This Proves

1. **RL can learn from sparse PnL signals** - The agent only gets reward when positions close. No intermediate feedback during the 15-minute window. Despite this sparsity, it learns profitable patterns. 109% ROI on paper trades over ~2 hours.

2. **Multi-source data fusion works** - Combining Binance futures order flow and Polymarket orderbook state into a single 18-dim observation gives the agent useful signal.

3. **Low win rate can be profitable** - The agent wins only 21% of trades but profits because binary markets have asymmetric payoffs. Buy at 0.40, win pays 0.60; lose costs 0.40.

4. **On-device training is viable** - MLX on Apple Silicon handles real-time PPO updates during live market hours without cloud GPU costs.

**Important caveat**: Training uses probability-based PnL (exit_prob - entry_prob), not actual binary outcomes. This is a proxy signal - the agent learns "did probability move my way?" rather than "did I correctly predict UP vs DOWN?"

## What This Doesn't Prove

1. **Live profitability** - Paper trading assumes instant fills at mid-price. Real trading faces latency, slippage, and market impact. Expect 20-50% performance degradation.

2. **Statistical significance** - 72 updates over 2 hours isn't enough to confirm edge. Could be variance. Needs weeks of out-of-sample testing.

3. **Scalability** - $5 positions are invisible to the market. At $100+ the agent's own orders would move prices and consume liquidity.

4. **Persistence of edge** - Markets adapt. If this strategy worked, others would copy it and arbitrage it away.

## Path to Live Trading

To move from paper to real:

1. **Execution layer** - Integrate Polymarket CLOB API for order placement
2. **Slippage modeling** - Simulate walking the book at realistic sizes
3. **Latency compensation** - Account for 50-200ms round-trip to Polymarket
4. **Risk management** - Position limits, drawdown stops, exposure caps
5. **Extended validation** - Weeks of paper trading across market regimes

See [TRAINING_JOURNAL.md](TRAINING_JOURNAL.md) for detailed training analysis.

---

## Training Status

| Phase | Updates | Trades | PnL | Win Rate | Entropy |
|-------|---------|--------|-----|----------|---------|
| 1 (Shaped rewards) | 36 | 1,545 | $3.90 | 20.2% | 0.36 (collapsed) |
| 2 (Pure PnL) | 36 | 3,330 | $10.93 | 21.2% | 1.05 (healthy) |

**Capital**: $5/trade × 4 markets = $20 max exposure
**Phase 2 ROI**: 55% on max exposure (~2 hours)

**Key insight**: Phase 1 failed because shaped rewards let the agent "game" bonuses without profitable trading. Phase 2 used probability-based PnL (normalized) - sparse but honest signal.

---

## Architecture

```
├── run.py                    # Main trading engine
├── dashboard.py              # Real-time web dashboard
├── strategies/
│   ├── base.py               # Action, MarketState, Strategy base classes
│   ├── rl_mlx.py             # PPO implementation (MLX)
│   ├── momentum.py           # Momentum baseline
│   ├── mean_revert.py        # Mean reversion baseline
│   └── fade_spike.py         # Spike fading baseline
└── helpers/
    ├── polymarket_api.py     # Polymarket REST API
    ├── binance_wss.py        # Binance WebSocket (spot price reference)
    ├── binance_futures.py    # Futures data (returns, order flow, CVD)
    └── orderbook_wss.py      # Polymarket CLOB WebSocket
```

## State Space (18 dimensions)

| Category | Features | Source |
|----------|----------|--------|
| Momentum | `returns_1m`, `returns_5m`, `returns_10m` | Binance futures |
| Order Flow | `ob_imbalance_l1`, `ob_imbalance_l5`, `trade_flow`, `cvd_accel` | Binance futures |
| Microstructure | `spread_pct`, `trade_intensity`, `large_trade_flag` | Polymarket CLOB |
| Volatility | `vol_5m`, `vol_expansion` | Polymarket (local), Binance futures |
| Position | `has_position`, `position_side`, `position_pnl`, `time_remaining` | Internal |
| Regime | `vol_regime`, `trend_regime` | Derived |

**Note**: Returns and spread are scaled by 100x. CVD acceleration is divided by 1e6. `vol_5m` is computed locally from Polymarket prob history; `vol_expansion` comes from Binance futures.

## Action Space

| Action | Description |
|--------|-------------|
| HOLD (0) | No action |
| BUY (1) | Long UP token (bet price goes up) |
| SELL (2) | Long DOWN token (bet price goes down) |

Fixed 50% position sizing. Originally had 7 actions with variable sizing (25/50/100%), simplified to reduce complexity.

## Network

```
Actor:  18 → 128 (tanh) → 128 (tanh) → 3 (softmax)
Critic: 18 → 128 (tanh) → 128 (tanh) → 1
```

## PPO Hyperparameters

| Parameter | Value |
|-----------|-------|
| `lr_actor` | 1e-4 |
| `lr_critic` | 3e-4 |
| `gamma` | 0.99 |
| `gae_lambda` | 0.95 |
| `clip_epsilon` | 0.2 |
| `entropy_coef` | 0.10 |
| `buffer_size` | 512 |
| `batch_size` | 64 |
| `n_epochs` | 10 |

---

## Usage

```bash
# Training
python run.py --strategy rl --train --size 50

# Inference (load trained model)
python run.py --strategy rl --load rl_model --size 100

# Dashboard (separate terminal)
python dashboard.py --port 5001

# Baselines
python run.py --strategy momentum
python run.py --strategy mean_revert
python run.py --strategy random
```

## Requirements

```
mlx>=0.5.0
websockets>=12.0
flask>=3.0.0
flask-socketio>=5.3.0
numpy>=1.24.0
requests>=2.31.0
```

## Installation

```bash
cd experiments/03_polymarket
python -m venv venv
source venv/bin/activate
pip install mlx websockets flask flask-socketio numpy requests
```

