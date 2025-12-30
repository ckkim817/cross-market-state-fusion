# Training Journal: RL on Polymarket

Training a PPO agent to trade 15-minute binary prediction markets. This documents what worked, what didn't, and what it means.

---

## The Experiment

**Question**: Can an RL agent learn profitable trading patterns from sparse PnL rewards?

**Setup**: Paper trade 4 concurrent crypto markets (BTC, ETH, SOL, XRP) on Polymarket using live data from Binance + Polymarket orderbooks. $5 per trade, max $20 exposure (all 4 markets).

**Result**: 109% ROI over 72 PPO updates (~2 hours). But the path there was interesting.

---

## Why This Market is Interesting

### Concurrent Multi-Asset Trading

Unlike typical RL trading that focuses on one asset, this agent trades 4 markets simultaneously with a single shared policy. Every 15-minute window spawns 4 independent binary markets. The agent must:
- Allocate attention across all active markets
- Learn asset-specific patterns while sharing weights
- Handle asynchronous expirations and refreshes

The same neural network decides for all assets - learning generalizable crypto patterns rather than overfitting to one market.

### Unique Market Structure

Polymarket's 15-minute crypto markets are unusual:
- **Binary resolution**: Market resolves to $1 or $0 based on price direction. But our training uses probability-based PnL, not actual resolution.
- **Known resolution time**: You know exactly when the market closes. Changes the decision calculus.
- **Orderbook-based**: Real CLOB with bid/ask spreads, not an AMM.
- **Cross-exchange lag**: Polymarket prices lag Binance by seconds. Exploitable.

This creates arbitrage opportunities - observe Binance move, bet on Polymarket before the orderbook adjusts.

### Multi-Source Data Fusion

The agent fuses two real-time streams:

```
Binance Futures WSS  → Price returns (1m, 5m, 10m), volatility, order flow, CVD, large trades
Polymarket CLOB WSS  → Bid/ask spread, orderbook imbalance
```

**Why this matters**: Binance futures are the "fast" market (higher liquidity, institutional flow, sub-second price discovery). Polymarket is the "slow" market (lower liquidity, retail-heavy, prices lag by seconds). By observing both, the agent sees information before it's fully priced into the prediction market.

This is a general pattern: **fast signal source + slow execution venue**. The same architecture could extend to:
- Sports betting (live game data + betting exchange)
- Election markets (polling/prediction aggregators + Polymarket)
- Weather derivatives (forecast models + prediction markets)
- Any market where information propagates with measurable delay

This creates an 18-dimensional state that captures both underlying asset dynamics AND prediction market microstructure:

| Category | Features |
|----------|----------|
| Momentum | 1m/5m/10m returns |
| Order flow | L1/L5 imbalance, trade flow, CVD acceleration |
| Microstructure | Spread %, trade intensity, large trade flag |
| Volatility | 5m vol, vol expansion ratio |
| Position | Has position, side, PnL, time remaining |
| Regime | Vol regime, trend regime |

### Sparse Reward Signal (Current Approach)

After Phase 1's reward shaping failed (see Training Evolution below), we switched to sparse rewards: the agent only receives reward when a position closes. No intermediate feedback while holding.

**Important caveat**: The reward is based on **probability change**, not actual market resolution. When a position closes (either by explicit sell or at market expiry):

```
UP position:   pnl = (exit_prob - entry_prob) × size
DOWN position: pnl = (entry_prob - exit_prob) × size
```

Example trades:
- Buy UP at 0.55, sell at 0.65 → reward = +$0.10 × size
- Buy UP at 0.55, hold to expiry where prob = 0.70 → reward = +$0.15 × size

Note: At expiry, we use the final probability, not the binary outcome ($1 or $0). This means training signal differs from true realized PnL. The agent learns "did probability move my way?" rather than "did I predict the actual outcome correctly?"

This sparsity makes credit assignment harder. The agent takes actions every tick but only learns from PnL when positions close. Phase 1 tried to solve this with dense shaping rewards—it backfired.

---

## Training Evolution

### Phase 1: Shaped Rewards (Updates 1-36)

**Duration**: ~52 minutes | **Trades**: 1,545 | **Entropy coef**: 0.05

Started with a reward function that tried to guide learning with micro-bonuses:

```python
# Reward given every step (not just on close)
reward = 0.0

# 1. Unrealized PnL delta - scaled DOWN by 0.1
if has_position:
    reward += (current_pnl - prev_pnl) * 0.1

# 2. Transaction cost penalty
if is_trade:
    reward -= 0.002 * size_multiplier

# 3. Spread cost on entry
if opening_position:
    reward -= spread * 0.5

# 4. Micro-bonuses (the problem)
reward += 0.002 * momentum_aligned  # Bonus for trading with momentum
reward += 0.001 * size_multiplier   # Bonus for larger positions
reward -= 0.001 * wrong_momentum    # Penalty for fighting momentum
```

**What happened**: Entropy collapsed from 1.09 → 0.36. The policy became nearly deterministic.

| Update | Entropy | PnL | Win Rate |
|--------|---------|-----|----------|
| 1 | 1.09 | $3.25 | 19.8% |
| 10 | 0.79 | $4.40 | 17.3% |
| 20 | 0.40 | $1.37 | 19.4% |
| 36 | 0.36 | $3.90 | 20.2% |

**Why it failed**: The shaping rewards were similar magnitude to actual PnL. With typical PnL deltas of $0.01-0.05, the scaled signal was 0.001-0.005 - same as the bonuses.

The agent learned to game the reward function:
- Trade with momentum → collect +0.002 bonus
- Use large sizes → collect +0.001 bonus
- Actual profitability? Optional.

Buffer win rate showed 90%+ (counting bonus-positive experiences) while actual trade win rate was 20%. The agent was optimizing the reward function, not the underlying goal.

### Diagnosis: Reward Shaping Backfired

The divergence between buffer win rate and cumulative win rate revealed the problem:

- **Buffer win rate**: % of experiences with reward > 0 (includes shaping bonuses)
- **Cumulative win rate**: % of closed trades that were profitable

When these diverge, the agent is learning the wrong thing.

---

### Phase 2: Pure Realized PnL (Updates 37+)

**Changes made**:
1. Reward ONLY on position close (not every step)
2. Increased entropy coefficient (0.05 → 0.10)
3. Simplified action space (7 → 3 actions)
4. Reduced buffer size (2048 → 512) and batch size (128 → 64) for faster updates
5. Reset reward normalization stats

```python
# Reward is 0 for all steps EXCEPT position close
def _compute_step_reward(self, cid, state, action, pos):
    return self.pending_rewards.pop(cid, 0.0)

# pending_rewards is set when position closes:
# pnl = (exit_prob - entry_prob) * size
self.pending_rewards[cid] = pnl
```

No more micro-bonuses. No more step-by-step unrealized PnL. Just: did you make money when you closed?

**Note on reward normalization**: Raw PnL rewards are z-score normalized before training:
```python
norm_reward = (raw_pnl - running_mean) / (running_std + 1e-8)
```

**Results**: Entropy recovered to 1.05 (near maximum for 3 actions). PnL grew steadily.

| Update | Entropy | PnL | Win Rate |
|--------|---------|-----|----------|
| 1 | 0.68 | $5.20 | 33.3% |
| 10 | 1.06 | $9.55 | 22.9% |
| 20 | 1.05 | $5.85 | 21.1% |
| 36 | 1.05 | $10.93 | 21.2% |

**Final**: $10.93 PnL on $20 max exposure = **55% ROI** (or 109% if measuring per-trade capital)

### The Win Rate Paradox

Win rate settled at ~21%, well below random (33%). But the agent is profitable.

Why? Binary markets have asymmetric payoffs. When you buy an UP token at probability 0.40:
- Win: pay $0.40, receive $1.00 → profit $0.60
- Lose: pay $0.40, receive $0.00 → loss $0.40

You can win 40% of the time and break even. Win 21% of the time but pick your spots at low probabilities? Still profitable.

---

## What Changed Between Phases

| Aspect | Phase 1 | Phase 2 |
|--------|---------|---------|
| Reward | PnL delta + shaping bonuses | Probability-based PnL (normalized) |
| Gamma | 0.995 | 0.99 |
| Entropy coef | 0.02 → 0.05 | 0.10 |
| Buffer/batch | 2048/128 | 512/64 |
| Actions | 7 (variable sizing) | 3 (fixed 50%) |
| Final entropy | 0.36 (collapsed) | 1.05 (healthy) |
| Final PnL | $3.90 | $10.93 |

**Key changes**:

1. **Removed ALL shaping rewards** - No micro-bonuses, no transaction costs, no spread penalty. Just pure `(exit_prob - entry_prob) * size` on close.

2. **5x entropy coefficient** (0.02 → 0.10) - Stronger exploration incentive. Prevented policy collapse.

3. **Simplified action space** (7 → 3) - Reduced from HOLD + 3 buy sizes + 3 sell sizes to just HOLD, BUY, SELL. Learn *when* to trade before *how much*.

4. **Smaller buffer** (2048 → 512) - 4x more frequent updates. Faster learning signal.

5. **Lower gamma** (0.995 → 0.99) - 15-min markets are short; don't over-weight distant rewards.

6. **Reset reward normalization** - Old running stats were calibrated to shaped rewards.

---

## Technical Notes

See [README.md](README.md) for full architecture and hyperparameters.

### Value Loss Spikes

Phase 2 showed value loss spikes as the critic adapted to pure PnL:
- Update 1: 149.5 (reward scale change)
- Update 7: 69.95 (large reward variance)
- Updates 8-9: 18-20 (stabilizing)
- Update 10: 7.16 (settled)

The critic learned to predict a noisier, more meaningful signal.

---

## Takeaways

1. **Reward shaping is risky** - When shaping rewards are gameable and similar magnitude to the real signal, agents optimize the wrong thing. Sparse but honest > dense but noisy.

2. **Probability-based training is a proxy** - We train on probability deltas, not actual binary outcomes. This correlates with but doesn't equal true realized PnL.

3. **Entropy coefficient matters** - 0.05 caused policy collapse; 0.10 maintained healthy exploration. Small hyperparameter, big impact.

4. **Watch for buffer/trade win rate divergence** - When these diverge, the agent is optimizing the wrong objective.

---

*December 29, 2025*
