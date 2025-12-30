# Training Journal: RL on Polymarket

Training a PPO agent to trade 15-minute binary prediction markets. This documents what worked, what didn't, and what it means.

---

## The Experiment

**Question**: Can an RL agent learn profitable trading patterns from sparse PnL rewards?

**Setup**: Paper trade 4 concurrent crypto markets (BTC, ETH, SOL, XRP) on Polymarket using live data from Binance + Polymarket orderbooks. $10 base capital, 50% position sizing.

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

The agent fuses three real-time streams:

```
Binance Futures WSS  → Price returns (1m, 5m, 10m), volatility, order flow, CVD, large trades
Polymarket CLOB WSS  → Bid/ask spread, orderbook imbalance
```

This creates an 18-dimensional state that captures both underlying asset dynamics AND prediction market microstructure:

| Category | Features |
|----------|----------|
| Momentum | 1m/5m/10m returns |
| Order flow | L1/L5 imbalance, trade flow, CVD acceleration |
| Microstructure | Spread %, trade intensity, large trade flag |
| Volatility | 5m vol, vol expansion ratio |
| Position | Has position, side, PnL, time remaining |
| Regime | Vol regime, trend regime |

### Sparse Reward Signal

The agent only receives reward when a position closes. No intermediate feedback while holding.

**Important caveat**: The reward is based on **probability change**, not actual market resolution. When a position closes (either by explicit sell or at market expiry):

```
reward = (exit_probability - entry_probability) × size
```

Example trades:
- Buy UP at 0.55, sell at 0.65 → reward = +$0.10 × size
- Buy UP at 0.55, hold to expiry where prob = 0.70 → reward = +$0.15 × size

Note: At expiry, we use the final probability, not the binary outcome ($1 or $0). This means training signal differs from true realized PnL. The agent learns "did probability move my way?" rather than "did I predict the actual outcome correctly?"

This sparsity makes credit assignment harder. The agent takes actions every tick but only learns from PnL when positions close.

---

## Training: Two Phases

### Phase 1: Shaped Rewards (Updates 1-36)

**Duration**: ~52 minutes | **Trades**: 1,545

Started with a reward function that tried to guide learning:

```python
reward = pnl_delta * 0.1           # Actual PnL (scaled down)
reward -= 0.001                    # Transaction cost penalty
reward += 0.002 * momentum_aligned # Bonus for trading with momentum
reward += 0.001 * size_multiplier  # Bonus for larger positions
```

**What happened**: Entropy collapsed from 1.09 → 0.36. The policy became nearly deterministic, fixating on a single action pattern.

| Update | Entropy | PnL | Win Rate |
|--------|---------|-----|----------|
| 1 | 1.09 | $3.25 | 19.8% |
| 10 | 0.79 | $4.40 | 17.3% |
| 20 | 0.40 | $1.37 | 19.4% |
| 36 | 0.36 | $3.90 | 20.2% |

**What we learned**: The shaping rewards were similar magnitude to actual PnL. With typical PnL deltas of $0.01-0.05, the scaled signal was 0.001-0.005 - same as the bonuses.

The agent learned to game the reward function:
- Trade with momentum → collect +0.002 bonus
- Use large sizes → collect +0.001 bonus
- Actual profitability? Optional.

Buffer win rate showed 90%+ (counting bonus-positive experiences) while actual trade win rate was 20%. The agent was optimizing the reward function, not the underlying goal.

### Diagnosis: Reward Shaping Backfired

The divergence between buffer win rate (what the agent optimized) and cumulative win rate (what we cared about) revealed the problem:

- **Buffer win rate**: % of experiences with reward > 0 (includes shaping bonuses)
- **Cumulative win rate**: % of closed trades that were profitable

When these diverge, the agent is learning the wrong thing.

---

### Phase 2: Pure PnL (Updates 37-72)

**Duration**: ~52 minutes | **Trades**: 3,330

Switched to probability-based PnL on position close:

```python
def reward(position_close):
    return (exit_prob - entry_prob) * size  # Probability delta, not binary outcome
```

Also:
- Doubled entropy coefficient (0.05 → 0.10)
- Simplified action space (7 → 3 actions)
- Reset reward normalization stats

**Note on reward normalization**: Raw PnL rewards are z-score normalized before training:
```python
norm_reward = (raw_pnl - running_mean) / (running_std + 1e-8)
```
This helps with gradient stability but means the network sees normalized values, not raw dollars.

**What happened**: Entropy recovered to 1.05 (near maximum for 3 actions). PnL grew steadily.

| Update | Entropy | PnL | Win Rate |
|--------|---------|-----|----------|
| 1 | 0.68 | $5.20 | 33.3% |
| 10 | 1.06 | $9.55 | 22.9% |
| 20 | 1.05 | $5.85 | 21.1% |
| 36 | 1.05 | $10.93 | 21.2% |

**Final**: $10.93 PnL on $10 base = **109% ROI**

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
| Entropy coef | 0.05 | 0.10 |
| Actions | 7 (variable sizing) | 3 (fixed 50%) |
| Final entropy | 0.36 (collapsed) | 1.05 (healthy) |
| Final PnL | $3.90 | $10.93 |

**Key changes**:

1. **Removed shaping rewards** - No more gameable bonuses. Sparse probability-delta signal only.

2. **Doubled entropy coefficient** - Stronger exploration incentive. Prevented policy collapse.

3. **Simplified action space** - Reduced from 7 actions (HOLD + 3 buy sizes + 3 sell sizes) to 3 (HOLD, BUY, SELL). Let the model learn *when* to trade before learning *how much*.

4. **Reset reward normalization** - Old running stats were calibrated to shaped rewards (mean≈-0.002, std≈0.01). Pure PnL has different distribution.

---

## Technical Details

### PPO Implementation (MLX)

```python
# GAE advantage estimation
advantages = compute_gae(rewards, values, dones, gamma=0.99, lambda_=0.95)
returns = advantages + values

# Normalize advantages
advantages = (advantages - mean) / (std + 1e-8)

# Clipped policy loss
ratio = new_prob / old_prob
surr1 = ratio * advantage
surr2 = clip(ratio, 1-0.2, 1+0.2) * advantage
policy_loss = -min(surr1, surr2).mean()

# Value loss + entropy bonus
value_loss = MSE(values, returns)
entropy = -(probs * log(probs)).sum(-1).mean()
loss = policy_loss + 0.5 * value_loss - 0.10 * entropy
```

### Network Architecture

```
Actor:  18 → 128 (tanh) → 128 (tanh) → 3 (softmax)
Critic: 18 → 128 (tanh) → 128 (tanh) → 1
```

### Hyperparameters

| Parameter | Value |
|-----------|-------|
| Buffer size | 512 experiences |
| Batch size | 64 |
| Epochs per update | 10 |
| Actor LR | 1e-4 |
| Critic LR | 3e-4 |
| Gamma | 0.99 |
| GAE lambda | 0.95 |
| Clip epsilon | 0.2 |
| Entropy coef | 0.10 |

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
