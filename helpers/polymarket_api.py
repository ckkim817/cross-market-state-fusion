"""
Polymarket API helpers for 15-min up/down markets.
Finds BTC, ETH, SOL, XRP markets using slug pattern.
"""
import asyncio
import logging
import aiohttp
import requests
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple

logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
API_TIMEOUT = 10

# 15-min assets (slug pattern: {asset}-updown-15m-{timestamp})
ASSETS_15M = ["btc", "eth", "sol", "xrp"]


@dataclass
class Market:
    """Active 15-min prediction market."""
    condition_id: str
    question: str
    asset: str
    end_time: datetime
    token_up: str
    token_down: str
    price_up: float = 0.5
    price_down: float = 0.5
    slug: str = ""


async def _fetch_event_async(
    session: aiohttp.ClientSession,
    asset: str,
    ts: int
) -> Optional[Tuple[str, Dict]]:
    """Fetch single event by slug (async)."""
    slug = f"{asset}-updown-15m-{ts}"
    url = f"{GAMMA_API}/events?slug={slug}"
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            if data:
                return (asset, data[0])
    except Exception:
        pass
    return None


async def _fetch_clob_async(
    session: aiohttp.ClientSession,
    condition_id: str
) -> Optional[Dict]:
    """Fetch CLOB market data (async)."""
    url = f"{CLOB_API}/markets/{condition_id}"
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


async def get_15m_markets_async(assets: List[str] = None) -> List[Market]:
    """
    Get currently active 15-min up/down markets (async, parallel).

    Args:
        assets: List of assets (default: btc, eth, sol, xrp)

    Returns:
        List of active Market objects sorted by end time.
    """
    if assets is None:
        assets = ASSETS_15M
    else:
        assets = [a.lower() for a in assets]

    now = datetime.now(timezone.utc)
    current_ts = int(now.timestamp())
    window_start = (current_ts // 900) * 900
    timestamps = [window_start + (i * 900) for i in range(4)]

    markets = []
    found_assets = set()

    # Limit concurrent requests to avoid rate limiting
    semaphore = asyncio.Semaphore(5)

    async def fetch_with_limit(session, asset, ts):
        async with semaphore:
            return await _fetch_event_async(session, asset, ts)

    async def fetch_clob_with_limit(session, cid):
        async with semaphore:
            return await _fetch_clob_async(session, cid)

    timeout = aiohttp.ClientTimeout(total=API_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Phase 1: Fetch all events in parallel (with rate limit)
        event_tasks = [
            fetch_with_limit(session, asset, ts)
            for asset in assets
            for ts in timestamps
        ]
        event_results = await asyncio.gather(*event_tasks, return_exceptions=True)

        # Filter valid events
        valid_events = []
        for result in event_results:
            if isinstance(result, tuple) and result is not None:
                asset, event = result
                if asset not in found_assets:
                    end_str = event.get("endDate", "")
                    if end_str:
                        end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                        if end_dt > now:
                            condition_id = None
                            for m in event.get("markets", []):
                                condition_id = m.get("conditionId")
                                if condition_id:
                                    break
                            if condition_id:
                                valid_events.append((asset, condition_id, end_dt))
                                found_assets.add(asset)

        # Phase 2: Fetch CLOB data in parallel (with rate limit)
        clob_tasks = [
            fetch_clob_with_limit(session, cid)
            for _, cid, _ in valid_events
        ]
        clob_results = await asyncio.gather(*clob_tasks, return_exceptions=True)

        # Build market objects
        for (asset, condition_id, end_dt), clob_data in zip(valid_events, clob_results):
            if not isinstance(clob_data, dict):
                continue
            if not clob_data.get("active") or clob_data.get("closed"):
                continue

            tokens = clob_data.get("tokens", [])
            token_up = token_down = None
            price_up = price_down = 0.5

            for t in tokens:
                outcome = t.get("outcome", "").lower()
                if outcome == "up":
                    token_up = t.get("token_id")
                    price_up = t.get("price", 0.5)
                elif outcome == "down":
                    token_down = t.get("token_id")
                    price_down = t.get("price", 0.5)

            if token_up and token_down:
                markets.append(Market(
                    condition_id=condition_id,
                    question=clob_data.get("question", ""),
                    asset=asset.upper(),
                    end_time=end_dt,
                    token_up=token_up,
                    token_down=token_down,
                    price_up=price_up,
                    price_down=price_down,
                    slug=f"{asset}-updown-15m-{int(end_dt.timestamp())-900}",
                ))

    markets.sort(key=lambda m: m.end_time)
    return markets


def get_market_from_clob(condition_id: str) -> Optional[Dict]:
    """Get market details from CLOB API including token IDs (sync, for compatibility)."""
    url = f"{CLOB_API}/markets/{condition_id}"
    resp = requests.get(url, timeout=API_TIMEOUT)
    if resp.status_code != 200:
        return None
    try:
        return resp.json()
    except ValueError:
        return None


def get_15m_markets(assets: List[str] = None) -> List[Market]:
    """
    Get currently active 15-min up/down markets (sync wrapper).

    Uses parallel async requests internally for better performance.

    Args:
        assets: List of assets (default: btc, eth, sol, xrp)

    Returns:
        List of active Market objects sorted by end time.
    """
    try:
        return asyncio.run(get_15m_markets_async(assets))
    except RuntimeError:
        # Already in async context, create new event loop
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(get_15m_markets_async(assets))
        finally:
            loop.close()


def get_next_market(asset: str) -> Optional[Market]:
    """Get the next closing 15-min market for a specific asset."""
    markets = get_15m_markets(assets=[asset])
    return markets[0] if markets else None


# Backwards compat
get_active_markets = get_15m_markets


if __name__ == "__main__":
    print("=" * 60)
    print("15-MIN UP/DOWN MARKETS")
    print("=" * 60)

    markets = get_15m_markets()
    now = datetime.now(timezone.utc)

    if not markets:
        print("\nNo active 15-min markets found!")
    else:
        for m in markets:
            mins_left = (m.end_time - now).total_seconds() / 60
            print(f"\n{m.asset} 15m")
            print(f"  {m.question}")
            print(f"  Closes in: {mins_left:.1f} min")
            print(f"  UP: {m.price_up:.3f} | DOWN: {m.price_down:.3f}")
            print(f"  Condition: {m.condition_id}")
            print(f"  Token UP: {m.token_up}")
            print(f"  Token DOWN: {m.token_down}")
