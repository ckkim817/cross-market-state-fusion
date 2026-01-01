"""
Binance WebSocket helpers for real-time crypto price data.
"""
import asyncio
import json
import logging
import requests
import websockets
from collections import deque
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Dict, Deque, List, Callable, Optional

logger = logging.getLogger(__name__)

BINANCE_WSS = "wss://stream.binance.com:9443"

# Asset to Binance symbol mapping
SYMBOLS = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "SOL": "solusdt",
    "XRP": "xrpusdt",
}


@dataclass
class PriceState:
    """Real-time price state for an asset."""
    asset: str
    price: float = 0.0
    last_update: Optional[datetime] = None
    history: Deque[float] = field(default_factory=lambda: deque(maxlen=1000))

    def update(self, price: float):
        self.price = price
        self.last_update = datetime.now(timezone.utc)
        self.history.append(price)  # deque auto-discards oldest when full


class BinanceStreamer:
    """Stream real-time prices from Binance for multiple assets."""

    def __init__(self, assets: List[str] = None):
        """
        Initialize streamer.

        Args:
            assets: List of assets to track (e.g., ["BTC", "ETH", "SOL"])
        """
        self.assets = assets or ["BTC", "ETH", "SOL"]
        self.states: Dict[str, PriceState] = {}
        self.running = False
        self.callbacks: List[Callable] = []

        for asset in self.assets:
            self.states[asset] = PriceState(asset=asset)

    def on_price(self, callback: Callable):
        """Register a callback for price updates."""
        self.callbacks.append(callback)

    def get_price(self, asset: str) -> float:
        """Get current price for an asset."""
        state = self.states.get(asset)
        return state.price if state else 0.0

    def get_history(self, asset: str, n: int = 100) -> List[float]:
        """Get price history for an asset."""
        state = self.states.get(asset)
        if not state:
            return []
        # Convert deque slice to list for compatibility
        return list(state.history)[-n:]

    async def stream(self):
        """Start streaming prices."""
        self.running = True

        # Build stream URL
        symbols = [SYMBOLS[a] for a in self.assets if a in SYMBOLS]
        streams = "/".join([f"{s}@trade" for s in symbols])
        url = f"{BINANCE_WSS}/stream?streams={streams}"

        logger.info(f"Connecting to Binance WSS for {', '.join(self.assets)}...")

        while self.running:
            try:
                async with websockets.connect(url) as ws:
                    logger.info("Connected to Binance")

                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                            data = json.loads(msg)

                            if "data" in data:
                                trade = data["data"]
                                symbol = trade["s"].upper()
                                price = float(trade["p"])

                                # Map to asset
                                for asset, sym in SYMBOLS.items():
                                    if sym.upper() == symbol:
                                        state = self.states.get(asset)
                                        if state:
                                            state.update(price)

                                            # Call callbacks
                                            for cb in self.callbacks:
                                                try:
                                                    cb(asset, price)
                                                except Exception as e:
                                                    logger.error(f"Callback error: {e}")
                                        break

                        except asyncio.TimeoutError:
                            pass
                        except json.JSONDecodeError:
                            pass

            except Exception as e:
                logger.warning(f"WSS error: {e}, reconnecting...")
                await asyncio.sleep(1)

    def stop(self):
        """Stop streaming."""
        self.running = False


async def get_current_prices(assets: List[str] = None) -> Dict[str, float]:
    """
    Get current prices for assets (one-shot, not streaming).

    Args:
        assets: List of assets (default: BTC, ETH, SOL)

    Returns:
        Dict mapping asset to price
    """
    if assets is None:
        assets = ["BTC", "ETH", "SOL"]

    prices = {}

    for asset in assets:
        symbol = SYMBOLS.get(asset)
        if not symbol:
            continue

        try:
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol.upper()}"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                prices[asset] = float(data["price"])
        except Exception as e:
            logger.error(f"Error fetching price for {asset}: {e}")

    return prices


if __name__ == "__main__":
    import asyncio

    async def test():
        prices = await get_current_prices(["BTC", "ETH", "SOL", "XRP"])
        print("Current prices:")
        for asset, price in prices.items():
            print(f"  {asset}: ${price:,.2f}")

    asyncio.run(test())
