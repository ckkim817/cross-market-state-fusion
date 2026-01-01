"""
Polymarket CLOB WebSocket helpers for orderbook streaming.
"""
import asyncio
import json
import logging
import websockets
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Dict, List, Callable, Optional

logger = logging.getLogger(__name__)

CLOB_WSS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass
class OrderbookState:
    """Orderbook state for a market."""
    condition_id: str
    token_id: str
    side: str  # "UP" or "DOWN"
    bids: List[tuple] = field(default_factory=list)  # [(price, size), ...]
    asks: List[tuple] = field(default_factory=list)
    last_update: Optional[datetime] = None

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0][0] if self.asks else None

    @property
    def mid_price(self) -> Optional[float]:
        if self.best_bid and self.best_ask:
            return (self.best_bid + self.best_ask) / 2
        return self.best_bid or self.best_ask

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid and self.best_ask:
            return self.best_ask - self.best_bid
        return None


class OrderbookStreamer:
    """Stream orderbook data from Polymarket CLOB."""

    def __init__(self):
        self.orderbooks: Dict[str, OrderbookState] = {}
        self.running = False
        self.callbacks: List[Callable] = []
        self._subscriptions: List[tuple] = []  # [(condition_id, token_id, side), ...]
        self._pending_subs: List[str] = []  # New token IDs to subscribe
        self._pending_unsubs: List[str] = []  # Token IDs to unsubscribe
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

    def subscribe(self, condition_id: str, token_up: str, token_down: str):
        """Subscribe to orderbook for a market."""
        # Check if already subscribed
        existing_tokens = {t for _, t, _ in self._subscriptions}
        added = []

        if token_up not in existing_tokens:
            self._subscriptions.append((condition_id, token_up, "UP"))
            self._pending_subs.append(token_up)
            added.append("UP")

        if token_down not in existing_tokens:
            self._subscriptions.append((condition_id, token_down, "DOWN"))
            self._pending_subs.append(token_down)
            added.append("DOWN")

        if added:
            logger.debug(f"Queued {condition_id[:8]}... ({', '.join(added)}) - pending: {len(self._pending_subs)}")

        # Initialize orderbook states
        self.orderbooks[f"{condition_id}_UP"] = OrderbookState(
            condition_id=condition_id,
            token_id=token_up,
            side="UP"
        )
        self.orderbooks[f"{condition_id}_DOWN"] = OrderbookState(
            condition_id=condition_id,
            token_id=token_down,
            side="DOWN"
        )

    async def unsubscribe(self, token_ids: List[str]):
        """Unsubscribe from specific tokens (keeps connection alive)."""
        if not self._ws or not token_ids:
            return

        try:
            msg = {
                "assets_ids": token_ids,
                "operation": "unsubscribe"  # Per API docs
            }
            await self._ws.send(json.dumps(msg))
            logger.info(f"Unsubscribed from {len(token_ids)} tokens")
        except Exception as e:
            logger.error(f"Unsubscribe error: {e}")

    async def clear_stale_async(self, active_condition_ids: set):
        """Remove orderbooks for expired markets and unsubscribe (async)."""
        stale_keys = [k for k in self.orderbooks.keys()
                      if k.rsplit('_', 1)[0] not in active_condition_ids]

        if not stale_keys:
            return

        # Collect stale token IDs before deleting
        stale_tokens = []
        for k in stale_keys:
            ob = self.orderbooks.get(k)
            if ob:
                stale_tokens.append(ob.token_id)
            del self.orderbooks[k]

        # Clean up subscriptions list
        self._subscriptions = [(cid, tid, side) for cid, tid, side in self._subscriptions
                               if cid in active_condition_ids]

        # Unsubscribe from stale tokens (no reconnect needed!)
        if stale_tokens:
            await self.unsubscribe(stale_tokens)
            logger.info(f"Cleared {len(stale_keys)} stale orderbooks")

    def clear_stale(self, active_condition_ids: set):
        """Sync wrapper for clear_stale_async (schedules unsubscribe)."""
        stale_keys = [k for k in self.orderbooks.keys()
                      if k.rsplit('_', 1)[0] not in active_condition_ids]

        if not stale_keys:
            return

        # Collect stale token IDs
        stale_tokens = []
        for k in stale_keys:
            ob = self.orderbooks.get(k)
            if ob:
                stale_tokens.append(ob.token_id)
            del self.orderbooks[k]

        # Clean up subscriptions list
        self._subscriptions = [(cid, tid, side) for cid, tid, side in self._subscriptions
                               if cid in active_condition_ids]

        # Queue unsubscribe (will be processed in stream loop)
        if stale_tokens:
            self._pending_unsubs.extend(stale_tokens)
            logger.info(f"Queued unsubscribe for {len(stale_tokens)} tokens")

    def on_update(self, callback: Callable):
        """Register a callback for orderbook updates."""
        self.callbacks.append(callback)

    def get_orderbook(self, condition_id: str, side: str) -> Optional[OrderbookState]:
        """Get orderbook state for a market side."""
        return self.orderbooks.get(f"{condition_id}_{side}")

    async def stream(self):
        """Start streaming orderbooks."""
        self.running = True

        while self.running:
            # Wait for subscriptions if none exist yet
            if not self._subscriptions and not self._pending_subs:
                await asyncio.sleep(0.5)
                continue

            try:
                async with websockets.connect(CLOB_WSS) as ws:
                    logger.info("Connected to Polymarket CLOB WSS")

                    # Collect all token IDs for initial subscription
                    token_ids = [token_id for _, token_id, _ in self._subscriptions]

                    # Also include any pending subs
                    if self._pending_subs:
                        token_ids.extend(self._pending_subs)
                        self._pending_subs.clear()

                    if token_ids:
                        # Send single subscription with all assets
                        sub_msg = {
                            "assets_ids": token_ids,
                            "type": "market"
                        }
                        await ws.send(json.dumps(sub_msg))
                        logger.info(f"Subscribed to {len(token_ids)} orderbooks")

                    self._ws = ws

                    # Listen for updates
                    while self.running:
                        try:
                            # Process pending unsubscribes (stale markets)
                            if self._pending_unsubs:
                                unsub_tokens = self._pending_unsubs.copy()
                                self._pending_unsubs.clear()
                                unsub_msg = {
                                    "assets_ids": unsub_tokens,
                                    "operation": "unsubscribe"
                                }
                                await ws.send(json.dumps(unsub_msg))
                                logger.debug(f"Sent unsubscribe for {len(unsub_tokens)} tokens")

                            # Process pending subscriptions (new markets)
                            if self._pending_subs:
                                new_tokens = self._pending_subs.copy()
                                self._pending_subs.clear()
                                sub_msg = {
                                    "assets_ids": new_tokens,
                                    "operation": "subscribe"
                                }
                                await ws.send(json.dumps(sub_msg))
                                logger.debug(f"Sent subscription for {len(new_tokens)} new tokens")

                            # Short timeout to check pending subs frequently
                            msg = await asyncio.wait_for(ws.recv(), timeout=0.1)
                            data = json.loads(msg)

                            # Handle different message types
                            if isinstance(data, list):
                                # Initial snapshot is an array
                                for item in data:
                                    if isinstance(item, dict):
                                        self._handle_book_update(item)
                            elif isinstance(data, dict):
                                # Check for orderbook update (has bids/asks)
                                if "bids" in data or "asks" in data:
                                    self._handle_book_update(data)
                                # Check for price_changes
                                elif "price_changes" in data:
                                    self._handle_price_change(data)

                        except asyncio.TimeoutError:
                            pass
                        except json.JSONDecodeError:
                            pass

                    self._ws = None

            except Exception as e:
                logger.warning(f"CLOB WSS error: {e}, reconnecting...")
                await asyncio.sleep(1)

    def _handle_book_update(self, data: dict):
        """Handle orderbook update message."""
        asset_id = data.get("asset_id")
        bids = data.get("bids", [])
        asks = data.get("asks", [])

        # Find matching orderbook
        for key, ob in self.orderbooks.items():
            if ob.token_id == asset_id:
                # Parse and sort: bids descending, asks ascending
                parsed_bids = [(float(b["price"]), float(b["size"])) for b in bids]
                parsed_asks = [(float(a["price"]), float(a["size"])) for a in asks]

                # Sort bids high to low, asks low to high
                ob.bids = sorted(parsed_bids, key=lambda x: x[0], reverse=True)[:10]
                ob.asks = sorted(parsed_asks, key=lambda x: x[0])[:10]
                ob.last_update = datetime.now(timezone.utc)

                # Call callbacks
                for cb in self.callbacks:
                    try:
                        cb(ob)
                    except Exception as e:
                        logger.error(f"Callback error: {e}")
                break

    def _handle_price_change(self, data: dict):
        """Handle price change message (simpler update)."""
        changes = data.get("price_changes", [])
        for change in changes:
            asset_id = change.get("asset_id")
            price = change.get("price")

            # Find matching orderbook and update mid estimate
            for key, ob in self.orderbooks.items():
                if ob.token_id == asset_id:
                    ob.last_update = datetime.now(timezone.utc)
                    break

    async def reconnect(self):
        """Force a reconnection (closes current connection)."""
        logger.debug("Manual reconnect requested")
        if self._ws:
            await self._ws.close()

    def stop(self):
        """Stop streaming."""
        self.running = False


if __name__ == "__main__":
    # Test with a real market
    from polymarket_api import get_active_markets

    print("Testing Orderbook WSS...")

    async def test():
        markets = get_active_markets()

        if not markets:
            print("No active markets!")
            return

        m = markets[0]
        print(f"\nSubscribing to: {m.question[:50]}...")

        streamer = OrderbookStreamer()
        streamer.subscribe(m.condition_id, m.token_up, m.token_down)

        def on_update(ob: OrderbookState):
            print(f"  {ob.side}: bid={ob.best_bid:.3f} ask={ob.best_ask:.3f} spread={ob.spread:.3f}")

        streamer.on_update(on_update)

        # Run for 15 seconds
        task = asyncio.create_task(streamer.stream())
        await asyncio.sleep(15)
        streamer.stop()
        task.cancel()

    asyncio.run(test())
