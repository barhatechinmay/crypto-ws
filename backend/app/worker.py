import asyncio
import json
import logging
from typing import List, Dict, Any
import websockets

logger = logging.getLogger("crypto-ws.worker")


class BinanceListener:
    BASE_WS = "wss://stream.binance.com:9443/stream?streams="

    def __init__(self, symbols: List[str], output_queue: asyncio.Queue, reconnect_delay: float = 5.0):
        self.symbols = [s.lower() for s in symbols]
        self.output_queue = output_queue
        self.reconnect_delay = reconnect_delay
        self._running = True

    def _build_url(self) -> str:
        streams = "/".join(f"{s}@ticker" for s in self.symbols)
        return f"{self.BASE_WS}{streams}"

    async def _handle_message(self, raw: str) -> Dict[str, Any]:
        obj = json.loads(raw)
        data = obj.get("data", obj)
        symbol = data.get("s")
        last_price = data.get("c")
        change_pct = data.get("P")
        ts = data.get("E") or data.get("closeTime") or None

        parsed = {
            "symbol": symbol,
            "last_price": last_price,
            "change_pct_24h": change_pct,
            "timestamp": ts,
            "raw": data,
        }
        return parsed

    async def run_forever(self):
        backoff = self.reconnect_delay
        while self._running:
            url = self._build_url()
            logger.info("Connecting to Binance: %s", url)
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    logger.info("Connected to Binance")
                    backoff = self.reconnect_delay
                    async for message in ws:
                        try:
                            parsed = await self._handle_message(message)
                            await self.output_queue.put(parsed)
                        except Exception as e:
                            logger.exception("Error parsing message: %s", e)
            except Exception as e:
                logger.exception("Binance connection failed: %s. Reconnecting in %.1fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
