import asyncio,worker
import json
import logging
from typing import Dict, Set

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from worker import BinanceListener

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("crypto-ws")

app = FastAPI(title="Crypto Price WS Broker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

latest_prices: Dict[str, Dict] = {}
clients: Set[WebSocket] = set()
clients_lock = asyncio.Lock()
broadcast_queue: asyncio.Queue = asyncio.Queue()


@app.on_event("startup")
async def startup_event():
    symbols = ["btcusdt", "ethusdt"]
    listener = BinanceListener(symbols=symbols, output_queue=broadcast_queue)
    asyncio.create_task(listener.run_forever())
    asyncio.create_task(broadcaster())


async def broadcaster():
    while True:
        data = await broadcast_queue.get()
        symbol = data.get("symbol")
        if symbol:
            latest_prices[symbol] = data
        message_text = json.dumps(data)
        await _broadcast_to_clients(message_text)


async def _broadcast_to_clients(message: str):
    async with clients_lock:
        to_remove = []
        for ws in list(clients):
            try:
                await ws.send_text(message)
            except Exception as e:
                logger.warning("Error sending to client, scheduling removal: %s", e)
                to_remove.append(ws)
        for ws in to_remove:
            clients.discard(ws)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    async with clients_lock:
        clients.add(websocket)
    logger.info("Client connected. Total clients: %d", len(clients))
    try:
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=60.0)
            except asyncio.TimeoutError:
                await websocket.send_text(json.dumps({"type": "heartbeat"}))
    except WebSocketDisconnect:
        logger.info("Client disconnected")
    finally:
        async with clients_lock:
            clients.discard(websocket)
        logger.info("Client removed. Total clients: %d", len(clients))


@app.get("/price")
async def get_latest_price(symbol: str = "BTCUSDT"):
    s = symbol.upper()
    if s in latest_prices:
        return JSONResponse(latest_prices[s])
    return JSONResponse({"error": "no data for symbol yet"}, status_code=404)
