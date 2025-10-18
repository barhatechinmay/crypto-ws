# Backend (FastAPI)

## Run locally
1. cd backend
2. pip install -r requirements.txt
3. uvicorn app.main:app --reload --port 8000

## Docker
docker build -t crypto-ws-backend .
docker run -p 8000:8000 crypto-ws-backend

## Notes
- The backend connects to Binance public websocket and broadcasts to connected clients.
- WebSocket endpoint: /ws
- REST: /price?symbol=BTCUSDT
