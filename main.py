import os
from collections import defaultdict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="Siwoo Phone Monitor Relay V1")
TOKEN = os.getenv("CCTV_TOKEN", "change-me")

senders = {}
viewers = defaultdict(set)

@app.get("/")
async def root():
    return JSONResponse({"ok": True, "service": "Siwoo Phone Monitor Relay V1"})

@app.websocket("/ws")
async def ws_endpoint(
    ws: WebSocket,
    role: str = Query(...),
    device: str = Query(...),
    token: str = Query(...)
):
    if token != TOKEN:
        await ws.close(code=1008)
        return

    await ws.accept()

    if role == "sender":
        old = senders.get(device)
        if old and old is not ws:
            try:
                await old.close(code=1012)
            except Exception:
                pass
        senders[device] = ws

        try:
            while True:
                msg = await ws.receive()
                if "bytes" in msg and msg["bytes"] is not None:
                    dead = []
                    for v in list(viewers[device]):
                        try:
                            await v.send_bytes(msg["bytes"])
                        except Exception:
                            dead.append(v)
                    for v in dead:
                        viewers[device].discard(v)
                elif "text" in msg and msg["text"] is not None:
                    dead = []
                    for v in list(viewers[device]):
                        try:
                            await v.send_text(msg["text"])
                        except Exception:
                            dead.append(v)
                    for v in dead:
                        viewers[device].discard(v)
        except WebSocketDisconnect:
            pass
        finally:
            if senders.get(device) is ws:
                senders.pop(device, None)

    elif role == "viewer":
        viewers[device].add(ws)
        try:
            while True:
                msg = await ws.receive()
                s = senders.get(device)
                if not s:
                    continue
                if "text" in msg and msg["text"] is not None:
                    await s.send_text(msg["text"])
                elif "bytes" in msg and msg["bytes"] is not None:
                    await s.send_bytes(msg["bytes"])
        except WebSocketDisconnect:
            pass
        finally:
            viewers[device].discard(ws)
    else:
        await ws.close(code=1008)
