import os
import json
import asyncio
from collections import defaultdict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="Living Relay V1.5.12 MultiToken")

raw_tokens = os.getenv("CCTV_TOKENS", "").strip()
if raw_tokens:
    ALLOWED_TOKENS = {t.strip() for t in raw_tokens.split(",") if t.strip()}
else:
    legacy = os.getenv("CCTV_TOKEN", "").strip()
    ALLOWED_TOKENS = {legacy} if legacy else set()

senders = defaultdict(dict)          # token -> {device: websocket}
sender_names = defaultdict(dict)     # token -> {device: name}
viewers = defaultdict(lambda: defaultdict(set))  # token -> device -> viewers
directories = defaultdict(set)      # token -> directory sockets
lock = asyncio.Lock()

@app.get("/")
async def root():
    online = sum(len(v) for v in senders.values())
    return JSONResponse({
        "ok": True,
        "service": "Living Relay V1.5.12 MultiToken",
        "online": online,
        "token_groups": len(ALLOWED_TOKENS),
    })

async def directory_payload(token: str):
    devices = [
        {"id": device, "name": sender_names[token].get(device, device)}
        for device in sorted(senders[token].keys())
    ]
    return json.dumps(
        {"type": "device_list", "devices": devices},
        ensure_ascii=False
    )

async def broadcast_directory(token: str):
    payload = await directory_payload(token)
    dead = []

    for ws in list(directories[token]):
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)

    for ws in dead:
        directories[token].discard(ws)

@app.websocket("/ws")
async def ws_endpoint(
    ws: WebSocket,
    role: str = Query(...),
    device: str = Query(""),
    token: str = Query(...),
    name: str = Query("")
):
    if token not in ALLOWED_TOKENS:
        await ws.close(code=4403)
        return

    await ws.accept()

    if role == "directory":
        directories[token].add(ws)
        try:
            await ws.send_text(await directory_payload(token))
            while True:
                await ws.receive_text()
                await ws.send_text(await directory_payload(token))
        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            directories[token].discard(ws)
        return

    if not device:
        await ws.close(code=1008)
        return

    if role == "sender":
        async with lock:
            old = senders[token].get(device)
            if old and old is not ws:
                try:
                    await old.close(code=1012)
                except Exception:
                    pass

            senders[token][device] = ws
            sender_names[token][device] = name or device

        await broadcast_directory(token)

        try:
            while True:
                msg = await ws.receive()

                if msg.get("bytes") is not None:
                    dead = []
                    for v in list(viewers[token][device]):
                        try:
                            await v.send_bytes(msg["bytes"])
                        except Exception:
                            dead.append(v)

                    for v in dead:
                        viewers[token][device].discard(v)

                elif msg.get("text") is not None:
                    dead = []
                    for v in list(viewers[token][device]):
                        try:
                            await v.send_text(msg["text"])
                        except Exception:
                            dead.append(v)

                    for v in dead:
                        viewers[token][device].discard(v)

        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            async with lock:
                if senders[token].get(device) is ws:
                    senders[token].pop(device, None)
                    sender_names[token].pop(device, None)

            await broadcast_directory(token)

    elif role == "viewer":
        viewers[token][device].add(ws)

        try:
            while True:
                msg = await ws.receive()
                s = senders[token].get(device)

                if not s:
                    continue

                try:
                    if msg.get("text") is not None:
                        await s.send_text(msg["text"])
                    elif msg.get("bytes") is not None:
                        await s.send_bytes(msg["bytes"])
                except Exception:
                    pass

        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            viewers[token][device].discard(ws)

    else:
        await ws.close(code=1008)
