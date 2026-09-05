import os
import json
import asyncio
from collections import defaultdict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="Living Relay V1.5")
TOKEN = os.getenv("CCTV_TOKEN", "change-me")

senders: dict[str, WebSocket] = {}
sender_names: dict[str, str] = {}
viewers = defaultdict(set)
directories: set[WebSocket] = set()
lock = asyncio.Lock()

@app.get("/")
async def root():
    return JSONResponse({"ok": True, "service": "Living Relay V1.5", "online": len(senders)})

async def directory_payload():
    devices = [
        {"id": device, "name": sender_names.get(device, device)}
        for device in sorted(senders.keys())
    ]
    return json.dumps({"type": "device_list", "devices": devices}, ensure_ascii=False)

async def broadcast_directory():
    payload = await directory_payload()
    dead = []
    for ws in list(directories):
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        directories.discard(ws)

@app.websocket("/ws")
async def ws_endpoint(
    ws: WebSocket,
    role: str = Query(...),
    device: str = Query(""),
    token: str = Query(...),
    name: str = Query("")
):
    if token != TOKEN:
        await ws.close(code=1008)
        return

    await ws.accept()

    if role == "directory":
        directories.add(ws)
        try:
            await ws.send_text(await directory_payload())
            while True:
                # Keep the socket alive; optional refresh messages are accepted.
                await ws.receive_text()
                await ws.send_text(await directory_payload())
        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            directories.discard(ws)
        return

    if not device:
        await ws.close(code=1008)
        return

    if role == "sender":
        async with lock:
            old = senders.get(device)
            if old and old is not ws:
                try:
                    await old.close(code=1012)
                except Exception:
                    pass
            senders[device] = ws
            sender_names[device] = name or device
        await broadcast_directory()

        try:
            while True:
                msg = await ws.receive()
                if msg.get("bytes") is not None:
                    dead = []
                    for v in list(viewers[device]):
                        try:
                            await v.send_bytes(msg["bytes"])
                        except Exception:
                            dead.append(v)
                    for v in dead:
                        viewers[device].discard(v)
                elif msg.get("text") is not None:
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
        except Exception:
            pass
        finally:
            async with lock:
                if senders.get(device) is ws:
                    senders.pop(device, None)
                    sender_names.pop(device, None)
            await broadcast_directory()

    elif role == "viewer":
        viewers[device].add(ws)
        try:
            while True:
                msg = await ws.receive()
                s = senders.get(device)
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
            viewers[device].discard(ws)
    else:
        await ws.close(code=1008)
