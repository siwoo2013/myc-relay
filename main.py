import os
import json
from collections import defaultdict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query

app = FastAPI(title="Living Relay V1.5.12 MultiToken")

def allowed_tokens():
    raw = os.getenv("CCTV_TOKENS", "").strip()
    if not raw:
        legacy = os.getenv("CCTV_TOKEN", "").strip()
        return {legacy} if legacy else set()
    return {x.strip() for x in raw.split(",") if x.strip()}

# token -> device -> websocket
senders = defaultdict(dict)
sender_names = defaultdict(dict)
# token -> device -> set(websocket)
viewers = defaultdict(lambda: defaultdict(set))
# token -> set(directory websocket)
directories = defaultdict(set)

async def broadcast_directory(token: str):
    payload = json.dumps({
        "type": "device_list",
        "devices": [
            {"id": device_id, "name": sender_names[token].get(device_id, device_id)}
            for device_id in sorted(senders[token].keys())
        ],
    })
    dead = []
    for ws in list(directories[token]):
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        directories[token].discard(ws)

@app.get("/")
def root():
    return {"ok": True, "service": "Living Relay V1.5.12 MultiToken"}

@app.websocket("/ws")
async def ws_endpoint(
    websocket: WebSocket,
    role: str = Query(...),
    token: str = Query(...),
    device: str | None = Query(None),
    name: str | None = Query(None),
):
    if token not in allowed_tokens():
        await websocket.close(code=4403)
        return

    await websocket.accept()

    if role == "directory":
        directories[token].add(websocket)
        try:
            await broadcast_directory(token)
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            directories[token].discard(websocket)
        return

    if role == "sender":
        if not device:
            await websocket.close(code=4400)
            return
        senders[token][device] = websocket
        sender_names[token][device] = name or device
        await broadcast_directory(token)
        try:
            while True:
                message = await websocket.receive()
                if message.get("bytes") is not None:
                    data = message["bytes"]
                    for vw in list(viewers[token][device]):
                        try:
                            await vw.send_bytes(data)
                        except Exception:
                            viewers[token][device].discard(vw)
                elif message.get("text") is not None:
                    text = message["text"]
                    for vw in list(viewers[token][device]):
                        try:
                            await vw.send_text(text)
                        except Exception:
                            viewers[token][device].discard(vw)
        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            if senders[token].get(device) is websocket:
                senders[token].pop(device, None)
                sender_names[token].pop(device, None)
            await broadcast_directory(token)
        return

    if role == "viewer":
        if not device:
            await websocket.close(code=4400)
            return
        viewers[token][device].add(websocket)
        try:
            while True:
                message = await websocket.receive()
                sender = senders[token].get(device)
                if sender is None:
                    continue
                if message.get("bytes") is not None:
                    await sender.send_bytes(message["bytes"])
                elif message.get("text") is not None:
                    await sender.send_text(message["text"])
        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            viewers[token][device].discard(websocket)
            # Safety: if the last viewer disappears (normal close, app kill, or network loss),
            # immediately tell the server phone to release camera and microphone.
            if not viewers[token][device]:
                sender = senders[token].get(device)
                if sender is not None:
                    try:
                        await sender.send_text(json.dumps({"type": "control", "mode": "live_off"}))
                        await sender.send_text(json.dumps({"type": "control", "mode": "audio_off"}))
                    except Exception:
                        pass
        return

    await websocket.close(code=4400)
