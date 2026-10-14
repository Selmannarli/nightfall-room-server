"""Portable room relay. Requires no Cloudflare services or purchased domain."""
import asyncio
import collections
import http
import json
import os
import re
import secrets
import signal
import struct
import time
from dataclasses import dataclass, field

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed


@dataclass
class Member:
    socket: object
    peer_id: int
    window: float = field(default_factory=time.monotonic)
    count: int = 0
    size: int = 0


@dataclass
class Room:
    code: str
    members: dict = field(default_factory=dict)


rooms = {}
attempts = {}
MAX_ROOMS = int(os.environ.get("MAX_ROOMS", "16"))


async def send(socket, payload):
    try:
        await socket.send(payload if isinstance(payload, bytes) else json.dumps(payload, ensure_ascii=False))
        return True
    except ConnectionClosed:
        return False


async def reject(socket, message):
    await send(socket, {"type": "error", "message": message})
    await socket.close(1008, message)


def rate_allowed(socket, creating):
    # The socket address is trusted; arbitrary forwarded headers cannot evade limits.
    ip = socket.remote_address[0] if socket.remote_address else "unknown"
    key = (ip, creating)
    now = time.monotonic()
    history = attempts.setdefault(key, collections.deque())
    while history and now - history[0] >= 60:
        history.popleft()
    if len(history) >= (10 if creating else 40):
        return False
    history.append(now)
    if len(attempts) > 1024:
        for old_key in list(attempts):
            if not attempts[old_key] or now - attempts[old_key][-1] >= 60:
                del attempts[old_key]
    return True


async def leave(room, member):
    # Remove state before awaiting network I/O, including repeated close notifications.
    if room.members.get(member.peer_id) is not member:
        return
    if member.peer_id == 1:
        if rooms.get(room.code) is room:
            del rooms[room.code]
        peers = list(room.members.values())
        room.members.clear()
        for peer in peers:
            if peer is not member:
                await send(peer.socket, {"type": "error", "message": "Oda sahibi oyundan ayrıldı."})
                await peer.socket.close(4001, "Host left")
    else:
        del room.members[member.peer_id]
        host = room.members.get(1)
        if host:
            await send(host.socket, {"type": "left", "id": member.peer_id})


async def relay(socket):
    path = socket.request.path.split("?", 1)[0]
    creating = path == "/create"
    match = re.fullmatch(r"/join/([0-9]{6})", path)
    if not creating and not match:
        return await reject(socket, "Altı haneli oda kodunu kontrol et.")
    if not rate_allowed(socket, creating):
        return await reject(socket, "Çok sık deneme yapıldı. Bir dakika bekle.")

    # No await between lookup, capacity validation, and reservation.
    if creating:
        if len(rooms) >= MAX_ROOMS:
            return await reject(socket, "Sunucu şu anda dolu. Biraz sonra yeniden dene.")
        while True:
            code = str(100000 + secrets.randbelow(900000))
            if code not in rooms:
                break
        room = rooms[code] = Room(code)
        peer_id = 1
    else:
        code = match[1]
        room = rooms.get(code)
        if not room or 1 not in room.members:
            return await reject(socket, "Oda bulunamadı veya oda sahibi çıktı.")
        if len(room.members) >= 4:
            return await reject(socket, "Oda dolu. En fazla dört oyuncu katılabilir.")
        peer_id = next(i for i in (2, 3, 4) if i not in room.members)

    member = Member(socket, peer_id)
    room.members[peer_id] = member
    try:
        if not await send(socket, {"type": "welcome", "id": peer_id, "code": code}):
            return
        if peer_id != 1:
            host = room.members.get(1)
            if not host or not await send(host.socket, {"type": "joined", "id": peer_id}):
                return await reject(socket, "Oda sahibi oyundan ayrıldı.")
        async for message in socket:
            if isinstance(message, str):
                if len(message) > 1024:
                    await socket.close(1009, "Command too large")
                    break
                try:
                    command = json.loads(message)
                except (ValueError, TypeError):
                    await socket.close(1008, "Invalid command")
                    break
                if not isinstance(command, dict):
                    await socket.close(1008, "Invalid command")
                    break
                if command.get("type") == "leave":
                    await leave(room, member)
                    await socket.close()
                    break
                if member.peer_id == 1 and command.get("type") == "kick":
                    target = room.members.get(command.get("id"))
                    if target and target.peer_id != 1:
                        await target.socket.close(1008, "Removed by host")
                continue

            now = time.monotonic()
            if now - member.window >= 1:
                member.window, member.count, member.size = now, 0, 0
            member.count += 1
            member.size += len(message)
            if len(message) < 12 or len(message) > 1048588 or member.count > 240 or member.size > 4194304:
                await socket.close(1008, "Packet limit")
                break
            target, channel, mode = struct.unpack("<iII", message[:12])
            if channel > 255 or mode > 2 or member.peer_id != 1 and target != 1:
                await socket.close(1008, "Invalid target or channel")
                break
            packet = struct.pack("<III", member.peer_id, channel, mode) + message[12:]
            for recipient in list(room.members.values()):
                peer = recipient.peer_id
                if peer != member.peer_id and (target == 0 or target == peer or target < 0 and peer != -target):
                    await send(recipient.socket, packet)
    except ConnectionClosed:
        pass
    finally:
        await leave(room, member)


def health(connection, request):
    if request.path.split("?", 1)[0] in ("/", "/health") and request.headers.get("Upgrade", "").lower() != "websocket":
        return connection.respond(http.HTTPStatus.OK, json.dumps({"service": "nightfall-rooms", "protocol": 1}))


async def main():
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    host, port = os.environ.get("HOST", "0.0.0.0"), int(os.environ.get("PORT", "8080"))
    async with serve(relay, host, port, process_request=health, max_size=1048588,
                     max_queue=4, write_limit=65536, ping_interval=20, ping_timeout=20,
                     close_timeout=3, compression=None):
        print(f"ROOM_SERVER_READY port={port}", flush=True)
        await stop.wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

