# -*- coding: utf-8 -*-
# ═══════════════════════════════════════════════════════════════════
#  TEAM 84FF v6 — Reconnect-After-Match + Movement (No Cache)
#  TCP: 9015 | Region: ME (LOCKED) | KeepAlive: 0219
#  ✅ إصلاح منطق الفريق (لا يبدأ إلا بـ 4/4)
#  ❌ بدون كاش (تسجيل دخول من جديد بعد كل مباراة)
# ═══════════════════════════════════════════════════════════════════
import sys
import os
os.environ['PYTHONUNBUFFERED'] = '1'
os.environ.setdefault('PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION', 'python')
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

if sys.platform == "win32":
    try:
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8', errors='replace')
            sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

import asyncio
import httpx
import random
import json
import socket
import struct
import time
import uuid
import itertools
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

from google_play_scraper import app as play_scraper
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from protobuf_decoder.protobuf_decoder import Parser
from message_ids import MESSAGE_ID_TO_NAME
import thunderFF_pb2

# ═══════════════════════════ CONFIG ═══════════════════════════
START_MATCH_INTERVAL = 4.0
MAX_MATCH_DURATION = 900
MATCH_IDLE_TIMEOUT = 30.0
NON_MATCH_RECONNECT_DELAY = 1.0
NEW_MATCH_DELAY = 15.0    # زيادة الوقت لمنع البان

MODE_ID = 1
MAP_ID = 1

ACCOUNT_REGION = "ME"

# ─── TEAM CONFIG ───
TEAM_MODE               = True
TEAM_SIZE               = 4
TEAM_READY_DELAY        = 5.0
TEAM_CREATE_WAIT        = 4.0
TEAM_INVITE_CYCLE_WAIT  = 4.0
TEAM_INVITE_MAX_TIME    = 120.0
TEAM_HEARTBEAT_INTERVAL = 3.0
TEAM_FINAL_WAIT         = 0.0    # ✅ بدء فوري عند 4/4
TEAM_PEER_WAIT_TIMEOUT  = 300.0
TEAM_MEMBER_START_DELAY = 20.0
TEAM_IDLE_ITERS_TEAM    = 1800
TEAM_MAX_INVITE_ATTEMPTS = 4      # ✅ بعد 4 محاولات فاشلة → إعادة تشكيل
MATCH_UDP_IDLE_TIMEOUT  = 60.0
DEBUG_TEAM_PACKETS      = True
_accept_friend_request  = True

# ─── MOVEMENT CONFIG ───
ENABLE_MOVEMENT         = True
MOVEMENT_MSG_KEY        = 0x6B
MOVEMENT_RADIUS         = 180
MOVEMENT_PATTERN        = "circle"

ACCOUNTS_FILE = "accounts.json"

xK, xV = b'Yg&tc%DEuh6%Zc^8', b'6oyZDr22E3ychjM%'
AES_KEY = xK
AES_IV = xV

REGION_LANG = {
    "BD": "bn", "IND": "hi", "PK": "ur", "SG": "en", "ID": "id",
    "ME": "ar", "VN": "vi", "TW": "zh", "CIS": "ru", "TH": "th",
    "EU": "en", "US": "en", "SAC": "es", "LK": "en", "BR": "pt",
    "PH": "en", "MY": "en", "HK": "zh", "NA": "en", "EUROPE": "en",
}

REGION_PREFIX = {
    "ME": "031900", "IND": "031400", "BD": "031400",
    "SG": "031500", "TH": "031500", "PH": "031500", "VN": "031500",
    "MY": "031500", "ID": "031500", "HK": "031500", "TW": "031500",
    "PK": "031500", "CIS": "031500", "BR": "031500", "NA": "031500",
    "SAC": "031500", "EUROPE": "031500",
}

CRC7_TABLE = bytes([
    0,9,18,27,36,45,54,63,72,65,90,83,108,101,126,119,25,16,11,2,61,52,47,38,
    81,88,67,74,117,124,103,110,50,59,32,41,22,31,4,13,122,115,104,97,94,87,76,69,
    43,34,57,48,15,6,29,20,99,106,113,120,71,78,85,92,100,109,118,127,64,73,82,91,
    44,37,62,55,8,1,26,19,125,116,111,102,89,80,75,66,53,60,39,46,17,24,3,10,
    86,95,68,77,114,123,96,105,30,23,12,5,58,51,40,33,79,70,93,84,107,98,121,112,
    7,14,21,28,35,42,49,56,65,72,83,90,101,108,119,126,9,0,27,18,45,36,63,54,
    88,81,74,67,124,117,110,103,16,25,2,11,52,61,38,47,115,122,97,104,87,94,69,76,
    59,50,41,32,31,22,13,4,106,99,120,113,78,71,92,85,34,43,48,57,6,15,20,29,
    37,44,55,62,1,8,19,26,109,100,127,118,73,64,91,82,60,53,46,39,24,17,10,3,
    116,125,102,111,80,89,66,75,23,30,5,12,51,58,33,40,95,86,77,68,123,114,105,96,
    14,7,28,21,42,35,56,49,70,79,84,93,98,107,112,121,
])

_DELTA = 0x9E3779B9
_ROUNDS = 16
_FIELD_SIZES = {0:1, 1:2, 2:2, 3:1, 4:2}
_FIELD_NAMES = {0:"sendOption", 1:"cmd", 2:"orderId", 3:"flags", 4:"length"}

CLOUDFLARE_PRIMARY_DNS = "1.1.1.1"
CLOUDFLARE_SECONDARY_DNS = "1.0.0.1"
_DNS_CACHE: Dict[str, Tuple[str, float]] = {}
_DNS_CACHE_TTL = 300.0

client = httpx.AsyncClient(
    verify=False, timeout=15.0,
    limits=httpx.Limits(max_connections=300, max_keepalive_connections=150)
)
_LOGIN_SEMAPHORE = asyncio.Semaphore(4)

headers = {
    'User-Agent': 'UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)',
    'Connection': 'Keep-Alive',
    'Accept-Encoding': 'gzip',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Expect': '100-continue',
    'X-Unity-Version': '2018.4.12f1',
    'X-GA-SV': '1789535859',
    'X-GA': 'v1 1',
    'ReleaseVersion': 'OB55'
}


def log(msg): print(msg, flush=True)
def print_error(msg): log(f"[-] {msg}")
def print_success(msg): log(f"[+] {msg}")
def print_info(msg): log(f"[i] {msg}")
def print_warning(msg): log(f"[!] {msg}")


def get_proto_field(d, key, default=None):
    if not d or not isinstance(d, dict):
        return default
    if key in d:
        val = d[key].get('data')
        return val if val is not None else default
    if str(key) in d:
        val = d[str(key)].get('data')
        return val if val is not None else default
    return default


def _pb_varint(n):
    if n < 0:
        n = (1 << 64) + n
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n: b |= 0x80
        out.append(b)
        if not n: break
    return bytes(out)


def _pb_tag(f, w):
    return _pb_varint((f << 3) | w)


def _pb_field(f, v):
    if isinstance(v, bool): v = int(v)
    if isinstance(v, int): return _pb_tag(f, 0) + _pb_varint(v)
    if isinstance(v, str):
        d = v.encode('utf-8')
        return _pb_tag(f, 2) + _pb_varint(len(d)) + d
    if isinstance(v, (bytes, bytearray)):
        d = bytes(v)
        return _pb_tag(f, 2) + _pb_varint(len(d)) + d
    return b""


def _pb_length(fn, val):
    enc = val.encode('utf-8') if isinstance(val, str) else val
    return _pb_tag(fn, 2) + _pb_varint(len(enc)) + enc


def create_proto(fields):
    packet = bytearray()
    for field, value in fields.items():
        if isinstance(field, str): field = int(field)
        if isinstance(value, dict):
            packet.extend(_pb_length(field, bytes(create_proto(value))))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    packet.extend(_pb_length(field, bytes(create_proto(item))))
                elif isinstance(item, str):
                    packet.extend(_pb_length(field, item))
                elif isinstance(item, (bytes, bytearray)):
                    packet.extend(_pb_length(field, bytes(item)))
                elif isinstance(item, bool):
                    packet.extend(_pb_tag(field, 0) + _pb_varint(1 if item else 0))
                elif isinstance(item, int):
                    packet.extend(_pb_tag(field, 0) + _pb_varint(item))
        elif isinstance(value, bool):
            packet.extend(_pb_tag(field, 0) + _pb_varint(1 if value else 0))
        elif isinstance(value, int):
            packet.extend(_pb_tag(field, 0) + _pb_varint(value))
        elif isinstance(value, str):
            packet.extend(_pb_length(field, value))
        elif isinstance(value, (bytes, bytearray)):
            packet.extend(_pb_length(field, bytes(value)))
    return bytes(packet)


def create_packet(Pk, N, K, V):
    if isinstance(K, str): K = bytes.fromhex(K) if len(K) == 32 else K.encode()
    if isinstance(V, str): V = bytes.fromhex(V) if len(V) == 32 else V.encode()
    PkEnc = AES.new(K, AES.MODE_CBC, V).encrypt(pad(bytes.fromhex(Pk), 16)).hex()
    L = len(PkEnc) // 2
    hexlen = hex(L)[2:]
    if len(hexlen) % 2 != 0: hexlen = '0' + hexlen
    pad_len = 12 - len(N) - len(hexlen)
    if pad_len < 0: pad_len = 0
    return bytes.fromhex(N + ('0' * pad_len) + hexlen + PkEnc)


def create_team(key, iv, game_version="1.132.9"):
    fields = {
        1: 1, 2: {2: bytes.fromhex("01"), 3: 1, 4: 3, 5: "en",
            8: {1: "IDC3", 2: 123, 3: "ME"}, 9: 1,
            10: bytes.fromhex("01090a0b1219202729"), 11: 2, 13: 1,
            14: {1: "08FBA33B4BABE2790205390333330000009700050092005F72697EB2134DCBB2467625142201040e38e848dc0e748c3f6aa04d81000000ff1113090dcacfa16d",
                 2: 478, 3: "7e585d55", 4: "y[Z]", 6: 12, 7: "15686677735a7506101c",
                 8: str(game_version), 9: 3, 10: 1, 11: "03626253513637754174"},
            19: 329, 21: bytes.fromhex("374f521955"), 24: {1: 21},
            27: "a_6534489873065906362"}}
    return create_packet(create_proto(fields).hex(), "0515", key, iv)


def team_fresh(account_id, key, iv):
    return create_packet(create_proto({1: 72, 2: {1: int(account_id)}}).hex(), "0515", key, iv)


def invite_player_in_team(target_uid, key, iv, region="ME"):
    return create_packet(create_proto({1: 2, 2: {1: int(target_uid), 2: region, 4: 1}}).hex(),
                         "0515", key, iv)


def accept_team_invite(target_uid, group_id, key, iv):
    fields = {1: 4, 2: {1: int(target_uid), 3: int(target_uid),
        4: bytes.fromhex("0107090a0b120f19202729"), 8: 1,
        9: {1: "08FBA33B0522052002041B0222220007000C00010004000547E265AE1080779546762514110104076fa2e8770e748c3f6a68ed75000000ff08080500cacfa16d",
            2: 235, 3: {14: 80, 11: 86}, 4: "{Y\\R", 6: 13, 7: {2: 4},
            8: "1.132.9", 9: 2, 10: 1, 11: "0362625351"},
        10: str(group_id),
        11: {"1": "IDC3", "2": 107, "3": "ME"}, 13: "en", 16: "374f5219",
        20: {1: 21}, 23: "a_6534489873065906362",
        24: "https://dl-sg-production.freefiremobile.com/",
        27: {1: 2, 2: 4}}}
    return create_packet(create_proto(fields).hex(), "0515", key, iv)


def start_match_team(account_id, key, iv, region="ME"):
    fields = {1: 9, 2: {1: int(account_id), 7: {1: "IDC3", 2: 410, 3: region}}}
    return create_packet(create_proto(fields).hex(), "0515", key, iv)


def ready_for_game(uid, key, iv):
    return create_packet(create_proto({1: 15, 2: {1: uid, 2: 1}}).hex(), "0519", key, iv)


# ═══════════════════════════ TEAM STATE ═══════════════════════════
_team_state = {
    "peers": {},
    "leader_uid": None,
    "team_code": None,
    "members": set(),
    "members_lock": asyncio.Lock(),
    "lock": asyncio.Lock(),
}


async def _team_member_join(uid):
    uid = str(uid)
    async with _team_state["members_lock"]:
        before = len(_team_state["members"])
        _team_state["members"].add(uid)
        after = len(_team_state["members"])
    if after != before:
        print_success(f"[TEAM] 👥 In team ({after}/{TEAM_SIZE}) | joined: {uid}")
    return after


async def _team_member_leave(uid):
    async with _team_state["members_lock"]:
        _team_state["members"].discard(str(uid))


async def _team_member_count():
    async with _team_state["members_lock"]:
        return len(_team_state["members"])


async def _team_member_reset(keep_leader=True):
    async with _team_state["members_lock"]:
        _team_state["members"].clear()
        if keep_leader and _team_state["leader_uid"]:
            _team_state["members"].add(_team_state["leader_uid"])


# ═══════════════════════════ DEVICE ═══════════════════════════
def _generate_new_device() -> dict:
    device_list = [
        ("Samsung", "SM-G998B", "Adreno (TM) 660", "Android OS 12 / API-31"),
        ("Xiaomi", "2201122G", "Adreno (TM) 730", "Android OS 13 / API-33"),
        ("Realme", "RMX3700", "Mali-G710", "Android OS 14 / API-34"),
        ("OnePlus", "CPH2451", "Adreno (TM) 740", "Android OS 13 / API-33"),
        ("OPPO", "CPH2611", "Adreno (TM) 720", "Android OS 14 / API-34"),
        ("Vivo", "V2203", "Mali-G710", "Android OS 12 / API-31"),
        ("Poco", "M2102J20SG", "Adreno (TM) 660", "Android OS 13 / API-33"),
    ]
    brand, model, gpu, os_ver = random.choice(device_list)
    return {
        "unique_device_id": f"Google|{str(uuid.uuid4())}",
        "brand": brand, "model": model, "gpu_renderer": gpu, "system_software": os_ver,
        "screen_width": random.choice([1080, 1440, 720, 1280]),
        "screen_height": random.choice([2400, 3200, 1600, 2400]),
        "screen_dpi": str(random.randint(300, 420)),
        "memory": random.randint(2800, 6500),
        "processor_details": f"ARM64 FP ASIMD AES VMH | {random.randint(2200, 3200)} | {random.randint(6, 12)}",
        "client_ip": f"{random.randint(103, 223)}.{random.randint(10, 250)}.{random.randint(10, 250)}.{random.randint(10, 250)}"
    }


def get_device_for_account(account_identifier: str) -> dict:
    return _generate_new_device()


# ═══════════════════════════ DNS ═══════════════════════════
async def resolve_host_cloudflare(hostname: str) -> str:
    if not hostname: return hostname
    parts = hostname.split('.')
    if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
        return hostname
    now = time.time()
    if hostname in _DNS_CACHE:
        ip, exp = _DNS_CACHE[hostname]
        if now < exp: return ip

    def _query_cloudflare(server_ip: str) -> Optional[str]:
        s = None
        try:
            tx_id = random.randint(1000, 65535)
            header = struct.pack(">HHHHHH", tx_id, 0x0100, 1, 0, 0, 0)
            qname = b"".join(bytes([len(part)]) + part.encode('ascii') for part in hostname.split('.')) + b"\x00"
            query_pkt = header + qname + struct.pack(">HH", 1, 1)
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(1.2)
            s.sendto(query_pkt, (server_ip, 53))
            resp, _ = s.recvfrom(1024)
            if len(resp) >= 12:
                ancount = struct.unpack(">H", resp[6:8])[0]
                if ancount > 0:
                    offset = 12 + len(qname) + 4
                    for _ in range(ancount):
                        if offset >= len(resp): break
                        if (resp[offset] & 0xC0) == 0xC0:
                            offset += 2
                        else:
                            while offset < len(resp) and resp[offset] != 0:
                                offset += 1 + resp[offset]
                            offset += 1
                        if offset + 10 > len(resp): break
                        rtype, rclass, ttl, rdlen = struct.unpack(">HHIH", resp[offset:offset+10])
                        offset += 10
                        if rtype == 1 and rdlen == 4 and offset + 4 <= len(resp):
                            return socket.inet_ntoa(resp[offset:offset+4])
                        offset += rdlen
        except Exception: pass
        finally:
            if s:
                try: s.close()
                except Exception: pass
        return None

    loop = asyncio.get_running_loop()
    ip = await loop.run_in_executor(None, _query_cloudflare, CLOUDFLARE_PRIMARY_DNS)
    if not ip:
        ip = await loop.run_in_executor(None, _query_cloudflare, CLOUDFLARE_SECONDARY_DNS)
    if not ip:
        try:
            ip_info = await loop.getaddrinfo(hostname, None, family=socket.AF_INET)
            if ip_info: ip = ip_info[0][4][0]
        except Exception:
            ip = hostname
    if ip:
        _DNS_CACHE[hostname] = (ip, now + _DNS_CACHE_TTL)
    return ip or hostname


def optimize_tcp_socket(sock: socket.socket):
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, "SIO_KEEPALIVE_VALS") and os.name == 'nt':
            try: sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, 10000, 2000))
            except Exception: pass
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 131072)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
    except Exception: pass


def optimize_udp_socket(sock: socket.socket):
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 131072)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
        if hasattr(socket, 'SIO_UDP_CONNRESET') and os.name == 'nt':
            try: sock.ioctl(socket.SIO_UDP_CONNRESET, False)
            except Exception: pass
    except Exception: pass


async def safe_close_writer(writer):
    if not writer: return
    try:
        if not writer.is_closing(): writer.close()
        await asyncio.wait_for(writer.wait_closed(), timeout=1.5)
    except Exception: pass


# ═══════════════════════════ CRYPTO ═══════════════════════════
async def aes_encrypt(payload, key, iv):
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return cipher.encrypt(pad(payload, AES.block_size))


def crc7(data):
    c = 0
    for b in data:
        c = CRC7_TABLE[((2 * (c & 0xFF)) ^ (b & 0xFF)) & 0xFF] & 0x7F
    return c & 0x7F


def uleb_encode(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n: b |= 0x80
        out.append(b)
        if not n: break
    return bytes(out)


def has_ssan_zig(n):
    z = (n << 1) & 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while z >= 0x80:
        out.append((z & 0x7F) | 0x80)
        z >>= 7
    out.append(z)
    return bytes(out)


# ═══════════════════════════ C2S MOVEMENT ═══════════════════════════
@dataclass
class C2SMovement:
    slot: int = 8
    x: int = 0
    y: int = 0
    z: int = 0
    stick_a_held: int = 0
    stick_b_held: int = 0
    input_a: int = 0
    input_b: int = 0
    vel_x: int = 0
    vel_y: int = 0
    vel_z: int = 0
    physx_state: int = 0
    physx_pose: int = 0
    dir_x: float = 0.0
    dir_y: float = 0.0
    dir_z: float = 0.0
    net_tick: int = 0
    counter_ms: int = 0
    entity_id: Optional[int] = None
    phase: str = "arena"


def _mv_uvarint(n: int) -> bytes:
    if n < 0:
        n = (1 << 64) + n
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n: out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _mv_zigzag(n: int) -> int:
    return ((n << 1) ^ (n >> 63)) & 0xFFFFFFFFFFFFFFFF


def _mv_f32(x: float) -> int:
    return int.from_bytes(struct.pack(">f", float(x)), "big")


def build_movement_body(m: C2SMovement) -> bytes:
    entity = m.entity_id if m.entity_id is not None else ((1 << 24) | m.slot)
    if m.phase == "game":
        TAIL = (b"\x00"*7 + b"\x93\xca\xd5\xf9\x0b" + b"\x8e\xfe\xfc\xf7\x0b"
                + b"\xf4\xe0\xfe\xf7\x03" + b"\x00\x00")
    else:
        TAIL = (b"\x00"*7 + b"\xbd\xb6\xe1\xf3\x03" + b"\x00"
                + b"\xc4\xb6\xe1\xfb\x0b" + b"\x00\x00")
    ff_body = bytearray()
    ff_body += _mv_uvarint(entity)
    ff_body += _mv_uvarint(m.physx_state)
    ff_body += _mv_zigzag(m.x)
    ff_body += _mv_zigzag(m.y)
    ff_body += _mv_zigzag(m.z)
    ff_body += b"\x00"
    ff_body += _mv_uvarint(m.stick_a_held)
    ff_body += TAIL
    ff_body += _mv_uvarint(m.net_tick)
    ff_body += b"\x00\x00\x00"
    ff_body += _mv_uvarint(m.counter_ms)
    return bytes(ff_body)


async def build_movement_packet(key_bytes, msg_key, player_state, x, y, z,
                                 tick, counter, move_state, account_uid):
    m = C2SMovement(
        entity_id=int(account_uid),
        x=x, y=y, z=z,
        physx_state=int(player_state),
        stick_a_held=int(move_state),
        net_tick=int(tick),
        counter_ms=int(counter),
    )
    body = build_movement_body(m)
    k = key_bytes[0]
    v80 = ((k << 8) | k) & 0xFFFF
    h = bytearray()
    h.append(msg_key & 0xFF)
    h.append(0x00)
    h.append(0x00 ^ k)
    h += ((2001 ^ v80) & 0xFFFF).to_bytes(2, "little")
    h += ((len(body) ^ v80) & 0xFFFF).to_bytes(2, "little")
    h.append(0x00 ^ k)
    h[1] = await crc7_buff(0, bytes(h[2:]) + body) & 0x7F
    return bytes(h) + body


class MovementTracker:
    PLAYER_STATE = {"idle":0, "walk":1, "run":2, "sprint":3, "jump":4, "crouch":5, "prone":6}

    def __init__(self, pattern="circle", radius=180):
        self.pattern = pattern
        self.radius = int(radius)
        self.tick = 0
        self.counter = 0
        self.angle = random.uniform(0, 6.2832)
        self.state = "walk"
        self.state_start = time.time()
        self.state_duration = random.uniform(2.0, 4.0)
        self.dir_flip = 1

    def _pick_state(self):
        r = random.random()
        if r < 0.10: return "idle", random.uniform(0.6, 1.6)
        elif r < 0.38: return "walk", random.uniform(1.6, 3.0)
        elif r < 0.70: return "run", random.uniform(2.0, 4.0)
        elif r < 0.80: return "sprint", random.uniform(1.2, 2.5)
        elif r < 0.88: return "jump", random.uniform(0.35, 0.65)
        elif r < 0.94: return "crouch", random.uniform(1.2, 2.5)
        else: return "prone", random.uniform(1.5, 3.5)

    def _gen_pos(self):
        p = (self.pattern or "circle").lower()
        if p == "circle":
            self.angle += 0.35 * self.dir_flip
            if self.angle > 6.2832: self.angle -= 6.2832
            elif self.angle < 0: self.angle += 6.2832
            x = int(self.radius * random.uniform(0.9, 1.1) * (1.0 if self.angle % 3.14 < 1.57 else -1.0))
            y = int(self.radius * random.uniform(0.4, 0.6))
            z = int(self.radius * random.uniform(0.9, 1.1) * (1.0 if self.angle % 6.28 < 3.14 else -1.0))
            ms = 0x0F
        else:
            x = random.randint(-self.radius, self.radius)
            y = random.randint(-40, 40)
            z = random.randint(-self.radius, self.radius)
            ms = random.choice([0x01, 0x02, 0x04, 0x08, 0x0F])
        return x, y, z, ms

    def next(self):
        now = time.time()
        if now - self.state_start > self.state_duration:
            self.state, self.state_duration = self._pick_state()
            self.state_start = now
        self.tick = (self.tick + 1) & 0xFFFFFFFF
        self.counter = (self.counter + 1) & 0xFFFFFFFF
        ps = self.PLAYER_STATE.get(self.state, 1)
        if self.state in ("idle", "crouch", "prone"):
            x = y = z = 0; ms = 0
        else:
            x, y, z, ms = self._gen_pos()
        return ps, x, y, z, ms, self.tick, self.counter, self.state


async def movement_worker(sock, ip, port, udp_key_bytes, match_code, account_id,
                          stop_event, match_index):
    loop = asyncio.get_event_loop()
    addr = (ip, port)
    tracker = MovementTracker(pattern=MOVEMENT_PATTERN, radius=MOVEMENT_RADIUS)
    sent = 0
    try:
        await asyncio.sleep(3.0)
        while not stop_event.is_set():
            try:
                ps, x, y, z, ms, tick, cnt, st = tracker.next()
                pkt = await build_movement_packet(
                    udp_key_bytes, MOVEMENT_MSG_KEY, ps, x, y, z, tick, cnt, ms, account_id
                )
                await loop.sock_sendto(sock, pkt, addr)
                sent += 1
                if sent % 50 == 0:
                    print_info(f"[MOVE #{match_index}] sent={sent} state={st}")
                wait = random.uniform(0.20, 0.35)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=wait)
                    break
                except asyncio.TimeoutError:
                    pass
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.5)
    except asyncio.CancelledError:
        pass
    finally:
        if sent > 0:
            print_info(f"[MOVE #{match_index}] stopped (sent={sent})")


# ═══════════════════════════ VERSION / LOGIN ═══════════════════════════
async def get_playstore_version():
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            None, lambda: play_scraper('com.dts.freefireth', lang='hi', country='id')
        )
        return result.get("version")
    except Exception:
        return "1.132.8"


async def version_config():
    app_version = await get_playstore_version()
    lang = REGION_LANG.get(ACCOUNT_REGION, "ar")
    api_url = (
        "https://version.ggwhitehawk.com/live/ver.php"
        f"?version={app_version}"
        f"&lang={lang}&device=android&channel=android"
        f"&appstore=googleplay&region={ACCOUNT_REGION}"
        "&whitelist_version=1.3.0&whitelist_sp_version=1.0.0"
    )
    try:
        response = await client.get(api_url)
        response.raise_for_status()
        data = response.json()
        server_url = data.get("server_url")
        remote_version = data.get("remote_version")
        latest_release_version = data.get("latest_release_version")
        if not server_url or not remote_version or not latest_release_version:
            return None
        return latest_release_version, remote_version, server_url
    except Exception:
        return None


async def get_access_token(uid, password):
    url = "https://100067.connect.garena.com/oauth/guest/token/grant"
    hdrs = {
        "Host": "100067.connect.garena.com",
        "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 12; SM-G998B Build/SP1A.210812.016)",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close"
    }
    data = {
        "uid": uid, "password": password,
        "response_type": "token", "client_type": "2",
        "client_secret": "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3",
        "client_id": "100067"
    }
    for attempt in range(5):
        try:
            response = await client.post(url, headers=hdrs, data=data)
            if response.status_code == 200:
                rd = response.json()
                open_id = rd.get("open_id")
                access_token = rd.get("access_token")
                platform = rd.get("platform", 4)
                if open_id and access_token:
                    return open_id, access_token, platform
            if response.status_code == 429:
                await asyncio.sleep(1); continue
        except Exception: pass
        await asyncio.sleep(0.5)
    return None


async def parse_results(parsed_results):
    result_dict = {}
    for result in parsed_results:
        field_data = {"wire_type": result.wire_type}
        if result.wire_type in ("varint", "string", "bytes"):
            field_data["data"] = result.data
        elif result.wire_type == "length_delimited":
            if hasattr(result.data, "results"):
                field_data["data"] = await parse_results(result.data.results)
            elif isinstance(result.data, list):
                field_data["data"] = await parse_results(result.data)
            else:
                field_data["data"] = str(result.data)
        result_dict[str(result.field)] = field_data
    return result_dict


async def decode_protobuf(data):
    parsed_results = Parser().parse(data)
    parsed_results_dict = await parse_results(parsed_results)
    return json.dumps(parsed_results_dict)


def _decode_proto_manual(data):
    result = {}; offset = 0; data_len = len(data)
    while offset < data_len:
        header = 0; shift = 0
        while offset < data_len:
            b = data[offset]; offset += 1
            header |= (b & 0x7F) << shift
            if not (b & 0x80): break
            shift += 7
            if shift > 28: break
        if header == 0: break
        fn = header >> 3; wt = header & 0x7
        if fn == 0: break
        if wt == 0:
            v = 0; shift = 0
            while offset < data_len:
                b = data[offset]; offset += 1
                v |= (b & 0x7F) << shift
                if not (b & 0x80): break
                shift += 7
                if shift > 63: break
            result[fn] = v
        elif wt == 2:
            length = 0; shift = 0
            while offset < data_len:
                b = data[offset]; offset += 1
                length |= (b & 0x7F) << shift
                if not (b & 0x80): break
                shift += 7
                if shift > 28: break
            if length > data_len - offset: break
            value = data[offset:offset+length]; offset += length
            try:
                s = value.decode('utf-8')
                if s and all(31 < ord(c) < 127 or ord(c) in (9,10,13) for c in s):
                    result[fn] = s
                else:
                    nested = _decode_proto_manual(value)
                    result[fn] = nested if nested else value.hex()
            except UnicodeDecodeError:
                nested = _decode_proto_manual(value)
                result[fn] = nested if nested else value.hex()
        elif wt == 5: offset += 4
        elif wt == 1: offset += 8
        else: break
    return result


async def build_majorlogin_payload(open_id, access_token, platform, client_version, device_info, verr=None):
    try:
        if verr is None: verr = client_version
        proto = thunderFF_pb2.MajorLoginReq()
        proto.event_time = str(datetime.now())[:-7]
        proto.game_name = "free fire"
        proto.platform_id = 1
        proto.client_version = str(verr)
        proto.client_version_code = "2019121229"
        proto.system_software = "Android OS 15 / API-35 (AP3A.240905.015.A2/185014)"
        proto.system_hardware = "Handheld"
        proto.device_type = "Handheld"
        proto.screen_width = 1600
        proto.screen_height = 719
        proto.screen_dpi = "234"
        proto.processor_details = "ARM64 FP ASIMD AES | 1820 | 8"
        proto.memory = 2798
        proto.gpu_renderer = "Mali-G57"
        proto.gpu_version = "OpenGL ES 3.2 v1.r49p1-04eac0.2848c17a2fd4e9340e06555168eaa3c9"
        proto.unique_device_id = "Google|f744e396-5694-4e65-995d-97a958f2bd1f"
        proto.client_ip = "197.0.137.129"
        proto.language = "ar"
        proto.open_id = str(open_id)
        proto.open_id_type = "4"
        proto.login_open_id_type = 4
        proto.access_token = str(access_token)
        proto.login_by = 2
        proto.platform_sdk_id = 1
        proto.origin_platform_type = "4"
        proto.primary_platform_type = "4"
        proto.reg_avatar = 1
        proto.channel_type = 3
        proto.telecom_operator = "TUNTEL"
        proto.network_operator_a = "TUNTEL"
        proto.network_type = "WIFI"
        proto.network_type_a = "WIFI"
        proto.cpu_type = 2
        proto.cpu_architecture = "64"
        proto.graphics_api = "OpenGLES2"
        proto.supported_astc_bitset = 8191
        proto.client_using_version = "7428b253defc164018c604a1ebbfebdf"
        proto.loading_time = 15078
        proto.release_channel = "android"
        proto.extra_info = "KqsHTx3+QOmBRR1WKvaWewlcpqJBfjki+PPHQoQG8+0yV+Uos7gUFFjHMQ/e7u6han6Fl77r7c3vMN3p8UbKgN+nfycQCgwBmWgBzomx2gj84c+p"
        proto.android_engine_init_flag = 111207
        proto.if_push = 1
        proto.is_vpn = 0

        memory_available = proto.memory_available
        memory_available.version = 55
        memory_available.hidden_value = 81

        proto.external_storage_total = 49973
        proto.external_storage_available = 11338
        proto.internal_storage_total = 854
        proto.internal_storage_available = 11466
        proto.game_disk_storage_total = 49973
        proto.game_disk_storage_available = 11466
        proto.external_sdcard_total_storage = 49973
        proto.external_sdcard_avail_storage = 11466

        proto.library_path = "/data/app/~~lHFxTCCbupG2QVJmsURtZw==/com.dts.freefireth-N3aCHpHNXpdxjD80uIIbww==/lib/arm64"
        proto.library_token = "b8e0cd5e295eee42f5860d3c86e483dd|/data/app/~~lHFxTCCbupG2QVJmsURtZw==/com.dts.freefireth-N3aCHpHNXpdxjD80uIIbww==/base.apk"

        base_payload = proto.SerializeToString()
        extra = b""
        extra += _pb_field(96, '{"cur_rate":[90,60,120],"support_etc2":false}')
        extra += _pb_field(97, 1)
        extra += _pb_field(99, "4")
        extra += _pb_field(100, "4")
        extra += _pb_field(102, b"\x17]ENWU\x0eR5")
        extra += _pb_field(104, 52882)
        extra += _pb_field(105, 1)
        extra += _pb_field(106, "https://dl.ak.freefiremobile.com/live/ABHotUpdates/|https://core-ak.freefiremobile.com/live/ABHotUpdates/|6b2078db9d22dd98f8e9386a39af8462")
        extra += _pb_field(107, "c8e41b7a93f02d56e1a94c7b8203f5d1")

        full_payload = base_payload + extra
        return await aes_encrypt(full_payload, AES_KEY, AES_IV)
    except Exception:
        return None


async def send_majorlogin(data, release_version, server_url):
    try:
        url = f"{server_url}MajorLogin" if server_url.endswith('/') else f"{server_url}/MajorLogin"
        req_headers = headers.copy()
        req_headers["ReleaseVersion"] = str(release_version)
        response = await client.post(url, headers=req_headers, data=data)
        if response.status_code != 200: return None
        response_content = response.content
        if len(response_content) < 40: return None

        res_proto = thunderFF_pb2.MajorLoginRes()
        try: res_proto.ParseFromString(response_content)
        except Exception: pass

        dict_res = {}
        try:
            parsed = Parser().parse(response_content.hex())
            dict_res = await parse_results(parsed)
        except Exception: pass

        key_val = get_proto_field(dict_res, 22)
        iv_val = get_proto_field(dict_res, 23)
        if not key_val: key_val = res_proto.aes_ak
        if not iv_val: iv_val = res_proto.iv_i

        if isinstance(key_val, str):
            try: key_val = bytes.fromhex(key_val)
            except Exception: pass
        if isinstance(iv_val, str):
            try: iv_val = bytes.fromhex(iv_val)
            except Exception: pass

        if not key_val or not iv_val:
            for offset in range(min(128, len(response_content))):
                try:
                    candidate = thunderFF_pb2.MajorLoginRes()
                    candidate.ParseFromString(response_content[offset:])
                    if candidate.region and candidate.token:
                        res_proto = candidate
                        break
                except Exception: pass

        if key_val:
            try: res_proto.aes_ak = key_val
            except Exception: pass
        if iv_val:
            try: res_proto.iv_i = iv_val
            except Exception: pass

        return res_proto
    except Exception as e:
        log(f"[-] send_majorlogin error: {e}")
        return None


async def send_getlogin(data, base_url, token, release_version):
    try:
        url = f"{base_url.rstrip('/')}/GetLoginData"
        req_headers = headers.copy()
        req_headers["ReleaseVersion"] = release_version
        req_headers['Authorization'] = f"Bearer {token}"
        req_headers['Host'] = "clientbp.ppmainecoonghj.com"
        response = await client.post(url, headers=req_headers, data=data)
        if response.status_code != 200: return None
        response_content = response.content

        res_proto = thunderFF_pb2.GetLoginDataRes()
        try: res_proto.ParseFromString(response_content)
        except Exception: pass

        def _extract_addr(raw_bytes, field_no):
            try:
                pos = 0; n = len(raw_bytes)
                def read_varint(buf, p):
                    res = 0; sh = 0
                    while p < len(buf):
                        b = buf[p]; p += 1
                        res |= (b & 0x7F) << sh
                        if not (b & 0x80): break
                        sh += 7
                    return res, p
                fields = {}
                while pos < n:
                    try: key, pos = read_varint(raw_bytes, pos)
                    except Exception: break
                    fn = key >> 3; wt = key & 0x07
                    try:
                        if wt == 0:
                            val, pos = read_varint(raw_bytes, pos)
                        elif wt == 2:
                            ln, pos = read_varint(raw_bytes, pos)
                            val = raw_bytes[pos:pos + ln]; pos += ln
                        elif wt == 5:
                            val = raw_bytes[pos:pos + 4]; pos += 4
                        elif wt == 1:
                            val = raw_bytes[pos:pos + 8]; pos += 8
                        else: break
                    except Exception: break
                    fields.setdefault(fn, []).append(val)
                if field_no in fields:
                    v = fields[field_no][0]
                    if isinstance(v, bytes):
                        try: return v.decode('utf-8', 'ignore')
                        except Exception: return None
                    return str(v)
            except Exception: return None

        try:
            for offset in range(0, min(80, len(response_content))):
                fa = _extract_addr(response_content[offset:], 14)
                ia = _extract_addr(response_content[offset:], 32)
                if fa and ":" in fa and ia and ":" in ia:
                    res_proto.functional_addrs = fa
                    res_proto.informational_addrs = ia
                    break
        except Exception: pass

        dict_res = {}
        try:
            parsed = Parser().parse(response_content.hex())
            dict_res = await parse_results(parsed)
        except Exception: pass

        return res_proto, dict_res
    except Exception as e:
        log(f"[-] send_getlogin error: {e}")
        return None


# ═══════════════════════════ TCP STARTUP ═══════════════════════════
async def build_tcp_startup_packet(account_id, token, server_time, key, iv, region=None, typ='OnLine'):
    if region is None: region = ACCOUNT_REGION
    uid_hex = f"{int(account_id):016x}"
    timestamp_hex = f"{int(server_time):08x}"
    encode_token = token.encode()
    encrypted_packet = AES.new(key, AES.MODE_CBC, iv).encrypt(pad(encode_token, 16)).hex()
    encrypted_packet_length = f"{len(encrypted_packet) // 2:08x}"
    if typ == 'OnLine':
        return f"9015{uid_hex}{timestamp_hex}00000000{encrypted_packet_length}{encrypted_packet}"
    else:
        return f"9015{uid_hex}{timestamp_hex}{encrypted_packet_length}{encrypted_packet}"


async def send_keep_alive(region=None):
    try:
        if region is None: region = ACCOUNT_REGION
        reg = str(region).upper() if region else ACCOUNT_REGION
        if reg == "ME":
            ka_hex = "0219"
        elif reg == "IND":
            ka_hex = "0214"
        else:
            ka_hex = "0215"
        return bytes.fromhex(ka_hex)
    except Exception:
        return bytes.fromhex("0219")


# ═══════════════════════════ MATCH SEARCH ═══════════════════════════
BR_HEX = (
    "080112800a0a010110013a110a044944433110aa011a064555524f50453a100a044944433210311a064555524f5045"
    "40014a0801090a0b1219202758016291090a80013038384638324246303241393637363730323031303130303030"
    "30303030303030303031363030303130303135303030323232463937454545304630303030303034363736323531"
    "34303030303030303030303030303030303030303030303030303030303030303030303030306666303030303030"
    "3030636163666131366410241afb02735d5e571400024a775d45414d1a041b1c001f11010449715f4243481a001e"
    "1d071c1703004b1a4066785c524570735c51486775421b5c5a4c07504042685a63610816054e19025e7519600147"
    "7c015165406370195f5547404e4550640103020f1304064863754268676c755f65576e40467e5f0a417a4701026d"
    "675d6e73670b1108495a4c6a0b78470b740065645e525a057258425f584a447d4e6759440c11044e7c596d7f4b62"
    "5f7d04055a47505c4e1d6b5b4107447d7201057d7f0f14084e430457674f7e517d72015172415d027473577c4d61"
    "5f79535256780911030f4d5e027a797f614165067806505d53777750475e75064257076500460817014e741e7e50"
    "78487e7a7c465e7669767153497064605a7376677773550d160148037e18675966787f4c42607a645f577e7b44"
    "1b460776026b18685d0b110205490060020f70676175654674706671797f41067346677c4e06585e780f15074c57"
    "047b40517075415f6364027259674b5b0166407f7340600407770a22047a5d5c52300b3a0a167305067162727516"
    "134208312e3133302e3232480350015ae90403626253513635686e556f4e36416456324b796f566c636f47777648"
    "4f624e56526c4d727073504b4f43654177616848494176795556497273743752737149734a7a786b324752526837"
    "7a2f637664626d504f6a73552f79626d38547a4c69586d2f474351696d494b53486833447955726f39515152756c"
    "34545350626d6d624b7949565937545671577059455372323646572f59624578507338514f706d317372785455736c"
    "30796a434144444d4f34616a654b615753366361496c554b4963797a494e396d52516f715277687939797257476d"
    "337a644345337a6a61436f492f5a585233656f65365a42647a64677654636b6b665733356e4d4c6a6a56507256"
    "4b6433523172756174394e50514150724a5546627859696c4c5a3859707336654d5447666b6649793574666a526c"
    "314d4648706b51774c6373374439656378566c41636f374e664f6d2b30654756466c443474447870677138553359"
    "5973587645384842502f70666c767a737138316a32524f4d7857437556445442492f684735625462773166456e42"
    "49725162762b636144775147696f74554e316d4c4b77734379456f4766706746614251457645672b736a764c4c78"
    "704743334c304a5344532f74526169504354553344374e6249306547516651622f5a466f4c36455630775a324d6f"
    "583932414c572f5049752f56634663584e70596b356f7966326151416a536971486a2f363276354843644f525551"
    "303578754e6171795251625653704654303137655237675255636b4966366c6f447476342b514e4a4670766d7475"
    "7077707774396a5a5974437a4b56743657726d6e36785837706658456251555434684f3758a201050803108703a2"
    "01050804108103a20105080510c001a20105081d10cc01a2010408161078a20105080e10af01a201020815"
)


async def start_game_battle_royale(region, client_version, writer, key, iv):
    packet = bytes.fromhex(BR_HEX)
    try:
        proto = thunderFF_pb2.StartMatch()
        proto.ParseFromString(packet)
        if hasattr(proto.main, 'region_list') and len(proto.main.region_list) > 0:
            proto.main.region_list[0].region = region
            if len(proto.main.region_list) > 1:
                proto.main.region_list[1].region = region
        if hasattr(proto.main, 'client_version'):
            proto.main.client_version.remote_version = client_version
        packet = proto.SerializeToString()
    except Exception: pass
    encrypted_packet = (await aes_encrypt(packet, key, iv)).hex()
    packet_length = len(encrypted_packet) // 2
    hex_length = hex(packet_length)[2:]
    hex_length = hex_length if len(hex_length) > 1 else "0" + hex_length
    reg = str(region).upper() if region else ACCOUNT_REGION
    reg_prefix = REGION_PREFIX.get(reg, "031900")
    final_packet = reg_prefix + "0" * (6 - len(hex_length)) + hex_length + encrypted_packet
    writer.write(bytes.fromhex(final_packet))
    await writer.drain()
    log(f"[BR] Match search sent ({packet_length} bytes) | Region: {reg}")


# ═══════════════════════════ TEA ENCRYPTION ═══════════════════════════
async def tea_enc(v0, v1, k0, k1, k2, k3):
    s = 0
    for _ in range(_ROUNDS):
        s = (s + _DELTA) & 0xFFFFFFFF
        v0 = (v0 + (((((v1 << 4) & 0xFFFFFFFF) + k0) & 0xFFFFFFFF ^
                      ((v1 + s) & 0xFFFFFFFF) ^
                      (((v1 >> 5) + k1) & 0xFFFFFFFF)))) & 0xFFFFFFFF
        v1 = (v1 + (((((v0 << 4) & 0xFFFFFFFF) + k2) & 0xFFFFFFFF ^
                      ((v0 + s) & 0xFFFFFFFF) ^
                      (((v0 >> 5) + k3) & 0xFFFFFFFF)))) & 0xFFFFFFFF
    return v0, v1


async def tea_dec(v0, v1, k0, k1, k2, k3):
    s = (_DELTA * _ROUNDS) & 0xFFFFFFFF
    for _ in range(_ROUNDS):
        v1 = (v1 - (((((v0 << 4) & 0xFFFFFFFF) + k2) & 0xFFFFFFFF ^
                      ((v0 + s) & 0xFFFFFFFF) ^
                      (((v0 >> 5) + k3) & 0xFFFFFFFF)))) & 0xFFFFFFFF
        v0 = (v0 - (((((v1 << 4) & 0xFFFFFFFF) + k0) & 0xFFFFFFFF ^
                      ((v1 + s) & 0xFFFFFFFF) ^
                      (((v1 >> 5) + k1) & 0xFFFFFFFF)))) & 0xFFFFFFFF
        s = (s - _DELTA) & 0xFFFFFFFF
    return v0, v1


async def tea_cbc_encrypt(padded, key_bytes):
    k0, k1, k2, k3 = (struct.unpack_from("<I", key_bytes, o)[0] for o in (0, 4, 8, 12))
    out = bytearray(len(padded))
    prev_cipher = bytearray(8)
    prev_intermediate = bytearray(8)
    for i in range(0, len(padded), 8):
        xored = bytearray(8)
        for j in range(8):
            xored[j] = padded[i + j] ^ prev_cipher[j]
        e0, e1 = await tea_enc(
            struct.unpack_from("<I", xored, 0)[0],
            struct.unpack_from("<I", xored, 4)[0],
            k0, k1, k2, k3,
        )
        enc = bytearray(8)
        struct.pack_into("<I", enc, 0, e0)
        struct.pack_into("<I", enc, 4, e1)
        for j in range(8):
            out[i + j] = enc[j] ^ prev_intermediate[j]
        prev_cipher[:] = out[i:i + 8]
        prev_intermediate[:] = xored
    return bytes(out)


async def build_padded(content):
    pad_len = (8 - (len(content) + 10) % 8) % 8
    return bytes([pad_len, 0, 0]) + b"\x00" * pad_len + content + b"\x00" * 7


async def encode_header(layout, send_option, cmd, order_id, flags, length, k, v80):
    out = bytearray()
    for code in layout:
        value = {0: send_option, 1: cmd, 2: order_id, 3: flags, 4: length}[code]
        if _FIELD_SIZES[code] == 1:
            out.append((value & 0xFF) ^ k)
        else:
            v = ((value & 0xFFFF) ^ v80) & 0xFFFF
            out.append(v & 0xFF)
            out.append((v >> 8) & 0xFF)
    return bytes(out)


async def crc7_buff(crc, buf):
    c = crc & 0x7F
    for b in buf:
        c = CRC7_TABLE[((2 * (c & 0xFF)) ^ (b & 0xFF)) & 0xFF] & 0x7F
    return c & 0x7F


async def sv_frame(msg_key, layout, send_option, cmd, order_id, flags, content, key, encrypted=True):
    k = key[0]
    v80 = ((k << 8) | k) & 0xFFFF
    body = await tea_cbc_encrypt(await build_padded(content), key) if encrypted else content
    hdr = bytearray([msg_key, 0]) + await encode_header(layout, send_option, cmd, order_id, flags, len(body), k, v80)
    packet = bytearray(hdr + body)
    packet[1] = await crc7_buff(0, bytes(packet[2:])) & 0x7F
    return bytes(packet)


async def build_match_startup_packets(token, udp_key, match_code, account_id, block_val,
                                      server_ip="", region=None, client_version="1.132.8",
                                      client_version_code="2019121229", access_token="",
                                      mode_id=MODE_ID, map_id=MAP_ID):
    if region is None: region = ACCOUNT_REGION
    token = token.strip()
    udp_key = bytes.fromhex(udp_key)
    match_code = [int(ch) for ch in str(match_code).strip()]
    thunder_jwt = token[:660] if len(token) > 660 else token
    sharma_jwt = token[660:] if len(token) > 660 else ""
    encoded_thunder_jwt = thunder_jwt.encode() if isinstance(thunder_jwt, str) else thunder_jwt
    encoded_sharma_jwt = sharma_jwt.encode() if isinstance(sharma_jwt, str) else sharma_jwt
    garena420 = has_ssan_zig(len(encoded_thunder_jwt)) + encoded_thunder_jwt
    reg = str(region).upper() if region else ACCOUNT_REGION
    csoversea_block = bytes.fromhex(
        "ca0163736f7665727365612e7374726f6e67686f6c642e66726565666972656d6f62696c652e636f6d"
        "3b302e302e302e303b33342e3132362e37362e34353b33342e38372e3137372e31343b33342e38372e"
        "3137302e3233303b33352e3138352e3138332e35370000000000000100000000000000000000000001"
        "00000800000100000000000100a8a2d7bebd8d8bdf110200"
    )
    mid = bytes.fromhex('0000000001000102030101') + has_ssan_zig(len(reg)) + reg.encode()
    mid += bytes.fromhex('0001030003000004')
    mid += has_ssan_zig(len(client_version)) + client_version.encode()
    mid += has_ssan_zig(len(client_version_code)) + client_version_code.encode()
    mid += csoversea_block
    clean_ip = server_ip.split(':')[0] if server_ip else "0.0.0.0"
    mid += has_ssan_zig(len(clean_ip)) + clean_ip.encode()
    clean_acc_tok = access_token.strip() if access_token else ""
    if clean_acc_tok:
        mid += has_ssan_zig(len(clean_acc_tok)) + clean_acc_tok.encode()
    mid += has_ssan_zig(len(encoded_sharma_jwt)) + encoded_sharma_jwt
    tg_garena420 = (
        uleb_encode(int(account_id)) +
        uleb_encode(int(block_val)) +
        uleb_encode(1) +
        uleb_encode(int(mode_id)) +
        uleb_encode(int(block_val)) +
        uleb_encode(int(map_id)) +
        mid
    )
    process = await sv_frame(0x5E, match_code, 2, 447, 0, 1, garena420, udp_key)
    loading = await sv_frame(0x5A, match_code, 2, 448, 1, 1, tg_garena420, udp_key)
    return process.hex(), loading.hex()


async def tea_cbc_decrypt(body, key_bytes):
    k0, k1, k2, k3 = (struct.unpack_from("<I", key_bytes, o)[0] for o in (0, 4, 8, 12))
    out = bytearray(len(body))
    prev_intermediate = bytearray(8)
    prev_cipher = bytearray(8)
    xored = bytearray(8)
    dec = bytearray(8)
    for i in range(0, len(body), 8):
        for j in range(8):
            xored[j] = body[i + j] ^ prev_intermediate[j]
        d0, d1 = await tea_dec(
            struct.unpack_from("<I", xored, 0)[0],
            struct.unpack_from("<I", xored, 4)[0],
            k0, k1, k2, k3
        )
        struct.pack_into("<I", dec, 0, d0)
        struct.pack_into("<I", dec, 4, d1)
        for j in range(8):
            out[i + j] = dec[j] ^ prev_cipher[j]
        prev_cipher[:] = body[i:i + 8]
        prev_intermediate[:] = dec
    return bytes(out)


async def build_hello_packet(text, key, layout):
    data = text.encode("utf-8")
    if len(data) > 25: raise ValueError(f"Too long: {len(data)}B")
    content = b"\x10\x00\x00\x00" + data + b"\x00" * (29 - 4 - len(data))
    k = key[0] if key else 10
    v80 = ((k << 8) | k) & 0xFFFF
    layout = [int(c) for c in str(layout).strip()] if isinstance(layout, str) else list(layout)
    padded = await build_padded(content)
    enc_body = await tea_cbc_encrypt(padded, key)
    header_bytes = await encode_header(layout, 1, 1, 0, 1, len(enc_body), k, v80)
    packet = bytearray([0x63, 0x00]) + header_bytes + enc_body
    packet[1] = await crc7_buff(0, packet[2:]) & 0x7F
    return bytes(packet).hex()


async def classify(frame):
    cmd = frame["cmd"]
    msg_name = MESSAGE_ID_TO_NAME.get(cmd, f"UNKNOWN_{cmd}")
    if msg_name == "UDP_HELLO": return "HELLO"
    if msg_name == "UDP_ACK": return "ACK"
    if msg_name == "UDP_PING": return "PING"
    if msg_name == "RUDP_JOIN_MATCH": return "JOIN_MATCH"
    if msg_name.startswith("RUDP_"): return msg_name
    if msg_name.startswith("UDP_"): return msg_name
    return "DATA"


async def build_packet(msg_key, layout, send_option, cmd, order_id, flags, content, key, encrypted=True):
    k = key[0]
    v80 = ((k << 8) | k) & 0xFFFF
    body = await tea_cbc_encrypt(await build_padded(content), key) if encrypted else content
    hdr = bytearray([msg_key, 0])
    for code in layout:
        value = {0: send_option, 1: cmd, 2: order_id, 3: flags, 4: len(body)}[code]
        if _FIELD_SIZES[code] == 1:
            hdr.append((value & 0xFF) ^ k)
        else:
            v = ((value & 0xFFFF) ^ v80) & 0xFFFF
            hdr.append(v & 0xFF)
            hdr.append((v >> 8) & 0xFF)
    packet = bytearray(hdr + body)
    packet[1] = await crc7_buff(0, bytes(packet[2:])) & 0x7F
    return bytes(packet)


async def layouts_from_mask(mask):
    ru = [int(c) for c in str(mask).strip()]
    nr = [c for c in ru if c != 2]
    return ru, nr


async def reply_for(frame, key, mask, ack_key=0x68, ping_key=0x6D, hello_key=0x5B, ack_style="short"):
    ru, nr = await layouts_from_mask(mask)
    typ = await classify(frame)
    if typ == "HELLO":
        if ack_style == "echo":
            content = frame["content"] if frame["content"] else b"\x10\x00\x00\x00"
            return typ, await build_packet(hello_key, nr, 1, 1, None, 1, content, key)
        return typ, await build_packet(ack_key, nr, 0, 2, None, 1, b"\x01\x00", key)
    if typ == "ACK":
        content = frame["content"] if frame["content"] else b"\x01\x00"
        return typ, await build_packet(ack_key, nr, 0, 2, None, 1, content, key)
    if typ == "PING":
        c = frame["content"]
        counter = c[:4] if len(c) >= 4 else c
        return typ, await build_packet(ping_key, nr, 0, 3, None, 0, counter + b"\x00\x00\x00", key, encrypted=False)
    if typ == "JOIN_MATCH":
        return typ, await build_packet(ack_key, nr, 0, 2, None, 1, b"\x02\x00", key)
    return typ, None


async def keepalive_ping(sock, ip, port, key_bytes, mask, stop_event):
    nr = (await layouts_from_mask(mask))[1]
    ping_keys = [0x66, 0x6D, 0x69, 0x6C, 0x6B, 0x6E, 0x6F, 0x70]
    loop = asyncio.get_event_loop()
    i = 0
    while not stop_event.is_set():
        pk = ping_keys[i % len(ping_keys)]
        counter = int(time.time() * 1000) & 0xFFFFFFFF
        pkt = await build_packet(pk, nr, 0, 3, None, 0, struct.pack("<I", counter) + b"\x00\x00\x00", key_bytes, encrypted=False)
        try: await loop.sock_sendto(sock, pkt, (ip, port))
        except Exception: pass
        i += 1
        try: await asyncio.wait_for(stop_event.wait(), timeout=3.0)
        except asyncio.TimeoutError: pass


async def try_header(buf, layout, k, v80):
    off = 2; out = {}
    for code in layout:
        size = _FIELD_SIZES[code]
        if off + size > len(buf): return None
        out[_FIELD_NAMES[code]] = (buf[off] ^ k) if size == 1 else ((buf[off] | (buf[off + 1] << 8)) ^ v80) & 0xFFFF
        off += size
    out["headerLen"] = off
    return out


async def oicq_unpad(padded):
    if not padded or len(padded) < 8: return None
    if not all(padded[-1 - i] == 0 for i in range(7)): return None
    pad_len = padded[0] & 0x07
    s = 3 + pad_len
    e = len(padded) - 7
    return padded[s:e] if s < e else b""


async def decode_packet(packet, key, mask=None):
    data = bytes(packet) if isinstance(packet, bytes) else bytes.fromhex(packet)
    if len(data) < 8: return None
    k = key[0]
    v80 = ((k << 8) | k) & 0xFFFF
    crc_ok = (data[1] & 0x7F) == await crc7_buff(0, data[2:])
    candidates = []
    if mask:
        ru, nr = await layouts_from_mask(mask)
        layouts = [("RUDP", ru), ("nonRUDP", nr)]
    else:
        layouts = [("RUDP", list(p)) for p in itertools.permutations([0, 1, 2, 3, 4])]
        layouts += [("nonRUDP", list(p)) for p in itertools.permutations([0, 1, 3, 4])]
    for kind, layout in layouts:
        f = await try_header(data, layout, k, v80)
        if not f: continue
        if f["flags"] > 7 or f["sendOption"] > 7: continue
        if f["length"] != len(data) - f["headerLen"]: continue
        body = data[f["headerLen"]:f["headerLen"] + f["length"]]
        content = None
        if f["flags"] & 1:
            if len(body) < 8 or len(body) % 8 != 0: continue
            padded = await tea_cbc_decrypt(body, key)
            content = await oicq_unpad(padded)
            if content is None: continue
        else:
            content = body
        score = (1 if crc_ok else 0) + (1 if content is not None else 0)
        candidates.append({"kind": kind, "layout": layout, "headerLen": f["headerLen"],
            "msgKey": data[0], "cmd": f["cmd"], "flags": f["flags"],
            "sendOption": f["sendOption"], "orderId": f.get("orderId"),
            "length": f["length"], "content": content, "crcOk": crc_ok,
            "score": score, "total": len(data)})
    if not candidates: return None
    candidates.sort(key=lambda c: c["score"], reverse=True)
    return candidates[0]


# ═══════════════════════════ PLAY GAME ═══════════════════════════
async def play_game(server_ip_port, thunder, sharma, udp_key, match_code,
                    account_id, player_region, client_version, key, iv, match_index):
    match_start_time = time.time()
    ping_task = None
    sock = None
    ping_stop = asyncio.Event()
    move_task = None
    move_stop = asyncio.Event()
    uid_str = str(account_id)
    completed_cleanly = False

    try:
        ip, port = server_ip_port.split(":")
        port = int(port)
        resolved_ip = await resolve_host_cloudflare(ip)
        loop = asyncio.get_event_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try: sock.bind(('0.0.0.0', 0))
        except Exception: pass
        optimize_udp_socket(sock); sock.setblocking(False)
        udp_key_bytes = bytes.fromhex(udp_key)
        hello_packet = await build_hello_packet(f"{account_id}_2585", udp_key_bytes, match_code)
        try:
            await loop.sock_sendto(sock, bytes.fromhex(hello_packet), (resolved_ip, port))
            log(f"[MATCH #{match_index}] HELLO sent {len(bytes.fromhex(hello_packet))}B")
        except Exception as e:
            log(f"[MATCH #{match_index}] HELLO fail: {e}")

        ack_state = "waiting_for_hello_reply"
        thunder_sent = False
        join_match_received = False
        local_closed = False
        send_lock = asyncio.Lock()
        ping_task = asyncio.create_task(keepalive_ping(sock, resolved_ip, port, udp_key_bytes, match_code, ping_stop))
        last_activity = time.time()

        async def send_thunder_sharma_inline():
            nonlocal ack_state, thunder_sent, move_task
            if thunder_sent: return
            async with send_lock:
                if thunder_sent: return
                try:
                    thunder_b = bytes.fromhex(thunder)
                    await loop.sock_sendto(sock, thunder_b, (resolved_ip, port))
                    thunder_sent = True
                    await asyncio.sleep(0.3)
                    prepare_ack = await build_packet(0x68, (await layouts_from_mask(match_code))[1], 0, 2, None, 1, b"\x01\x00", udp_key_bytes)
                    await loop.sock_sendto(sock, prepare_ack, (resolved_ip, port))
                    await asyncio.sleep(0.4)
                    sharma_b = bytes.fromhex(sharma)
                    await loop.sock_sendto(sock, sharma_b, (resolved_ip, port))
                    ack_state = "thunder_sharma_sent"
                    log(f"[MATCH #{match_index}] Startup delivered — playing")
                    # ✅ تشغيل الحركة فوراً
                    if ENABLE_MOVEMENT and move_task is None:
                        move_task = asyncio.create_task(movement_worker(
                            sock, resolved_ip, port, udp_key_bytes,
                            match_code, account_id, move_stop, match_index))
                        print_success(f"[MOVE #{match_index}] Worker started")
                except Exception as e:
                    log(f"[MATCH #{match_index}] startup fail: {e}")

        while not local_closed:
            if time.time() - match_start_time > MAX_MATCH_DURATION:
                print_warning(f"[MATCH #{match_index}] max duration — ending")
                completed_cleanly = True
                break

            if thunder_sent and (time.time() - last_activity) > MATCH_UDP_IDLE_TIMEOUT:
                print_warning(f"[MATCH #{match_index}] idle {int(MATCH_UDP_IDLE_TIMEOUT)}s — ending")
                completed_cleanly = True
                local_closed = True
                break

            try:
                response, server_addr = await asyncio.wait_for(loop.sock_recvfrom(sock, 65535), timeout=2.0)
                if response:
                    last_activity = time.time()
                    frame = await decode_packet(response, udp_key_bytes, match_code)
                    if frame:
                        ptype = await classify(frame)
                        cmd = frame['cmd']
                        if cmd in [103, 107]:
                            log(f"[★] Match #{match_index} finished (cmd {cmd})")
                            completed_cleanly = True
                            local_closed = True
                            continue
                        if cmd == 101:
                            try:
                                ack_pkt = await build_packet(0x68, (await layouts_from_mask(match_code))[1], 0, 2, None, 1, b"\x01\x00", udp_key_bytes)
                                await loop.sock_sendto(sock, ack_pkt, server_addr)
                            except Exception: pass
                        if ptype in ["ACK", "PING", "HELLO", "JOIN_MATCH"]:
                            if ptype == "HELLO" and ack_state == "waiting_for_hello_reply":
                                _, reply = await reply_for(frame, udp_key_bytes, match_code)
                                if reply: await loop.sock_sendto(sock, reply, server_addr)
                                ack_state = "ack_sent_waiting"
                            elif ptype == "ACK":
                                if ack_state == "waiting_for_hello_reply":
                                    _, reply = await reply_for(frame, udp_key_bytes, match_code)
                                    if reply: await loop.sock_sendto(sock, reply, server_addr)
                                    ack_state = "ready_to_send_thunder"
                                elif ack_state == "ack_sent_waiting":
                                    ack_state = "ready_to_send_thunder"
                                else:
                                    _, reply = await reply_for(frame, udp_key_bytes, match_code)
                                    if reply: await loop.sock_sendto(sock, reply, server_addr)
                            elif ptype == "PING":
                                _, reply = await reply_for(frame, udp_key_bytes, match_code)
                                if reply: await loop.sock_sendto(sock, reply, server_addr)
                            elif ptype == "JOIN_MATCH" and not join_match_received:
                                _, reply = await reply_for(frame, udp_key_bytes, match_code)
                                if reply:
                                    await loop.sock_sendto(sock, reply, server_addr)
                                    join_match_received = True
            except asyncio.TimeoutError:
                if ack_state == "ready_to_send_thunder" and not thunder_sent:
                    await send_thunder_sharma_inline()
                elif ack_state == "waiting_for_hello_reply":
                    if (time.time() - last_activity) > 7.0:
                        try:
                            pkt = await build_hello_packet(f"{account_id}_2585", udp_key_bytes, match_code)
                            await loop.sock_sendto(sock, bytes.fromhex(pkt), (resolved_ip, port))
                        except Exception: pass
                        last_activity = time.time()
                    if (time.time() - match_start_time) > 25.0:
                        break
            except BlockingIOError:
                await asyncio.sleep(0.05)
            except OSError:
                await asyncio.sleep(0.5); continue
            except Exception:
                await asyncio.sleep(0.5); continue
            if ack_state == "ready_to_send_thunder" and not thunder_sent:
                await send_thunder_sharma_inline()
        return f"match #{match_index} finished"
    except Exception as e:
        log(f"[MATCH #{match_index}] Session error: {e}")
        return f"match #{match_index} error"
    finally:
        if completed_cleanly:
            try: print_success(f"[★] Match #{match_index} Complete | UID: {uid_str}")
            except Exception: pass
        move_stop.set()
        if move_task:
            move_task.cancel()
            try: await move_task
            except asyncio.CancelledError: pass
        ping_stop.set()
        if ping_task:
            ping_task.cancel()
            try: await ping_task
            except asyncio.CancelledError: pass
        if sock:
            try: sock.close()
            except Exception: pass


# ═══════════════════════════ SOCIAL & TEAM INVITE HANDLER ═══════════════════════════
async def _process_social_packet(raw_bytes, account_data, tag="MAIN"):
    try:
        hex_data = raw_bytes.hex()
        candidates = []
        for off in [5, 6, 10, 8, 12, 4, 0]:
            if off >= len(hex_data): continue
            try: inner = bytes.fromhex(hex_data[off:])
            except Exception: continue
            try:
                parsed = Parser().parse(inner)
                d0 = await parse_results(parsed)
                if d0: candidates.append((off, d0))
            except Exception: pass
        if not candidates: return
        best_off, decoded = candidates[0]
        def _v(c, k):
            if not isinstance(c, dict): return None
            v = c.get(k) if k in c else c.get(str(k))
            if isinstance(v, dict) and "data" in v: return v["data"]
            return v
        f5wrap = decoded.get("5") or decoded.get(5)
        f5 = f5wrap.get("data") if isinstance(f5wrap, dict) and "data" in f5wrap else f5wrap
        target = None
        if isinstance(f5, dict):
            for fk in [1, 2, 3, 4, 6]:
                c = _v(f5, fk)
                if c and str(c).isdigit() and 6 <= len(str(c)) <= 15:
                    target = str(c); break
        if not target: return
        own = str(account_data.get('account_id', '')) if account_data else ''
        if own and target == own: return
        if not _accept_friend_request or not account_data: return
        jwt = account_data.get('token'); srv = account_data.get('server_url')
        rel = account_data.get('release_version'); own_id = account_data.get('account_id')
        if not (jwt and srv and rel and own_id): return
        url = f"{srv.rstrip('/')}/ConfirmFriendRequest"
        fields = {"1": int(target), "2": int(own_id)}
        try:
            payload = AES.new(AES_KEY, AES.MODE_CBC, AES_IV).encrypt(
                pad(create_proto(fields), AES.block_size))
            hdrs = {
                "authorization": f"Bearer {jwt}",
                "host": srv.replace("https://", "").replace("http://", "").split("/")[0],
                "x-ga": "v1 1", "content-type": "application/x-www-form-urlencoded",
                "accept-encoding": "deflate, gzip", "releaseversion": f"{rel}",
                "user-agent": "UnityPlayer/2022.3.47f1", "accept": "*/*",
                "x-unity-version": "2022.3.47f1",
            }
            async with httpx.AsyncClient(verify=False, timeout=8.0) as s:
                r = await s.post(url, headers=hdrs, content=payload)
                if r.status_code == 200 and len(r.content) > 0:
                    print_success(f"[FRIEND-{tag}] ✅ {target}")
        except Exception: pass
    except Exception: pass


async def _handle_team_invite(hex_data, pl, uid_str, current_account_data,
                              current_key, current_iv, writer):
    try:
        print_info(f"[TEAM-IN {uid_str}] 🔔 len={pl} prefix={hex_data[:16]}")

        candidates = []
        for off in [10, 8, 6, 12, 14, 4, 16, 0, 20, 2]:
            if off >= len(hex_data) or len(hex_data) - off < 4: continue
            try: inner = bytes.fromhex(hex_data[off:])
            except Exception: continue

            try:
                parsed = Parser().parse(inner)
                d = await parse_results(parsed)
                if d and isinstance(d, dict) and len(d) > 0:
                    candidates.append((off, "parser", d))
                    continue
            except Exception: pass

            try:
                alt = _decode_proto_manual(inner)
                if alt and len(alt) > 0:
                    candidates.append((off, "manual", {str(k): {"data": v} for k, v in alt.items()}))
            except Exception: pass

        if not candidates:
            print_warning(f"[TEAM-IN {uid_str}] ❌ unparseable")
            return False

        candidates.sort(key=lambda x: len(x[2]) if isinstance(x[2], dict) else 0, reverse=True)
        best_off, decoder, dec = candidates[0]

        def _unwrap(d, keys):
            if not isinstance(d, dict): return None
            for k in keys:
                v = d.get(k) if k in d else d.get(str(k))
                if isinstance(v, dict) and "data" in v: return v["data"]
                if isinstance(v, dict): return v
            return None
        target_dict = None
        for kp in [["5"], ["4"], ["6"], ["2"], ["1"]]:
            u = _unwrap(dec, kp)
            if isinstance(u, dict) and len(u) >= 3:
                target_dict = u
                break
        if target_dict is None: target_dict = dec

        def _g(d, k):
            if not isinstance(d, dict): return None
            v = d.get(k) if k in d else d.get(str(k))
            if isinstance(v, dict) and "data" in v: return v["data"]
            if isinstance(v, dict):
                for kk in ["1","2","3","4","5","6","7","8"]:
                    vv = v.get(kk) if kk in v else v.get(int(kk))
                    if isinstance(vv, dict) and "data" in vv: return vv["data"]
            return v

        group_id = None
        for k in list(target_dict.keys()):
            c = _g(target_dict, k)
            if isinstance(c, str) and 20 <= len(c) <= 40:
                group_id = c
                print_success(f"[TEAM-IN {uid_str}] 📋 group_id: {c[:24]}...")
                break
        if not group_id:
            for k in list(dec.keys()):
                c = _g(dec, k)
                if isinstance(c, str) and 20 <= len(c) <= 40:
                    group_id = c
                    print_success(f"[TEAM-IN {uid_str}] 📋 group_id@root: {c[:24]}...")
                    break

        own_acc = str(current_account_data.get('account_id')) if current_account_data else None
        inviter = None
        all_f = list(target_dict.keys()) + list(dec.keys())
        for k in all_f:
            c = _g(target_dict, k) or _g(dec, k)
            if c and str(c).isdigit() and 6 <= len(str(c)) <= 15:
                if str(c) != uid_str and str(c) != own_acc:
                    inviter = str(c)
                    print_success(f"[TEAM-IN {uid_str}] 👤 inviter: {c}")
                    break

        is_member = (_team_state["leader_uid"] != uid_str)
        if inviter and group_id and is_member:
            async with _team_state["members_lock"]:
                already = uid_str in _team_state["members"]
            if already: return True
            print_success(f"[TEAM-IN {uid_str}] 📨 ACCEPT inviter={inviter}")
            accepted = False
            for attempt in range(3):
                try:
                    acc = accept_team_invite(int(inviter), group_id, current_key, current_iv)
                    if writer and not writer.is_closing():
                        writer.write(acc); await writer.drain()
                        print_success(f"[TEAM-IN {uid_str}] ✅ accept #{attempt+1}")
                        accepted = True; break
                except Exception as e:
                    print_warning(f"[TEAM-IN {uid_str}] accept err: {e}")
                await asyncio.sleep(0.8)
            if accepted:
                await _team_member_join(uid_str)
            return True
        return False
    except Exception as e:
        print_warning(f"[TEAM-IN {uid_str}] error: {e}")
        return False


# ═══════════════════════════ FUNCTIONAL BR ═══════════════════════════
async def functional_br(addrs, starter_packet, account_region, client_version,
                        key, iv, account_id="", account_data=None, max_reconnects=10):
    reconnects = 0
    ip, port = addrs.split(":")
    play_matches: List[asyncio.Task] = []
    no_response_count = 0
    uid_str = str(account_id)
    current_token = starter_packet
    current_key = key
    current_iv = iv
    current_account_data = account_data
    _writer_ref = {"w": None}
    _team_active = {"flag": True}

    async def _team_coordinator():
        try:
            while _team_active["flag"]:
                try:
                    async with _team_state["lock"]:
                        is_leader = (_team_state["leader_uid"] == uid_str)
                    if not is_leader:
                        print_info(f"[TEAM] {uid_str} waiting as member...")
                        return

                    # انتظر جميع الأقران (حتى 300s)
                    deadline = time.time() + TEAM_PEER_WAIT_TIMEOUT
                    while time.time() < deadline:
                        if not _team_active["flag"]: return
                        async with _team_state["lock"]:
                            n = len(_team_state["peers"])
                            is_leader = (_team_state["leader_uid"] == uid_str)
                        if not is_leader: return
                        if n >= TEAM_SIZE: break
                        await asyncio.sleep(0.5)

                    async with _team_state["lock"]:
                        n = len(_team_state["peers"])
                        is_leader = (_team_state["leader_uid"] == uid_str)
                        peers_snapshot = dict(_team_state["peers"])

                    if not is_leader: return

                    if n < TEAM_SIZE:
                        print_warning(f"[TEAM] Only {n}/{TEAM_SIZE} peers — retry 15s")
                        await asyncio.sleep(15)
                        continue

                    print_success(f"[TEAM] ✅ All {n} peers online")
                    await asyncio.sleep(TEAM_READY_DELAY)
                    print_success(f"[TEAM] 👑 {uid_str} creating team...")

                    async with _team_state["lock"]:
                        _team_state["team_code"] = None

                    own_id_num = None
                    if current_account_data: own_id_num = current_account_data.get('account_id')
                    if not own_id_num:
                        try: own_id_num = int(uid_str)
                        except Exception: own_id_num = 0

                    try:
                        pkt = create_team(current_key, current_iv, client_version)
                        if _writer_ref["w"] and not _writer_ref["w"].is_closing():
                            _writer_ref["w"].write(pkt); await _writer_ref["w"].drain()
                            print_info(f"[TEAM] create_team sent ({len(pkt)}B)")
                            await _team_member_reset(keep_leader=False)
                            await _team_member_join(uid_str)
                    except Exception as e:
                        print_error(f"[TEAM] create_team: {e}")

                    await asyncio.sleep(TEAM_CREATE_WAIT)

                    peer_uids = []
                    for peer_uid, info in peers_snapshot.items():
                        if peer_uid == uid_str: continue
                        pa = None
                        try: pa = info["data"].get("account_id")
                        except Exception: pass
                        if not pa: pa = peer_uid
                        peer_uids.append(int(pa))
                    print_info(f"[TEAM] inviting: {peer_uids}")

                    heartbeat_stop = asyncio.Event()
                    async def heartbeat_loop():
                        while not heartbeat_stop.is_set():
                            try:
                                if _writer_ref["w"] and not _writer_ref["w"].is_closing():
                                    pkt = team_fresh(int(own_id_num), current_key, current_iv)
                                    _writer_ref["w"].write(pkt); await _writer_ref["w"].drain()
                            except Exception: pass
                            try: await asyncio.wait_for(heartbeat_stop.wait(), timeout=TEAM_HEARTBEAT_INTERVAL)
                            except asyncio.TimeoutError: pass
                    hb_task = asyncio.create_task(heartbeat_loop())

                    # ═══════ INVITE LOOP مع تخطي بعد 4 محاولات ═══════
                    MAX_ATT = TEAM_MAX_INVITE_ATTEMPTS
                    invite_attempts = {int(u): 0 for u in peer_uids}
                    print_success(f"[TEAM] 🎯 Invite-loop (max {MAX_ATT} attempts per peer)")
                    invite_start = time.time()
                    cycle = 0
                    while time.time() - invite_start < TEAM_INVITE_MAX_TIME:
                        if not _team_active["flag"]: break
                        cnt = await _team_member_count()
                        if cnt >= TEAM_SIZE:
                            print_success(f"[TEAM] 🎉 Full team ({cnt}/{TEAM_SIZE})!")
                            break

                        # ✅ هل كل الأعضاء المتبقين استنفدوا 4 محاولات؟
                        async with _team_state["members_lock"]:
                            pending = [int(u) for u in peer_uids if str(u) not in _team_state["members"]]
                        if not pending:
                            break
                        all_exhausted = all(invite_attempts.get(u, 0) >= MAX_ATT for u in pending)
                        if all_exhausted:
                            print_warning(f"[TEAM] ⏭ All pending peers exhausted {MAX_ATT} attempts — aborting formation")
                            await asyncio.sleep(2.0)
                            break

                        cycle += 1
                        for tuid in peer_uids:
                            async with _team_state["members_lock"]:
                                joined = str(tuid) in _team_state["members"]
                            if joined: continue
                            if invite_attempts.get(tuid, 0) >= MAX_ATT:
                                continue
                            try:
                                inv = invite_player_in_team(tuid, current_key, current_iv, region=account_region)
                                if _writer_ref["w"] and not _writer_ref["w"].is_closing():
                                    _writer_ref["w"].write(inv); await _writer_ref["w"].drain()
                                    invite_attempts[tuid] = invite_attempts.get(tuid, 0) + 1
                                    print_success(f"[TEAM] 📨 invite {tuid} (#{invite_attempts[tuid]}/{MAX_ATT})")
                            except Exception as e:
                                print_warning(f"[TEAM] invite err: {e}")
                            await asyncio.sleep(0.5)
                        try: await asyncio.wait_for(heartbeat_stop.wait(), timeout=TEAM_INVITE_CYCLE_WAIT)
                        except asyncio.TimeoutError: pass

                    final_cnt = await _team_member_count()
                    print_info(f"[TEAM] FINAL: {final_cnt}/{TEAM_SIZE}")

                    heartbeat_stop.set()
                    try: hb_task.cancel(); await hb_task
                    except Exception: pass

                    # ✅ الانتظار الصارم حتى 4/4
                    if final_cnt < TEAM_SIZE:
                        print_warning(f"[TEAM] ⚠ {final_cnt}/{TEAM_SIZE} — retry formation 5s")
                        async with _team_state["members_lock"]:
                            _team_state["members"].clear()
                        await asyncio.sleep(5)
                        continue

                    # TEAM_FINAL_WAIT = 0 → بدء فوري
                    if TEAM_FINAL_WAIT > 0:
                        await asyncio.sleep(TEAM_FINAL_WAIT)
                    print_success(f"[TEAM] 🚀 {uid_str}: ready → start loop (4/4)")

                    while _team_active["flag"]:
                        try:
                            async with _team_state["lock"]:
                                if _team_state["leader_uid"] != uid_str: break
                            oid = current_account_data.get('account_id', own_id_num) if current_account_data else own_id_num
                            try:
                                rdy = ready_for_game(int(oid), current_key, current_iv)
                                if _writer_ref["w"] and not _writer_ref["w"].is_closing():
                                    _writer_ref["w"].write(rdy); await _writer_ref["w"].drain()
                            except Exception as e: print_warning(f"[TEAM] ready err: {e}")
                            await asyncio.sleep(1.0)
                            sm = start_match_team(int(oid), current_key, current_iv, region=account_region)
                            if _writer_ref["w"] and not _writer_ref["w"].is_closing():
                                _writer_ref["w"].write(sm); await _writer_ref["w"].drain()
                        except Exception as e: print_warning(f"[TEAM] start err: {e}")
                        try: await asyncio.wait_for(asyncio.sleep(5.0), timeout=5.5)
                        except asyncio.TimeoutError: pass

                    if _team_active["flag"]:
                        print_warning(f"[TEAM] {uid_str} exited loop — retry 10s")
                        async with _team_state["members_lock"]:
                            _team_state["members"].clear()
                        await asyncio.sleep(10)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print_error(f"[TEAM] coordinator err: {e}")
                    await asyncio.sleep(5)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print_error(f"[TEAM] coordinator outer: {e}")

    async def _member_match_trigger():
        try:
            await asyncio.sleep(TEAM_READY_DELAY + TEAM_MEMBER_START_DELAY)
            async with _team_state["lock"]:
                if _team_state["leader_uid"] == uid_str: return

            my_id = current_account_data.get('account_id', uid_str) if current_account_data else uid_str

            while _team_active["flag"]:
                # المرحلة 1: انتظر الدعوة
                print_info(f"[TEAM] {uid_str} waiting for invite...")
                join_deadline = time.time() + TEAM_PEER_WAIT_TIMEOUT
                while _team_active["flag"] and time.time() < join_deadline:
                    async with _team_state["members_lock"]:
                        in_team = uid_str in _team_state["members"]
                    if in_team: break
                    await asyncio.sleep(1.0)

                if not _team_active["flag"]: return
                async with _team_state["members_lock"]:
                    in_team = uid_str in _team_state["members"]
                if not in_team:
                    print_info(f"[TEAM] {uid_str} still not invited — keep waiting")
                    continue

                # المرحلة 2: في الفريق — انتظار 4/4
                print_info(f"[TEAM] {uid_str} in team — waiting for 4/4...")
                while _team_active["flag"]:
                    try:
                        async with _team_state["lock"]:
                            if _team_state["leader_uid"] == uid_str: return
                        async with _team_state["members_lock"]:
                            in_team = uid_str in _team_state["members"]
                            team_full = len(_team_state["members"]) >= TEAM_SIZE

                        if not in_team:
                            print_info(f"[TEAM] {uid_str} removed from team — back to wait")
                            break

                        if not team_full:
                            await asyncio.sleep(0.5)
                            continue

                        # 4/4 — أرسل ready/start
                        if _writer_ref["w"] and not _writer_ref["w"].is_closing():
                            try:
                                rdy = ready_for_game(int(my_id), current_key, current_iv)
                                _writer_ref["w"].write(rdy); await _writer_ref["w"].drain()
                            except Exception as e: print_warning(f"[TEAM] member ready: {e}")
                            await asyncio.sleep(0.5)
                            try:
                                sm = start_match_team(int(my_id), current_key, current_iv, region=account_region)
                                _writer_ref["w"].write(sm); await _writer_ref["w"].drain()
                            except Exception as e: print_warning(f"[TEAM] member start: {e}")
                    except Exception as e: print_warning(f"[TEAM] member: {e}")
                    try: await asyncio.wait_for(asyncio.sleep(3.0), timeout=3.5)
                    except asyncio.TimeoutError: pass
        except asyncio.CancelledError: raise
        except Exception as e: print_warning(f"[TEAM] member outer: {e}")

    try:
        while True:
            writer = None
            gateway_ping_task = None
            team_coord_task = None
            member_trigger_task = None
            try:
                resolved_ip = await resolve_host_cloudflare(ip)
                reader, writer = await asyncio.open_connection(resolved_ip, int(port))
                _writer_ref["w"] = writer
                raw_sock = writer.get_extra_info('socket')
                if raw_sock: optimize_tcp_socket(raw_sock)
                writer.write(bytes.fromhex(current_token)); await writer.drain()

                try:
                    init_ka = await send_keep_alive(account_region)
                    if init_ka and writer and not writer.is_closing():
                        writer.write(init_ka)
                        await asyncio.wait_for(writer.drain(), timeout=3)
                except Exception: pass

                async def func_gateway_keepalive():
                    ka_bytes = await send_keep_alive(account_region)
                    while True:
                        await asyncio.sleep(5)
                        try:
                            if writer and not writer.is_closing():
                                writer.write(ka_bytes); await writer.drain()
                        except Exception: break
                gateway_ping_task = asyncio.create_task(func_gateway_keepalive())

                log(f"[+] TCP connected | UID: {uid_str}")
                reconnects = 0
                no_response_count = 0

                async with _team_state["lock"]:
                    _team_state["peers"][uid_str] = {
                        "writer": writer, "data": current_account_data,
                        "key": current_key, "iv": current_iv}
                    if _team_state["leader_uid"] is None:
                        _team_state["leader_uid"] = uid_str
                        print_success(f"[TEAM] 👑 {uid_str} is leader")

                if TEAM_MODE:
                    team_coord_task = asyncio.create_task(_team_coordinator())
                    member_trigger_task = asyncio.create_task(_member_match_trigger())
                else:
                    await start_game_battle_royale(account_region, client_version, writer, current_key, current_iv)

                while True:
                    play_matches[:] = [m for m in play_matches if not m.done()]

                    try:
                        data = await asyncio.wait_for(reader.read(8192), timeout=0.5)
                    except asyncio.TimeoutError:
                        no_response_count += 1
                        if TEAM_MODE and no_response_count > TEAM_IDLE_ITERS_TEAM:
                            raise ConnectionError("Team idle timeout")
                        if not TEAM_MODE and no_response_count > 80:
                            raise ConnectionError("idle")
                        continue

                    if not data: raise ConnectionError("closed")
                    hex_data = data.hex()
                    pl = len(data)
                    no_response_count = 0

                    if not hex_data.startswith("0300"):
                        print_info(f"[RECV {uid_str}] len={pl} prefix={hex_data[:16]}")

                    if hex_data.startswith("0600") and pl > 20:
                        await _process_social_packet(data, current_account_data, tag="MAIN")
                        continue

                    if (hex_data.startswith("05") or hex_data.startswith("06")) and pl > 10:
                        handled = await _handle_team_invite(hex_data, pl, uid_str, current_account_data, current_key, current_iv, writer)
                        if handled: continue

                    if hex_data.startswith("0300") and 10 < pl < 30:
                        continue

                    if hex_data.startswith("0300") and pl >= 300:
                        print_success(f"[⚔] Match Found | UID: {uid_str}")
                        for tk in (team_coord_task, member_trigger_task):
                            if tk and not tk.done():
                                tk.cancel()
                                try: await tk
                                except asyncio.CancelledError: pass
                        team_coord_task = None; member_trigger_task = None
                        _team_active["flag"] = False

                        try:
                            res = json.loads(await decode_protobuf(hex_data[10:]))
                            token = udp_key = match_code = server_ip_port = None
                            match_account_id = block_val = None
                            if '42' in res and 'data' in res['42']:
                                match_code = res['42']['data']
                            if '5' in res and 'data' in res['5']:
                                r5 = res['5']['data']
                                server_ip_port = r5.get('2', {}).get('data')
                                udp_key = r5.get('3', {}).get('data')
                                token = r5.get('4', {}).get('data')
                                if '42' in r5: match_code = r5['42']['data']
                            if '1' in res and 'data' in res['1']:
                                match_account_id = res['1']['data']
                            if '5' in res and 'data' in res['5']:
                                block_val = res['5']['data'].get('1', {}).get('data')

                            effective = match_account_id or account_id or "ME_BOT"
                            if token and udp_key and match_code and server_ip_port:
                                acc_tok = current_account_data.get('access_token', '') if current_account_data else ""
                                thunder, sharma = await build_match_startup_packets(
                                    token, udp_key, match_code, effective, block_val or 0,
                                    server_ip=server_ip_port, region=account_region,
                                    client_version=client_version, access_token=acc_tok,
                                    mode_id=MODE_ID, map_id=MAP_ID)
                                match_index = len(play_matches) + 1
                                log(f"[+] Match #{match_index} -> {server_ip_port}")

                                new_match = asyncio.create_task(play_game(
                                    server_ip_port, thunder, sharma, udp_key, match_code,
                                    effective, account_region, client_version,
                                    current_key, current_iv, match_index=match_index))
                                play_matches.append(new_match)

                                async def drain_gw():
                                    while not new_match.done():
                                        try:
                                            dg = await asyncio.wait_for(reader.read(4096), timeout=1.0)
                                            if not dg: break
                                            gh = dg.hex()
                                            if gh.startswith("0600") and len(dg) > 20:
                                                await _process_social_packet(dg, current_account_data, tag="DRAIN")
                                        except asyncio.TimeoutError: continue
                                        except Exception: break
                                drain_task = asyncio.create_task(drain_gw())
                                try: await new_match
                                except Exception as e: log(f"[MATCH #{match_index}] err: {e}")
                                finally:
                                    drain_task.cancel()
                                    try: await drain_task
                                    except asyncio.CancelledError: pass
                                play_matches[:] = [m for m in play_matches if not m.done()]

                                print_success(f"[IDLE] {uid_str} match done — reconnecting in {NEW_MATCH_DELAY}s")

                                async with _team_state["lock"]:
                                    _team_state["peers"].pop(uid_str, None)
                                    if _team_state["leader_uid"] == uid_str:
                                        _team_state["leader_uid"] = None
                                        _team_state["team_code"] = None
                                await _team_member_leave(uid_str)
                                _team_active["flag"] = True
                                try:
                                    writer.close()
                                    await writer.wait_closed()
                                except Exception: pass
                                await asyncio.sleep(NEW_MATCH_DELAY)
                                break
                        except Exception as e:
                            print_warning(f"[MATCH] {e}"); continue

                    if 30 <= pl <= 40: continue

            except asyncio.CancelledError: raise
            except Exception as e:
                if "Cache expired" in str(e):
                    print_warning(f"[!] Token expired {uid_str}"); break
                reconnects += 1
                if reconnects > max_reconnects:
                    reconnects = 0; break
                print_warning(f"[{uid_str}] Reconnect #{reconnects}: {e}")
                await asyncio.sleep(min(reconnects * 0.5, 2.0))
            finally:
                _team_active["flag"] = False
                async with _team_state["lock"]:
                    _team_state["peers"].pop(uid_str, None)
                    if _team_state["leader_uid"] == uid_str:
                        _team_state["leader_uid"] = None
                        _team_state["team_code"] = None
                for tk in (team_coord_task, member_trigger_task):
                    if tk and not tk.done():
                        tk.cancel()
                        try: await tk
                        except asyncio.CancelledError: pass
                if gateway_ping_task: gateway_ping_task.cancel()
                await safe_close_writer(writer)
                _team_active["flag"] = True
    except asyncio.CancelledError:
        print_info(f"[FUNCTIONAL] cancelled {uid_str}")
        raise
    finally:
        for m in play_matches:
            if not m.done(): m.cancel()


# ═══════════════════════════ INFORMATIONAL ═══════════════════════════
async def informational(addrs, starter_packet, key, iv, region=None, account_id="", max_reconnects=3):
    if region is None: region = ACCOUNT_REGION
    reconnects = 0
    ip, port = addrs.split(":")
    while True:
        writer = None; ping_task = None
        try:
            resolved_ip = await resolve_host_cloudflare(ip)
            reader, writer = await asyncio.open_connection(resolved_ip, int(port))
            raw_sock = writer.get_extra_info('socket')
            if raw_sock: optimize_tcp_socket(raw_sock)
            writer.write(bytes.fromhex(starter_packet)); await writer.drain()
            reconnects = 0
            try:
                init_ka = await send_keep_alive(region)
                if init_ka and writer and not writer.is_closing():
                    writer.write(init_ka)
                    await asyncio.wait_for(writer.drain(), timeout=3)
            except Exception: pass
            async def info_keepalive():
                ka_bytes = await send_keep_alive(region)
                while True:
                    await asyncio.sleep(5)
                    try:
                        if writer and not writer.is_closing():
                            writer.write(ka_bytes); await writer.drain()
                    except Exception: break
            ping_task = asyncio.create_task(info_keepalive())
            while True:
                try:
                    data = await asyncio.wait_for(reader.read(8192), timeout=1.0)
                except asyncio.TimeoutError: continue
                if not data: raise ConnectionError("closed")
        except asyncio.CancelledError:
            if ping_task: ping_task.cancel()
            await safe_close_writer(writer)
            raise
        except Exception:
            if ping_task: ping_task.cancel()
            await safe_close_writer(writer)
            reconnects += 1
            if reconnects > max_reconnects:
                await asyncio.sleep(3); reconnects = 0
            else: await asyncio.sleep(1)


# ═══════════════════════════ ACCOUNT HELPERS ═══════════════════════════
async def refresh_account_profile(account_data: Dict):
    try:
        if not account_data: return
        url = account_data.get('server_url')
        token = account_data.get('token')
        release_version = account_data.get('release_version')
        payload = account_data.get('login_payload_data')
        if not (url and token and release_version and payload): return
        res = await send_getlogin(payload, url, token, release_version)
        if not res: return
        res_proto, dict_res = res
        level = int(get_proto_field(dict_res, 6, 1))
        exp = int(get_proto_field(dict_res, 7, 0))
        nickname = res_proto.nickname or get_proto_field(dict_res, 4, "")
        if level <= 0: level = 1
        if exp < 0: exp = 0
        account_data['level'] = level
        account_data['exp'] = exp
        if nickname: account_data['nickname'] = nickname
    except Exception: pass


async def process_account_uid_pass(uid: str, password: str) -> Optional[Dict]:
    log(f"[*] Logging in UID: {uid}")
    try:
        async with _LOGIN_SEMAPHORE:
            verconfig_res = await version_config()
            if verconfig_res is None: return None
            release_version, client_version, server_url = verconfig_res
            tokengrant_response = await get_access_token(uid, password)
            if tokengrant_response is None:
                log(f"[-] OAuth failed for UID {uid}")
                return None
            open_id, access_token, platform = tokengrant_response
            device_info = get_device_for_account(uid)
            login_payload_data = await build_majorlogin_payload(open_id, access_token, platform, client_version, device_info)
            if login_payload_data is None: return None
            majorlogin_response = await send_majorlogin(login_payload_data, release_version, server_url)
            if majorlogin_response is None:
                log(f"[-] MajorLogin failed for {uid}")
                return None
            getlogin_result = await send_getlogin(
                login_payload_data, majorlogin_response.url,
                majorlogin_response.token, release_version)
            if getlogin_result is None:
                log(f"[-] GetLoginData failed for {uid}")
                return None
            res_proto, dict_res = getlogin_result

        acc_id = str(majorlogin_response.account_id)
        if acc_id == "0" or not acc_id or acc_id == "None":
            log(f"[-] Invalid account_id for UID {uid}")
            return None

        level = int(get_proto_field(dict_res, 6, 1))
        exp = int(get_proto_field(dict_res, 7, 0))
        nickname = res_proto.nickname or get_proto_field(dict_res, 4, f"Player_{acc_id}")
        # 🔒 ME locked
        region = "ME"
        print_info(f"[REGION] Locked to ME (was: {majorlogin_response.region or 'unknown'})")
        if level <= 0: level = 1
        if exp < 0: exp = 0

        func_addr = res_proto.functional_addrs
        info_addr = res_proto.informational_addrs
        if not isinstance(func_addr, str) or not func_addr or ":" not in func_addr: func_addr = None
        if not isinstance(info_addr, str) or not info_addr or ":" not in info_addr: info_addr = None

        key_val = majorlogin_response.aes_ak
        iv_val = majorlogin_response.iv_i
        if not isinstance(key_val, (bytes, bytearray)) or len(key_val) < 16: key_val = AES_KEY
        if not isinstance(iv_val, (bytes, bytearray)) or len(iv_val) < 16: iv_val = AES_IV

        log(f"[+] Login OK | UID {acc_id} | {nickname} | Lvl {level}")

        account_data = {
            'account_id': majorlogin_response.account_id,
            'nickname': nickname, 'region': region,
            'level': level, 'exp': exp,
            'open_id': open_id, 'access_token': access_token, 'platform': str(platform),
            'token': majorlogin_response.token, 'server_time': majorlogin_response.server_time,
            'aes_ak': key_val, 'iv_i': iv_val,
            'functional_addrs': func_addr, 'informational_addrs': info_addr,
            'release_version': release_version, 'client_version': client_version,
            'server_url': majorlogin_response.url,
            'login_payload_data': login_payload_data,
            'auth_type': 'guest', 'auth_uid': uid, 'auth_password': password
        }
        return account_data
    except Exception as e:
        log(f"[-] process_account error: {e}")
        return None


async def run_account_worker(account_data: Dict, label: str):
    acc_id = str(account_data['account_id'])
    info_task = None; exp_task = None; func_task = None
    try:
        reg = account_data.get('region', ACCOUNT_REGION)
        func_addr = account_data.get('functional_addrs')
        info_addr = account_data.get('informational_addrs')

        if not func_addr or not isinstance(func_addr, str) or ":" not in func_addr:
            print_warning(f"[!] No functional_addrs for {acc_id}")
            while True: await asyncio.sleep(60)
            return

        tcp_online = await build_tcp_startup_packet(
            account_data['account_id'], account_data['token'], account_data['server_time'],
            account_data['aes_ak'], account_data['iv_i'], region=reg, typ='OnLine')
        tcp_chat = await build_tcp_startup_packet(
            account_data['account_id'], account_data['token'], account_data['server_time'],
            account_data['aes_ak'], account_data['iv_i'], region=reg, typ='ChaT')

        if info_addr and isinstance(info_addr, str) and ":" in info_addr:
            info_task = asyncio.create_task(
                informational(info_addr, tcp_chat,
                              account_data['aes_ak'], account_data['iv_i'],
                              region=reg, account_id=acc_id))

        async def exp_refresher():
            while True:
                await asyncio.sleep(120 + random.uniform(-10.0, 10.0))
                try: await refresh_account_profile(account_data)
                except Exception: pass
        exp_task = asyncio.create_task(exp_refresher())

        func_task = asyncio.create_task(
            functional_br(func_addr, tcp_online,
                          account_data['region'], account_data['client_version'],
                          account_data['aes_ak'], account_data['iv_i'],
                          account_id=acc_id, account_data=account_data))
        await func_task
    except asyncio.CancelledError: raise
    except Exception as e:
        print_error(f"run_account_worker {label}: {e}")
    finally:
        for t in (info_task, exp_task, func_task):
            if t and not t.done(): t.cancel()
        for t in (info_task, exp_task, func_task):
            if t:
                try: await t
                except (asyncio.CancelledError, Exception): pass


async def account_loop_guest(uid: str, password: str):
    uid_str = str(uid)
    while True:
        try:
            log(f"[*] Starting login for UID: {uid_str}")
            account_data = await process_account_uid_pass(uid_str, password)
            if not account_data:
                log(f"[-] Login failed for {uid_str}. Retry 15s")
                await asyncio.sleep(15)
                continue
            await run_account_worker(account_data, uid_str)
            log(f"[!] Session ended for {uid_str}. Reconnect 3s")
            await asyncio.sleep(3)
        except asyncio.CancelledError:
            log(f"[!] Worker stopped {uid_str}")
            break
        except Exception as e:
            log(f"[-] Error {uid_str}: {e}. Retry 10s")
            await asyncio.sleep(10)


# ═══════════════════════════ ACCOUNTS LOADER ═══════════════════════════
def load_accounts() -> List[Dict]:
    accounts = []
    if os.path.exists(ACCOUNTS_FILE):
        try:
            with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list): accounts = data
        except Exception as e:
            print_error(f"load: {e}")
    return accounts


def _prompt_region():
    print("[i] Region LOCKED → ME (Middle East)")
    return "ME"


# ═══════════════════════════ MAIN ═══════════════════════════
async def main():
    global ACCOUNT_REGION

    print("=" * 60)
    print("    TEAM 84FF v6 — Reconnect + Movement + Skip Failed")
    print("   TCP: 9015 | Region: ME (LOCKED) | KeepAlive: 0219")
    print(f"   🔄 Match done → reconnect in {NEW_MATCH_DELAY}s")
    print(f"   👥 Team formation: 4/4 or skip after {TEAM_MAX_INVITE_ATTEMPTS} attempts")
    print(f"   🏃 Movement: {'ON' if ENABLE_MOVEMENT else 'OFF'}")
    print("=" * 60)
    print(f"[i] Peer Wait: {TEAM_PEER_WAIT_TIMEOUT}s | Match Idle: {MATCH_UDP_IDLE_TIMEOUT}s")
    print(f"[i] Max Match Duration: {MAX_MATCH_DURATION}s")
    print("=" * 60)

    ACCOUNT_REGION = "ME"
    print_success(f"[REGION] Middle East (ME) — LOCKED")

    accounts = load_accounts()
    if not accounts:
        print_error(f"No accounts in {ACCOUNTS_FILE}")
        print_error('Format: [{"uid": "123456", "password": "ABC123..."}]')
        return

    print_success(f"Loaded {len(accounts)} account(s)")
    if len(accounts) < TEAM_SIZE:
        print_warning(f"Need {TEAM_SIZE} accounts, only {len(accounts)} found")

    tasks = []
    for i, acc in enumerate(accounts, 1):
        uid = str(acc.get("uid", "")).strip()
        password = str(acc.get("password", "")).strip()
        if not uid or not password: continue
        log(f"[*] Launching worker {i} | UID: {uid}")
        tasks.append(asyncio.create_task(account_loop_guest(uid, password)))
        await asyncio.sleep(0.5)

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log("[!] Shutting down...")
        for t in tasks: t.cancel()
        for t in tasks:
            try: await t
            except (asyncio.CancelledError, Exception): pass
        log("[+] Closed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")