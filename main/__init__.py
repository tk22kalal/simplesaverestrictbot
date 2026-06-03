#Join me at telegram @dev_gagan

# ── uvloop: must be the absolute first thing — before any asyncio/Telethon/
#    Pyrogram usage.  __init__.py is imported by Python before __main__.py runs
#    (Python loads the package first when you do `python3 -m main`), so this is
#    the correct place to install the policy and seed the event loop.
#    uvloop's policy raises RuntimeError on get_event_loop() if no loop is set,
#    unlike the default asyncio policy which auto-creates one — so we explicitly
#    create and set one right after installing the policy.
try:
    import uvloop as _uvloop
    import asyncio as _asyncio
    _uvloop.install()                        # 1. install uvloop policy
    _asyncio.set_event_loop(_uvloop.new_event_loop())  # 2. seed the current loop
    del _uvloop, _asyncio
except ImportError:
    pass

import os

from pyrogram import Client

from telethon.sessions import StringSession
from telethon.sync import TelegramClient

import logging, time, sys
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)

# ── Credentials: env vars first, hardcoded fallback ──────────────────────────
API_ID    = int(os.environ.get("API_ID",    "24058425"))
API_HASH  = os.environ.get("API_HASH",      "694b063e55c24287a3d30aed90191373")
BOT_TOKEN = os.environ.get("BOT_TOKEN",     "8600580531:AAFnpo9I-3e2PH9NnpfEy0KG3i8_zJMLR90")
SESSION   = os.environ.get("SESSION",       "").strip()
FORCESUB  = os.environ.get("FORCESUB",      "forcesubpavo3")
AUTH      = os.environ.get("AUTH",          "7390527029")
DB_CHANNEL= os.environ.get("DB_CHANNEL",    "-1002120403585")
MONGO_URL = os.environ.get("MONGO_URL",     "mongodb+srv://tk22kalal:iwEHHWQn7dG1zjrs@cluster0.xdgbx.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
MONGO_URL = os.environ.get("MONGO_URL",     "mongodb+srv://tk22kalal:iwEHHWQn7dG1zjrs@cluster0.xdgbx.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")



# ── Optional extra bot tokens (BOT_TOKEN2, BOT_TOKEN3, BOT_TOKEN4) ───────────
_EXTRA_TOKENS = [
    os.environ.get("BOT_TOKEN2", "").strip(),
    os.environ.get("BOT_TOKEN3", "").strip(),
    os.environ.get("BOT_TOKEN4", "").strip(),
]

SUDO_USERS = set()
if AUTH.strip():
    SUDO_USERS = {int(x.strip()) for x in AUTH.split()}

# ── Telethon bot (always required) ────────────────────────────────────────────
bot = TelegramClient('bot', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# ── Speed-optimisation parameters applied to every Pyrogram client ────────────
#  sleep_threshold=60         → auto-retry FloodWaits under 60 s internally
#  workers=8                  → 8 async worker coroutines for handling updates
#  max_concurrent_transmissions=15 → 15 parallel chunk up/downloads per file
#                               This is the primary lever for per-file speed:
#                               pyrofork splits each file into 512 KB parts and
#                               sends them in parallel — more parts at once =
#                               faster transfers, especially on premium accounts.
_PYRO_SPEED = dict(
    sleep_threshold=60,
    workers=8,
    max_concurrent_transmissions=15,
)

# ── Pyrogram userbot (optional — shared by ALL bots) ─────────────────────────
userbot = None
if SESSION:
    try:
        userbot = Client(
            "myacc",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=SESSION,
            **_PYRO_SPEED,
        )
        userbot.start()
        print("Global userbot started successfully.")
    except BaseException as e:
        print(f"Warning: Could not start global userbot: {e}")
        print("SESSION env var may be invalid or expired.")
        print("Users can authenticate via /login instead.")
        userbot = None
else:
    print("No SESSION provided — global userbot disabled.")
    print("Users must use /login to access restricted content.")

# ── Pyrogram bot (always required) ────────────────────────────────────────────
Bot = Client(
    "SaveRestricted",
    bot_token=BOT_TOKEN,
    api_id=int(API_ID),
    api_hash=API_HASH,
    **_PYRO_SPEED,
)

try:
    Bot.start()
except Exception as e:
    print(f"Fatal: Could not start Bot client: {e}")
    sys.exit(1)

# ── Extra bots (optional — BOT_TOKEN2 / BOT_TOKEN3 / BOT_TOKEN4) ─────────────
# extra_clients is a list of (TelegramClient, PyrogramClient) tuples.
# All extra bots share the same `userbot` session defined above.
extra_clients = []

for _idx, _token in enumerate(_EXTRA_TOKENS, start=2):
    if not _token:
        continue
    try:
        _tel = TelegramClient(f'bot{_idx}', API_ID, API_HASH).start(bot_token=_token)
        _pyro = Client(
            f"SaveRestricted{_idx}",
            bot_token=_token,
            api_id=int(API_ID),
            api_hash=API_HASH,
            **_PYRO_SPEED,
        )
        _pyro.start()
        extra_clients.append((_tel, _pyro))
        print(f"Extra bot #{_idx} started successfully.")
    except Exception as _e:
        print(f"Warning: Could not start extra bot #{_idx}: {_e}")
        
