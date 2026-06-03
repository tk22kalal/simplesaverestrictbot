"""
Session storage — persists user Pyrogram session strings.

Priority:
  1. MongoDB (Motor async) — used when MONGO_URL env var is set.
     Sessions survive reboots and Heroku redeploys.
  2. Local JSON file fallback — used when MONGO_URL is not set.
     Sessions are lost on filesystem wipe (Heroku redeploys).

Set MONGO_URL in your env/config vars to enable persistent sessions:
  MONGO_URL=mongodb+srv://user:pass@cluster.mongodb.net/mydb
"""
import json
import logging
import os

logger = logging.getLogger(__name__)

SESSIONS_FILE = "user_sessions.json"
MONGO_URL     = os.environ.get("MONGO_URL", "").strip()

# ── MongoDB (Motor) backend ───────────────────────────────────────────────────

_motor_client = None
_mongo_col    = None


def _get_mongo_col():
    global _motor_client, _mongo_col
    if _mongo_col is not None:
        return _mongo_col
    if not MONGO_URL:
        return None
    try:
        import motor.motor_asyncio as _motor
        _motor_client = _motor.AsyncIOMotorClient(MONGO_URL)
        db = _motor_client.get_default_database(default="saverestricted")
        _mongo_col = db["user_sessions"]
        logger.info("session_store: MongoDB backend initialised.")
    except Exception as e:
        logger.warning(f"session_store: MongoDB init failed ({e}); using file fallback.")
        _mongo_col = None
    return _mongo_col


# ── File backend (fallback) ───────────────────────────────────────────────────

def _load_file() -> dict:
    if os.path.exists(SESSIONS_FILE):
        try:
            with open(SESSIONS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_file(data: dict):
    try:
        with open(SESSIONS_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"session_store: could not write {SESSIONS_FILE}: {e}")


# ── Public async API ──────────────────────────────────────────────────────────

async def get_user_session(user_id) -> str | None:
    col = _get_mongo_col()
    if col is not None:
        try:
            doc = await col.find_one({"user_id": str(user_id)})
            return doc["session_string"] if doc else None
        except Exception as e:
            logger.warning(f"session_store: MongoDB read failed ({e}); trying file.")
    return _load_file().get(str(user_id))


async def store_session(user_id, session_string: str):
    col = _get_mongo_col()
    if col is not None:
        try:
            await col.update_one(
                {"user_id": str(user_id)},
                {"$set": {"session_string": session_string}},
                upsert=True,
            )
            logger.info(f"session_store: session saved to MongoDB for user {user_id}.")
            return
        except Exception as e:
            logger.warning(f"session_store: MongoDB write failed ({e}); falling back to file.")
    data = _load_file()
    data[str(user_id)] = session_string
    _save_file(data)


async def remove_session(user_id):
    col = _get_mongo_col()
    if col is not None:
        try:
            await col.delete_one({"user_id": str(user_id)})
            logger.info(f"session_store: session removed from MongoDB for user {user_id}.")
            return
        except Exception as e:
            logger.warning(f"session_store: MongoDB delete failed ({e}); falling back to file.")
    data = _load_file()
    data.pop(str(user_id), None)
    _save_file(data)


# ── Sync shim (used by batch.py _get_user_session which is called synchronously) ──

def get_user_session_sync(user_id) -> str | None:
    """
    Synchronous fallback — always reads from the file.
    Used by batch.py's _get_user_session() which is not async.
    MongoDB-stored sessions are NOT visible here; prefer the async API.
    """
    return _load_file().get(str(user_id))
