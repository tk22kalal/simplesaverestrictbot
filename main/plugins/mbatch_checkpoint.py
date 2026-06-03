"""
mbatch_checkpoint.py — MongoDB checkpoint for /batch multi-topic sequential processing.

Collection: mbatch_sessions

Schema per session:
  session_id         : str (uuid)
  user_id            : int
  bot_key            : str  ← numeric bot-ID from BOT_TOKEN (unique per Heroku deployment)
  chat_ref           : int | str
  topics             : list of {name, start_topic, start_msg, end_topic, end_msg,
                                status, file_ids, saved_count}
  current_topic_idx  : int  (0-based, currently processing)
  last_saved_msg_id  : int | None  (last source msg saved within current topic)
  total_saved        : int
  status             : "in_progress" | "done" | "cancelled"
  created_at, updated_at

bot_key allows multiple independent bots (each with a different BOT_TOKEN but sharing
the same MongoDB database) to store sessions without conflicting with each other.
"""

import os
import logging
import uuid
from datetime import datetime

logger = logging.getLogger(__name__)

try:
    from main import MONGO_URL as _MONGO_URL_MAIN
    MONGO_URL = str(_MONGO_URL_MAIN).strip() if _MONGO_URL_MAIN else ""
except Exception:
    MONGO_URL = os.environ.get("MONGO_URL", "").strip()

COLLECTION = "mbatch_sessions"

_motor_client = None
_col          = None


def _get_col():
    global _motor_client, _col
    if _col is not None:
        return _col
    if not MONGO_URL:
        logger.warning("mbatch_checkpoint: MONGO_URL not set — checkpointing disabled.")
        return None
    try:
        import motor.motor_asyncio as _motor
        _motor_client = _motor.AsyncIOMotorClient(MONGO_URL)
        db   = _motor_client.get_default_database(default="saverestricted")
        _col = db[COLLECTION]
        logger.info("mbatch_checkpoint: MongoDB connected.")
    except Exception as e:
        logger.warning(f"mbatch_checkpoint: MongoDB init failed: {e}")
        _col = None
    return _col


async def create_indexes():
    col = _get_col()
    if col is None:
        return
    try:
        await col.create_index("user_id")
        await col.create_index("status")
        await col.create_index("bot_key")
        await col.create_index([("session_id", 1)], unique=True)
    except Exception as e:
        logger.warning(f"mbatch_checkpoint: index creation failed: {e}")


async def create_session(user_id: int, chat_ref, topics: list, bot_key: str = "") -> "str | None":
    """
    topics: list of dicts with keys:
        name, start_topic, start_msg, end_topic, end_msg
    Returns session_id or None if MongoDB unavailable.
    bot_key is the numeric bot ID (first part of BOT_TOKEN) that scopes sessions
    so multiple bots sharing one MongoDB don't see each other's data.
    """
    col = _get_col()
    if col is None:
        return None

    session_id = str(uuid.uuid4())
    now = datetime.utcnow()

    topics_doc = []
    for t in topics:
        topics_doc.append({
            "name":         t["name"],
            "start_topic":  t.get("start_topic"),
            "start_msg":    t["start_msg"],
            "end_topic":    t.get("end_topic"),
            "end_msg":      t["end_msg"],
            "status":       "pending",
            "file_ids":     [],
            "saved_count":  0,
        })

    doc = {
        "session_id":        session_id,
        "user_id":           user_id,
        "bot_key":           bot_key,
        "chat_ref":          int(chat_ref) if isinstance(chat_ref, int) else str(chat_ref),
        "topics":            topics_doc,
        "current_topic_idx": 0,
        "last_saved_msg_id": None,
        "total_saved":       0,
        "status":            "in_progress",
        "created_at":        now,
        "updated_at":        now,
    }
    try:
        await col.insert_one(doc)
        logger.info(f"mbatch_checkpoint: session {session_id} created for user {user_id} bot {bot_key}.")
        return session_id
    except Exception as e:
        logger.warning(f"mbatch_checkpoint: create_session failed: {e}")
        return None


async def mark_topic_running(session_id: str, topic_idx: int):
    """Mark a topic as in_progress."""
    col = _get_col()
    if col is None:
        return
    try:
        await col.update_one(
            {"session_id": session_id},
            {"$set": {
                f"topics.{topic_idx}.status": "in_progress",
                "current_topic_idx":           topic_idx,
                "updated_at":                  datetime.utcnow(),
            }},
        )
    except Exception as e:
        logger.warning(f"mbatch_checkpoint: mark_topic_running failed: {e}")


async def checkpoint_message(session_id: str, topic_idx: int, source_msg_id: int):
    """
    Called after each successful upload.
    Persists current position for crash-safe resume.
    """
    col = _get_col()
    if col is None:
        return
    try:
        await col.update_one(
            {"session_id": session_id},
            {
                "$set": {
                    "current_topic_idx":              topic_idx,
                    "last_saved_msg_id":              source_msg_id,
                    f"topics.{topic_idx}.status":     "in_progress",
                    "updated_at":                     datetime.utcnow(),
                },
                "$inc": {
                    "total_saved":                       1,
                    f"topics.{topic_idx}.saved_count":   1,
                },
            },
        )
    except Exception as e:
        logger.warning(f"mbatch_checkpoint: checkpoint_message failed: {e}")


async def mark_topic_done(session_id: str, topic_idx: int, file_ids: list):
    """
    Mark a topic as done and save its file_ids (for forwarding).
    Also resets last_saved_msg_id (next topic starts fresh).
    """
    col = _get_col()
    if col is None:
        return
    try:
        await col.update_one(
            {"session_id": session_id},
            {"$set": {
                f"topics.{topic_idx}.status":    "done",
                f"topics.{topic_idx}.file_ids":  file_ids,
                "last_saved_msg_id":             None,
                "updated_at":                    datetime.utcnow(),
            }},
        )
    except Exception as e:
        logger.warning(f"mbatch_checkpoint: mark_topic_done failed: {e}")


async def advance_topic(session_id: str, next_idx: int):
    """Move the session pointer to the next topic."""
    col = _get_col()
    if col is None:
        return
    try:
        await col.update_one(
            {"session_id": session_id},
            {"$set": {
                "current_topic_idx": next_idx,
                "last_saved_msg_id": None,
                "updated_at":        datetime.utcnow(),
            }},
        )
    except Exception as e:
        logger.warning(f"mbatch_checkpoint: advance_topic failed: {e}")


async def mark_done(session_id: str):
    col = _get_col()
    if col is None:
        return
    try:
        await col.update_one(
            {"session_id": session_id},
            {"$set": {"status": "done", "updated_at": datetime.utcnow()}},
        )
    except Exception as e:
        logger.warning(f"mbatch_checkpoint: mark_done failed: {e}")


async def cancel_session(session_id: str):
    col = _get_col()
    if col is None:
        return
    try:
        await col.update_one(
            {"session_id": session_id},
            {"$set": {"status": "cancelled", "updated_at": datetime.utcnow()}},
        )
    except Exception as e:
        logger.warning(f"mbatch_checkpoint: cancel_session failed: {e}")


async def get_session(session_id: str) -> "dict | None":
    col = _get_col()
    if col is None:
        return None
    try:
        return await col.find_one({"session_id": session_id})
    except Exception as e:
        logger.warning(f"mbatch_checkpoint: get_session failed: {e}")
        return None


async def get_pending_sessions(user_id: int, bot_key: str = "") -> list:
    col = _get_col()
    if col is None:
        return []
    try:
        cursor = col.find(
            {"user_id": user_id, "bot_key": bot_key, "status": "in_progress"},
            sort=[("created_at", -1)],
        )
        return await cursor.to_list(length=10)
    except Exception as e:
        logger.warning(f"mbatch_checkpoint: get_pending_sessions failed: {e}")
        return []


async def has_pending_session(user_id: int, bot_key: str = "") -> bool:
    col = _get_col()
    if col is None:
        return False
    try:
        doc = await col.find_one(
            {"user_id": user_id, "bot_key": bot_key, "status": "in_progress"}
        )
        return doc is not None
    except Exception as e:
        logger.warning(f"mbatch_checkpoint: has_pending_session failed: {e}")
        return False


async def get_recent_sessions(user_id: int, hours: int = 24, bot_key: str = "") -> list:
    """
    Return all sessions updated within the last N hours, any status.
    Scoped to the given bot_key so bots don't see each other's history.
    """
    col = _get_col()
    if col is None:
        return []
    try:
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        cursor = col.find(
            {"user_id": user_id, "bot_key": bot_key, "updated_at": {"$gte": cutoff}},
            sort=[("updated_at", -1)],
        )
        return await cursor.to_list(length=10)
    except Exception as e:
        logger.warning(f"mbatch_checkpoint: get_recent_sessions failed: {e}")
        return []


async def get_latest_session(user_id: int, bot_key: str = "") -> "dict | None":
    """Return the single most-recently-updated session for this user+bot (any status)."""
    col = _get_col()
    if col is None:
        return None
    try:
        return await col.find_one(
            {"user_id": user_id, "bot_key": bot_key},
            sort=[("updated_at", -1)],
        )
    except Exception as e:
        logger.warning(f"mbatch_checkpoint: get_latest_session failed: {e}")
        return None


async def delete_old_sessions(user_id: int, bot_key: str = "") -> int:
    """
    Delete all completed/cancelled sessions for this user+bot.
    In-progress sessions are preserved so resume still works.
    Called automatically before each new /batch to keep history clean.
    Returns the number of documents deleted.
    """
    col = _get_col()
    if col is None:
        return 0
    try:
        result = await col.delete_many(
            {"user_id": user_id, "bot_key": bot_key,
             "status": {"$in": ["done", "cancelled"]}}
        )
        deleted = result.deleted_count
        if deleted:
            logger.info(
                f"mbatch_checkpoint: deleted {deleted} old session(s) "
                f"for user {user_id} bot {bot_key}."
            )
        return deleted
    except Exception as e:
        logger.warning(f"mbatch_checkpoint: delete_old_sessions failed: {e}")
        return 0
