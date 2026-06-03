"""
sbatch_checkpoint.py — MongoDB-backed checkpoint system for /sbatch.

Uses the same MONGO_URL env var as session_store.py.
Collection: sbatch_sessions

On each successful file save, checkpoint_message() is called with the
source message ID and topic ID.  On resume, each bot restarts from
(last_saved_topic, last_saved_msg_id+1), guaranteeing no duplicates
and no re-processing of already-completed topics.
"""

import os
import logging
import uuid
from datetime import datetime

logger = logging.getLogger(__name__)

MONGO_URL  = os.environ.get("MONGO_URL", "").strip()
COLLECTION = "sbatch_sessions"

_motor_client = None
_col          = None


def _get_col():
    global _motor_client, _col
    if _col is not None:
        return _col
    if not MONGO_URL:
        logger.warning("sbatch_checkpoint: MONGO_URL not set — checkpointing disabled.")
        return None
    try:
        import motor.motor_asyncio as _motor
        _motor_client = _motor.AsyncIOMotorClient(MONGO_URL)
        db   = _motor_client.get_default_database(default="saverestricted")
        _col = db[COLLECTION]
        logger.info("sbatch_checkpoint: MongoDB connected.")
    except Exception as e:
        logger.warning(f"sbatch_checkpoint: MongoDB init failed: {e}")
        _col = None
    return _col


# ── Indexes ────────────────────────────────────────────────────────────────────

async def create_indexes():
    col = _get_col()
    if col is None:
        return
    try:
        await col.create_index("user_id")
        await col.create_index("status")
        await col.create_index([("session_id", 1)], unique=True)
    except Exception as e:
        logger.warning(f"sbatch_checkpoint: index creation failed: {e}")


# ── Session lifecycle ──────────────────────────────────────────────────────────

async def create_session(user_id: int, chat_id: int, original_link: str,
                          chat_ref, chunks: list, db_channel) -> "str | None":
    """
    Create a new sbatch session document.

    chunks: list of dicts with keys:
        bot_index, bot_user_id, start_topic, start_msg, end_topic, end_msg

    chat_ref is stored at session level (all bots work the same source chat).
    Returns session_id string, or None if MongoDB is unavailable.
    """
    col = _get_col()
    if col is None:
        return None

    session_id = str(uuid.uuid4())
    now = datetime.utcnow()
    doc = {
        "session_id":    session_id,
        "user_id":       user_id,
        "chat_id":       chat_id,
        "original_link": original_link,
        # Store chat_ref preserving its type (int for private, str for username)
        "chat_ref":      int(chat_ref) if isinstance(chat_ref, int) else str(chat_ref),
        "created_at":    now,
        "updated_at":    now,
        "status":        "in_progress",
        "chunks":        chunks,
        "progress": [
            {
                "bot_index":          c["bot_index"],
                "last_saved_msg_id":  None,   # None = no messages saved yet
                "last_saved_topic":   None,   # None = no topic saved yet
                "forwarded_count":    0,
                "status":             "pending",
            }
            for c in chunks
        ],
        "total_forwarded": 0,
        "db_channel":      str(db_channel) if db_channel else None,
    }
    try:
        await col.insert_one(doc)
        logger.info(f"sbatch_checkpoint: session {session_id} created for user {user_id}.")
        return session_id
    except Exception as e:
        logger.warning(f"sbatch_checkpoint: create_session failed: {e}")
        return None


async def mark_bot_running(session_id: str, bot_index: int):
    """Mark a bot's progress entry as 'running'."""
    col = _get_col()
    if col is None:
        return
    try:
        await col.update_one(
            {"session_id": session_id, "progress.bot_index": bot_index},
            {"$set": {
                "progress.$.status": "running",
                "updated_at": datetime.utcnow(),
            }},
        )
    except Exception as e:
        logger.warning(f"sbatch_checkpoint: mark_bot_running failed: {e}")


async def checkpoint_message(session_id: str, bot_index: int,
                              source_msg_id: int, source_topic_id=None):
    """
    Called after EACH message is successfully saved/uploaded.
    Updates last_saved_msg_id, last_saved_topic, and increments
    forwarded_count / total_forwarded.

    CRITICAL: only call this AFTER the upload is confirmed successful.
    Do NOT pre-write expected IDs — this guarantees crash-safe resume.

    source_topic_id — the topic this message belongs to (None for plain channels).
    This is stored as last_saved_topic so resume knows which topic to continue from.
    """
    col = _get_col()
    if col is None:
        return
    try:
        set_fields = {
            "progress.$.last_saved_msg_id": source_msg_id,
            "progress.$.status": "running",
            "updated_at": datetime.utcnow(),
        }
        if source_topic_id is not None:
            set_fields["progress.$.last_saved_topic"] = source_topic_id

        await col.update_one(
            {"session_id": session_id, "progress.bot_index": bot_index},
            {
                "$set": set_fields,
                "$inc": {
                    "progress.$.forwarded_count": 1,
                    "total_forwarded": 1,
                },
            },
        )
    except Exception as e:
        logger.warning(f"sbatch_checkpoint: checkpoint_message failed: {e}")


async def mark_bot_done(session_id: str, bot_index: int):
    """Mark one bot's chunk as fully complete."""
    col = _get_col()
    if col is None:
        return
    try:
        await col.update_one(
            {"session_id": session_id, "progress.bot_index": bot_index},
            {"$set": {
                "progress.$.status": "done",
                "updated_at": datetime.utcnow(),
            }},
        )
    except Exception as e:
        logger.warning(f"sbatch_checkpoint: mark_bot_done failed: {e}")


async def mark_bot_failed(session_id: str, bot_index: int):
    """Mark one bot's chunk as failed."""
    col = _get_col()
    if col is None:
        return
    try:
        await col.update_one(
            {"session_id": session_id, "progress.bot_index": bot_index},
            {"$set": {
                "progress.$.status": "failed",
                "updated_at": datetime.utcnow(),
            }},
        )
    except Exception as e:
        logger.warning(f"sbatch_checkpoint: mark_bot_failed failed: {e}")


async def mark_session_done(session_id: str):
    """Mark the whole session as complete."""
    col = _get_col()
    if col is None:
        return
    try:
        await col.update_one(
            {"session_id": session_id},
            {"$set": {"status": "done", "updated_at": datetime.utcnow()}},
        )
    except Exception as e:
        logger.warning(f"sbatch_checkpoint: mark_session_done failed: {e}")


async def cancel_session(session_id: str):
    """Mark the session as cancelled (by user request)."""
    col = _get_col()
    if col is None:
        return
    try:
        await col.update_one(
            {"session_id": session_id},
            {"$set": {"status": "cancelled", "updated_at": datetime.utcnow()}},
        )
    except Exception as e:
        logger.warning(f"sbatch_checkpoint: cancel_session failed: {e}")


# ── Queries ────────────────────────────────────────────────────────────────────

async def get_session(session_id: str) -> "dict | None":
    col = _get_col()
    if col is None:
        return None
    try:
        return await col.find_one({"session_id": session_id})
    except Exception as e:
        logger.warning(f"sbatch_checkpoint: get_session failed: {e}")
        return None


async def get_pending_sessions(user_id: int) -> list:
    """Return all in_progress sessions for a user, newest first."""
    col = _get_col()
    if col is None:
        return []
    try:
        cursor = col.find(
            {"user_id": user_id, "status": "in_progress"},
            sort=[("created_at", -1)],
        )
        return await cursor.to_list(length=20)
    except Exception as e:
        logger.warning(f"sbatch_checkpoint: get_pending_sessions failed: {e}")
        return []


async def get_all_pending_sessions() -> list:
    """Return ALL in_progress sessions (used at startup to notify users)."""
    col = _get_col()
    if col is None:
        return []
    try:
        cursor = col.find({"status": "in_progress"})
        return await cursor.to_list(length=200)
    except Exception as e:
        logger.warning(f"sbatch_checkpoint: get_all_pending_sessions failed: {e}")
        return []


async def has_pending_session(user_id: int) -> bool:
    """True if the user has an active in_progress session (concurrency guard)."""
    col = _get_col()
    if col is None:
        return False   # no MongoDB → no guard (safe to proceed)
    try:
        doc = await col.find_one({"user_id": user_id, "status": "in_progress"})
        return doc is not None
    except Exception as e:
        logger.warning(f"sbatch_checkpoint: has_pending_session failed: {e}")
        return False
