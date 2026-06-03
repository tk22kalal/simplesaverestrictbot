"""
fbatch.py — /fbatch: Forum Batch Topic Scanner

Flow (mirrors /batch UX):
  1. User sends /fbatch
  2. Bot asks for the start–end link range
  3. Bot scans every message ID in that range in batches of 100
  4. Groups messages by forum topic, finds first & last msg per topic
  5. Sends a .txt file with all active topics and their link ranges
"""

import asyncio
import logging
import os
import tempfile

from pyrogram import Client
from telethon import events, Button

from .. import bot as gagan, userbot, Bot, API_ID, API_HASH
from main.plugins.batch import _parse_range, _get_user_session

logger = logging.getLogger(__name__)

_CHUNK     = 100    # IDs per get_messages() call
_DELAY     = 0.35   # seconds between chunks (flood-safe)
_NAME_DELAY= 0.2    # seconds between topic-name fetches


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _raw_chat(chat_ref) -> str:
    """Convert -1002932205861 → '2932205861' for URL building."""
    if isinstance(chat_ref, int):
        return str(abs(chat_ref))[3:]
    return str(chat_ref)


def _msg_url(chat_ref, topic_id: int, msg_id: int) -> str:
    rc = _raw_chat(chat_ref)
    if isinstance(chat_ref, int):
        return f"https://t.me/c/{rc}/{topic_id}/{msg_id}"
    return f"https://t.me/{rc}/{topic_id}/{msg_id}"


def _topic_url(chat_ref, topic_id: int) -> str:
    rc = _raw_chat(chat_ref)
    if isinstance(chat_ref, int):
        return f"https://t.me/c/{rc}/{topic_id}"
    return f"https://t.me/{rc}/{topic_id}"


def _get_topic_id(msg) -> "int | None":
    """
    Extract the forum-topic ID from a Pyrogram Message.
    Tries every known attribute path across pyrogram / pyrofork builds.
    """
    # 1. Message IS the topic header (service message)
    if getattr(msg, "forum_topic_created", None) is not None:
        return msg.id

    # 2. Standard pyrogram / pyrofork field
    tid = getattr(msg, "reply_to_top_message_id", None)
    if tid:
        return int(tid)

    # 3. Alternate alias
    tid = getattr(msg, "message_thread_id", None)
    if tid:
        return int(tid)

    # 4. Dig into pyrogram internals — raw reply_to header
    raw = getattr(msg, "_raw", None)
    if raw is None:
        # pyrofork stores raw as ._wrapped or directly on the object
        raw = msg
    reply_to = getattr(raw, "reply_to", None)
    if reply_to:
        tid = getattr(reply_to, "reply_to_top_id", None)
        if tid:
            return int(tid)
        # direct reply to topic root (depth-1 messages use reply_to_msg_id)
        tid = getattr(reply_to, "reply_to_msg_id", None)
        if tid:
            return int(tid)

    return None


# ─── Core scan ────────────────────────────────────────────────────────────────

async def _scan(acc, chat_ref, scan_start: int, scan_end: int,
                status_msg) -> "dict[int, dict]":
    """
    Fetch every message ID in [scan_start, scan_end] in chunks of _CHUNK.
    Returns { topic_id: {'min': int, 'max': int, 'name': None} }
    """
    topics: "dict[int, dict]" = {}
    total    = scan_end - scan_start + 1
    done     = 0
    last_upd = -10   # force first update

    for chunk_start in range(scan_start, scan_end + 1, _CHUNK):
        ids = list(range(chunk_start,
                         min(chunk_start + _CHUNK, scan_end + 1)))
        try:
            result = await acc.get_messages(chat_ref, ids)
            msgs = result if isinstance(result, list) else [result]
        except Exception as e:
            logger.warning(f"fbatch get_messages error: {e}")
            await asyncio.sleep(1.5)
            continue

        for msg in msgs:
            if msg is None or getattr(msg, "empty", True):
                continue
            # ── FIX: skip topic header service messages ──────────────────────
            if getattr(msg, "forum_topic_created", None) is not None:
                continue
            tid = _get_topic_id(msg)
            if tid is None:
                continue
            mid = msg.id
            if tid not in topics:
                topics[tid] = {"min": mid, "max": mid, "name": None}
            else:
                if mid < topics[tid]["min"]:
                    topics[tid]["min"] = mid
                if mid > topics[tid]["max"]:
                    topics[tid]["max"] = mid

        done += len(ids)
        pct = done * 100 // total
        if pct >= last_upd + 10:
            last_upd = pct
            try:
                await status_msg.edit(
                    f"🔍 Scanning… {pct}% ({done}/{total} IDs)\n"
                    f"Topics found so far: **{len(topics)}**"
                )
            except Exception:
                pass

        await asyncio.sleep(_DELAY)

    return topics


async def _fetch_names(acc, chat_ref, topics: dict) -> None:
    """Fill topics[tid]['name'] by fetching each topic's header message."""
    for tid in list(topics.keys()):
        try:
            msg = await acc.get_messages(chat_ref, tid)
            if msg and not getattr(msg, "empty", True):
                ftc = getattr(msg, "forum_topic_created", None)
                if ftc:
                    topics[tid]["name"] = (
                        getattr(ftc, "name", None)
                        or getattr(ftc, "title", None)
                    )
        except Exception:
            pass
        await asyncio.sleep(_NAME_DELAY)


# ─── /fbatch command ──────────────────────────────────────────────────────────

@gagan.on(events.NewMessage(incoming=True, pattern=r'^/fbatch(?:\s|$|@)'))
async def fbatch_command(event):
    uid = event.sender_id

    # ── Step 1: ask for range link ────────────────────────────────────────────
    raw_range = None
    async with gagan.conversation(event.chat_id, timeout=120) as conv:
        try:
            await conv.send_message(
                "📋 **Forum Topic Scanner**\n\n"
                "Send the **start–end link range** to scan:\n\n"
                "**Format:** `START_LINK-END_LINK`\n\n"
                "**Example:**\n"
                "`https://t.me/c/2932205861/116/117"
                "-https://t.me/c/2932205861/1040/1642`",
                buttons=Button.force_reply()
            )
            reply = await conv.get_reply()
            raw_range = reply.text.strip() if reply and reply.text else ""
        except asyncio.TimeoutError:
            await event.respond("⏳ Timed out. Send /fbatch to try again.")
            return
        except Exception as e:
            logger.warning(f"fbatch conv error: {e}")
            return

    if not raw_range:
        await event.respond("❌ No link received. Send /fbatch to try again.")
        return

    # ── Step 2: parse ─────────────────────────────────────────────────────────
    parsed = _parse_range(raw_range)
    if not parsed:
        await event.respond(
            "❌ Could not parse that link.\n\n"
            "Use the format:\n"
            "`https://t.me/c/CHATID/TOPIC/MSGID"
            "-https://t.me/c/CHATID/TOPIC2/MSGID2`"
        )
        return

    chat_ref, _st, scan_start, _et, scan_end = parsed

    if scan_end < scan_start:
        await event.respond("❌ End message ID must be ≥ start message ID.")
        return

    # ── Step 3: get user session ──────────────────────────────────────────────
    acc = userbot
    personal_acc = None

    if acc is None:
        sess = await _get_user_session(uid)
        if sess:
            try:
                personal_acc = Client(
                    f"fbatch_{uid}",
                    session_string=sess,
                    api_id=int(API_ID),
                    api_hash=API_HASH,
                    in_memory=True,
                )
                await personal_acc.start()
                acc = personal_acc
            except Exception as e:
                await event.respond(f"⚠️ Could not start your session: `{e}`")
                return

    if acc is None:
        await event.respond(
            "❌ **No user session available.**\n\n"
            "A Telegram user account is required to read private channels.\n"
            "Use /login to authenticate, then try /fbatch again."
        )
        return

    # ── Step 4: scan ──────────────────────────────────────────────────────────
    total_ids = scan_end - scan_start + 1
    status = await event.respond(
        f"🔍 **Forum Topic Scanner** — started\n\n"
        f"Chat : `{chat_ref}`\n"
        f"Range: `{scan_start}` → `{scan_end}` ({total_ids} IDs)\n\n"
        f"⏳ Scanning…"
    )

    try:
        topics = await _scan(acc, chat_ref, scan_start, scan_end, status)
    except Exception as e:
        logger.error(f"fbatch scan: {e}")
        await status.edit(f"❌ Scan failed: `{e}`")
        return

    if not topics:
        await status.edit(
            f"⚠️ **No forum topics found** in range `{scan_start}` → `{scan_end}`.\n\n"
            "• Make sure the account can read this group.\n"
            "• Confirm it is a forum supergroup (Topics enabled).\n"
            "• All messages in range may be deleted."
        )
        if personal_acc:
            try: await personal_acc.stop()
            except Exception: pass
        return

    # ── Step 5: fetch topic names ─────────────────────────────────────────────
    try:
        await status.edit(f"✅ Found **{len(topics)}** topic(s) — fetching names…")
        await _fetch_names(acc, chat_ref, topics)
    except Exception as e:
        logger.warning(f"fbatch names: {e}")
    finally:
        if personal_acc:
            try: await personal_acc.stop()
            except Exception: pass

    # ── Step 6: build .txt output ─────────────────────────────────────────────
    lines = []
    lines.append(
        f"Forum Topic Scan Results\n"
        f"Chat     : {chat_ref}\n"
        f"Range    : {scan_start} → {scan_end}  ({total_ids} IDs scanned)\n"
        f"Topics   : {len(topics)} active\n"
        f"{'=' * 55}\n"
    )

    for tid in sorted(topics.keys()):
        info      = topics[tid]
        name      = info["name"] or f"Topic {tid}"
        first_mid = info["min"]
        last_mid  = info["max"]

        t_link     = _topic_url(chat_ref, tid)
        first_link = _msg_url(chat_ref, tid, first_mid)
        last_link  = _msg_url(chat_ref, tid, last_mid)

        lines.append(
            f"Topic  : {name}\n"
            f"ID     : {tid}\n"
            f"URL    : {t_link}\n"
            f"First  : {first_link}\n"
            f"Last   : {last_link}\n"
            f"{'-' * 55}"
        )

    txt_content = "\n".join(lines)

    # ── Step 7: send the file ─────────────────────────────────────────────────
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(suffix=".txt",
                                        prefix=f"fbatch_{uid}_")
        os.close(fd)
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(txt_content)

        caption = (
            f"✅ **{len(topics)} topics** found in range "
            f"`{scan_start}` → `{scan_end}`"
        )
        await gagan.send_file(uid, tmp_path, caption=caption)
    except Exception as e:
        logger.error(f"fbatch send_file: {e}")
        await event.respond(f"❌ Could not send result file: `{e}`")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except Exception: pass

    try:
        await status.delete()
    except Exception:
        pass

    # ── Step 8: send 4 copiable group messages ────────────────────────────────
    # Sort topics by first message ID (chronological order in the channel)
    sorted_topics = sorted(topics.items(), key=lambda kv: kv[1]["min"])
    n_total = len(sorted_topics)

    if n_total == 0:
        return

    # Divide into 4 groups (ceiling division so early groups are slightly larger)
    n_groups   = 4
    group_size = max(1, (n_total + n_groups - 1) // n_groups)
    groups     = []
    for i in range(0, n_total, group_size):
        grp = sorted_topics[i : i + group_size]
        if grp:
            groups.append(grp)

    for g_idx, group in enumerate(groups, 1):
        lines = [f"### Group {g_idx} ({len(group)} Topics)\n"]
        for tid, info in group:
            name       = info["name"] or f"Topic {tid}"
            first_link = _msg_url(chat_ref, tid, info["min"])
            last_link  = _msg_url(chat_ref, tid, info["max"])
            lines.append(f"{name}:- {first_link}-{last_link}")

        group_text = "\n".join(lines)
        try:
            await gagan.send_message(uid, group_text)
        except Exception as eg:
            logger.warning(f"fbatch: could not send group {g_idx} message: {eg}")
