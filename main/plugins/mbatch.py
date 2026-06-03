"""
mbatch.py — New /batch: Multi-topic sequential batch extraction.

Flow:
  1. /batch  → prompt user to paste multi-line topic links
  2. Parse each line: "TopicName:- start_link-last_link"
  3. Save session to MongoDB (mbatch_sessions collection)
  4. Process topics one by one sequentially
  5. After each file: checkpoint to MongoDB (crash-safe resume)
  6. After each topic: record file_ids for forwarding
  7. On completion: send summary + "▶ Forward All" button

Resume:
  /batch_status  → shows pending sessions with Resume / Cancel buttons

Forward:
  "▶ Forward All" button → copies all saved files to DB_CHANNEL in topic order

Cancel:
  /bcancel  → cancels the currently running batch for this user
"""

import asyncio
import logging
import re

from pyrogram import filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from telethon import events, Button

from .. import API_ID, API_HASH, Bot, bot as gagan, userbot, BOT_KEY
from .. import DB_CHANNEL as _DB_CHANNEL_RAW
from main.plugins.batch import _parse_range, _get_user_session, _run_batch
from main.plugins.mbatch_checkpoint import (
    create_indexes        as _cp_create_indexes,
    create_session        as _cp_create,
    mark_topic_running    as _cp_topic_running,
    checkpoint_message    as _cp_message,
    mark_topic_done       as _cp_topic_done,
    advance_topic         as _cp_advance,
    mark_done             as _cp_mark_done,
    cancel_session        as _cp_cancel,
    get_session           as _cp_get,
    get_pending_sessions  as _cp_pending,
    has_pending_session   as _cp_has_pending,
    get_recent_sessions   as _cp_recent,
    get_latest_session    as _cp_latest,
    delete_old_sessions   as _cp_delete_old,
)

logger = logging.getLogger(__name__)

_db_str    = str(_DB_CHANNEL_RAW).strip() if _DB_CHANNEL_RAW else ""
DB_CHANNEL = (
    int(_db_str) if _db_str.lstrip("-").isdigit()
    else (_db_str or None)
)

_active_mbatches: "dict[int, dict]" = {}

# Holds strong references to background resume tasks so the GC can't
# collect them mid-execution (asyncio only keeps weak refs to tasks).
_resume_tasks: "set[asyncio.Task]" = set()


# ── Helpers ────────────────────────────────────────────────────────────────────

def _db_channel_link_base(db_channel) -> str:
    """Return the https://t.me/c/<id>/ prefix for a DB_CHANNEL integer."""
    s = str(db_channel)
    if s.startswith('-100'):
        return f"https://t.me/c/{s[4:]}/"
    return f"https://t.me/c/{s.lstrip('-')}/"


async def _forward_topics_and_summarize(uid, client, per_topic_result, db_channel, progress_msg=None):
    """
    Forward files topic-by-topic to db_channel.

    per_topic_result: list of {"name": str, "file_ids": [int, ...]}

    Returns (fwd_ok, errors, summary_text).
    summary_text is the multi-line F/L report (ready to copy-paste), or None.
    """
    base        = _db_channel_link_base(db_channel)
    total_files = sum(len(t.get("file_ids", [])) for t in per_topic_result)

    fwd_ok        = 0
    errors        = []
    topic_summary = []   # [{name, first_id, last_id}]
    done_so_far   = 0

    for t in per_topic_result:
        t_name   = t.get("name", "Topic")
        file_ids = t.get("file_ids", [])
        if not file_ids:
            continue

        first_db_id = None
        last_db_id  = None

        for msg_id in file_ids:
            try:
                sent = await client.copy_message(
                    chat_id=db_channel,
                    from_chat_id=uid,
                    message_id=msg_id,
                )
                if sent and sent.id:
                    if first_db_id is None:
                        first_db_id = sent.id
                    last_db_id = sent.id
                fwd_ok += 1
            except Exception as e:
                logger.error(f"forward: copy msg {msg_id}: {e}")
                errors.append(f"msg {msg_id}: {str(e)[:80]}")

            done_so_far += 1
            if progress_msg and total_files and (
                done_so_far % 10 == 0 or done_so_far == total_files
            ):
                filled = int(10 * done_so_far / total_files)
                bar    = "█" * filled + "░" * (10 - filled)
                pct    = int(100 * done_so_far / total_files)
                try:
                    await progress_msg.edit_text(
                        f"📤 <b>Forwarding…</b>\n\n"
                        f"[{bar}] {pct}%  {done_so_far}/{total_files}",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass

            await asyncio.sleep(0.3)

        if first_db_id and last_db_id:
            topic_summary.append({
                "name":     t_name,
                "first_id": first_db_id,
                "last_id":  last_db_id,
            })

    # Build the F/L summary text
    summary_text = None
    if topic_summary:
        lines = []
        for entry in topic_summary:
            lines.append(entry["name"].upper())
            lines.append(f"F - {base}{entry['first_id']}")
            lines.append(f"L - {base}{entry['last_id']}")
            lines.append("")
        summary_text = "\n".join(lines).strip()

    return fwd_ok, errors, summary_text


# ── Input parser ──────────────────────────────────────────────────────────────

def _parse_topic_lines(text: str) -> "list[dict] | None":
    """
    Parse multi-line input of the form:
        TopicName:- START_LINK-END_LINK
        TopicName:- START_LINK-END_LINK
        ...

    Returns list of topic dicts, or None on total parse failure.
    Each dict: {name, chat_ref, start_topic, start_msg, end_topic, end_msg}
    """
    results = []
    chat_ref_global = None

    for raw_line in text.strip().splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if ":-" not in line:
            continue
        name_part, range_part = line.split(":-", 1)
        name       = name_part.strip()
        range_part = range_part.strip()

        parsed = _parse_range(range_part)
        if not parsed:
            logger.warning(f"mbatch: could not parse line: {raw_line!r}")
            continue

        chat_ref, start_topic, start_msg, end_topic, end_msg = parsed

        if chat_ref_global is None:
            chat_ref_global = chat_ref
        elif chat_ref_global != chat_ref:
            logger.warning(f"mbatch: mixed chat_refs in input — {chat_ref_global} vs {chat_ref}")

        results.append({
            "name":        name,
            "chat_ref":    int(chat_ref) if isinstance(chat_ref, int) else str(chat_ref),
            "start_topic": start_topic,
            "start_msg":   start_msg,
            "end_topic":   end_topic,
            "end_msg":     end_msg,
        })

    return results if results else None


# ── Core executor ─────────────────────────────────────────────────────────────

async def _execute_mbatch(
    acc,
    uid: int,
    chat_ref,
    topics: list,
    session_id: "str | None",
    start_idx: int = 0,
    resume_msg_id: "int | None" = None,
    client=None,
) -> "tuple[int, list, bool]":
    """
    Process topics sequentially from start_idx.
    resume_msg_id: if set, skip msgs <= this ID in the first topic.
    client: Pyrogram bot to use for sending messages (defaults to main Bot).

    Returns (total_saved, all_file_ids, was_cancelled).
    """
    _client = client if client is not None else Bot

    total_saved       = 0
    per_topic_result  = []   # [{"name": str, "file_ids": [int, ...]}]
    cancelled         = False
    _total_file_count = [0]  # cumulative upload count — used for periodic cooldown

    for idx in range(start_idx, len(topics)):
        t = topics[idx]

        effective_start = t["start_msg"]
        if idx == start_idx and resume_msg_id is not None:
            effective_start = resume_msg_id + 1
            if effective_start > t["end_msg"]:
                logger.info(f"mbatch: topic {idx} ({t['name']}) already done — skipping.")
                continue

        if session_id:
            await _cp_topic_running(session_id, idx)

        topic_bdict    = {}
        _active_mbatches[uid] = topic_bdict
        topic_file_ids = []

        async def _cpfn(source_mid, source_topic=None, _idx=idx):
            if session_id:
                await _cp_message(session_id, _idx, source_mid)
            _total_file_count[0] += 1
            # Periodic cooldown: pause 25s every 50 uploaded files to avoid FloodWait
            if _total_file_count[0] % 50 == 0:
                logger.info(f"mbatch: 50-file cooldown pause (total #{_total_file_count[0]})")
                await asyncio.sleep(25)

        try:
            status_msg = await _client.send_message(
                uid,
                f"📦 <b>Topic {idx + 1}/{len(topics)}:</b> {t['name']}\n"
                f"⏳ Starting…",
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.error(f"mbatch: could not send status for topic {idx}: {e}")
            status_msg = None

        try:
            await _run_batch(
                acc, _client, uid, chat_ref,
                t["start_topic"], effective_start,
                t["end_topic"],   t["end_msg"],
                batches_dict  = topic_bdict,
                status_msg    = status_msg,
                collected_ids = topic_file_ids,
                no_prescan    = True,
                checkpoint_fn = _cpfn,
            )
        except Exception as e:
            logger.error(f"mbatch: _run_batch error (topic {idx}, {t['name']}): {e}")
            try:
                if status_msg:
                    await status_msg.edit_text(f"❌ Error in {t['name']}: {str(e)[:200]}")
            except Exception:
                pass

        if session_id:
            await _cp_topic_done(session_id, idx, topic_file_ids)

        total_saved += len(topic_file_ids)
        per_topic_result.append({"name": t["name"], "file_ids": list(topic_file_ids)})

        if topic_bdict.get(uid):
            cancelled = True
            if session_id:
                await _cp_cancel(session_id)
            break

        if idx + 1 < len(topics) and session_id:
            await _cp_advance(session_id, idx + 1)

        # Inter-topic cooldown: 5 s gap between topics
        if idx + 1 < len(topics):
            await asyncio.sleep(5)

    _active_mbatches.pop(uid, None)

    if not cancelled and session_id:
        await _cp_mark_done(session_id)

    return total_saved, per_topic_result, cancelled


# ── /batch command ─────────────────────────────────────────────────────────────

@gagan.on(events.NewMessage(incoming=True, pattern=r"^/batch(?:\s|$|@)"))
async def mbatch_cmd(event):
    uid = event.sender_id

    if uid in _active_mbatches:
        await event.respond(
            "⚠️ A batch is already running.\n"
            "Use /bcancel to stop it first."
        )
        return

    if await _cp_has_pending(uid, bot_key=BOT_KEY):
        await Bot.send_message(
            uid,
            "⚠️ You have an unfinished /batch session.\n"
            "Use /batch_status to view progress and resume or cancel it."
        )
        return

    raw_input = None
    async with gagan.conversation(event.chat_id, timeout=180) as conv:
        try:
            await conv.send_message(
                "📋 <b>Multi-Topic Batch</b>\n\n"
                "Paste your topic links — one per line in this format:\n\n"
                "<code>TopicName:- start_link-end_link</code>\n\n"
                "<b>Example:</b>\n"
                "<code>Medicine:- https://t.me/c/2932205861/1032/1041-https://t.me/c/2932205861/1032/1198\n"
                "Surgery:- https://t.me/c/2932205861/1033/1199-https://t.me/c/2932205861/1033/1343\n"
                "Pediatrics:- https://t.me/c/2932205861/1034/1344-https://t.me/c/2932205861/1034/1425</code>\n\n"
                "You can paste multiple topics at once.",
                parse_mode="html",
                buttons=Button.force_reply(),
            )
            reply    = await conv.get_reply()
            raw_input = reply.text.strip() if reply and reply.text else ""
        except asyncio.TimeoutError:
            await event.respond("⏳ Timed out. Send /batch to try again.")
            return
        except Exception as e:
            logger.error(f"mbatch_cmd conv: {e}")
            return

    if not raw_input:
        await event.respond("❌ No input received. Send /batch to try again.")
        return

    topics = _parse_topic_lines(raw_input)
    if not topics:
        await Bot.send_message(
            uid,
            "❌ Could not parse any topics.\n\n"
            "Each line must be:\n"
            "<code>TopicName:- start_link-end_link</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    chat_ref = topics[0]["chat_ref"]

    acc      = userbot
    _personal = None
    if acc is None:
        sess = await _get_user_session(uid)
        if sess:
            try:
                from pyrogram import Client as _Client
                _personal = _Client(
                    f"mbatch_{uid}",
                    session_string=sess,
                    api_id=int(API_ID),
                    api_hash=API_HASH,
                    in_memory=True,
                )
                await _personal.start()
                acc = _personal
            except Exception as e:
                logger.error(f"mbatch: personal acc start failed: {e}")

    if acc is None and isinstance(chat_ref, int):
        await Bot.send_message(
            uid,
            "❌ No user session available.\n"
            "Use /login to authenticate, then try /batch again.",
        )
        return

    n = len(topics)
    preview_lines = [f"  • {t['name']}" for t in topics[:10]]
    if n > 10:
        preview_lines.append(f"  … and {n - 10} more")
    await Bot.send_message(
        uid,
        f"⚡ <b>Batch starting</b> — {n} topic(s)\n\n"
        + "\n".join(preview_lines),
        parse_mode=ParseMode.HTML,
    )

    await _cp_delete_old(uid, bot_key=BOT_KEY)   # wipe completed/cancelled history before new batch
    session_id = await _cp_create(uid, chat_ref, topics, bot_key=BOT_KEY)
    if session_id:
        logger.info(f"mbatch: session {session_id} created.")

    try:
        total_saved, per_topic, cancelled = await _execute_mbatch(
            acc, uid, chat_ref, topics, session_id,
            start_idx=0, resume_msg_id=None,
        )
    finally:
        if _personal:
            try:
                await _personal.stop()
            except Exception:
                pass

    has_files = any(t["file_ids"] for t in per_topic)
    _send_summary(uid, n, total_saved, has_files, cancelled, session_id)


def _send_summary(uid, n_topics, total_saved, file_ids, cancelled, session_id):
    asyncio.create_task(
        _async_send_summary(uid, n_topics, total_saved, file_ids, cancelled, session_id)
    )


async def _async_send_summary(uid, n_topics, total_saved, file_ids, cancelled, session_id):
    status_word = "🚫 Batch cancelled" if cancelled else "✅ Batch complete"
    text = (
        f"{status_word}\n\n"
        f"📂 <b>Topics:</b> {n_topics}\n"
        f"📦 <b>Files saved:</b> {total_saved}"
    )

    if file_ids and DB_CHANNEL and not cancelled:
        await Bot.send_message(
            uid, text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "📤 Forward All to DB_CHANNEL",
                    callback_data=f"mbfwd:{session_id}",
                ),
            ]]),
        )
    else:
        note = ""
        if not DB_CHANNEL and not cancelled:
            note = "\n\n<i>(DB_CHANNEL not set — forward disabled)</i>"
        await Bot.send_message(uid, text + note, parse_mode=ParseMode.HTML)


# ── /bcancel ───────────────────────────────────────────────────────────────────

@gagan.on(events.NewMessage(incoming=True, pattern=r"^/bcancel$"))
async def bcancel_cmd(event):
    uid = event.sender_id
    bdict = _active_mbatches.get(uid)
    if bdict is None:
        await event.respond("No running /batch to cancel.")
        return
    bdict[uid] = True
    await event.respond(
        "🚫 Cancel signal sent. The batch will stop after the current file finishes."
    )


# ── /batch_status ──────────────────────────────────────────────────────────────

@gagan.on(events.NewMessage(incoming=True, pattern=r"^/batch_status$"))
async def batch_status_cmd(event):
    uid = event.sender_id
    s   = await _cp_latest(uid, bot_key=BOT_KEY)

    if not s:
        await Bot.send_message(
            uid,
            "ℹ️ No batch history found.\n\nStart one with /batch."
        )
        return

    sid    = s["session_id"]
    sid8   = sid[:8]
    status = s.get("status", "?")
    total  = s.get("total_saved", 0)
    topics_list = s.get("topics", [])
    n_topics    = len(topics_list)

    if status == "in_progress":
        cur   = s.get("current_topic_idx", 0)
        last  = s.get("last_saved_msg_id")
        ts    = s.get("created_at", "?")

        done_topics  = [t["name"] for t in topics_list if t.get("status") == "done"]
        active_topic = topics_list[cur]["name"] if cur < n_topics else "—"
        remain_count = n_topics - len(done_topics) - (1 if cur < n_topics else 0)
        done_str     = ", ".join(done_topics) if done_topics else "none yet"
        last_str     = f"msg {last}" if last else "none yet"

        lines = [
            "🔄 <b>IN PROGRESS</b>",
            f"📋 <b>Session:</b> <code>{sid8}…</code>",
            f"🕐 <b>Started:</b> {ts}",
            f"📂 <b>Topics:</b> {n_topics} total",
            f"✅ <b>Completed:</b> {done_str}",
            f"🔄 <b>Current:</b> {active_topic} (last: {last_str})",
            f"⏳ <b>Remaining:</b> ~{remain_count} more",
            f"📦 <b>Files saved:</b> {total}",
        ]
        await Bot.send_message(
            uid, "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("▶ Resume", callback_data=f"mb_resume:{sid}"),
                InlineKeyboardButton("✕ Cancel", callback_data=f"mb_cancel:{sid}"),
            ]]),
        )
    else:
        icon    = "✅" if status == "done" else "🚫"
        label   = "COMPLETED" if status == "done" else "CANCELLED"
        updated = s.get("updated_at", "?")
        lines   = [
            f"{icon} <b>{label}</b>",
            f"📋 <b>Session:</b> <code>{sid8}…</code>",
            f"🕐 <b>Finished:</b> {updated}",
            f"📂 <b>Topics:</b> {n_topics}",
            f"📦 <b>Files saved:</b> {total}",
        ]
        text = "\n".join(lines)
        if total > 0 and DB_CHANNEL and status == "done":
            await Bot.send_message(
                uid, text,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "📤 Forward to DB_CHANNEL",
                        callback_data=f"mbfwd:{sid}",
                    ),
                ]]),
            )
        else:
            await Bot.send_message(uid, text, parse_mode=ParseMode.HTML)


# ── Resume background worker ───────────────────────────────────────────────────

async def _do_resume_mbatch(uid, acc, _personal, chat_ref, topics, session_id,
                             start_idx, resume_msg_id, existing_file_ids):
    """
    Runs the actual resume work as a detached background task so the
    Pyrogram callback handler can return immediately (avoids dispatcher timeout).
    """
    try:
        total_saved, new_per_topic, cancelled = await _execute_mbatch(
            acc, uid, chat_ref, topics, session_id,
            start_idx=start_idx,
            resume_msg_id=resume_msg_id,
        )
    except Exception as e:
        logger.error(f"mbatch resume _execute_mbatch raised: {e}", exc_info=True)
        try:
            await Bot.send_message(uid, f"❌ Resume error: {str(e)[:300]}")
        except Exception:
            pass
        return
    finally:
        if _personal:
            try:
                await _personal.stop()
            except Exception:
                pass

    has_files = bool(existing_file_ids or any(t.get("file_ids") for t in new_per_topic))
    n_topics  = len(topics)
    await _async_send_summary(uid, n_topics, total_saved, has_files, cancelled, session_id)


# ── Resume callback ────────────────────────────────────────────────────────────

@Bot.on_callback_query(filters.regex(r"^mb_resume:"))
async def mbatch_resume_cb(client, query):
    await query.answer("▶️ Starting resume…")
    uid        = query.from_user.id
    session_id = query.data[len("mb_resume:"):]

    if uid in _active_mbatches:
        await query.message.edit_text(
            "⚠️ A batch is already running. Wait for it to finish first."
        )
        return

    session = await _cp_get(session_id)
    if not session:
        await query.message.edit_text("❌ Session not found or already expired.")
        return
    if session["status"] == "done":
        await query.message.edit_text("✅ This session is already complete.")
        return
    if session["status"] == "cancelled":
        await query.message.edit_text("🚫 This session was cancelled.")
        return

    await query.message.edit_text(
        f"▶️ <b>Resuming session</b> <code>{session_id[:8]}…</code>\n"
        "Continuing from last checkpoint…",
        parse_mode=ParseMode.HTML,
    )

    chat_ref = session.get("chat_ref")
    if isinstance(chat_ref, float):
        chat_ref = int(chat_ref)

    topics          = session.get("topics", [])
    start_idx       = session.get("current_topic_idx", 0)
    resume_msg_id   = session.get("last_saved_msg_id")

    if start_idx >= len(topics):
        await _cp_mark_done(session_id)
        await Bot.send_message(uid, "✅ All topics already completed — session marked done.")
        return

    ann_lines = []
    for i, t in enumerate(topics):
        st = t.get("status", "pending")
        icon = "✅" if st == "done" else ("🔄" if i == start_idx else "⏳")
        ann_lines.append(f"  {icon} {t['name']}")

    await Bot.send_message(
        uid,
        f"📋 <b>Resuming {len(topics)} topic(s)</b> from topic {start_idx + 1}:\n"
        + "\n".join(ann_lines),
        parse_mode=ParseMode.HTML,
    )

    acc       = userbot
    _personal = None
    if acc is None:
        sess = await _get_user_session(uid)
        if sess:
            try:
                from pyrogram import Client as _Client
                _personal = _Client(
                    f"mbresume_{uid}",
                    session_string=sess,
                    api_id=int(API_ID),
                    api_hash=API_HASH,
                    in_memory=True,
                )
                await _personal.start()
                acc = _personal
            except Exception as e:
                logger.error(f"mbatch resume: personal acc failed: {e}")

    if acc is None and isinstance(chat_ref, int):
        await Bot.send_message(uid, "❌ No session available. Use /login first.")
        return

    existing_file_ids = []
    for t in topics[:start_idx]:
        existing_file_ids.extend(t.get("file_ids", []))

    # Detach the long-running batch from the callback handler so the
    # Pyrogram dispatcher is not blocked while files are downloading.
    # NOTE: we must hold a strong reference — asyncio only keeps weak refs
    # to tasks, so without this the GC can cancel the task mid-run.
    _task = asyncio.create_task(
        _do_resume_mbatch(uid, acc, _personal, chat_ref, topics, session_id,
                          start_idx, resume_msg_id, existing_file_ids)
    )
    _resume_tasks.add(_task)
    _task.add_done_callback(_resume_tasks.discard)


# ── Cancel callback ────────────────────────────────────────────────────────────

@Bot.on_callback_query(filters.regex(r"^mb_cancel:"))
async def mbatch_cancel_cb(client, query):
    await query.answer("🚫 Cancelling…")
    session_id = query.data[len("mb_cancel:"):]
    session    = await _cp_get(session_id)
    if not session:
        await query.message.edit_text("❌ Session not found.")
        return
    await _cp_cancel(session_id)
    await query.message.edit_text(
        f"🚫 Session <code>{session_id[:8]}…</code> cancelled.\n"
        "You can start a new /batch anytime.",
        parse_mode=ParseMode.HTML,
    )


# ── Forward All callback ───────────────────────────────────────────────────────

@Bot.on_callback_query(filters.regex(r"^mbfwd:"))
async def mbatch_forward_cb(client, query):
    await query.answer("⏳ Forwarding to DB_CHANNEL…")
    uid        = query.from_user.id
    session_id = query.data[len("mbfwd:"):]

    session = await _cp_get(session_id)
    if not session:
        await query.message.edit_text(
            "❌ Session expired — please run /batch again."
        )
        return

    if not DB_CHANNEL:
        await query.message.edit_text("❌ `DB_CHANNEL` is not configured.")
        return

    topics = session.get("topics", [])
    per_topic_result = [
        {"name": t.get("name", f"Topic {i+1}"), "file_ids": t.get("file_ids", [])}
        for i, t in enumerate(topics)
    ]
    total_files = sum(len(t["file_ids"]) for t in per_topic_result)

    if total_files == 0:
        await query.message.edit_text("⚠️ No files to forward.")
        return

    await query.message.edit_text(
        f"📤 <b>Forwarding {total_files} file(s) to DB_CHANNEL…</b>",
        parse_mode=ParseMode.HTML,
    )

    fwd_ok, errors, summary_text = await _forward_topics_and_summarize(
        uid, Bot, per_topic_result, DB_CHANNEL, progress_msg=query.message
    )

    err_text = ("\n\n⚠️ <b>Errors:</b>\n" + "\n".join(errors[:10])) if errors else ""
    try:
        await query.message.edit_text(
            f"✅ <b>Forward complete</b> — {fwd_ok}/{total_files} file(s) → DB_CHANNEL"
            + err_text,
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass

    if summary_text:
        await Bot.send_message(uid, summary_text)
