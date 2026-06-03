"""
multi_bot.py — registers all standard handlers on each extra bot
(BOT_TOKEN2 / BOT_TOKEN3 / BOT_TOKEN4).

Key design:
- Every extra bot has its OWN active_batches dict (fully isolated).
  Bot2's /cancel only cancels Bot2's batch; Bot1 is unaffected.
- All extra bots share the same userbot session — login once, works everywhere.
- Parallel /batch jobs are possible: each bot is an independent Telegram connection.
- All extra bots save batch sessions to MongoDB (same collection as main bot),
  so /batch_status + Resume/Cancel work correctly after restarts.
"""

import asyncio
import logging
import os
import time

from telethon import events, Button
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import FloodWait

from .. import extra_clients, userbot, API_ID, API_HASH
from main.plugins.batch import _parse_range, _run_batch, temp_log_file
from main.plugins.batch import _get_user_session
from main.plugins.mbatch import (
    _parse_topic_lines, _execute_mbatch,
    _forward_topics_and_summarize, DB_CHANNEL as _MBATCH_DB_CHANNEL,
)
from main.plugins.mbatch_checkpoint import (
    create_session        as _cp_create,
    has_pending_session   as _cp_has_pending,
    get_pending_sessions  as _cp_pending,
    get_recent_sessions   as _cp_recent,
    get_session           as _cp_get,
    cancel_session        as _cp_cancel,
    mark_done             as _cp_mark_done,
    get_latest_session    as _cp_latest,
    delete_old_sessions   as _cp_delete_old,
)
from main.plugins.helpers import get_link, join
from main.plugins.pyroplug import ggn_new, user_chat_ids

logger = logging.getLogger(__name__)

_COMMANDS = [
    '/dl', '/batch', '/sbatch', '/cancel', '/bcancel', '/login', '/logout',
    '/mysession', '/start', '/help', '/logs', '/setchat', '/remthumb',
    '/ivalid', '/batch_status',
]

_START_PIC = "https://graph.org/file/1dfb96bd8f00a7c05f164.gif"
_START_TEXT = (
    "Send me the Link of any message of Restricted Channels to Clone it here.\n"
    "For private channel's messages, send the Invite Link first.\n\n"
    "👉🏻 Execute /batch for bulk process upto 10K files range."
)
_REPO_URL = "https://github.com/devgaganin"
_HELP_TEXT = """Here are the available commands:

➡️ /batch - Bulk process up to 10K message range.

➡️ /setchat - Forward messages to a group/channel/user.
```Use: /setchat <chatID>```

➡️ /remthumb - Delete your custom thumbnail.

➡️ /cancel - Cancel your running batch on this bot.

➡️ /dl - Download from YouTube, LinkedIn, etc.

Note: Send a photo (no command) to set a custom thumbnail.

[GitHub](%s)
""" % _REPO_URL


def _is_range_link(raw: str) -> bool:
    if raw.count('https://') >= 2 or raw.count('http://') >= 2:
        return True
    clean = raw.strip().rstrip("/").split("?")[0]
    last = clean.split("/")[-1]
    if "-" in last:
        parts = last.split("-", 1)
        return parts[0].isdigit() and parts[1].isdigit()
    return False


def _register(tel_bot, pyro_bot, bot_index: int):
    """Bind all handlers to one extra (tel_bot, pyro_bot) pair."""

    _batches = {}   # uid → False (running) | True (cancel requested)
    # Holds strong refs to background resume tasks to prevent GC cancellation.
    _resume_tasks: set = set()

    # ── /cancel ───────────────────────────────────────────────────────────────
    @tel_bot.on(events.NewMessage(incoming=True, pattern=r'^/cancel\b'))
    async def _cancel(event):
        uid = event.sender_id
        if _batches.get(uid) is False:
            _batches[uid] = True
            await event.respond("✅ Batch cancelled.")
        else:
            await event.respond("No running batch to cancel on this bot.")

    @tel_bot.on(events.NewMessage(incoming=True, pattern=r'^/bcancel\b'))
    async def _bcancel(event):
        uid = event.sender_id
        if _batches.get(uid) is False:
            _batches[uid] = True
            await event.respond("✅ Batch cancelled.")
        else:
            await event.respond("No running batch to cancel on this bot.")

    # ── /batch_status — shows the single most recent session only ────────────
    @tel_bot.on(events.NewMessage(incoming=True, pattern=r'^/batch_status\b'))
    async def _batch_status(event):
        uid = event.sender_id

        if _batches.get(uid) is False:
            await event.respond(
                f"🔄 <b>Batch in progress</b> (Bot #{bot_index})\n\n"
                "Use /cancel or /bcancel to stop it.",
                parse_mode="html",
            )
            return

        s = await _cp_latest(uid, bot_index=bot_index)
        if not s:
            await pyro_bot.send_message(
                uid,
                f"ℹ️ No batch history found (Bot #{bot_index}).\n\nStart one with /batch."
            )
            return

        sid         = s["session_id"]
        sid8        = sid[:8]
        status      = s.get("status", "?")
        total       = s.get("total_saved", 0)
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
                f"🔄 <b>IN PROGRESS</b> (Bot #{bot_index})",
                f"📋 <b>Session:</b> <code>{sid8}…</code>",
                f"🕐 <b>Started:</b> {ts}",
                f"📂 <b>Topics:</b> {n_topics} total",
                f"✅ <b>Completed:</b> {done_str}",
                f"🔄 <b>Current:</b> {active_topic} (last: {last_str})",
                f"⏳ <b>Remaining:</b> ~{remain_count} more",
                f"📦 <b>Files saved:</b> {total}",
            ]
            await pyro_bot.send_message(
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
                f"{icon} <b>{label}</b> (Bot #{bot_index})",
                f"📋 <b>Session:</b> <code>{sid8}…</code>",
                f"🕐 <b>Finished:</b> {updated}",
                f"📂 <b>Topics:</b> {n_topics}",
                f"📦 <b>Files saved:</b> {total}",
            ]
            text = "\n".join(lines)
            if total > 0 and _MBATCH_DB_CHANNEL and status == "done":
                await pyro_bot.send_message(
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
                await pyro_bot.send_message(uid, text, parse_mode=ParseMode.HTML)

    # ── /logs ─────────────────────────────────────────────────────────────────
    @tel_bot.on(events.NewMessage(incoming=True, pattern='/logs'))
    async def _send_log(event):
        if os.path.exists(temp_log_file):
            await tel_bot.send_file(event.sender_id, temp_log_file,
                                    caption="Log file (last 3 min).")
        else:
            await event.respond("Log file not found.")

    # ── /start ────────────────────────────────────────────────────────────────
    @tel_bot.on(events.NewMessage(incoming=True, pattern='^/start'))
    async def _start(event):
        buttons = [
            [Button.url("Join Channel", url="https://t.me/devggn")],
            [Button.url("Contact Me", url="https://t.me/ggnhere")],
        ]
        await tel_bot.send_file(event.chat_id, file=_START_PIC,
                                caption=_START_TEXT, buttons=buttons)

    # ── /help ─────────────────────────────────────────────────────────────────
    @tel_bot.on(events.NewMessage(incoming=True, pattern='/help'))
    async def _help(event):
        buttons = [[Button.url("REPO", url=_REPO_URL)]]
        await event.respond(_HELP_TEXT, buttons=buttons, link_preview=False)

    # ── /setchat ──────────────────────────────────────────────────────────────
    @tel_bot.on(events.NewMessage(incoming=True, pattern='/setchat'))
    async def _setchat(event):
        try:
            chat_id = int(event.raw_text.split(" ", 1)[1])
            user_chat_ids[event.sender_id] = chat_id
            await event.reply("Chat ID set successfully!")
        except (ValueError, IndexError):
            await event.reply("Usage: /setchat <chat_id>")

    # ── /remthumb ─────────────────────────────────────────────────────────────
    @tel_bot.on(events.NewMessage(incoming=True, pattern='/remthumb'))
    async def _remthumb(event):
        user_id = event.sender_id
        try:
            os.remove(f'{user_id}.jpg')
            await event.respond('Thumbnail removed successfully!')
        except FileNotFoundError:
            await event.respond("No thumbnail found to remove.")

    # ── Photo → save as thumbnail ─────────────────────────────────────────────
    @tel_bot.on(events.NewMessage(incoming=True,
                                  func=lambda e: e.photo and e.is_private))
    async def _save_thumb(event):
        user_id = event.sender_id
        temp_path = await tel_bot.download_media(event.media)
        if os.path.exists(f'{user_id}.jpg'):
            os.remove(f'{user_id}.jpg')
        os.rename(temp_path, f'./{user_id}.jpg')
        await event.respond('Thumbnail saved successfully!')

    # ── /batch ────────────────────────────────────────────────────────────────
    @tel_bot.on(events.NewMessage(incoming=True, pattern=r'^/batch\b'))
    async def _bulk(event):
        uid = event.sender_id

        if _batches.get(uid) is False:
            return await event.reply(
                "A batch is already running on this bot. Use /cancel to stop it."
            )

        if await _cp_has_pending(uid, bot_index=bot_index):
            await pyro_bot.send_message(
                uid,
                f"⚠️ You have an unfinished /batch session (Bot #{bot_index}).\n"
                "Use /batch_status to view progress and resume or cancel it."
            )
            return

        raw_input = None

        async with tel_bot.conversation(event.chat_id, timeout=180) as conv:
            try:
                await conv.send_message(
                    "📋 **Multi-Topic Batch** (Bot #{bot_idx})\n\n"
                    "Paste your topic links — one per line:\n\n"
                    "`TopicName:- start_link-end_link`\n\n"
                    "**Example:**\n"
                    "`Medicine:- https://t.me/c/123/10/1-https://t.me/c/123/10/50`\n"
                    "`Surgery:- https://t.me/c/123/11/51-https://t.me/c/123/11/100`\n\n"
                    "_Single range also works: `START_LINK-END_LINK`_".format(
                        bot_idx=bot_index
                    ),
                    buttons=Button.force_reply(),
                )
                reply_msg = await conv.get_reply()
                raw_input = reply_msg.text.strip() if reply_msg and reply_msg.text else ""
            except asyncio.TimeoutError:
                await event.respond("⏳ Timed out. Please try /batch again.")
                return
            except Exception as e:
                logger.info(e)
                await event.respond(f"Error: {e}")
                return

        if not raw_input:
            await event.respond("❌ No input received. Send /batch to try again.")
            return

        topics = _parse_topic_lines(raw_input)
        if not topics:
            parsed = _parse_range(raw_input)
            if not parsed:
                await pyro_bot.send_message(
                    uid,
                    "❌ Could not parse input.\n\n"
                    "Use multi-topic format:\n"
                    "`TopicName:- start_link-end_link`\n\n"
                    "Or single range: `START_LINK-END_LINK`"
                )
                return
            chat_ref, start_topic, start_msg, end_topic, end_msg = parsed
            topics = [{
                "name":        "Batch",
                "chat_ref":    chat_ref,
                "start_topic": start_topic,
                "start_msg":   start_msg,
                "end_topic":   end_topic,
                "end_msg":     end_msg,
            }]

        chat_ref = topics[0]["chat_ref"]

        acc = userbot
        personal_acc = None

        if acc is None:
            sess = await _get_user_session(uid)
            if sess:
                try:
                    personal_acc = Client(
                        f"batch_{uid}_b{bot_index}",
                        session_string=sess,
                        api_id=int(API_ID),
                        api_hash=API_HASH,
                        in_memory=True,
                    )
                    await personal_acc.start()
                    acc = personal_acc
                except Exception as e:
                    await pyro_bot.send_message(
                        uid, f"⚠️ Could not start your session: `{e}`"
                    )
                    personal_acc = None

        if acc is None and isinstance(chat_ref, int):
            return await pyro_bot.send_message(
                uid,
                "❌ **No user session available.**\n\n"
                "A Telegram user account is required to access private/restricted channels.\n"
                "👉 Use /login on the **main bot** to authenticate, then try /batch again.",
            )

        n = len(topics)
        preview = "\n".join(f"  • {t['name']}" for t in topics[:10])
        if n > 10:
            preview += f"\n  … and {n - 10} more"
        await pyro_bot.send_message(
            uid,
            f"⚡ <b>Batch starting</b> (Bot #{bot_index}) — {n} topic(s)\n\n"
            f"{preview}\n\nUse /cancel to stop.",
            parse_mode=ParseMode.HTML,
        )

        # ── Create MongoDB session (wipe old history first) ───────────────────
        await _cp_delete_old(uid, bot_index=bot_index)
        session_id = await _cp_create(uid, chat_ref, topics, bot_index=bot_index)
        if session_id:
            logger.info(f"multi_bot bot#{bot_index}: session {session_id} created for uid={uid}")

        _batches[uid] = False

        try:
            total_saved, per_topic, cancelled = await _execute_mbatch(
                acc, uid, chat_ref, topics,
                session_id=session_id,   # ← now saved to MongoDB
                client=pyro_bot,
            )

            has_files   = any(t["file_ids"] for t in per_topic)
            status_word = "🚫 Batch cancelled" if cancelled else "✅ Batch complete"
            summary_txt = (
                f"{status_word} (Bot #{bot_index})\n\n"
                f"📂 <b>Topics:</b> {n}\n"
                f"📦 <b>Files saved:</b> {total_saved}"
            )
            if has_files and _MBATCH_DB_CHANNEL and not cancelled and session_id:
                await pyro_bot.send_message(
                    uid, summary_txt,
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
                if not _MBATCH_DB_CHANNEL and not cancelled:
                    note = "\n\n<i>(DB_CHANNEL not set — forward disabled)</i>"
                await pyro_bot.send_message(
                    uid, summary_txt + note, parse_mode=ParseMode.HTML
                )

            if has_files and _MBATCH_DB_CHANNEL and not cancelled:
                prog_msg = await pyro_bot.send_message(uid, "📤 Forwarding to DB_CHANNEL…")
                try:
                    fwd_ok, errors, summary_text = await _forward_topics_and_summarize(
                        uid, pyro_bot, per_topic, _MBATCH_DB_CHANNEL, progress_msg=prog_msg
                    )
                    err_note = ("\n⚠️ Errors:\n" + "\n".join(errors[:5])) if errors else ""
                    try:
                        await prog_msg.edit_text(
                            f"✅ Forwarded {fwd_ok} file(s) → DB_CHANNEL" + err_note
                        )
                    except Exception:
                        pass
                    if summary_text:
                        await pyro_bot.send_message(uid, summary_text)
                except Exception as fe:
                    logger.error(f"multi_bot forward error (bot#{bot_index}): {fe}")
                    try:
                        await prog_msg.edit_text(f"❌ Forward error: {str(fe)[:200]}")
                    except Exception:
                        pass

        except Exception as e:
            logger.error(f"multi_bot /batch error (bot#{bot_index}, uid={uid}): {e}")
            await pyro_bot.send_message(uid, f"❌ Batch error: {str(e)[:300]}")
        finally:
            if personal_acc:
                try:
                    await personal_acc.stop()
                except Exception:
                    pass
            _batches.pop(uid, None)

    # ── Resume background worker ──────────────────────────────────────────────
    async def _do_resume(uid, acc, _personal, chat_ref, topics, session_id,
                         start_idx, resume_msg_id, existing_file_ids):
        """
        Detached background task: runs the actual resume so the Pyrogram
        callback handler returns immediately (avoids dispatcher timeout).
        """
        try:
            total_saved, new_per_topic, cancelled = await _execute_mbatch(
                acc, uid, chat_ref, topics, session_id,
                start_idx=start_idx,
                resume_msg_id=resume_msg_id,
                client=pyro_bot,
            )
        except Exception as e:
            logger.error(f"multi_bot resume bot#{bot_index}: _execute_mbatch raised: {e}", exc_info=True)
            try:
                await pyro_bot.send_message(uid, f"❌ Resume error (Bot #{bot_index}): {str(e)[:300]}")
            except Exception:
                pass
            return
        finally:
            if _personal:
                try:
                    await _personal.stop()
                except Exception:
                    pass
            _batches.pop(uid, None)

        has_files   = bool(existing_file_ids or any(t.get("file_ids") for t in new_per_topic))
        n_topics    = len(topics)
        status_word = "🚫 Batch cancelled" if cancelled else "✅ Batch complete"
        summary_txt = (
            f"{status_word} (Bot #{bot_index})\n\n"
            f"📂 <b>Topics:</b> {n_topics}\n"
            f"📦 <b>Files saved:</b> {total_saved}"
        )
        if has_files and _MBATCH_DB_CHANNEL and not cancelled and session_id:
            await pyro_bot.send_message(
                uid, summary_txt,
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
            if not _MBATCH_DB_CHANNEL and not cancelled:
                note = "\n\n<i>(DB_CHANNEL not set — forward disabled)</i>"
            await pyro_bot.send_message(
                uid, summary_txt + note, parse_mode=ParseMode.HTML
            )

    # ── Resume callback ───────────────────────────────────────────────────────
    @pyro_bot.on_callback_query(filters.regex(r"^mb_resume:"))
    async def _resume_cb(client, query):
        await query.answer("▶️ Starting resume…")
        uid        = query.from_user.id
        session_id = query.data[len("mb_resume:"):]

        if _batches.get(uid) is False:
            await query.message.edit_text(
                "⚠️ A batch is already running on this bot. Wait for it to finish first."
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
            await pyro_bot.send_message(uid, "✅ All topics already completed — session marked done.")
            return

        ann_lines = []
        for i, t in enumerate(topics):
            st   = t.get("status", "pending")
            icon = "✅" if st == "done" else ("🔄" if i == start_idx else "⏳")
            ann_lines.append(f"  {icon} {t['name']}")

        await pyro_bot.send_message(
            uid,
            f"📋 <b>Resuming {len(topics)} topic(s)</b> from topic {start_idx + 1} (Bot #{bot_index}):\n"
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
                        f"mbresume_{uid}_b{bot_index}",
                        session_string=sess,
                        api_id=int(API_ID),
                        api_hash=API_HASH,
                        in_memory=True,
                    )
                    await _personal.start()
                    acc = _personal
                except Exception as e:
                    logger.error(f"multi_bot resume bot#{bot_index}: personal acc failed: {e}")

        if acc is None and isinstance(chat_ref, int):
            await pyro_bot.send_message(uid, "❌ No session available. Use /login first.")
            return

        existing_file_ids = []
        for t in topics[:start_idx]:
            existing_file_ids.extend(t.get("file_ids", []))

        _batches[uid] = False
        # Detach the long-running batch from the callback so the Pyrogram
        # dispatcher is not blocked while files are downloading.
        # NOTE: hold a strong reference — asyncio only keeps weak refs to
        # tasks, so without this the GC can cancel the task mid-run.
        _task = asyncio.create_task(
            _do_resume(uid, acc, _personal, chat_ref, topics, session_id,
                       start_idx, resume_msg_id, existing_file_ids)
        )
        _resume_tasks.add(_task)
        _task.add_done_callback(_resume_tasks.discard)

    # ── Cancel callback ───────────────────────────────────────────────────────
    @pyro_bot.on_callback_query(filters.regex(r"^mb_cancel:"))
    async def _cancel_cb(client, query):
        await query.answer("🚫 Cancelling…")
        session_id = query.data[len("mb_cancel:"):]
        session    = await _cp_get(session_id)
        if not session:
            await query.message.edit_text("❌ Session not found.")
            return
        await _cp_cancel(session_id)
        _batches.pop(query.from_user.id, None)
        await query.message.edit_text(
            f"🚫 Session <code>{session_id[:8]}…</code> cancelled.\n"
            "You can start a new /batch anytime.",
            parse_mode=ParseMode.HTML,
        )

    # ── Forward All callback ──────────────────────────────────────────────────
    @pyro_bot.on_callback_query(filters.regex(r"^mbfwd:"))
    async def _fwd_cb(client, query):
        await query.answer("⏳ Forwarding to DB_CHANNEL…")
        uid        = query.from_user.id
        session_id = query.data[len("mbfwd:"):]

        session = await _cp_get(session_id)
        if not session:
            await query.message.edit_text("❌ Session expired — please run /batch again.")
            return
        if not _MBATCH_DB_CHANNEL:
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
            uid, pyro_bot, per_topic_result, _MBATCH_DB_CHANNEL,
            progress_msg=query.message,
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
            await pyro_bot.send_message(uid, summary_text)

    # ── Single-link clone ─────────────────────────────────────────────────────
    @tel_bot.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
    async def _clone(event):
        file_name = ''

        if event.message.text and any(
            event.message.text.strip().startswith(cmd) for cmd in _COMMANDS
        ):
            return

        lit = event.text
        if not lit:
            return

        lines = lit.split("\n")
        if len(lines) > 10:
            await event.respond("max 10 links per message")
            return

        for line in lines:
            if _is_range_link(line):
                return

            try:
                link = get_link(line)
                if not link:
                    return
            except TypeError:
                return

            if "|" in line:
                parts = line.split("|")
                if len(parts) == 2:
                    file_name = parts[1].strip()

            edit = await event.respond("Processing!")

            acc = userbot
            tmp_client = None

            if acc is None:
                from main.plugins.session_store import get_user_session as _gs
                sess = await _gs(event.sender_id)
                if sess:
                    try:
                        tmp_client = Client(
                            f"tmp_{event.sender_id}_b{bot_index}",
                            session_string=sess,
                            api_id=int(API_ID),
                            api_hash=API_HASH,
                            in_memory=True,
                        )
                        await tmp_client.start()
                        acc = tmp_client
                    except Exception as e:
                        logger.warning(f"Could not start personal session: {e}")
                        acc = None

            if acc is None and 't.me/c/' in link:
                await edit.edit(
                    "❌ No session available to access restricted content.\n"
                    "Use /login on the main bot to log in."
                )
                return

            try:
                if 't.me/' not in link:
                    await edit.edit("invalid link")
                    return

                if 't.me/+' in link:
                    _client = acc if acc else pyro_bot
                    q = await join(_client, link)
                    await edit.edit(q)
                    return

                msg_id = 0
                try:
                    msg_id = int(link.split("/")[-1])
                except ValueError:
                    if '?single' in link:
                        msg_id = int(link.split("?single")[0].split("/")[-1])
                    else:
                        msg_id = -1

                _acc = acc if acc else userbot
                await ggn_new(_acc, pyro_bot, event.sender_id,
                              edit.id, link, msg_id, file_name)

            except FloodWait as fw:
                await tel_bot.send_message(
                    event.sender_id,
                    f'Try again after {fw.value}s due to FloodWait.'
                )
                await edit.delete()
            except Exception as e:
                logger.info(e)
                await tel_bot.send_message(event.sender_id, f"Error: {str(e)}")
                await edit.delete()
            finally:
                if tmp_client:
                    try:
                        await tmp_client.stop()
                    except Exception:
                        pass

            time.sleep(1)


# ── Register on every configured extra bot ────────────────────────────────────

if extra_clients:
    for _i, (_tel, _pyro) in enumerate(extra_clients, start=2):
        _register(_tel, _pyro, _i)
    print(f"[multi_bot] Registered handlers on {len(extra_clients)} extra bot(s).")
else:
    print("[multi_bot] No extra bots configured (BOT_TOKEN2/3/4 not set).")
