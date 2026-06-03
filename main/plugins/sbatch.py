"""
sbatch.py — /sbatch: Split Batch across all available bots simultaneously.

Flow:
  1. /sbatch  → prompt user for a range link (same format as /batch)
  2. Parse link, split the range into N equal chunks (N = bots available)
  3. Create a MongoDB checkpoint session (if MONGO_URL is set)
  4. All N bots fire at once via asyncio.gather, each with its own acc
  5. After every successful file save, checkpoint_message() is recorded
  6. On Heroku restart: startup notifies user → /sbatch_status → Resume button
  7. Resume reconstructs each bot's chunk from last_saved_msg_id + 1
  8. After all finish, send summary + "▶ Forward All" inline button
"""

import asyncio
import logging
import os

from pyrogram import filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from telethon import events, Button

from .. import API_ID, API_HASH, Bot, bot as gagan, extra_clients, userbot
from .. import DB_CHANNEL as _DB_CHANNEL_RAW
from main.plugins.batch import (
    _get_user_session,
    _parse_range,
    _run_batch,
    _fetch_topic_last_id,
)
from main.plugins.sbatch_checkpoint import (
    create_session        as _cp_create,
    mark_bot_running      as _cp_bot_running,
    mark_bot_done         as _cp_bot_done,
    mark_bot_failed       as _cp_bot_failed,
    mark_session_done     as _cp_session_done,
    cancel_session        as _cp_cancel,
    checkpoint_message    as _cp_message,
    get_pending_sessions  as _cp_pending,
    get_session           as _cp_get,
    has_pending_session   as _cp_has_pending,
    create_indexes        as _cp_create_indexes,
)

logger = logging.getLogger(__name__)

# ── DB_CHANNEL ─────────────────────────────────────────────────────────────────
_db_str    = str(_DB_CHANNEL_RAW).strip() if _DB_CHANNEL_RAW else ""
DB_CHANNEL = (
    int(_db_str) if _db_str.lstrip("-").isdigit()
    else (_db_str or None)
)

# ── Bot user-ID cache (populated on first use) ────────────────────────────────
_bot_uid_cache: dict[int, int] = {}

async def _get_bot_uid(pyro_client) -> int:
    key = id(pyro_client)
    if key not in _bot_uid_cache:
        me = await pyro_client.get_me()
        _bot_uid_cache[key] = me.id
    return _bot_uid_cache[key]

# ── Per-run result store (keyed by sbatch_id) ─────────────────────────────────
_sessions: dict[str, list] = {}

# ── Active sbatch tracking — used by /scancel ─────────────────────────────────
_active_sbatches: dict[int, list[dict]] = {}

_sbatch_seq = 0

def _next_id(user_id: int) -> str:
    global _sbatch_seq
    _sbatch_seq += 1
    return f"{user_id}_{_sbatch_seq}"


# ── Range splitting helpers ───────────────────────────────────────────────────

def _split_msg_range(start: int, end: int, n: int) -> list[tuple[int, int]]:
    """Split [start, end] into at most n equal chunks by message ID."""
    total = end - start + 1
    size  = max(1, total // n)
    chunks, cur = [], start
    for i in range(n):
        chunk_end = end if i == n - 1 else min(cur + size - 1, end)
        if cur <= chunk_end:
            chunks.append((cur, chunk_end))
        cur = chunk_end + 1
    return chunks


def _split_topic_range(
    start_topic: int, start_msg: int,
    end_topic:   int, end_msg:   int,
    n: int,
) -> list[tuple[int, int, int, int]]:
    """
    Split [start_topic … end_topic] into at most n sequential groups.
    Returns list of (st, sm, et, em).

    Every topic in [start_topic, end_topic] is guaranteed to appear in exactly
    one chunk — the remainder (len(topics) % n) is merged into the last chunk
    so no topics are ever silently dropped.
    """
    topics = list(range(start_topic, end_topic + 1))
    if len(topics) <= n:
        # Fewer topics than bots — one topic per bot (no splitting needed)
        chunks = []
        for t in topics:
            sm = start_msg if t == start_topic else 1
            em = end_msg   if t == end_topic   else 999_999_999
            chunks.append((t, sm, t, em))
        return chunks

    size   = max(1, len(topics) // n)
    chunks = []
    for i in range(n):
        slice_start = i * size
        # Last bot always takes everything to the end (absorbs the remainder)
        slice_end   = len(topics) if i == n - 1 else slice_start + size
        grp         = topics[slice_start:slice_end]
        if not grp:
            continue
        st, et = grp[0], grp[-1]
        sm     = start_msg if st == start_topic else 1
        em     = end_msg   if et == end_topic   else 999_999_999
        chunks.append((st, sm, et, em))
    return chunks


# ── Core multi-bot executor ───────────────────────────────────────────────────
# Shared by fresh /sbatch AND resume so logic is never duplicated.

async def _execute_sbatch(
    uid: int,
    chat_ref,
    acc,
    _acc_session_str: "str | None",
    used_pyro: list,
    chunks: list,           # list of (st, sm, et, em)
    bot_indices: list,      # which global bot index each entry maps to
    sbatch_id: str,
    cp_session_id: "str | None",
) -> list:
    """
    Fire all bots in parallel.  Each bot gets its own MTProto connection
    (separate Pyrogram Client from the same session string) so downloads
    run without any cross-bot lock contention.

    cp_session_id — MongoDB session ID for checkpointing; None disables it.
    Returns the _sessions[sbatch_id] result list.
    """
    n_used     = len(used_pyro)
    bot_bdicts: list[dict] = [{} for _ in range(n_used)]
    _active_sbatches[uid]  = bot_bdicts
    _sessions[sbatch_id]   = [None] * n_used

    async def _run_one(local_idx: int, bot_idx: int, pyro_bot, chunk: tuple):
        st, sm, et, em  = chunk
        local_bdict     = bot_bdicts[local_idx]

        # ── Per-bot independent downloader session ─────────────────────────
        bot_acc  = None
        _own_acc = False
        if _acc_session_str:
            from pyrogram import Client as _PC
            try:
                bot_acc = _PC(
                    f"sbatch_{uid}_{bot_idx}",
                    session_string=_acc_session_str,
                    api_id=int(API_ID),
                    api_hash=API_HASH,
                    in_memory=True,
                )
                await bot_acc.start()
                _own_acc = True
            except Exception as e:
                logger.warning(f"sbatch bot{bot_idx+1}: per-bot acc failed ({e}); sharing main acc")
                if bot_acc:
                    try: await bot_acc.stop()
                    except: pass
                    bot_acc = None
        acc_used = bot_acc if bot_acc else acc

        # ── Human-readable description for status message ──────────────────
        _OPEN_END = 999_999_999
        _em_disp  = "end" if em == _OPEN_END else str(em)
        if st is not None and st != et:
            desc = f"topics `{st}`→`{et}`, msgs `{sm}`→`{_em_disp}`"
        else:
            tpfx = f"topic `{st}`, " if st else ""
            desc = f"{tpfx}msgs `{sm}`→`{_em_disp}`"

        # ── Try to open DM with the user via this bot ──────────────────────
        try:
            status_msg = await pyro_bot.send_message(
                uid,
                f"🤖 <b>Bot{bot_idx+1}</b> — {desc}\n⏳ Scanning…",
                parse_mode=ParseMode.HTML,
            )
            start_id = status_msg.id
        except Exception as e:
            logger.error(f"sbatch bot{bot_idx+1}: send_message failed: {e}")
            try:
                me        = await pyro_bot.get_me()
                username  = me.username or ""
                start_url = f"https://t.me/{username}?start=hi" if username else "(unknown)"
            except Exception:
                start_url = "(could not fetch bot username)"
            await Bot.send_message(
                uid,
                f"⚠️ <b>Bot{bot_idx+1}</b> could not reach you.\n\n"
                f"Please start it first, then retry /sbatch:\n"
                f'👉 <a href="{start_url}">Start Bot{bot_idx+1}</a>',
                parse_mode=ParseMode.HTML,
            )
            if _own_acc and bot_acc:
                try: await bot_acc.stop()
                except: pass
            return

        file_ids: list[int] = []

        # ── Checkpoint callback — fires after each successful save ─────────
        # Writes last_saved_msg_id + last_saved_topic to MongoDB so a
        # resume picks up from the exact topic+msg position.
        if cp_session_id:
            await _cp_bot_running(cp_session_id, bot_idx)

        async def _cp_fn(source_mid: int, source_topic=None):
            if cp_session_id:
                await _cp_message(cp_session_id, bot_idx, source_mid, source_topic)

        try:
            await _run_batch(
                acc_used, pyro_bot, uid, chat_ref,
                st, sm, et, em,
                batches_dict=local_bdict,
                status_msg=status_msg,
                collected_ids=file_ids,
                no_prescan=True,
                checkpoint_fn=_cp_fn,
            )
            # Mark this bot's chunk as fully done in MongoDB
            if cp_session_id:
                await _cp_bot_done(cp_session_id, bot_idx)

        except Exception as e:
            logger.error(f"sbatch bot{bot_idx+1} _run_batch: {e}", exc_info=True)
            try:
                await status_msg.edit_text(f"❌ Bot{bot_idx+1} error: {str(e)[:200]}")
            except Exception:
                pass
            if cp_session_id:
                await _cp_bot_failed(cp_session_id, bot_idx)
            if _own_acc and bot_acc:
                try: await bot_acc.stop()
                except: pass
            return
        finally:
            if _own_acc and bot_acc:
                try: await bot_acc.stop()
                except: pass

        # ── Record result for the Forward All / summary step ───────────────
        try:
            marker = await pyro_bot.send_message(uid, "\u200b")
            end_id = marker.id - 1
            await marker.delete()
        except Exception:
            end_id = start_id

        buid = await _get_bot_uid(pyro_bot)
        _sessions[sbatch_id][local_idx] = {
            "pyro_bot": pyro_bot,
            "bot_uid":  buid,
            "start_id": start_id,
            "end_id":   end_id,
            "file_ids": file_ids,
        }

    # ── Fire all bots simultaneously ───────────────────────────────────────
    try:
        await asyncio.gather(
            *[_run_one(i, bot_indices[i], used_pyro[i], chunks[i])
              for i in range(n_used)],
            return_exceptions=True,
        )
    finally:
        _active_sbatches.pop(uid, None)

    # ── Finalize MongoDB session when all bots are done ────────────────────
    if cp_session_id:
        updated = await _cp_get(cp_session_id)
        if updated:
            all_done = all(
                p["status"] == "done"
                for p in updated.get("progress", [])
            )
            if all_done:
                await _cp_session_done(cp_session_id)

    return _sessions[sbatch_id]


# ── /sbatch command ───────────────────────────────────────────────────────────

@gagan.on(events.NewMessage(incoming=True, pattern=r"^/sbatch$"))
async def sbatch_cmd(event):
    uid = event.sender_id

    # ── Concurrency guard — one active session per user ────────────────────
    if await _cp_has_pending(uid):
        await Bot.send_message(
            uid,
            "⚠️ You have an unfinished /sbatch session.\n"
            "Use /sbatch_status to view progress and resume or cancel it first."
        )
        return

    all_pyro = [Bot] + [pyro for (_tel, pyro) in extra_clients]
    n_bots   = len(all_pyro)

    # ── Collect range link from user ──────────────────────────────────────────
    parsed    = None
    _link_raw = ""   # preserved for MongoDB session doc
    async with gagan.conversation(event.chat_id, timeout=120) as conv:
        try:
            await conv.send_message(
                f"⚡ **Split Batch** — {n_bots} bot(s) available\n\n"
                "Send the range link (same format as /batch):\n"
                "`START_LINK-END_LINK`\n\n"
                "_Example:_\n"
                "`https://t.me/c/2133410746/926447-https://t.me/c/2133410746/926460`",
                buttons=Button.force_reply(),
            )
            reply     = await conv.get_reply()
            raw       = (reply.text or "").strip()
            _link_raw = raw

            if not raw:
                await conv.send_message("No link received. Please try /sbatch again.")
                return

            parsed = _parse_range(raw)
            if not parsed:
                await conv.send_message(
                    "❌ Could not parse that link.\n"
                    "Use: `START_LINK-END_LINK`"
                )
                return

            chat_ref, start_topic, start_msg, end_topic, end_msg = parsed
            if end_msg < start_msg and start_topic == end_topic:
                await conv.send_message("❌ End message ID must be ≥ start.")
                return

        except asyncio.TimeoutError:
            await event.respond("⏳ Timed out. Please try /sbatch again.")
            return
        except Exception as e:
            logger.error(f"sbatch_cmd conv error: {e}", exc_info=True)
            await event.respond(f"❌ Error: {e}")
            return

    chat_ref, start_topic, start_msg, end_topic, end_msg = parsed

    # ── Build per-bot chunks ──────────────────────────────────────────────────
    if start_topic is not None and start_topic != end_topic:
        chunks = _split_topic_range(
            start_topic, start_msg, end_topic, end_msg, n_bots
        )
    else:
        msg_chunks = _split_msg_range(start_msg, end_msg, n_bots)
        chunks     = [(start_topic, s, end_topic, e) for (s, e) in msg_chunks]

    n_used      = len(chunks)
    used_pyro   = all_pyro[:n_used]
    bot_indices = list(range(n_used))

    # ── Resolve userbot / personal session ────────────────────────────────────
    acc      = userbot
    _personal = None
    if acc is None:
        sess = await _get_user_session(uid)
        if sess:
            try:
                from pyrogram import Client as _Client
                _personal = _Client(
                    f"sbatch_{uid}",
                    session_string=sess,
                    api_id=int(API_ID),
                    api_hash=API_HASH,
                    in_memory=True,
                )
                await _personal.start()
                acc = _personal
            except Exception as e:
                logger.error(f"sbatch: personal_acc start failed: {e}")

    if acc is None:
        await Bot.send_message(uid, "❌ No userbot session available. Use /login first.")
        return

    # ── Export session string for per-bot parallel connections ────────────────
    _acc_session_str: "str | None" = None
    try:
        _acc_session_str = await acc.export_session_string()
    except Exception as e:
        logger.warning(f"sbatch: could not export session string ({e}); bots will share acc")

    # ── Announce split plan ───────────────────────────────────────────────────
    sbatch_id = _next_id(uid)

    bot_uids = await asyncio.gather(
        *[_get_bot_uid(p) for p in used_pyro],
        return_exceptions=True,
    )

    _OPEN_END      = 999_999_999
    overview_lines = []
    for i, ((st, sm, et, em), buid) in enumerate(zip(chunks, bot_uids)):
        buid_str = str(buid) if not isinstance(buid, BaseException) else "?"
        em_disp  = "end" if em == _OPEN_END else str(em)
        if st is not None and st != et:
            overview_lines.append(
                f"**Bot{i+1}** (`{buid_str}`): topics `{st}`→`{et}`, msgs `{sm}`→`{em_disp}`"
            )
        else:
            topic_note = f" topic `{st}`," if st else ""
            overview_lines.append(
                f"**Bot{i+1}** (`{buid_str}`){topic_note} msgs `{sm}`→`{em_disp}`"
            )

    await Bot.send_message(
        uid,
        "⚡ **Split Batch starting** — all bots working simultaneously\n\n"
        + "\n".join(overview_lines),
    )

    # ── Create MongoDB checkpoint session ─────────────────────────────────────
    chunk_docs = []
    for i, ((st, sm, et, em), buid) in enumerate(zip(chunks, bot_uids)):
        chunk_docs.append({
            "bot_index":   i,
            "bot_user_id": buid if not isinstance(buid, BaseException) else 0,
            "start_topic": st,
            "start_msg":   sm,
            "end_topic":   et,
            "end_msg":     em,
        })
    cp_session_id = await _cp_create(
        user_id=uid,
        chat_id=uid,
        original_link=_link_raw,
        chat_ref=chat_ref,
        chunks=chunk_docs,
        db_channel=DB_CHANNEL,
    )
    if cp_session_id:
        logger.info(f"sbatch: checkpoint session {cp_session_id} created.")

    # ── Execute (all bots in parallel with per-message checkpointing) ─────────
    results = await _execute_sbatch(
        uid=uid,
        chat_ref=chat_ref,
        acc=acc,
        _acc_session_str=_acc_session_str,
        used_pyro=used_pyro,
        chunks=chunks,
        bot_indices=bot_indices,
        sbatch_id=sbatch_id,
        cp_session_id=cp_session_id,
    )

    if _personal is not None:
        try: await _personal.stop()
        except: pass

    # ── Build summary (HTML so tg:// links are clickable) ─────────────────────
    has_ok    = any(r is not None for r in results)
    sum_lines = []
    for i, r in enumerate(results):
        if r is None:
            sum_lines.append(f"<b>Bot{i+1}</b> — ❌ failed")
        else:
            buid, s, e = r["bot_uid"], r["start_id"], r["end_id"]
            sum_lines.append(
                f"<b>Bot{i+1}</b> — "
                f'<a href="tg://openmessage?user_id={buid}&message_id={s}">start</a>'
                f" to "
                f'<a href="tg://openmessage?user_id={buid}&message_id={e}">end</a>'
            )

    summary = "⚡ <b>Split Batch Complete</b>\n\n" + "\n".join(sum_lines)

    if has_ok and DB_CHANNEL:
        await Bot.send_message(
            uid, summary, parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("▶ Forward All", callback_data=f"sbfwd:{sbatch_id}"),
            ]]),
        )
    else:
        note = (
            "\n\n<i>(<code>DB_CHANNEL</code> not configured — set it to enable Forward All)</i>"
            if not DB_CHANNEL else ""
        )
        await Bot.send_message(uid, summary + note, parse_mode=ParseMode.HTML)


# ── /scancel ──────────────────────────────────────────────────────────────────

@gagan.on(events.NewMessage(incoming=True, pattern=r"^/scancel$"))
async def scancel_cmd(event):
    uid    = event.sender_id
    bdicts = _active_sbatches.get(uid)
    if not bdicts:
        await event.respond("No running split-batch to cancel.")
        return
    for d in bdicts:
        d[uid] = True
    await event.respond(
        f"🚫 Cancel signal sent to all {len(bdicts)} bot(s). "
        "They will stop after the current file finishes."
    )


# ── /sbatch_status ────────────────────────────────────────────────────────────

@gagan.on(events.NewMessage(incoming=True, pattern=r"^/sbatch_status$"))
async def sbatch_status_cmd(event):
    uid      = event.sender_id
    sessions = await _cp_pending(uid)

    if not sessions:
        await Bot.send_message(
            uid,
            "✅ No unfinished /sbatch sessions found.\n\n"
            "Start a new one with /sbatch."
        )
        return

    for s in sessions:
        sid    = s["session_id"]
        sid8   = sid[:8]
        ts     = s.get("created_at", "?")
        link   = s.get("original_link", "?")
        total  = s.get("total_forwarded", 0)
        chunk_map = {c["bot_index"]: c for c in s.get("chunks", [])}

        lines = [
            f"📋 <b>Session:</b> <code>{sid8}…</code>",
            f"🕐 <b>Started:</b> {ts}",
            f"🔗 <b>Link:</b> <code>{str(link)[:80]}</code>",
            f"📊 <b>Total saved so far:</b> {total}",
            "",
            "<b>Per-bot progress:</b>",
        ]

        icon_map = {"done": "✅", "running": "🔄", "failed": "❌", "pending": "⏳"}
        for p in s.get("progress", []):
            bi              = p["bot_index"]
            status          = p.get("status", "pending")
            last_mid        = p.get("last_saved_msg_id")
            last_topic      = p.get("last_saved_topic")
            count           = p.get("forwarded_count", 0)
            chunk           = chunk_map.get(bi, {})
            st              = chunk.get("start_topic")
            sm              = chunk.get("start_msg", "?")
            et              = chunk.get("end_topic")
            em              = chunk.get("end_msg", "?")
            em_str          = "end" if em == 999_999_999 else str(em)
            icon            = icon_map.get(status, "❓")

            # Build "last saved" string with both topic and msg ID when available
            if last_mid is not None:
                if last_topic is not None:
                    last_str = f"topic {last_topic} msg {last_mid}"
                else:
                    last_str = f"msg {last_mid}"
            else:
                last_str = "none yet"

            # Build "assigned range" string
            if st is not None and st != et:
                range_str = f"topics {st}→{et}, msgs {sm}→{em_str}"
            elif st is not None:
                range_str = f"topic {st}, msgs {sm}→{em_str}"
            else:
                range_str = f"msgs {sm}→{em_str}"

            # Calculate remaining messages (best-effort estimate)
            if last_mid is not None and em != "?" and em != 999_999_999:
                try:
                    remaining = int(em) - int(last_mid)
                    remain_str = f" | ~{remaining} left"
                except Exception:
                    remain_str = ""
            else:
                remain_str = ""

            lines.append(
                f"  {icon} <b>Bot{bi+1}</b>: {range_str}\n"
                f"       last saved: {last_str} | {count} file(s){remain_str}"
            )

        lines.append(f"\n<b>Status:</b> {s.get('status', '?').upper()}")

        await Bot.send_message(
            uid,
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("▶ Resume", callback_data=f"sb_resume:{sid}"),
                InlineKeyboardButton("✕ Cancel", callback_data=f"sb_cancel:{sid}"),
            ]]),
        )


# ── Resume callback ────────────────────────────────────────────────────────────

@Bot.on_callback_query(filters.regex(r"^sb_resume:"))
async def sbatch_resume_cb(client, query):
    await query.answer("▶️ Starting resume…")
    uid        = query.from_user.id
    session_id = query.data[len("sb_resume:"):]

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
    if uid in _active_sbatches:
        await query.message.edit_text(
            "⚠️ A batch is already running. Wait for it to finish first."
        )
        return

    await query.message.edit_text(
        f"▶️ <b>Resuming session</b> <code>{session_id[:8]}…</code>\n"
        "Setting up bots and continuing from last checkpoint…",
        parse_mode=ParseMode.HTML,
    )

    # ── Reconstruct context from saved session ─────────────────────────────
    all_pyro  = [Bot] + [pyro for (_, pyro) in extra_clients]
    chat_ref  = session.get("chat_ref")
    if isinstance(chat_ref, float):
        chat_ref = int(chat_ref)

    resume_chunks   = []
    resume_bot_idxs = []
    resume_pyro     = []
    progress_map    = {p["bot_index"]: p for p in session.get("progress", [])}

    for chunk in session.get("chunks", []):
        bi     = chunk["bot_index"]
        prog   = progress_map.get(bi, {})
        status = prog.get("status", "pending")

        if status == "done":
            continue   # fully finished — skip

        orig_st  = chunk.get("start_topic")
        orig_sm  = chunk.get("start_msg", 1)
        et       = chunk.get("end_topic")
        end_val  = chunk.get("end_msg", 999_999_999)

        last_saved_mid   = prog.get("last_saved_msg_id")
        last_saved_topic = prog.get("last_saved_topic")

        # ── Determine exact resume position ────────────────────────────────
        # We track both which TOPIC the bot was on (last_saved_topic) and
        # which MSG was last saved (last_saved_mid).  This lets multi-topic
        # batches resume from the right topic, not the original start topic.
        if last_saved_mid is not None:
            resume_sm = last_saved_mid + 1
            # Use saved topic if available; otherwise fall back to orig_st
            # (old checkpoint format without topic field)
            resume_st = last_saved_topic if last_saved_topic is not None else orig_st
        else:
            # No progress at all — start from the very beginning of this chunk
            resume_sm = orig_sm
            resume_st = orig_st

        # ── Edge-case: bot completed but wasn't marked done ────────────────
        # This happens when the process died between the last checkpoint
        # and the mark_bot_done() call.
        fully_past_end = False
        if et is not None and resume_st is not None and resume_st > et:
            fully_past_end = True
        elif (resume_st == et and end_val != 999_999_999 and resume_sm > end_val):
            fully_past_end = True
        elif (et is None and end_val != 999_999_999 and resume_sm > end_val):
            fully_past_end = True

        if fully_past_end:
            asyncio.create_task(_cp_bot_done(session_id, bi))
            continue

        pyro_bot = all_pyro[bi] if bi < len(all_pyro) else Bot
        resume_chunks.append((resume_st, resume_sm, et, end_val))
        resume_bot_idxs.append(bi)
        resume_pyro.append(pyro_bot)

    if not resume_chunks:
        await _cp_session_done(session_id)
        await Bot.send_message(uid, "✅ All bots already completed — session marked done.")
        return

    # ── Resolve acc ────────────────────────────────────────────────────────
    acc      = userbot
    _personal = None
    if acc is None:
        sess = await _get_user_session(uid)
        if sess:
            from pyrogram import Client as _Client
            try:
                _personal = _Client(
                    f"resume_{uid}",
                    session_string=sess,
                    api_id=int(API_ID),
                    api_hash=API_HASH,
                    in_memory=True,
                )
                await _personal.start()
                acc = _personal
            except Exception as e:
                logger.error(f"resume: personal_acc start failed: {e}")

    if acc is None:
        await Bot.send_message(uid, "❌ No session available. Use /login first.")
        return

    _acc_session_str = None
    try:
        _acc_session_str = await acc.export_session_string()
    except Exception as e:
        logger.warning(f"resume: could not export session string: {e}")

    sbatch_id = _next_id(uid)

    # ── Announce resume plan ───────────────────────────────────────────────
    ann_lines = []
    for (st, sm, et, em), bi in zip(resume_chunks, resume_bot_idxs):
        em_str  = "end" if em == 999_999_999 else str(em)
        et_str  = str(et) if et is not None else "—"
        if st is not None and st != et:
            loc = (f"topic <code>{st}</code>→<code>{et_str}</code>, "
                   f"msg <code>{sm}</code>→<code>{em_str}</code>")
        elif st is not None:
            loc = f"topic <code>{st}</code>, msg <code>{sm}</code>→<code>{em_str}</code>"
        else:
            loc = f"msg <code>{sm}</code>→<code>{em_str}</code>"
        ann_lines.append(f"  🤖 Bot{bi+1}: resuming from {loc}")
    await Bot.send_message(
        uid,
        f"⚡ <b>Resuming {len(resume_pyro)} bot(s)</b>\n" + "\n".join(ann_lines),
        parse_mode=ParseMode.HTML,
    )

    results = await _execute_sbatch(
        uid=uid,
        chat_ref=chat_ref,
        acc=acc,
        _acc_session_str=_acc_session_str,
        used_pyro=resume_pyro,
        chunks=resume_chunks,
        bot_indices=resume_bot_idxs,
        sbatch_id=sbatch_id,
        cp_session_id=session_id,
    )

    if _personal:
        try: await _personal.stop()
        except: pass

    done_count = sum(1 for r in results if r is not None)
    n          = len(results)
    await Bot.send_message(
        uid,
        f"✅ <b>Resume complete</b> — {done_count}/{n} bot(s) finished.\n"
        f"Use /sbatch_status to check the full session.",
        parse_mode=ParseMode.HTML,
    )


# ── Cancel checkpoint session callback ────────────────────────────────────────

@Bot.on_callback_query(filters.regex(r"^sb_cancel:"))
async def sbatch_cancel_cb(client, query):
    await query.answer("🚫 Cancelling…")
    session_id = query.data[len("sb_cancel:"):]

    session = await _cp_get(session_id)
    if not session:
        await query.message.edit_text("❌ Session not found.")
        return

    await _cp_cancel(session_id)
    await query.message.edit_text(
        f"🚫 Session <code>{session_id[:8]}…</code> cancelled.\n"
        "You can start a new /sbatch anytime.",
        parse_mode=ParseMode.HTML,
    )


# ── "▶ Forward All" callback handler ─────────────────────────────────────────

@Bot.on_callback_query(filters.regex(r"^sbfwd:"))
async def sbatch_forward_cb(client, query):
    await query.answer("⏳ Forwarding to DB_CHANNEL…")

    sbatch_id = query.data[len("sbfwd:"):]
    uid       = query.from_user.id
    results   = _sessions.get(sbatch_id)

    if not results:
        await query.message.edit_text(
            "❌ Session expired — please run /sbatch again."
        )
        return

    if not DB_CHANNEL:
        await query.message.edit_text("❌ `DB_CHANNEL` is not configured.")
        return

    await query.message.edit_text(
        "⏳ **Forwarding all chunks to DB_CHANNEL…**\n"
        "_(in order: Bot1 → Bot2 → … → BotN)_"
    )

    total_files = sum(len(r["file_ids"]) for r in results if r is not None)
    total_fwd   = 0
    errors: list[str] = []

    async def _update_progress(current: int, total: int, bot_n: int, bot_done: int):
        if total == 0:
            return
        filled = int(10 * current / total)
        bar    = "█" * filled + "░" * (10 - filled)
        pct    = int(100 * current / total)
        try:
            await query.message.edit_text(
                f"📤 <b>Forwarding to DB_CHANNEL…</b>\n\n"
                f"[{bar}] {pct}%\n"
                f"{current}/{total} file(s) copied\n"
                f"<i>Bot{bot_n} — {bot_done} file(s) this bot</i>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    for i, r in enumerate(results):
        if r is None:
            errors.append(f"Bot{i+1}: skipped (batch failed)")
            continue

        pyro_bot = r["pyro_bot"]
        file_ids = r.get("file_ids", [])

        if not file_ids:
            errors.append(f"Bot{i+1}: no files uploaded")
            continue

        bot_done = 0
        for msg_id in file_ids:
            try:
                await pyro_bot.copy_message(
                    chat_id=DB_CHANNEL,
                    from_chat_id=uid,
                    message_id=msg_id,
                )
                bot_done  += 1
                total_fwd += 1
            except Exception as e:
                logger.error(f"sbatch copy_message bot{i+1} id={msg_id}: {e}")
                errors.append(f"Bot{i+1} msg {msg_id}: {str(e)[:100]}")
                break
            await asyncio.sleep(0.3)
            await _update_progress(total_fwd, total_files, i + 1, bot_done)

    err_text = ("\n\n⚠️ <b>Errors:</b>\n" + "\n".join(errors)) if errors else ""
    try:
        await query.message.edit_text(
            f"✅ <b>Forward complete</b> — {total_fwd}/{total_files} file(s) → DB_CHANNEL"
            + err_text,
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass
