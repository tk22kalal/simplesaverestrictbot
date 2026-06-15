
import logging
import os
import sys
import asyncio
import json
import re

from .. import bot as gagan
from .. import userbot, Bot, API_ID, API_HASH

from main.plugins.pyroplug import download_msg, upload_downloaded, prefetch_msg
from main.plugins.helpers import get_link

from telethon import events, Button
from pyrogram import Client
from pyrogram.errors import FloodWait


# ── Per-user session helper ───────────────────────────────────────────────────

async def _get_user_session(user_id):
    """Async — reads from MongoDB if configured, else falls back to file."""
    from main.plugins.session_store import get_user_session as _gs
    return await _gs(user_id)


# ── Logging / stdout redirect ─────────────────────────────────────────────────

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)

temp_log_file = "logs.txt"

if not os.path.exists(temp_log_file):
    with open(temp_log_file, "w"):
        pass


class StreamToLogger:
    def __init__(self, lg, level, path):
        self.logger = lg
        self.log_level = level
        self.log_file = path

    def write(self, buf):
        with open(self.log_file, 'a') as f:
            f.write(buf)
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass

    def fileno(self):
        return 0


for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(filename=temp_log_file, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

sys.stdout = StreamToLogger(logging.getLogger('STDOUT'), logging.INFO, temp_log_file)
sys.stderr = StreamToLogger(logging.getLogger('STDERR'), logging.ERROR, temp_log_file)


def _reset_log():
    try:
        open(temp_log_file, "w").close()
    except Exception:
        pass


async def _log_loop():
    while True:
        await asyncio.sleep(180)
        _reset_log()


try:
    asyncio.get_running_loop().create_task(_log_loop())
except RuntimeError:
    pass


# ── /logs ─────────────────────────────────────────────────────────────

@gagan.on(events.NewMessage(incoming=True, pattern='/logs'))
async def send_log(event):
    if os.path.exists(temp_log_file):
        await gagan.send_file(event.sender_id, temp_log_file,
                              caption="Log file (last 3 min).")
    else:
        await event.respond("Log file not found.")


# ── active batch tracker ──────────────────────────────────────────────────────
# user_id → False = running, True = cancelled

active_batches = {}


# ── /cancel ─────────────────────────────────────────────────────────────

@gagan.on(events.NewMessage(incoming=True, pattern='/cancel'))
async def cancel_command(event):
    uid = event.sender_id
    if active_batches.get(uid) is False:
        active_batches[uid] = True
        await event.respond("✅ Batch cancelled.")
    else:
        await event.respond("There is no running batch to cancel.")


# ── Link parsers ───────────────────────────────────────────────────────────

def _parse_single_link(link: str):
    """
    Parse one complete Telegram message URL.
    Returns (chat_ref, topic_id_or_None, msg_id) or None.
    """
    clean = link.rstrip("/").split("?")[0]
    parts = clean.split("/")
    if len(parts) < 5:
        return None

    if "t.me/c/" in link:
        try:
            chat_ref = int("-100" + parts[4])
        except (ValueError, IndexError):
            return None
        if len(parts) >= 7:
            try:
                topic_id = int(parts[5])
                msg_id   = int(parts[6])
            except (ValueError, IndexError):
                return None
            return chat_ref, topic_id, msg_id
        else:
            try:
                msg_id = int(parts[5])
            except (ValueError, IndexError):
                return None
            return chat_ref, None, msg_id
    else:
        try:
            username = parts[3]
            msg_id   = int(parts[-1])
        except (ValueError, IndexError):
            return None
        if not username or username.lower() in ("c", "b"):
            return None
        return username, None, msg_id


def _parse_range(raw: str):
    """
    Parse a batch range in either of two formats:

    NEW  —  two full URLs joined by a hyphen:
      https://t.me/c/CHATID/TOPICID/MSGID-https://t.me/c/CHATID/TOPICID/MSGID
      https://t.me/c/CHATID/MSGID-https://t.me/c/CHATID/MSGID
      https://t.me/USERNAME/MSGID-https://t.me/USERNAME/MSGID

    OLD  —  single URL whose last segment is START-END:
      https://t.me/c/CHATID/TOPICID/START-END
      https://t.me/c/CHATID/START-END
      https://t.me/USERNAME/START-END

    Returns (chat_ref, start_topic, start_msg, end_topic, end_msg) or None.
    """
    raw = raw.strip()

    if raw.count('https://') >= 2 or raw.count('http://') >= 2:
        for sep in ('-https://', '- https://'):
            idx = raw.find(sep)
            if idx != -1:
                link1 = raw[:idx].strip()
                link2 = 'https://' + raw[idx + len(sep):]
                p1 = _parse_single_link(link1)
                p2 = _parse_single_link(link2)
                if p1 and p2 and p1[0] == p2[0]:
                    return p1[0], p1[1], p1[2], p2[1], p2[2]
        return None

    link = get_link(raw) or raw
    clean = link.rstrip("/").split("?")[0]
    parts = clean.split("/")
    if not parts:
        return None

    last = parts[-1]
    if "-" in last:
        segs = last.split("-", 1)
        try:
            start_msg = int(segs[0])
            end_msg   = int(segs[1])
        except ValueError:
            return None
        base_link = "/".join(parts[:-1] + [str(start_msg)])
    else:
        try:
            start_msg = end_msg = int(last)
        except ValueError:
            return None
        base_link = link

    p = _parse_single_link(base_link)
    if not p:
        return None
    chat_ref, topic_id, _ = p
    return chat_ref, topic_id, start_msg, topic_id, end_msg


# /batch is now handled by mbatch.py (multi-topic sequential batch).
# This stub is kept so old references don't break.


# ── Fast last-message-ID resolver (used by no-prescan stream mode) ────────────

async def _fetch_topic_last_id(acc, chat_ref, topic_id: int) -> int:
    """
    Return the ID of the most recent message in a forum topic via GetReplies.
    Returns 0 on any error (caller should skip the topic).
    Properly handles FloodWait — waits and retries instead of silently returning 0.
    """
    from pyrogram.raw.functions.messages import GetReplies
    from pyrogram.errors import FloodWait

    async def _invoke_once():
        peer   = await acc.resolve_peer(chat_ref)
        result = await acc.invoke(
            GetReplies(
                peer=peer,
                msg_id=topic_id,
                offset_id=0,
                offset_date=0,
                add_offset=0,
                limit=1,
                max_id=0,
                min_id=0,
                hash=0,
            )
        )
        if result.messages:
            return result.messages[0].id
        return 0

    try:
        return await _invoke_once()
    except FloodWait as fw:
        wait = fw.value + 2
        logger.warning(f"_fetch_topic_last_id FloodWait {wait}s (topic={topic_id})")
        await asyncio.sleep(wait)
        try:
            return await _invoke_once()
        except Exception as e2:
            logger.warning(f"_fetch_topic_last_id retry failed (topic={topic_id}): {e2}")
    except Exception as e:
        logger.warning(f"_fetch_topic_last_id(topic={topic_id}): {e}")
    return 0


# ── Forum topic discovery (GetForumTopics) ────────────────────────────────────

_TOPIC_FETCH_TIMEOUT    = 30   # seconds — per-topic GetReplies (full fetch) max wait (increased resilience)
_TOPIC_DISCOVER_TIMEOUT = 15   # seconds — per-probe GetReplies(limit=1) max wait (increased resilience)
_DISCOVER_CONCURRENCY   = 4    # parallel topic-existence checks (safe increase)


async def _discover_forum_topics(acc, peer, start_id: int, end_id: int) -> "list[int] | None":
    """
    Return a sorted list of topic IDs within [start_id, end_id] that actually
    have at least one message (i.e. are real, non-empty topics).

    Uses GetReplies(limit=1) probes run _DISCOVER_CONCURRENCY at a time so
    discovery is faster while keeping API load reasonable.  Each probe retries
    up to 5 times with exponential back‑off to survive temporary network issues.
    Returns None on unexpected failure so the caller falls back to sequential
    probing.  An empty list means no messages were found in any topic in range.
    """
    from pyrogram.raw.functions.messages import GetReplies
    from pyrogram.errors import FloodWait

    sem = asyncio.Semaphore(_DISCOVER_CONCURRENCY)
    MAX_PROBE_RETRIES = 5

    async def _check(tid: int) -> "int | None":
        """Return tid if the topic has messages, else None."""
        async with sem:
            for attempt in range(MAX_PROBE_RETRIES):
                try:
                    r = await asyncio.wait_for(
                        acc.invoke(
                            GetReplies(
                                peer=peer,
                                msg_id=tid,
                                offset_id=0,
                                offset_date=0,
                                add_offset=0,
                                limit=1,        # only need to know "exists?"
                                max_id=0,
                                min_id=0,
                                hash=0,
                            )
                        ),
                        timeout=_TOPIC_DISCOVER_TIMEOUT,
                    )
                    return tid if r.messages else None
                except asyncio.TimeoutError:
                    if attempt < MAX_PROBE_RETRIES - 1:
                        # exponential back‑off: 2s, 4s, 6s, 8s
                        await asyncio.sleep(2 * (attempt + 1))
                    else:
                        logger.warning(f"_discover: topic {tid} timed out after {MAX_PROBE_RETRIES} attempts")
                        return None
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 1)
                    continue
                except Exception as exc:
                    # Transient network/RPC error — retry instead of treating
                    # the topic as non-existent.  Only give up on the last attempt.
                    if attempt < MAX_PROBE_RETRIES - 1:
                        await asyncio.sleep(2 * (attempt + 1))
                        continue
                    logger.warning(f"_discover: topic {tid} probe failed after {MAX_PROBE_RETRIES} attempts: {exc}")
                    return None
            return None

    all_ids = list(range(start_id, end_id + 1))
    try:
        results = await asyncio.gather(*[_check(tid) for tid in all_ids])
    except Exception as e:
        logger.warning(f"_discover_forum_topics: gather failed: {e}")
        return None

    found = sorted(tid for tid in results if tid is not None)
    return found   # empty list is valid: caller decides what to do


# ── Direct topic-message-ID fetcher (GetReplies pagination) ──────────────────

async def _fetch_topic_msg_ids(acc, chat_ref, topic_id: int,
                                min_id: int = 1, max_id: int = 0,
                                peer=None) -> list:
    """
    Return all message IDs that actually exist in a forum topic, sorted ascending.

    Uses GetReplies pagination (newest-first) and collects every ID >= min_id
    (and <= max_id when max_id > 0).  This avoids scanning the entire global
    message-ID space for a topic whose IDs may start at a very high number.

    Now with robust retries: timeouts retry up to 10 times with exponential
    back‑off (max 60s).  FloodWaits are handled automatically.  The function
    will NOT give up on a page just because of a few slow responses.
    """
    from pyrogram.raw.functions.messages import GetReplies
    from pyrogram.errors import FloodWait

    all_ids   = []
    offset_id = 0          # 0 → start from newest message
    seen      = set()
    timeout_retries = 0
    MAX_TIMEOUT_RETRIES = 10          # much more tolerant

    if peer is None:
        try:
            peer = await acc.resolve_peer(chat_ref)
        except Exception as e:
            logger.warning(f"_fetch_topic_msg_ids resolve_peer failed: {e}")
            return []

    while True:
        try:
            result = await asyncio.wait_for(
                acc.invoke(
                    GetReplies(
                        peer=peer,
                        msg_id=topic_id,
                        offset_id=offset_id,
                        offset_date=0,
                        add_offset=0,
                        limit=100,
                        max_id=0,
                        min_id=max(0, min_id - 1),   # exclusive lower bound
                        hash=0,
                    )
                ),
                timeout=_TOPIC_FETCH_TIMEOUT,
            )
            timeout_retries = 0   # successful page — reset retry counter
        except asyncio.TimeoutError:
            timeout_retries += 1
            if timeout_retries <= MAX_TIMEOUT_RETRIES:
                wait = min(2 ** timeout_retries, 60)   # exponential back‑off capped at 60s
                logger.warning(
                    f"_fetch_topic_msg_ids timeout {timeout_retries}/{MAX_TIMEOUT_RETRIES} "
                    f"(topic={topic_id}, offset={offset_id}), sleeping {wait}s"
                )
                await asyncio.sleep(wait)
                continue
            # Too many timeouts — unlikely to recover, stop here
            logger.error(
                f"_fetch_topic_msg_ids: too many timeouts for topic={topic_id}, "
                f"collected {len(all_ids)} messages so far – ABORTING"
            )
            break
        except FloodWait as fw:
            wait = fw.value + 2
            logger.warning(f"_fetch_topic_msg_ids FloodWait {wait}s (topic={topic_id})")
            await asyncio.sleep(wait)
            continue
        except Exception as e:
            logger.warning(f"_fetch_topic_msg_ids error (topic={topic_id}): {e}")
            break

        if not result.messages:
            break

        batch_min = offset_id   # track oldest ID in this batch
        for m in result.messages:
            mid = m.id
            if mid in seen:
                continue
            seen.add(mid)
            if mid < min_id:
                continue
            if max_id > 0 and mid > max_id:
                continue
            all_ids.append(mid)
            if batch_min == 0 or mid < batch_min:
                batch_min = mid

        # Stop if we've reached or passed the min_id boundary
        if batch_min <= min_id:
            break

        # Next page: fetch messages older than the oldest we just saw
        offset_id = batch_min

    return sorted(all_ids)


# ── Stream-mode batch (no prescan) ────────────────────────────────────────────

async def _run_batch_noScan(acc, client, sender, chat_ref, raw_chat,
                             start_topic, start_msg, end_topic, end_msg,
                             _bdict, status_msg, collected_ids,
                             checkpoint_fn=None):
    """
    Fast extraction without ID-range scanning.

    For topics: first tries GetForumTopics to discover which topic IDs actually
    exist in the range, then uses GetReplies only for those real topics.
    Falls back to sequential probing when GetForumTopics is unavailable.
    Every API call is guarded by a timeout so bots can never hang forever.

    For plain channels (no topic): uses a direct ID range as before.
    """
    has_topics   = start_topic is not None and start_topic != end_topic
    single_topic = start_topic is not None and start_topic == end_topic

    saved     = 0
    skipped   = 0
    failed    = 0   # media messages that failed download or upload (unexpected)
    cancelled = False

    def _make_link(topic, mid):
        if isinstance(chat_ref, int):
            if topic is not None:
                return f"https://t.me/c/{raw_chat}/{topic}/{mid}"
            return f"https://t.me/c/{raw_chat}/{mid}"
        return f"https://t.me/{raw_chat}/{mid}"

    # ── Pre-resolve peer ONCE for all topic/message lookups ──────────────────
    # Avoids a redundant resolve_peer() call per topic (which is an API round-trip).
    shared_peer = None
    if has_topics or single_topic:
        try:
            shared_peer = await acc.resolve_peer(chat_ref)
        except Exception as e:
            logger.warning(f"_run_batch_noScan: resolve_peer failed: {e}")

    # ── Discover real topic IDs in range (multi-topic mode) ───────────────────
    if has_topics and shared_peer is not None:
        n_ids = end_topic - start_topic + 1
        try:
            await status_msg.edit_text(
                f"🔍 Scanning {n_ids} topic IDs ({start_topic}→{end_topic}) "
                f"with {_DISCOVER_CONCURRENCY} parallel probes…"
            )
        except Exception:
            pass

        discovered = await _discover_forum_topics(
            acc, shared_peer, start_topic, end_topic
        )

        if discovered is not None and len(discovered) > 0:
            topics = discovered
            logger.info(
                f"_run_batch_noScan: discovered {len(topics)} real topics "
                f"in range {start_topic}→{end_topic} "
                f"(skipped {(end_topic - start_topic + 1) - len(topics)} non-existent IDs)"
            )
            try:
                await status_msg.edit_text(
                    f"✅ Found {len(topics)} topic(s) in range {start_topic}→{end_topic}\n"
                    f"⏳ Starting extraction…"
                )
            except Exception:
                pass
        elif discovered is not None and len(discovered) == 0:
            # Discovery returned empty — could be FloodWait killing all probes
            # (not just a truly empty range).  Fall back to sequential scan so
            # we never silently skip real content.
            topics = list(range(start_topic, end_topic + 1))
            logger.warning(
                f"_run_batch_noScan: discovery returned 0 topics in "
                f"{start_topic}→{end_topic} — falling back to sequential scan "
                f"({len(topics)} IDs)"
            )
            try:
                await status_msg.edit_text(
                    f"⚠️ Discovery found 0 topics — scanning {len(topics)} IDs sequentially…"
                )
            except Exception:
                pass
        else:
            # Discovery unavailable (not a forum or API error) — fall back
            topics = list(range(start_topic, end_topic + 1))
            logger.info(
                f"_run_batch_noScan: discovery unavailable — "
                f"probing {len(topics)} IDs sequentially"
            )
    elif has_topics:
        # No peer resolved — fall back to full range
        topics = list(range(start_topic, end_topic + 1))
    elif single_topic:
        topics = [start_topic]
    else:
        topics = [None]

    total_topics    = len(topics)
    consecutive_empty = 0   # track consecutive skipped/empty topics for status

    for topic_num, topic in enumerate(topics, 1):
        if _bdict.get(sender):
            cancelled = True
            break

        # ── Yield to event loop so other coroutines stay alive ────────────
        await asyncio.sleep(0)

        # ── Status update — only on real progress or every 10 skips ──────
        if total_topics > 1:
            show_status = (consecutive_empty == 0 or consecutive_empty % 10 == 0
                           or topic_num == 1 or topic_num == total_topics)
            if show_status:
                skip_note = (f" | ⏭️ {consecutive_empty} empty"
                             if consecutive_empty > 0 else "")
                try:
                    await status_msg.edit_text(
                        f"⏳ Topic `{topic}` ({topic_num}/{total_topics}){skip_note}\n"
                        f"✅ Saved: `{saved}` | ⏭️ Skipped: `{skipped}`"
                    )
                except Exception:
                    pass

        # ── Resolve the list of message IDs to process ─────────────────────
        if topic is not None:
            first_mid    = start_msg if topic == start_topic else 1
            last_mid_cap = end_msg   if (topic == end_topic and end_msg != 999_999_999) else 0

            if last_mid_cap > 0:
                # Known upper bound (from /fbatch output) — use direct scan so no
                # message is ever silently missed due to GetReplies pagination gaps.
                msg_ids = await _scan_topic(
                    acc, chat_ref, topic,
                    start_mid=first_mid,
                    end_mid=last_mid_cap,
                    seen_ids=set(),
                )
            else:
                # Open-ended topic — GetReplies is faster for unknown bounds.
                msg_ids = await _fetch_topic_msg_ids(
                    acc, chat_ref, topic,
                    min_id=first_mid,
                    max_id=last_mid_cap,
                    peer=shared_peer,
                )
            if not msg_ids:
                # Topic empty, deleted, timed-out, or doesn't exist — skip instantly
                consecutive_empty += 1
                skipped += 1
                continue

            consecutive_empty = 0          # reset consecutive counter on found topic
            end_link = _make_link(topic, msg_ids[-1])

        else:
            # Plain channel / no topic — keep original range approach
            first_mid = start_msg
            last_mid  = end_msg
            end_link  = _make_link(None, last_mid)
            msg_ids   = list(range(first_mid, last_mid + 1))

        # ── Process msg_ids in groups of GROUP_SIZE ─────────────────────────
        for group_start in range(0, len(msg_ids), GROUP_SIZE):
            if _bdict.get(sender):
                cancelled = True
                break

            group_ids = msg_ids[group_start : group_start + GROUP_SIZE]
            g         = len(group_ids)

            ready_ev = [asyncio.Event() for _ in range(g)]
            go_ev    = [asyncio.Event() for _ in range(g)]
            done_ev  = [asyncio.Event() for _ in range(g)]

            group_tasks = []

            for g_idx, mid in enumerate(group_ids):
                if _bdict.get(sender):
                    cancelled = True
                    for rem in range(g_idx, g):
                        ready_ev[rem].set()
                        done_ev[rem].set()
                    break

                link = _make_link(topic, mid)

                # ── prefetch with timeout so a hanging API call never stalls the batch ──
                try:
                    prefetched = await asyncio.wait_for(
                        prefetch_msg(acc, link, mid),
                        timeout=60,
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"stream: prefetch_msg timed out for mid={mid} — skipping")
                    ready_ev[g_idx].set()
                    done_ev[g_idx].set()
                    skipped += 1
                    continue
                except Exception as _pfe:
                    logger.warning(f"stream: prefetch_msg error mid={mid}: {_pfe} — skipping")
                    ready_ev[g_idx].set()
                    done_ev[g_idx].set()
                    skipped += 1
                    continue

                # ── Skip empty / deleted / non-media messages ──────────────
                if prefetched is None:
                    ready_ev[g_idx].set()
                    done_ev[g_idx].set()
                    skipped += 1
                    continue

                # ── Skip messages with no media at all (text-only) ────────────
                # Allow all media types: VIDEO, VIDEO_NOTE, ANIMATION, PHOTO,
                # DOCUMENT, AUDIO, VOICE, STICKER, etc.
                try:
                    if prefetched.media is None:
                        ready_ev[g_idx].set()
                        done_ev[g_idx].set()
                        skipped += 1
                        continue
                except Exception:
                    pass   # if check fails, proceed normally

                # For topic-based msg_ids we already filtered by topic via
                # GetReplies, so no secondary topic-match check is needed.
                # For the None-topic (plain channel) case there is no topic to check.

                msg_footer = f"🔗 {link}\n📋 End: {end_link}"

                try:
                    pm = await client.send_message(
                        sender, f"⬇️ Downloading…\n{link}"
                    )
                except Exception as e:
                    logger.error(f"stream: progress msg send failed ({link}): {e}")
                    ready_ev[g_idx].set()
                    done_ev[g_idx].set()
                    skipped += 1
                    continue

                # ── download with hard timeout — prevents infinite stall when
                #    Telegram stops responding to a rate-limited bot token ────
                try:
                    dl = await asyncio.wait_for(
                        download_msg(
                            acc, client, sender, link, mid,
                            source_link="⬇️ Downloading",
                            batch_range=msg_footer,
                            prefetched_msg=prefetched,
                            progress_msg=pm,
                        ),
                        timeout=900,   # 15 min max per file
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"stream: download_msg TIMED OUT mid={mid} — skipping")
                    try:
                        await pm.edit_text(f"⚠️ Skipped — download timed out\nmsg `{mid}`")
                    except Exception:
                        pass
                    ready_ev[g_idx].set()
                    done_ev[g_idx].set()
                    failed += 1
                    continue

                if dl is None:
                    # download_msg already edited pm with the failure reason.
                    # Write a fallback message in case that edit itself failed.
                    logger.warning(
                        f"stream: SKIP mid={mid} — download_msg returned None | link={link}"
                    )
                    try:
                        await pm.edit_text(
                            f"⚠️ Skipped — download failed\nmsg `{mid}`"
                        )
                    except Exception:
                        pass   # pm was already edited/deleted by download_msg
                    ready_ev[g_idx].set()
                    done_ev[g_idx].set()
                    failed += 1
                    continue

                msg_obj, file_str, returned_pm = dl

                # ── Barrier progress callback ──────────────────────────────
                from main.plugins.progress import progress_for_pyrogram as _base_prog
                _signalled = [False]
                _rdy       = ready_ev[g_idx]
                _go        = go_ev[g_idx]

                async def _pf(current, total, bot, ud_type, message, start, footer,
                               __signalled=_signalled, __rdy=_rdy, __go=_go):
                    remaining = total - current
                    if not __signalled[0] and (
                        total <= NEAR_FINISH_THRESHOLD or
                        remaining <= NEAR_FINISH_THRESHOLD or
                        current == total
                    ):
                        __signalled[0] = True
                        __rdy.set()
                        await __go.wait()
                    await _base_prog(current, total, bot, ud_type, message, start, footer)

                async def _upload_task(
                    _g_idx=g_idx, _link=link, _footer=msg_footer,
                    _msg=msg_obj, _fs=file_str, _pm=returned_pm, _pf_fn=_pf,
                    _done=done_ev[g_idx], _rdy=ready_ev[g_idx],
                    _mid=mid,                   # source msg ID for checkpointing
                    _topic=topic,               # source topic ID for checkpointing
                    _cpfn=checkpoint_fn,         # checkpoint callback (may be None)
                ):
                    try:
                        sent = await upload_downloaded(
                            acc, client, sender, _msg, _fs, _pm,
                            source_link="⬆️ Uploading",
                            batch_range=_footer,
                            _progress_fn=_pf_fn,
                        )
                        if sent is not None and collected_ids is not None:
                            try:
                                collected_ids.append(sent.id)
                            except Exception:
                                pass
                        if sent is not None and _cpfn is not None:
                            try:
                                await _cpfn(_mid, _topic)
                            except Exception as _ce:
                                logger.warning(f"checkpoint_fn error mid={_mid} topic={_topic}: {_ce}")
                        if sent is None:
                            logger.warning(
                                f"stream: UPLOAD FAILED (all methods exhausted) "
                                f"mid={_mid} | link={_link}"
                            )
                            try:
                                await client.send_message(
                                    sender,
                                    f"⚠️ Skipped — upload failed (all methods exhausted)\n"
                                    f"msg `{_mid}` · {_link}"
                                )
                            except Exception:
                                pass
                        return bool(sent)
                    except Exception as e:
                        logger.error(
                            f"stream: UPLOAD EXCEPTION mid={_mid} | link={_link} | {e}",
                            exc_info=True,
                        )
                        try:
                            await client.send_message(
                                sender,
                                f"⚠️ Skipped — upload error\n"
                                f"msg `{_mid}` · {_link}\n"
                                f"`{str(e)[:200]}`"
                            )
                        except Exception:
                            pass
                        return False
                    finally:
                        # Always unblock the coordinator even if _pf was never
                        # called (e.g. photos bypass the progress callback path).
                        _rdy.set()
                        _done.set()

                group_tasks.append((g_idx, asyncio.create_task(_upload_task())))

            # ── Group coordinator: ordered release ─────────────────────────
            async def _coord(_g=g, _rev=ready_ev, _gev=go_ev, _dev=done_ev):
                for ev in _rev:
                    await ev.wait()
                for i in range(_g):
                    _gev[i].set()
                    await _dev[i].wait()

            await _coord()
            if group_tasks:
                results = await asyncio.gather(
                    *[t for _, t in group_tasks], return_exceptions=True
                )
                for r in results:
                    if r is True:
                        saved += 1
                    else:
                        failed += 1   # False or Exception → upload failure

        if cancelled:
            break

    _failed_line = f"\n⚠️ **Errors (download/upload failed):** `{failed}`" if failed else ""
    summary = (
        f"{'🚫 Batch cancelled' if cancelled or _bdict.get(sender) else '✅ **Batch complete!**'}\n\n"
        f"📦 **Saved:** `{saved}`\n"
        f"⏭️ **Skipped** (no media / text-only): `{skipped}`"
        f"{_failed_line}"
    )
    try:
        await status_msg.edit_text(summary)
    except Exception:
        await client.send_message(sender, summary)


# ── Batch scan ────────────────────────────────────────────────────────────

SCAN_BATCH = 100          # max messages per get_messages call
MAX_EMPTY_BATCHES = 5     # stop an open-ended scan after this many all-empty batches


async def _scan_topic(acc, chat_id, topic_id, start_mid, end_mid, seen_ids):
    """
    Scan [start_mid, end_mid] (or open-ended if end_mid is None) and return
    sorted list of msg_ids that exist, belong to topic_id, and aren't in seen_ids.
    Updates seen_ids in place to prevent cross-topic duplicates.

    When end_mid is known (not None), the FULL range is ALWAYS scanned — no
    early exit on empty batches.  Empty-batch early-exit only applies to
    open-ended scans (end_mid is None) to prevent looping through billions of
    non-existent IDs with a sentinel like 999_999_999.
    """
    valid = []
    empty_batches = 0
    mid = start_mid
    _MAX_GET_RETRIES = 4   # retry get_messages on transient errors

    while True:
        if end_mid is not None and mid > end_mid:
            break
        # Only stop early on consecutive empty batches for open-ended scans.
        # When end_mid is known we MUST scan to the end — channels/groups can
        # have large gaps of deleted/empty IDs (>200) that would otherwise stop
        # the scan before reaching real messages further in the range.
        if end_mid is None and empty_batches >= MAX_EMPTY_BATCHES:
            break

        chunk_end = (mid + SCAN_BATCH - 1) if end_mid is None else min(mid + SCAN_BATCH - 1, end_mid)
        ids = list(range(mid, chunk_end + 1))

        msgs_list = None
        for _attempt in range(_MAX_GET_RETRIES):
            try:
                msgs_list = await asyncio.wait_for(
                    acc.get_messages(chat_id, ids),
                    timeout=30,
                )
                break
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
            except Exception as e:
                logger.error(
                    f"_scan_topic get_messages attempt {_attempt+1}/{_MAX_GET_RETRIES} "
                    f"(chat={chat_id}, ids={ids[0]}-{ids[-1]}): {e}"
                )
                if _attempt < _MAX_GET_RETRIES - 1:
                    await asyncio.sleep(3 * (_attempt + 1))

        if msgs_list is None:
            # All retries exhausted — advance past this chunk but do NOT stop
            logger.warning(
                f"_scan_topic: get_messages permanently failed for ids {ids[0]}-{ids[-1]}, "
                f"skipping chunk but continuing scan"
            )
            mid = chunk_end + 1
            empty_batches += 1
            continue

        found_in_chunk = False
        for m in sorted(msgs_list, key=lambda x: x.id):
            if m.empty or m.service:
                continue
            mid_val = m.id
            if mid_val in seen_ids:
                continue

            # Topic verification for forum supergroups.
            if topic_id is not None:
                tid = (
                    getattr(m, 'message_thread_id', None)
                    or getattr(m, 'reply_to_top_message_id', None)
                )
                if tid is not None and tid != topic_id:
                    continue

            seen_ids.add(mid_val)
            valid.append(mid_val)
            found_in_chunk = True

        empty_batches = 0 if found_in_chunk else empty_batches + 1
        mid = chunk_end + 1

    return sorted(valid)


async def _build_items(acc, client, sender, chat_ref, raw_chat,
                       start_topic, start_msg, end_topic, end_msg,
                       status_msg):
    """
    Collect all (topic, msg_id, link_str) triples in chronological order.
    """
    has_topics = start_topic is not None
    seen_ids = set()
    items = []

    def _make_link(topic, mid):
        if isinstance(chat_ref, int):
            if topic is not None:
                return f"https://t.me/c/{raw_chat}/{topic}/{mid}"
            return f"https://t.me/c/{raw_chat}/{mid}"
        return f"https://t.me/{raw_chat}/{mid}"

    if has_topics and start_topic != end_topic:
        topics = list(range(start_topic, end_topic + 1))
        total_topics = len(topics)

        for i, topic in enumerate(topics):
            first_mid = start_msg if topic == start_topic else 1
            last_mid  = end_msg   if topic == end_topic   else None

            try:
                await status_msg.edit_text(
                    f"🔍 Scanning topic `{topic}` ({i+1}/{total_topics})…"
                )
            except Exception:
                pass

            valid_ids = await _scan_topic(
                acc, chat_ref, topic, first_mid, last_mid, seen_ids
            )

            for mid in valid_ids:
                items.append((topic, mid, _make_link(topic, mid)))

    elif has_topics:
        valid_ids = await _scan_topic(
            acc, chat_ref, start_topic, start_msg, end_msg, seen_ids
        )
        for mid in valid_ids:
            items.append((start_topic, mid, _make_link(start_topic, mid)))

    else:
        for mid in range(start_msg, end_msg + 1):
            items.append((None, mid, _make_link(None, mid)))

    return items


# ── Batch runner (GROUP-BASED, ordered finish per group) ─────────────────────

GROUP_SIZE = 1                     # sequential — no race conditions
NEAR_FINISH_THRESHOLD = 3 * 1024 * 1024   # 3 MB


async def _run_batch(acc, client, sender, chat_ref,
                     start_topic, start_msg, end_topic, end_msg,
                     batches_dict=None, status_msg=None, collected_ids=None,
                     no_prescan=False, checkpoint_fn=None):
    _bdict = batches_dict if batches_dict is not None else active_batches

    if isinstance(chat_ref, int):
        raw_chat = str(chat_ref).replace("-100", "")
    else:
        raw_chat = str(chat_ref)

    # ── Send (or reuse) the single status message ─────────────────────────────
    if status_msg is None:
        if start_topic is not None and start_topic != end_topic:
            display = (f"Topics `{start_topic}`→`{end_topic}`, "
                       f"msgs `{start_msg}`→…→`{end_msg}`")
        elif start_topic is not None:
            display = f"Topic `{start_topic}`, msgs `{start_msg}`→`{end_msg}`"
        else:
            display = f"Msgs `{start_msg}` → `{end_msg}`"

        status_msg = await client.send_message(
            sender,
            f"🔍 **Batch starting**\nRange: {display}\n⏳ Starting…"
        )

    # ── Fast stream mode: skip scan phase entirely ────────────────────────────
    if no_prescan:
        await _run_batch_noScan(
            acc, client, sender, chat_ref, raw_chat,
            start_topic, start_msg, end_topic, end_msg,
            _bdict, status_msg, collected_ids,
            checkpoint_fn=checkpoint_fn,
        )
        return

    # ── Phase 1: scan ─────────────────────────────────────────────────────────
    items = await _build_items(
        acc, client, sender, chat_ref, raw_chat,
        start_topic, start_msg, end_topic, end_msg,
        status_msg
    )

    n = len(items)
    if n == 0:
        try:
            await status_msg.edit_text("⚠️ No messages found in the given range.")
        except Exception:
            await client.send_message(sender, "⚠️ No messages found in the given range.")
        return

    try:
        await status_msg.edit_text(
            f"✅ Found **{n}** message(s) — processing groups of {GROUP_SIZE}\n"
            f"⬇️ Downloads serial, ⬆️ uploads parallel | ordered finish per group"
        )
    except Exception:
        pass

    overall_range = f"{items[0][2]}-{items[-1][2]}"

    # ── Phase 2: pre-fetch ALL messages in parallel (so we don't wait later) ──
    fetched_all = await asyncio.gather(*[
        prefetch_msg(acc, link, mid)
        for (topic, mid, link) in items
    ], return_exceptions=True)
    fetched_all = [
        v if not isinstance(v, BaseException) else None for v in fetched_all
    ]

    # Barrier events for every item (global arrays)
    ready_events = [asyncio.Event() for _ in range(n)]
    go_events    = [asyncio.Event() for _ in range(n)]
    done_events  = [asyncio.Event() for _ in range(n)]

    def _make_barrier_progress(idx):
        """Progress callback that pauses near the finish line for item 'idx'."""
        from main.plugins.progress import progress_for_pyrogram as _base_prog
        signalled = [False]
        rdy = ready_events[idx]
        go  = go_events[idx]

        async def _prog(current, total, bot, ud_type, message, start, footer):
            nonlocal signalled
            remaining = total - current
            if not signalled[0] and (
                total <= NEAR_FINISH_THRESHOLD or
                remaining <= NEAR_FINISH_THRESHOLD or
                current == total
            ):
                signalled[0] = True
                rdy.set()
                await go.wait()
            await _base_prog(current, total, bot, ud_type, message, start, footer)

        return _prog

    saved     = 0
    skipped   = 0
    failed    = 0
    cancelled = False

    # ── Phase 3: process groups ───────────────────────────────────────────────
    for group_start in range(0, n, GROUP_SIZE):
        if _bdict.get(sender):
            cancelled = True
            skipped += n - group_start
            break

        group_end = min(group_start + GROUP_SIZE, n)
        group_indices = list(range(group_start, group_end))

        # ── Download each item in the group (sequential) ──────────────────
        group_tasks = []   # (idx, upload_task)

        for idx in group_indices:
            if _bdict.get(sender):
                cancelled = True
                # Mark remaining items as skipped
                for rem_idx in range(idx, group_end):
                    ready_events[rem_idx].set()
                    done_events[rem_idx].set()
                    skipped += 1
                break

            prefetched = fetched_all[idx]
            if prefetched is None:
                # No media – mark as done and skip
                ready_events[idx].set()
                done_events[idx].set()
                skipped += 1
                continue

            topic, mid, link = items[idx]

            # Create progress message
            try:
                pm = await client.send_message(sender, f"⬇️ Downloading…\n`{link}`")
            except Exception as e:
                logger.error(f"Failed to send progress msg for {link}: {e}")
                ready_events[idx].set()
                done_events[idx].set()
                skipped += 1
                continue

            # Download (blocks) — hard timeout prevents infinite hang when
            # Telegram stops responding to a rate-limited bot token
            try:
                dl = await asyncio.wait_for(
                    download_msg(
                        acc, client, sender, link, mid,
                        source_link=link,
                        batch_range=overall_range,
                        prefetched_msg=prefetched,
                        progress_msg=pm,
                    ),
                    timeout=900,   # 15 min max per file
                )
            except asyncio.TimeoutError:
                logger.warning(f"batch: download_msg TIMED OUT mid={mid} — skipping")
                try:
                    await pm.edit_text(f"⚠️ Skipped — download timed out\nmsg `{mid}`")
                except Exception:
                    pass
                ready_events[idx].set()
                done_events[idx].set()
                failed += 1
                continue

            if dl is None:
                logger.warning(
                    f"stream: SKIP mid={mid} — download_msg returned None | link={link}"
                )
                try:
                    await pm.edit_text(
                        f"⚠️ Skipped — download failed\nmsg `{mid}`"
                    )
                except Exception:
                    pass
                ready_events[idx].set()
                done_events[idx].set()
                failed += 1
                continue

            msg_obj, file_str, returned_pm = dl

            # Spawn upload task immediately (runs in parallel with next downloads)
            pf = _make_barrier_progress(idx)

            # IMPORTANT: msg_obj / file_str / returned_pm / pf are loop variables —
            # capture them as default-argument values so each closure snapshot is
            # independent.  Without this, all tasks in the group share the same
            # (last iteration) values via Python's late-binding closure.
            async def _upload_and_signal(
                idx=idx, link=link, _mid=mid,
                _msg=msg_obj, _fs=file_str, _pm=returned_pm, _pf=pf,
            ):
                try:
                    sent = await upload_downloaded(
                        acc, client, sender, _msg, _fs, _pm,
                        source_link=link,
                        batch_range=overall_range,
                        _progress_fn=_pf,
                    )
                    if sent is not None and collected_ids is not None:
                        try:
                            collected_ids.append(sent.id)
                        except Exception:
                            pass
                    if sent is None:
                        logger.warning(
                            f"stream: UPLOAD FAILED (all methods exhausted) "
                            f"mid={_mid} | link={link}"
                        )
                        try:
                            await client.send_message(
                                sender,
                                f"⚠️ Skipped — upload failed (all methods exhausted)\n"
                                f"msg `{_mid}` · {link}"
                            )
                        except Exception:
                            pass
                    return bool(sent)
                except Exception as e:
                    logger.error(
                        f"stream: UPLOAD EXCEPTION mid={_mid} | link={link} | {e}",
                        exc_info=True,
                    )
                    try:
                        await client.send_message(
                            sender,
                            f"⚠️ Skipped — upload error\n"
                            f"msg `{_mid}` · {link}\n"
                            f"`{str(e)[:200]}`"
                        )
                    except Exception:
                        pass
                    return False
                finally:
                    ready_events[idx].set()  # unblock coordinator even if _pf never fired
                    done_events[idx].set()

            group_tasks.append((idx, asyncio.create_task(_upload_and_signal())))

        # ── Wait for all uploads in this group to finish (ordered release) ─

        async def _group_coordinator():
            """Release go_events in strict index order, but only after
            all uploads in the group have signalled ready (hit the barrier)."""
            # Wait for all ready events of this group
            for idx in group_indices:
                await ready_events[idx].wait()

            # Now release them in order, waiting for each to complete
            for idx in group_indices:
                go_events[idx].set()
                await done_events[idx].wait()

        # Run the coordinator (it will run until all uploads in the group finish)
        await _group_coordinator()

        # Reconcile saved/failed counts from actual task results
        if group_tasks:
            _results = await asyncio.gather(*[t for _, t in group_tasks], return_exceptions=True)
            for _r in _results:
                if _r is True:
                    saved += 1
                else:
                    failed += 1   # False or Exception → upload failure

        # Status update after each group
        if (group_end) % 5 == 0 or group_end == n:
            try:
                _f_note = f" | ⚠️ Errors: `{failed}`" if failed else ""
                await status_msg.edit_text(
                    f"🔄 **Processing** ({group_end}/{n})\n"
                    f"✅ Saved: `{saved}` | ⏭️ Skipped: `{skipped}`{_f_note}"
                )
            except Exception:
                pass

    # ── Final summary ─────────────────────────────────────────────────────────
    _failed_line = f"\n⚠️ **Errors (download/upload failed):** `{failed}`" if failed else ""
    summary = (
        f"{'🚫 Batch cancelled' if cancelled or _bdict.get(sender) else '✅ **Batch complete!**'}\n\n"
        f"📦 **Saved:** `{saved}`\n"
        f"⏭️ **Skipped** (no media / text-only): `{skipped}`"
        f"{_failed_line}\n"
        f"📋 **Total scanned:** `{n}`"
    )
    try:
        await status_msg.edit_text(summary)
    except Exception:
        await client.send_message(sender, summary)

