# Join t.me/dev_gagan

import asyncio, time, os, uuid

from pyrogram.enums import ParseMode, MessageMediaType

DOWNLOADS_DIR = os.path.join(os.getcwd(), "downloads")
os.makedirs(DOWNLOADS_DIR, exist_ok=True)


def _unique_dl_prefix(sender, msg_id):
    """
    Return a unique file path PREFIX (no extension) inside DOWNLOADS_DIR.

    Pyrogram appends the correct extension (e.g. .mp4) automatically when
    no extension is present in file_name.  Using a unique prefix per
    download ensures no two concurrent bots or batches ever share the same
    .temp filename, which is the root cause of [Errno 2] collisions.
    """
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)   # re-ensure dir exists at call time
    short_id = uuid.uuid4().hex[:12]
    return os.path.join(DOWNLOADS_DIR, f"dl_{sender}_{msg_id}_{short_id}")

from .. import Bot, bot
from main.plugins.progress import progress_for_pyrogram
from main.plugins.helpers import screenshot

from pyrogram import Client, filters
from pyrogram.errors import ChannelBanned, ChannelInvalid, ChannelPrivate, ChatIdInvalid, ChatInvalid, FloodWait
from main.plugins.helpers import video_metadata
from telethon import events

import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.INFO)
logging.getLogger("telethon").setLevel(logging.INFO)

user_chat_ids = {}

def thumbnail(sender):
    return f'{sender}.jpg' if os.path.exists(f'{sender}.jpg') else f'thumb.jpg'


def _resolve_dl(file):
    """Normalize the path returned by Pyrogram's download_media.

    Pyrogram writes to a .temp file first and renames on success, but can
    return the .temp path even after the rename completed.  This helper
    returns the real final path as a plain str, or None when the file is gone.
    """
    if not file:
        return None
    s = str(file)
    if s.endswith('.temp'):
        final = s[:-5]          # strip trailing .temp
        if os.path.exists(final):
            return final        # rename already happened — use final path
        if os.path.exists(s):
            return s            # still in .temp state (very brief window)
        return None             # both gone — download failed
    if not os.path.exists(s):
        return None
    return s                    # always return plain str


def _safe_ext(filepath):
    """Return the file extension including dot, e.g. '.mp4'.
    Falls back to '' if no extension found.
    Uses os.path.splitext which correctly handles dots in directory names.
    """
    return os.path.splitext(str(filepath))[1].lower()


def _safe_stem(filepath):
    """Return the file path without its final extension.
    Uses os.path.splitext which correctly handles dots in filenames.
    """
    return os.path.splitext(str(filepath))[0]


async def _get_thumb(acc, msg, sender, file, duration):
    """
    Return a thumbnail path, tried in priority order:
      1. User's custom thumbnail  ({sender}.jpg)
      2. Original message's embedded thumbnail (downloaded from Telegram)
      3. Screenshot generated from the video file via FFmpeg
      4. None  (Telegram will show a black/blank frame — last resort)
    """
    if os.path.exists(f'{sender}.jpg'):
        return f'{sender}.jpg'

    try:
        media = msg.video or msg.document or msg.animation
        if media and getattr(media, 'thumbs', None):
            t = await asyncio.wait_for(
                acc.download_media(media.thumbs[-1], file_name=_unique_dl_prefix(sender, "thumb")),
                timeout=15,
            )
            t = _resolve_dl(t)
            if t and os.path.exists(t):
                return t
    except Exception:
        pass

    try:
        t = await screenshot(file, duration, sender)
        if t and os.path.exists(str(t)):
            return str(t)
    except Exception:
        pass

    return None


async def copy_message_with_chat_id(client, sender, chat_id, message_id):
    target_chat_id = user_chat_ids.get(sender, sender)
    try:
        await client.copy_message(target_chat_id, chat_id, message_id)
    except Exception as e:
        error_message = f"Error occurred while sending message to chat ID {target_chat_id}: {str(e)}"
        await client.send_message(sender, error_message)
        await client.send_message(sender, f"Make Bot admin in your Channel - {target_chat_id} and restart the process after /cancel")

async def send_message_with_chat_id(client, sender, message, parse_mode=None):
    chat_id = user_chat_ids.get(sender, sender)
    try:
        await client.send_message(chat_id, message, parse_mode=parse_mode)
    except Exception as e:
        error_message = f"Error occurred while sending message to chat ID {chat_id}: {str(e)}"
        await client.send_message(sender, error_message)
        await client.send_message(sender, f"Make Bot admin in your Channel - {chat_id} and restart the process after /cancel")

@bot.on(events.NewMessage(incoming=True, pattern='/setchat'))
async def set_chat_id(event):
    try:
        chat_id = int(event.raw_text.split(" ", 1)[1])
        user_chat_ids[event.sender_id] = chat_id
        await event.reply("Chat ID set successfully!")
    except ValueError:
        await event.reply("Invalid chat ID!")

async def send_video_with_chat_id(client, sender, path, caption, duration, hi, wi, thumb_path, upm,
                                   ud_type=None, footer="", _progress_fn=None):
    chat_id = user_chat_ids.get(sender, sender)
    path_str = str(path)

    if not os.path.exists(path_str) or os.path.getsize(path_str) == 0:
        logger.error(f"send_video_with_chat_id: file missing or empty at '{path_str}'")
        await client.send_message(
            sender,
            f"⚠️ Upload skipped — file was not found after download.\n"
            f"Path: `{path_str}`\n"
            "This can happen if a concurrent batch renamed or deleted the file. "
            "Please retry the message."
        )
        return

    if ud_type is None:
        ud_type = '**__Uploading: [Team SPY](https://t.me/dev_gagan)__**\n '

    _pf = _progress_fn if _progress_fn is not None else progress_for_pyrogram

    for _attempt in range(1, 4):
        try:
            sent = await client.send_video(
                chat_id=chat_id,
                video=path_str,
                caption=caption,
                supports_streaming=True,
                duration=duration,
                height=hi,
                width=wi,
                thumb=thumb_path,
                progress=_pf,
                progress_args=(
                    client,
                    ud_type,
                    upm,
                    time.time(),
                    footer
                )
            )
            return sent
        except FloodWait as fw:
            wait_s = fw.value + 5
            logger.warning(f"send_video FloodWait {fw.value}s — waiting {wait_s}s (attempt {_attempt})")
            await asyncio.sleep(wait_s)
        except Exception as e:
            err_str = str(e)
            # "File size equals to 0 B" — retry all 3 video attempts first; only
            # fall back to document after all video retries are exhausted.
            if "file size" in err_str.lower() and "0" in err_str:
                logger.warning(f"send_video 'file size 0' (attempt {_attempt}/3) — retrying video in 5s")
                await asyncio.sleep(5)
                continue   # retry send_video, do NOT fall to document yet
            error_message = f"Error occurred while sending video to chat ID {chat_id}: {err_str}"
            await client.send_message(sender, error_message)
            await client.send_message(sender, f"Make Bot admin in your Channel - {chat_id} and restart the process after /cancel")
            return None
    logger.warning(f"send_video: all 3 retries exhausted for '{path_str}' — signalling document fallback")
    return None


async def send_document_with_chat_id(client, sender, path, caption, thumb_path, upm,
                                      ud_type=None, footer="", _progress_fn=None):
    chat_id = user_chat_ids.get(sender, sender)
    path_str = str(path)

    if not os.path.exists(path_str) or os.path.getsize(path_str) == 0:
        logger.error(f"send_document_with_chat_id: file missing or empty at '{path_str}'")
        await client.send_message(
            sender,
            f"⚠️ Upload skipped — file was not found after download.\nPath: `{path_str}`"
        )
        return

    if ud_type is None:
        ud_type = '**__Uploading:__**\n**__Bot made by [Team SPY](https://t.me/dev_gagan)__**'

    _pf = _progress_fn if _progress_fn is not None else progress_for_pyrogram

    for _attempt in range(1, 4):
        try:
            sent = await client.send_document(
                chat_id=chat_id,
                document=path_str,
                caption=caption,
                thumb=thumb_path,
                progress=_pf,
                progress_args=(
                    client,
                    ud_type,
                    upm,
                    time.time(),
                    footer
                )
            )
            return sent
        except FloodWait as fw:
            wait_s = fw.value + 5
            logger.warning(f"send_document FloodWait {fw.value}s — waiting {wait_s}s (attempt {_attempt})")
            await asyncio.sleep(wait_s)
        except Exception as e:
            err_str = str(e)
            # "File size equals to 0 B" — retry after a short pause
            if "file size" in err_str.lower() and "0" in err_str:
                logger.warning(f"send_document 'file size 0' (attempt {_attempt}/3) — retrying in 5s")
                await asyncio.sleep(5)
                continue
            error_message = f"Error occurred while sending document to chat ID {chat_id}: {err_str}"
            await client.send_message(sender, error_message)
            await client.send_message(sender, f"Make Bot admin in your Channel - {chat_id} and restart the process after /cancel")
            return None
    logger.error(f"send_document: all 3 attempts failed (file size 0) for '{path_str}' — skipping")
    return None

async def check(userbot, client, link):
    logging.info(link)
    msg_id = 0
    try:
        msg_id = int(link.split("/")[-1])
    except ValueError:
        if '?single' not in link:
            return False, "**Invalid Link!**"
        link_ = link.split("?single")[0]
        msg_id = int(link_.split("/")[-1])
    if 't.me/c/' in link:
        if userbot is None:
            return False, "❌ No global session available. Please use /login first."
        try:
            chat = int('-100' + str(link.split("/")[-2]))
            await userbot.get_messages(chat, msg_id)
            return True, None
        except ValueError:
            return False, "**Invalid Link!**"
        except Exception as e:
            logging.info(e)
            return False, "Have you joined the channel?"
    else:
        try:
            chat = str(link.split("/")[-2])
            await client.get_messages(chat, msg_id)
            return True, None
        except Exception as e:
            logging.info(e)
            return False, "Maybe bot is banned from the chat, or your link is invalid!"


def _build_upload_path(file_str, file_n, ext):
    """
    Build the final upload path.
    - If file_n is provided and has an extension, use it as-is under DOWNLOADS_DIR.
    - If file_n is provided without an extension, append ext.
    - If file_n is empty, keep the current file path.
    Returns (new_path_str_or_None, rename_needed).
    """
    if not file_n:
        return file_str, False
    if '.' in os.path.basename(file_n):
        new_path = os.path.join(DOWNLOADS_DIR, file_n)
    else:
        new_path = os.path.join(DOWNLOADS_DIR, file_n + ext)
    return new_path, True


def _safe_rename(src, dst):
    """Rename src to dst. Returns dst on success, src on failure."""
    try:
        if src != dst:
            os.rename(src, dst)
        return dst
    except Exception as e:
        logger.error(f"os.rename({src!r} → {dst!r}) failed: {e}")
        return src


def _safe_remove(path):
    """Delete a file silently."""
    try:
        if path and os.path.exists(str(path)):
            os.remove(str(path))
    except Exception as e:
        logger.warning(f"Could not remove '{path}': {e}")


def _msg_is_video(msg):
    """
    Return True if the Telegram message should be treated as a streamable video.

    Checks the Pyrogram message object directly — this is reliable even when
    a video was sent as a Document (common in restricted channels), in which
    case msg.video is None but msg.document.mime_type starts with 'video/'.
    Extension-based detection is only used as a last-resort fallback.
    """
    VIDEO_EXTS = {'.mkv', '.mp4', '.webm', '.mpe4', '.mpeg', '.ts', '.avi', '.flv', '.org'}

    if msg.video or msg.animation:
        return True

    if msg.document and msg.document.mime_type:
        if msg.document.mime_type.startswith('video/'):
            return True

    # Fallback: check the document's stored filename extension
    if msg.document and msg.document.file_name:
        ext = os.path.splitext(msg.document.file_name)[1].lower()
        if ext in VIDEO_EXTS:
            return True

    return False


def _msg_is_photo(msg):
    """Return True if the message is a photo or image document."""
    PHOTO_EXTS = {'.jpg', '.jpeg', '.png', '.webp'}
    if msg.photo:
        return True
    if msg.document and msg.document.mime_type:
        if msg.document.mime_type.startswith('image/'):
            return True
    if msg.document and msg.document.file_name:
        ext = os.path.splitext(msg.document.file_name)[1].lower()
        if ext in PHOTO_EXTS:
            return True
    return False


async def _process_and_upload(userbot, client, sender, edit_id, msg, file_str, file_n, upm,
                              ud_type=None, footer="", _progress_fn=None):
    """
    Handle format conversion, renaming, thumbnail, and upload for a downloaded file.
    file_str must be an absolute path str of an existing file.
    Cleans up the file afterwards.

    _progress_fn: optional replacement for progress_for_pyrogram — used by the
    batch pipeline to inject a barrier callback that pauses upload near completion
    so files finish uploading in strict index order.

    Upload type is determined by the *Telegram message object* first (reliable),
    with the local file extension used only as a fallback.  This is critical
    because restricted channels often deliver videos as Documents, causing the
    unique-prefix download path to have no recognised extension.
    """
    CONVERT_EXTS = {'.webm', '.mkv', '.mpe4', '.mpeg', '.ts', '.avi', '.flv', '.org'}

    path = file_str  # current working path — updated on each rename
    ext  = _safe_ext(file_str)

    caption = (
        f"{msg.caption}\n\n__Unrestricted by **[Team SPY](https://t.me/dev_gagan)**__"
        if msg.caption
        else "__Unrestricted by **[Team SPY](https://t.me/dev_gagan)**__"
    )

    is_video = _msg_is_video(msg)
    is_photo = _msg_is_photo(msg) and not is_video

    # If extension-only fallback is needed (msg type check inconclusive)
    if not is_video and not is_photo:
        VIDEO_EXTS = {'.mkv', '.mp4', '.webm', '.mpe4', '.mpeg', '.ts', '.avi', '.flv', '.org'}
        PHOTO_EXTS = {'.jpg', '.jpeg', '.png', '.webp'}
        if ext in VIDEO_EXTS:
            is_video = True
        elif ext in PHOTO_EXTS:
            is_photo = True

    try:
        if is_video:
            # ── Step 1: ensure file has a .mp4 extension ──────────────────────
            # When downloaded via a prefix (no extension), the file may have no
            # extension at all, or an unexpected one.  Normalise to .mp4 first.
            if ext not in {'.mp4'} | CONVERT_EXTS:
                # No recognised video extension → rename to .mp4
                new_path = file_str + ".mp4"
                path = _safe_rename(path, new_path)
                ext = '.mp4'

            # ── Step 2: convert non-mp4 container to mp4 ─── ��─────────────────
            if ext in CONVERT_EXTS:
                new_path = _safe_stem(path) + ".mp4"
                path = _safe_rename(path, new_path)
                if not os.path.exists(path):
                    await client.send_message(
                        sender,
                        f"⚠️ Format conversion rename failed for `{os.path.basename(path)}`, skipping."
                    )
                    return

            # ── Step 3: apply custom filename if provided ─────────────────────
            if file_n:
                target, need_rename = _build_upload_path(path, file_n, _safe_ext(path))
                if need_rename:
                    path = _safe_rename(path, target)
                    if not os.path.exists(path):
                        await client.send_message(
                            sender,
                            f"⚠️ Rename to custom filename failed for `{file_n}`, skipping."
                        )
                        return

            # ── Step 4: gather video metadata ─────────────────────────────────
            try:
                data = video_metadata(path)
                duration = data["duration"]
                wi       = data["width"]
                hi       = data["height"]
            except Exception as e:
                logger.warning(f"video_metadata failed for '{path}': {e}")
                duration, wi, hi = 0, 0, 0

            # ── Step 5: thumbnail ─────────────────────────────────────────────
            thumb_path = await _get_thumb(userbot, msg, sender, path, duration)

            # ── Step 6: upload as streamable video ────────────────────────────
            sent_msg = await send_video_with_chat_id(
                client, sender, path, caption, duration, hi, wi, thumb_path, upm,
                ud_type=ud_type, footer=footer, _progress_fn=_progress_fn
            )

            # ── Step 7: fallback — send as document if video upload failed ────
            if sent_msg is None and os.path.exists(str(path)):
                logger.warning(f"Video upload failed, retrying as document: {path}")
                try:
                    await upm.edit_text("⚠️ Video send failed — retrying as file…")
                except Exception:
                    pass
                sent_msg = await send_document_with_chat_id(
                    client, sender, path, caption, thumb_path, upm,
                    ud_type=ud_type, footer=footer, _progress_fn=_progress_fn
                )

        elif is_photo:
            if file_n:
                target, need_rename = _build_upload_path(path, file_n, ext)
                if need_rename:
                    path = _safe_rename(path, target)

            await upm.edit("__Uploading photo...__")
            sent_msg = await bot.send_file(sender, path, caption=caption)

        else:
            if file_n:
                target, need_rename = _build_upload_path(path, file_n, ext)
                if need_rename:
                    path = _safe_rename(path, target)

            thumb_path = thumbnail(sender)
            sent_msg = await send_document_with_chat_id(
                client, sender, path, caption, thumb_path, upm,
                ud_type=ud_type, footer=footer, _progress_fn=_progress_fn
            )

        return sent_msg

    finally:
        _safe_remove(path)
        if path != file_str:
            _safe_remove(file_str)


async def get_msg(userbot, client, sender, edit_id, msg_link, i, file_n):
    edit = ""
    chat = ""
    msg_id = int(i)
    if msg_id == -1:
        await client.edit_message_text(sender, edit_id, "**Invalid Link!**")
        return None
    if 't.me/c/' in msg_link or 't.me/b/' in msg_link:
        if userbot is None:
            await client.edit_message_text(
                sender, edit_id,
                "❌ No session to access this restricted channel.\nUse /login to authenticate."
            )
            return None
        if "t.me/b" not in msg_link:
            # Always use parts[4] for chat ID — works for both plain and topic links:
            #   t.me/c/CHATID/MSGID       → parts[4]=CHATID ✓
            #   t.me/c/CHATID/TOPICID/MSGID → parts[4]=CHATID ✓
            # (the old parts[-2] broke topic links because it gave TOPICID)
            _lparts = msg_link.split("/")
            chat = int('-100' + str(_lparts[4]))
        else:
            chat = int(msg_link.split("/")[-2])
        try:
            msg = await userbot.get_messages(chat_id=chat, message_ids=msg_id)
            logging.info(msg)
            if msg.service is not None:
                await client.delete_messages(chat_id=sender, message_ids=edit_id)
                return False
            if msg.empty is not None:
                await client.delete_messages(chat_id=sender, message_ids=edit_id)
                return False

            # Text-only messages
            if not msg.media and msg.text:
                a = b = True
                edit = await client.edit_message_text(sender, edit_id, "Cloning.")
                if hasattr(msg.text, 'html') and ('--' in msg.text.html or '**' in msg.text.html or '__' in msg.text.html or '~~' in msg.text.html or '||' in msg.text.html or '```' in msg.text.html or '`' in msg.text.html):
                    await send_message_with_chat_id(client, sender, msg.text.html, parse_mode=ParseMode.HTML)
                    a = False
                if hasattr(msg.text, 'markdown') and ('<b>' in msg.text.markdown or '<i>' in msg.text.markdown or '<em>' in msg.text.markdown or '<u>' in msg.text.markdown or '<s>' in msg.text.markdown or '<spoiler>' in msg.text.markdown):
                    await send_message_with_chat_id(client, sender, msg.text.markdown, parse_mode=ParseMode.MARKDOWN)
                    b = False
                if a and b:
                    await send_message_with_chat_id(client, sender, msg.text.markdown, parse_mode=ParseMode.MARKDOWN)
                await edit.delete()
                return True

            if msg.media == MessageMediaType.POLL:
                await client.edit_message_text(sender, edit_id, 'poll media cant be saved')
                return False

            if msg.media:
                edit = await client.edit_message_text(sender, edit_id, "Trying to Download.")
                raw_file = None
                upm = None
                try:
                    raw_file = await userbot.download_media(
                        msg,
                        file_name=_unique_dl_prefix(sender, msg_id),
                        progress=progress_for_pyrogram,
                        progress_args=(
                            client,
                            "**__Unrestricting__: __[Team SPY](https://t.me/dev_gagan)__**\n ",
                            edit,
                            time.time()
                        )
                    )

                    file_str = _resolve_dl(raw_file)
                    if not file_str or not os.path.exists(file_str) or os.path.getsize(file_str) == 0:
                        logger.error(f"get_msg: download empty/missing. raw_file={raw_file!r} resolved={file_str!r}")
                        await client.edit_message_text(sender, edit_id, "⚠️ Download failed or file is empty, skipping.")
                        return False

                    await edit.delete()
                    upm = await client.send_message(sender, '__Preparing to Upload!__')

                    await _process_and_upload(userbot, client, sender, edit_id, msg, file_str, file_n, upm)

                    await upm.delete()
                    return True
                except Exception as e:
                    logger.error(f"get_msg: error for msg {msg_id}: {e}", exc_info=True)
                    try:
                        await client.edit_message_text(sender, edit_id, f"Could not download media: {str(e)[:200]}")
                    except Exception:
                        pass
                    if raw_file:
                        _safe_remove(_resolve_dl(raw_file))
                    return False
                finally:
                    if upm:
                        try:
                            await upm.delete()
                        except Exception:
                            pass
        except (ChannelBanned, ChannelInvalid, ChannelPrivate, ChatIdInvalid, ChatInvalid):
            await client.edit_message_text(sender, edit_id, "Bot is not in that channel/group.\nSend the invite link so the bot can join.")
            return False
    else:
        edit = await client.edit_message_text(sender, edit_id, "Cloning.")
        chat = msg_link.split("/")[-2]
        await copy_message_with_chat_id(client, sender, chat, msg_id)
        await edit.delete()
        return True


async def get_bulk_msg(userbot, client, sender, msg_link, i):
    x = await client.send_message(sender, "Processing!")
    file_name = ''
    result = await get_msg(userbot, client, sender, x.id, msg_link, i, file_name)
    return bool(result)


# ── Pipeline helpers (batch download/upload split) ────────────────────────────

def _extract_chat_from_link(msg_link):
    """Return Pyrogram chat_id (int or str username) from a t.me link."""
    if 't.me/b/' in msg_link:
        return int(msg_link.split('/')[-2])
    if 't.me/c/' in msg_link:
        parts = msg_link.split('/')
        return int('-100' + str(parts[4]))
    # Public username link: t.me/USERNAME/MSGID
    return msg_link.split('/')[-2]


async def prefetch_msg(acc, msg_link, msg_id):
    """
    Fetch a single Telegram message object without downloading or sending anything.
    Used by the batch pipeline to pre-fetch all message objects in parallel before
    starting downloads, so every item's download can begin at the same time.

    Returns the Pyrogram Message on success, or None if the message is genuinely
    empty/deleted/text-only.  Retries up to 4 times on transient network errors
    so a brief API hiccup never silently drops a real media message.
    """
    if acc is None:
        return None

    from pyrogram.enums import MessageMediaType
    from pyrogram.errors import FloodWait

    _MAX_RETRIES = 4
    chat = _extract_chat_from_link(msg_link)

    for _attempt in range(_MAX_RETRIES):
        try:
            msg = await asyncio.wait_for(
                acc.get_messages(chat_id=chat, message_ids=msg_id),
                timeout=30,
            )
            # Definitive "not a real media message" — no point retrying
            if msg is None or msg.empty:
                logger.debug(f"prefetch: msg_id={msg_id} is empty/None — expected skip")
                return None
            if msg.service:
                logger.debug(f"prefetch: msg_id={msg_id} is a service message — expected skip")
                return None
            if not msg.media:
                logger.debug(f"prefetch: msg_id={msg_id} is text-only — expected skip")
                return None
            if msg.media == MessageMediaType.POLL:
                logger.debug(f"prefetch: msg_id={msg_id} is a poll — expected skip")
                return None
            return msg
        except FloodWait as fw:
            wait_s = fw.value + 2
            logger.warning(f"prefetch_msg: FloodWait {fw.value}s for msg_id={msg_id} — waiting {wait_s}s")
            await asyncio.sleep(wait_s)
        except Exception as e:
            if _attempt < _MAX_RETRIES - 1:
                wait_s = 3 * (_attempt + 1)
                logger.warning(
                    f"prefetch_msg: transient error attempt {_attempt+1}/{_MAX_RETRIES} "
                    f"msg_id={msg_id} ({e}) — retrying in {wait_s}s"
                )
                await asyncio.sleep(wait_s)
            else:
                logger.error(
                    f"prefetch_msg: ALL {_MAX_RETRIES} ATTEMPTS FAILED "
                    f"msg_id={msg_id} link={msg_link}: {e}",
                    exc_info=True,
                )
                return None

    return None


async def download_msg(acc, client, sender, msg_link, msg_id,
                       source_link="", batch_range="",
                       prefetched_msg=None, progress_msg=None):
    """
    Download-only phase for the batch pipeline.

    prefetched_msg  — Pyrogram Message object already fetched by the caller.
                      When provided the internal get_messages() call is skipped.
    progress_msg    — Telegram message to use as the progress bar.
                      When provided the internal send_message() call is skipped
                      (the message is edited instead).  This lets the batch pipeline
                      pre-send all N progress messages at once so they all appear in
                      the chat simultaneously before any download data starts moving.

    Returns (msg_obj, file_str, progress_msg) on success, or None on failure.
    The caller owns the returned progress_msg and must delete it after uploading.
    """
    if acc is None:
        return None

    try:
        from pyrogram.enums import MessageMediaType

        # ── Resolve message object ─────────────────────────────────────────────
        if prefetched_msg is not None:
            msg = prefetched_msg
        else:
            chat = _extract_chat_from_link(msg_link)
            msg  = await asyncio.wait_for(
                acc.get_messages(chat_id=chat, message_ids=msg_id),
                timeout=30,
            )
            if msg is None or msg.empty or msg.service:
                return None
            if not msg.media:
                return None
            if msg.media == MessageMediaType.POLL:
                return None

        ud_type = f"**Downloading** — `{source_link}`"
        footer  = batch_range

        # ── Resolve / create progress message ─────────────────────────────────
        if progress_msg is not None:
            pm = progress_msg
            try:
                await pm.edit_text(f"⬇️ Downloading…\n`{source_link}`")
            except Exception:
                pass
        else:
            pm = await client.send_message(sender, f"⬇️ Downloading…\n`{source_link}`")

        _MAX_DL_RETRIES = 3
        raw_file = None
        file_str = None
        for _attempt in range(_MAX_DL_RETRIES):
            try:
                raw_file = await asyncio.wait_for(
                    acc.download_media(
                        msg,
                        file_name=_unique_dl_prefix(sender, msg_id),
                        progress=progress_for_pyrogram,
                        progress_args=(client, ud_type, pm, time.time(), footer)
                    ),
                    timeout=600,
                )
            except Exception as e:
                # ── FloodWait: Pyrogram has .value, Telethon has .seconds ──────
                flood_s = getattr(e, 'value', None) or getattr(e, 'seconds', None)
                if flood_s:
                    wait_s = int(flood_s) + 5
                    logger.warning(
                        f"download_msg: FloodWait {flood_s}s "
                        f"(attempt {_attempt+1}) — waiting {wait_s}s | msg_id={msg_id}"
                    )
                    try:
                        await pm.edit_text(f"⏳ FloodWait {flood_s}s — resuming download…")
                    except Exception:
                        pass
                    await asyncio.sleep(wait_s)
                    continue   # retry download after flood wait
                logger.warning(
                    f"download_msg: attempt {_attempt+1}/{_MAX_DL_RETRIES} failed "
                    f"| msg_id={msg_id} | {msg_link} | {e}"
                )
                if _attempt < _MAX_DL_RETRIES - 1:
                    await asyncio.sleep(3)
                    continue
                logger.error(
                    f"download_msg: ALL {_MAX_DL_RETRIES} ATTEMPTS FAILED "
                    f"| msg_id={msg_id} | {msg_link}"
                )
                try:
                    await pm.edit_text(
                        f"⚠️ Download failed after {_MAX_DL_RETRIES} attempts — skipping\n"
                        f"msg `{msg_id}`"
                    )
                except Exception:
                    try:
                        await pm.delete()
                    except Exception:
                        pass
                return None

            file_str = _resolve_dl(raw_file)
            if file_str and os.path.exists(file_str) and os.path.getsize(file_str) > 0:
                break   # download succeeded

            # 0-byte or missing — clean up and retry
            _safe_remove(file_str)
            logger.warning(
                f"download_msg: 0-byte/missing file on attempt {_attempt+1}/{_MAX_DL_RETRIES} "
                f"for {msg_link} (raw={raw_file!r}, resolved={file_str!r})"
            )
            if _attempt < _MAX_DL_RETRIES - 1:
                await asyncio.sleep(3)
                file_str = None
            else:
                logger.error(
                    f"download_msg: all {_MAX_DL_RETRIES} attempts produced 0-byte/missing file "
                    f"| msg_id={msg_id} | {msg_link}"
                )
                try:
                    await pm.edit_text(
                        f"⚠️ Download empty after {_MAX_DL_RETRIES} attempts — skipping\n"
                        f"msg `{msg_id}`"
                    )
                except Exception:
                    pass
                return None

        if not file_str:
            try:
                await pm.delete()
            except Exception:
                pass
            return None

        return msg, file_str, pm

    except Exception as e:
        logger.error(f"download_msg: unexpected error for {msg_link}: {e}")
        return None


async def upload_downloaded(acc, client, sender, msg_obj, file_str, pm,
                            source_link="", batch_range="", _progress_fn=None):
    """
    Upload-only phase for the batch pipeline.

    Takes the progress message (pm) returned by download_msg, edits it to show
    upload progress, sends the file to the user's target chat, then deletes pm.

    _progress_fn: optional barrier progress callback injected by the parallel
    batch pipeline to enforce ordered upload completion.

    Returns True on success, False on error.  Always cleans up file_str.
    """
    ud_type = f"**Uploading** — `{source_link}`"
    footer = batch_range

    try:
        await pm.edit_text(f"⬆️ Uploading…\n`{source_link}`")
    except Exception:
        pass

    try:
        sent = await _process_and_upload(
            acc, client, sender, None, msg_obj, file_str, '',
            pm, ud_type=ud_type, footer=footer, _progress_fn=_progress_fn
        )
        return sent      # Pyrogram/Telethon Message object (truthy) on success, None on failure
    except Exception as e:
        logger.error(f"upload_downloaded: error for {source_link}: {e}")
        _safe_remove(file_str)
        return None      # falsy — existing callers that do bool(ok) still work correctly
    finally:
        try:
            await pm.delete()
        except Exception:
            pass


async def ggn_new(userbot, client, sender, edit_id, msg_link, i, file_n):
    edit = ""
    chat = ""
    msg_id = int(i)
    if msg_id == -1:
        await client.edit_message_text(sender, edit_id, "**Invalid Link!**")
        return None
    if 't.me/c/' in msg_link or 't.me/b/' in msg_link:
        if "t.me/b" not in msg_link:
            parts = msg_link.split("/")
            chat = int('-100' + str(parts[4]))
        else:
            chat = int(msg_link.split("/")[-2])
        try:
            msg = await userbot.get_messages(chat_id=chat, message_ids=msg_id)
            logging.info(msg)
            if msg.service is not None:
                await client.delete_messages(chat_id=sender, message_ids=edit_id)
                return None
            if msg.empty is not None:
                await client.delete_messages(chat_id=sender, message_ids=edit_id)
                return None

            # Text-only messages
            if not msg.media and msg.text:
                a = b = True
                edit = await client.edit_message_text(sender, edit_id, "Cloning.")
                if hasattr(msg.text, 'html') and ('--' in msg.text.html or '**' in msg.text.html or '__' in msg.text.html or '~~' in msg.text.html or '||' in msg.text.html or '```' in msg.text.html or '`' in msg.text.html):
                    await send_message_with_chat_id(client, sender, msg.text.html, parse_mode=ParseMode.HTML)
                    a = False
                if hasattr(msg.text, 'markdown') and ('<b>' in msg.text.markdown or '<i>' in msg.text.markdown or '<em>' in msg.text.markdown or '<u>' in msg.text.markdown or '<s>' in msg.text.markdown or '<spoiler>' in msg.text.markdown):
                    await send_message_with_chat_id(client, sender, msg.text.markdown, parse_mode=ParseMode.MARKDOWN)
                    b = False
                if a and b:
                    await send_message_with_chat_id(client, sender, msg.text.markdown, parse_mode=ParseMode.MARKDOWN)
                await edit.delete()
                return None

            if msg.media == MessageMediaType.POLL:
                await client.edit_message_text(sender, edit_id, 'poll media cant be saved')
                return None

            if msg.media:
                edit = await client.edit_message_text(sender, edit_id, "Trying to Download.")
                raw_file = None
                upm = None
                try:
                    raw_file = await userbot.download_media(
                        msg,
                        file_name=_unique_dl_prefix(sender, msg_id),
                        progress=progress_for_pyrogram,
                        progress_args=(
                            client,
                            "**__Unrestricting__: __[Team SPY](https://t.me/dev_gagan)__**\n ",
                            edit,
                            time.time()
                        )
                    )

                    file_str = _resolve_dl(raw_file)
                    if not file_str or not os.path.exists(file_str) or os.path.getsize(file_str) == 0:
                        logger.error(f"ggn_new: download empty/missing. raw_file={raw_file!r} resolved={file_str!r}")
                        await client.edit_message_text(sender, edit_id, "⚠️ Download failed or file is empty, skipping.")
                        return None

                    await edit.delete()
                    upm = await client.send_message(sender, '__Preparing to Upload!__')

                    await _process_and_upload(userbot, client, sender, edit_id, msg, file_str, file_n, upm)

                    await upm.delete()
                    return None
                except Exception as e:
                    logger.error(f"ggn_new: error for msg {msg_id}: {e}", exc_info=True)
                    try:
                        await client.edit_message_text(sender, edit_id, f"Could not download media: {str(e)[:200]}")
                    except Exception:
                        pass
                    if raw_file:
                        _safe_remove(_resolve_dl(raw_file))
                    return None
                finally:
                    if upm:
                        try:
                            await upm.delete()
                        except Exception:
                            pass
        except (ChannelBanned, ChannelInvalid, ChannelPrivate, ChatIdInvalid, ChatInvalid):
            await client.edit_message_text(sender, edit_id, "Bot is not in that channel/group.\nSend the invite link so the bot can join.")
            return None
    else:
        edit = await client.edit_message_text(sender, edit_id, "Cloning.")
        chat = msg_link.split("/")[-2]
        await copy_message_with_chat_id(client, sender, chat, msg_id)
        await edit.delete()
        return None
        
