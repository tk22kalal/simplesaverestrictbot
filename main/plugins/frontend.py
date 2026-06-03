import time
import os
import json
import logging
import asyncio

from telethon import events
from pyrogram import Client
from pyrogram.errors import FloodWait

from .. import bot as gagan
from .. import userbot, Bot
from .. import API_ID, API_HASH
from main.plugins.pyroplug import get_msg, ggn_new
from main.plugins.helpers import get_link, join

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.INFO)
logging.getLogger("telethon").setLevel(logging.INFO)

message = "Send me the message link you want to start saving from, as a reply to this message."

process = []
timer = []
user = []

# Commands that bypass the link handler entirely
commands = ['/dl', '/batch', '/sbatch', '/cancel', '/scancel', '/login', '/logout', '/mysession',
            '/start', '/help', '/logs', '/setchat', '/remthumb', '/ivalid']


def _is_range_link(raw: str) -> bool:
    """
    Return True if the text is a batch range — must NOT be processed as a single file.

    Handles two formats:
      NEW  — two full URLs joined by a hyphen:
               https://t.me/c/CHAT/TOPIC/143-https://t.me/c/CHAT/TOPIC/144
      OLD  — single URL whose last segment is START-END:
               https://t.me/c/CHAT/TOPIC/23-25
    """
    if raw.count('https://') >= 2 or raw.count('http://') >= 2:
        return True
    clean = raw.strip().rstrip("/").split("?")[0]
    last = clean.split("/")[-1]
    if "-" in last:
        parts = last.split("-", 1)
        return parts[0].isdigit() and parts[1].isdigit()
    return False


def _read_user_session(user_id) -> str | None:
    """Read per-user session string from JSON (no cross-plugin import)."""
    path = "user_sessions.json"
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f).get(str(user_id))
        except Exception:
            return None
    return None


@gagan.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
async def clone(event):
    logging.info(event)
    file_name = ''

    # Skip all bot commands
    if event.message.text and any(
        event.message.text.strip().startswith(cmd) for cmd in commands
    ):
        return

    if event.is_reply:
        reply = await event.get_reply_message()
        if reply.text == message:
            return

    lit = event.text
    li = lit.split("\n")

    if len(li) > 10:
        await event.respond("max 10 links per message")
        return

    for li in li:
        # Check raw line first — covers both dual-URL and compact range formats
        if _is_range_link(li):
            return

        try:
            link = get_link(li)
            if not link:
                return
        except TypeError:
            return

        if f'{int(event.sender_id)}' in user:
            return await event.respond("Please don't spam links, wait until ongoing process is done.")
        user.append(f'{int(event.sender_id)}')

        edit = await event.respond("Processing!")

        if "|" in li:
            url = li
            url_parts = url.split("|")
            if len(url_parts) == 2:
                file_name = url_parts[1]

        if file_name is not None:
            file_name = file_name.strip()

        # ── Resolve which Pyrogram client to use ──────────────────────────────
        # Priority: global userbot → user's /login session → error
        acc = userbot
        tmp_client = None

        if acc is None:
            sess = _read_user_session(event.sender_id)
            if sess:
                try:
                    tmp_client = Client(
                        f"tmp_{event.sender_id}",
                        session_string=sess,
                        api_id=int(API_ID),
                        api_hash=API_HASH,
                        in_memory=True
                    )
                    await tmp_client.start()
                    acc = tmp_client
                except Exception as e:
                    logger.warning(f"Could not start personal session for {event.sender_id}: {e}")
                    acc = None

        if acc is None and 't.me/c/' in link:
            await edit.edit(
                "❌ No session available to access restricted content.\n"
                "Use /login to log in with your Telegram account."
            )
            ind = user.index(f'{int(event.sender_id)}')
            user.pop(ind)
            return

        try:
            if 't.me/' not in link:
                await edit.edit("invalid link")
                ind = user.index(f'{int(event.sender_id)}')
                user.pop(int(ind))
                return

            if 't.me/+' in link:
                _client = acc if acc else Bot
                q = await join(_client, link)
                await edit.edit(q)
                ind = user.index(f'{int(event.sender_id)}')
                user.pop(int(ind))
                return

            if 't.me/' in link:
                msg_id = 0
                try:
                    msg_id = int(link.split("/")[-1])
                except ValueError:
                    if '?single' in link:
                        link_ = link.split("?single")[0]
                        msg_id = int(link_.split("/")[-1])
                    else:
                        msg_id = -1
                m = msg_id

                _acc = acc if acc else userbot
                await ggn_new(_acc, Bot, event.sender_id, edit.id, link, m, file_name)

        except FloodWait as fw:
            await gagan.send_message(event.sender_id, f'Try again after {fw.value} seconds due to floodwait from telegram.')
            await edit.delete()
        except Exception as e:
            logging.info(e)
            await gagan.send_message(event.sender_id, f"An error occurred during cloning of `{link}`\n\n**Error:** {str(e)}")
            await edit.delete()
        finally:
            if tmp_client:
                try:
                    await tmp_client.stop()
                except Exception:
                    pass

        ind = user.index(f'{int(event.sender_id)}')
        user.pop(int(ind))
        time.sleep(1)
