import asyncio
import logging

from telethon import events, Button
from pyrogram import Client
from pyrogram.errors import (
    PhoneNumberInvalid, PhoneCodeInvalid, PhoneCodeExpired,
    SessionPasswordNeeded, PasswordHashInvalid, FloodWait
)

from .. import bot as gagan, API_ID, API_HASH
from main.plugins.session_store import (
    get_user_session,
    store_session,
    remove_session,
)

logger = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)


# ── /login ─────────────────────────────────────────────────────────────────────

@gagan.on(events.NewMessage(incoming=True, pattern="/login"))
async def login_command(event):
    if not event.is_private:
        return await event.reply("Please use /login in private chat with the bot.")

    user_id = event.sender_id

    if await get_user_session(user_id):
        return await event.reply(
            "✅ You are already logged in.\n"
            "Use /logout to log out first if you want to re-login.\n"
            "Use /mysession to see your saved session string."
        )

    async with gagan.conversation(event.chat_id, timeout=180) as conv:
        await conv.send_message(
            "📱 **Login with your Telegram account**\n\n"
            "Send your phone number with country code.\n"
            "Example: `+919876543210`",
            buttons=Button.force_reply()
        )
        try:
            phone_msg = await conv.get_reply()
        except asyncio.TimeoutError:
            return await conv.send_message("⏰ Timed out. Please try /login again.")

        phone = phone_msg.text.strip() if phone_msg.text else ""
        if not phone.startswith("+"):
            return await conv.send_message(
                "❌ Invalid phone number. Must start with +. Try /login again."
            )

        tmp_client = Client(
            f"login_tmp_{user_id}",
            api_id=int(API_ID),
            api_hash=API_HASH,
            in_memory=True
        )

        try:
            await tmp_client.connect()
        except Exception as e:
            return await conv.send_message(f"❌ Could not connect: `{e}`")

        try:
            sent = await tmp_client.send_code(phone)
        except PhoneNumberInvalid:
            await tmp_client.disconnect()
            return await conv.send_message("❌ Invalid phone number. Try /login again.")
        except FloodWait as fw:
            await tmp_client.disconnect()
            return await conv.send_message(
                f"⏳ FloodWait: please try again after {fw.value} seconds."
            )
        except Exception as e:
            await tmp_client.disconnect()
            return await conv.send_message(f"❌ Error sending code: `{e}`")

        await conv.send_message(
            "✅ OTP sent to your Telegram account.\n\n"
            "Enter the OTP now (digits only, e.g. `12345`).\n"
            "⚠️ Send it as plain text — do NOT use spaces.",
            buttons=Button.force_reply()
        )
        try:
            otp_msg = await conv.get_reply()
        except asyncio.TimeoutError:
            await tmp_client.disconnect()
            return await conv.send_message("⏰ Timed out waiting for OTP.")

        otp = otp_msg.text.strip().replace(" ", "") if otp_msg.text else ""

        try:
            await tmp_client.sign_in(phone, sent.phone_code_hash, otp)
        except PhoneCodeInvalid:
            await tmp_client.disconnect()
            return await conv.send_message("❌ Wrong OTP. Please try /login again.")
        except PhoneCodeExpired:
            await tmp_client.disconnect()
            return await conv.send_message("❌ OTP expired. Please try /login again.")
        except SessionPasswordNeeded:
            await conv.send_message(
                "🔐 Your account has **Two-Step Verification** enabled.\n"
                "Send your 2FA password:",
                buttons=Button.force_reply()
            )
            try:
                pwd_msg = await conv.get_reply()
            except asyncio.TimeoutError:
                await tmp_client.disconnect()
                return await conv.send_message("⏰ Timed out waiting for 2FA password.")

            password = pwd_msg.text.strip() if pwd_msg.text else ""
            try:
                await tmp_client.check_password(password)
            except PasswordHashInvalid:
                await tmp_client.disconnect()
                return await conv.send_message("❌ Wrong 2FA password. Try /login again.")
            except Exception as e:
                await tmp_client.disconnect()
                return await conv.send_message(f"❌ 2FA error: `{e}`")
        except FloodWait as fw:
            await tmp_client.disconnect()
            return await conv.send_message(
                f"⏳ FloodWait {fw.value}s. Try again later."
            )
        except Exception as e:
            await tmp_client.disconnect()
            return await conv.send_message(f"❌ Sign-in error: `{e}`")

        try:
            session_string = await tmp_client.export_session_string()
            await tmp_client.disconnect()
        except Exception as e:
            await tmp_client.disconnect()
            return await conv.send_message(f"❌ Could not export session: `{e}`")

        await store_session(user_id, session_string)
        logger.info(f"User {user_id} logged in successfully.")

        await conv.send_message(
            "✅ **Logged in successfully!**\n\n"
            "Your session is saved.\n"
            "Use /logout to remove your session anytime.\n\n"
            "─────────────────────────\n"
            "📌 **To survive Heroku redeploys without re-login:**\n"
            "Copy the session string from the next message and set it as:\n"
            "`SESSION = <your_string>`\n"
            "in your Heroku **Config Vars** (Settings → Config Vars).\n\n"
            "Once set, the bot loads it automatically on every startup — "
            "no phone/OTP needed again unless you revoke the session from another device."
        )

        # Send the raw session string in a separate message so user can copy it easily
        await gagan.send_message(
            user_id,
            f"🔑 **Your Session String** (keep this private!):\n\n"
            f"`{session_string}`\n\n"
            "⚠️ Anyone with this string has full access to your account.\n"
            "Do NOT share it. Set it only in your own Heroku Config Vars as `SESSION`."
        )


# ── /logout ────────────────────────────────────────────────────────────────────

@gagan.on(events.NewMessage(incoming=True, pattern="/logout"))
async def logout_command(event):
    if not event.is_private:
        return await event.reply("Please use /logout in private chat.")

    user_id = event.sender_id
    if not await get_user_session(user_id):
        return await event.reply("You are not logged in.")

    await remove_session(user_id)
    logger.info(f"User {user_id} logged out.")
    await event.reply(
        "✅ Logged out. Your session has been removed from the bot's storage.\n\n"
        "⚠️ If you had set `SESSION` in Heroku Config Vars, remove it there too "
        "to fully revoke access."
    )


# ── /mysession ─────────────────────────────────────────────────────────────────

@gagan.on(events.NewMessage(incoming=True, pattern="/mysession"))
async def mysession_command(event):
    if not event.is_private:
        return
    user_id = event.sender_id
    session_string = await get_user_session(user_id)
    if session_string:
        await event.reply(
            "✅ You have an active session stored.\n\n"
            "🔑 **Your Session String** (keep this private!):\n\n"
            f"`{session_string}`\n\n"
            "Set this as `SESSION` in Heroku Config Vars to avoid re-login on redeploys.\n"
            "⚠️ Do NOT share this string with anyone."
        )
    else:
        await event.reply(
            "❌ No session found. Use /login to log in.\n\n"
            "After login, copy the session string and set it as `SESSION` "
            "in your Heroku Config Vars so it persists across redeploys."
        )
