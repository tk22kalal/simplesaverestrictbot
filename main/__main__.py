#Join me @dev_gagan

import asyncio
import logging
import time

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)

botStartTime = time.time()

print("Successfully deployed!")
print("Bot Deployed : Team SPY")

if __name__ == "__main__":
    from . import bot, extra_clients
    import glob
    from pathlib import Path
    from main.utils import load_plugins

    path = "main/plugins/*.py"
    files = glob.glob(path)
    for name in files:
        with open(name) as a:
            patt = Path(a.name)
            plugin_name = patt.stem
            load_plugins(plugin_name.replace(".py", ""))

    logger.info("Bot Started :)")

    extra_count = len(extra_clients)
    if extra_count:
        logger.info(f"Running {1 + extra_count} bots in parallel.")
    else:
        logger.info("Running 1 bot (no extra tokens configured).")

    async def _check_pending_sbatch_sessions():
        """
        On startup, query MongoDB for any in-progress /sbatch sessions and
        notify the affected users so they know to use /sbatch_status → Resume.
        Runs 5 s after startup so all bots are fully connected first.
        """
        await asyncio.sleep(5)
        try:
            from main.plugins.sbatch_checkpoint import (
                get_all_pending_sessions,
                create_indexes,
            )
            from . import Bot as _pyro_bot
            await create_indexes()   # idempotent — safe to call every startup
            sessions = await get_all_pending_sessions()
            for s in sessions:
                user_id = s.get("user_id")
                total   = s.get("total_forwarded", 0)
                created = s.get("created_at", "?")
                sid8    = s.get("session_id", "?")[:8]
                if user_id:
                    try:
                        await _pyro_bot.send_message(
                            user_id,
                            f"⚠️ <b>Unfinished /sbatch session found</b> "
                            f"(<code>{sid8}…</code>)\n"
                            f"Started: {created}\n"
                            f"Progress: {total} file(s) saved before the bot restarted.\n\n"
                            f"Use /sbatch_status to view details and resume.",
                            parse_mode="html",
                        )
                    except Exception as _e:
                        logger.warning(
                            f"startup: could not notify user {user_id} about pending session: {_e}"
                        )
        except Exception as _e:
            logger.warning(f"startup: _check_pending_sbatch_sessions failed: {_e}")

    async def _run_all():
        # Start the log-rotation loop now that an event loop is actually running.
        # (batch.py defers this to avoid RuntimeError with uvloop at import time.)
        try:
            from main.plugins.batch import _log_loop
            asyncio.create_task(_log_loop())
        except Exception:
            pass

        # Notify users about any sessions that were interrupted by a restart/crash
        asyncio.create_task(_check_pending_sbatch_sessions())

        tasks = [bot.run_until_disconnected()]
        for tel_bot, _ in extra_clients:
            tasks.append(tel_bot.run_until_disconnected())
        await asyncio.gather(*tasks)

    bot.loop.run_until_complete(_run_all())
