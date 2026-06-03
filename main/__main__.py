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
    from . import bot
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

    async def _run_all():
        # Start the log-rotation loop now that an event loop is actually running.
        try:
            from main.plugins.batch import _log_loop
            asyncio.create_task(_log_loop())
        except Exception:
            pass

        await bot.run_until_disconnected()

    bot.loop.run_until_complete(_run_all())
