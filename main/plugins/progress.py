import math
import os
import time
from main.plugins.helpers import TimeFormatter, humanbytes

FINISHED_PROGRESS_STR = "🟢"
UN_FINISHED_PROGRESS_STR = "🔴"


async def progress_for_pyrogram(
    current,
    total,
    bot,
    ud_type,
    message,
    start,
    footer=""
):
    now = time.time()
    diff = now - start
    if diff < 1:
        diff = 1
    if round(diff % 5.00) == 0 or current == total:
        percentage = current * 100 / total
        speed = current / diff
        elapsed_time = round(diff) * 1
        time_to_completion = round((total - current) / speed) * 1 if speed > 0 else 0
        estimated_total_time = elapsed_time + time_to_completion

        elapsed_time = TimeFormatter(milliseconds=elapsed_time)
        estimated_total_time = TimeFormatter(milliseconds=estimated_total_time)

        progress = "**{0}{1}** \n".format(
            ''.join(
                FINISHED_PROGRESS_STR
                for _ in range(math.floor(percentage / 10))
            ),
            ''.join(
                UN_FINISHED_PROGRESS_STR
                for _ in range(10 - math.floor(percentage / 10))
            ),
        )

        tmp = progress + "**\n__Completed__:** {0} of {1}\n**__Speed__**: {2}/s\n**__Time__**: {3}\n".format(
            humanbytes(current),
            humanbytes(total),
            humanbytes(speed),
            estimated_total_time if estimated_total_time != '' else "0 s"
        )
        try:
            text = f"{ud_type}\n {tmp}"
            if footer:
                text = text.rstrip('\n') + f"\n{footer}"
            if message.text != text or message.caption != text:
                if not message.photo:
                    await message.edit_text(text=text)
                else:
                    await message.edit_caption(caption=text)
        except Exception:
            pass
