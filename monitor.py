
import asyncio
import logging
import re
from datetime import datetime, timedelta
from getpass import getpass
from zoneinfo import ZoneInfo

from telethon import TelegramClient
from telethon.errors import (
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)

log = logging.getLogger("lightbot.monitor")
TZ = ZoneInfo("Europe/Kyiv")

QUEUE_RE = re.compile(r"(?<!\d)(\d{1,2}[.,]\d{1,2})(?!\d)")

TIME_RANGE_RE = re.compile(
    r"(?<!\d)"
    r"(\d{1,2})(?::([0-5]\d))?"
    r"\s*[-–—]\s*"
    r"(\d{1,2})(?::([0-5]\d))?"
    r"(?!\d)"
)


def parse_schedule(text, queue):
    if not text:
        return []

    queue = str(queue).replace(",", ".").strip()
    lines = text.splitlines()

    matching = []
    active = False

    for line in lines:
        found_queues = [
            item.replace(",", ".")
            for item in QUEUE_RE.findall(line)
        ]

        if queue in found_queues:
            active = True
            matching.append(line)

        elif found_queues:
            active = False

        elif active and TIME_RANGE_RE.search(line):
            matching.append(line)

    if not matching:
        matching = [
            line for line in lines
            if queue in [
                item.replace(",", ".")
                for item in QUEUE_RE.findall(line)
            ]
        ]

    now = datetime.now(TZ)
    result = []

    for line in matching:
        for match in TIME_RANGE_RE.finditer(line):
            sh, sm, eh, em = match.groups()

            start_hour = int(sh)
            start_minute = int(sm or 0)
            end_hour = int(eh)
            end_minute = int(em or 0)

            if start_hour > 23:
                continue

            if end_hour > 24:
                continue

            if end_hour == 24 and end_minute != 0:
                continue

            start = now.replace(
                hour=start_hour,
                minute=start_minute,
                second=0,
                microsecond=0,
            )

            if end_hour == 24:
                end = now.replace(
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                ) + timedelta(days=1)
            else:
                end = now.replace(
                    hour=end_hour,
                    minute=end_minute,
                    second=0,
                    microsecond=0,
                )

            if end <= start:
                end += timedelta(days=1)

            if end < now - timedelta(hours=1):
                start += timedelta(days=1)
                end += timedelta(days=1)

            result.append({
                "start": start,
                "end": end,
            })

    unique = {
        (event["start"], event["end"]): event
        for event in result
    }

    return sorted(
        unique.values(),
        key=lambda event: event["start"],
    )


class ChannelMonitor:
    def __init__(self, api_id, api_hash, db):
        self.client = TelegramClient("user_session", int(api_id), api_hash)
        self.db = db
        self.seen = set()

    async def start(self):
        await self.client.connect()

        if self.client.is_connected() and await self.client.is_user_authorized():
            log.info("Telegram user session authorized")
            return

        phone = (
            await asyncio.to_thread(
                input, "Введи номер телефону Telegram у форматі +380XXXXXXXXX: "
            )
        ).strip()

        if not phone:
            raise ValueError("Номер телефону не введено.")

        try:
            sent_code = await self.client.send_code_request(phone)
        except PhoneNumberInvalidError as exc:
            raise ValueError(
                "Telegram не прийняв цей номер. Перевір формат, наприклад +380XXXXXXXXX."
            ) from exc

        while True:
            code = (
                await asyncio.to_thread(
                    input, "Введи НОВИЙ код входу з Telegram: "
                )
            ).strip().replace(" ", "")

            if not code:
                print("Код не може бути порожнім. Спробуй ще раз.")
                continue

            try:
                await self.client.sign_in(
                    phone=phone,
                    code=code,
                    phone_code_hash=sent_code.phone_code_hash,
                )
                break

            except PhoneCodeInvalidError:
                print("Код неправильний. Перевір повідомлення Telegram і введи його ще раз.")

            except PhoneCodeExpiredError:
                print("Термін дії коду минув. Запитую новий код...")
                sent_code = await self.client.send_code_request(phone)

            except SessionPasswordNeededError:
                password = await asyncio.to_thread(
                    getpass, "Увімкнена двоетапна перевірка. Введи пароль Telegram: "
                )
                await self.client.sign_in(password=password)
                break

        log.info("Telegram user session authorized")

    async def check_channels(self):
        users = self.db.get_users_with_settings()
        channels = sorted(set(
            user["channel"] for user in users if user["channel"]
        ))

        for channel in channels:
            try:
                entity = await self.client.get_entity(channel)

                async for message in self.client.iter_messages(entity, limit=20):
                    if not message.message:
                        continue

                    marker = (str(channel), message.id)

                    if marker in self.seen:
                        continue

                    self.seen.add(marker)

                    for user in users:
                        if user["channel"] != channel:
                            continue

                        queue = str(user["queue"]).replace(",", ".")
                        events = parse_schedule(message.message, queue)

                        if events:
                            self.db.save_events(
                                channel,
                                queue,
                                events,
                                message.date.isoformat() if message.date else None,
                            )
                            self.db.add_history(
                                user["id"],
                                f"Оновлено графік для черги {queue}"
                            )

            except (UsernameInvalidError, UsernameNotOccupiedError):
                log.error("Не вдалося знайти канал: %s", channel)

            except Exception:
                log.exception("Помилка читання каналу %s", channel)

            await asyncio.sleep(0.2)
