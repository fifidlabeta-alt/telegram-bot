"""
Telegram-бот графіків відключення світла (aiogram 3 + SQLite).

Графіки бот бере сам із ПУБЛІЧНОЇ вебсторінки каналу https://t.me/s/<канал>
(без Telethon, без особистого акаунта, без номера телефону).

Запуск:            python bot.py
Перевірка каналу:  python bot.py --check
Тест тексту:       python bot.py --parse-file post.txt
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from database import Database

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

log = logging.getLogger("lightbot")

# ======================================================================
# Налаштування (все можна змінити у файлі .env)
# ======================================================================
TZ = ZoneInfo("Europe/Kyiv")
UTC = timezone.utc


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        return default


def _normalize_channel(raw: str) -> str:
    raw = (raw or "").strip()
    raw = re.sub(r"^https?://t\.me/(s/)?", "", raw, flags=re.IGNORECASE)
    return raw.lstrip("@").strip("/") or "SvitloOleksandriyskohoRaionu"


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL = _normalize_channel(os.getenv("CHANNEL", "SvitloOleksandriyskohoRaionu"))
SCRAPE_SECONDS = _env_int("SCRAPE_SECONDS", 60, minimum=15)   # як часто читати канал
NOTIFY_TICK_SECONDS = _env_int("NOTIFY_TICK_SECONDS", 15, minimum=5)  # як часто перевіряти сповіщення
MIN_QUEUES = _env_int("MIN_QUEUES", 12, minimum=1)            # скільки черг має бути в "повному" графіку
DB_PATH = os.getenv("DB_PATH", "").strip() or str(BASE_DIR / "data.sqlite3")

WARN_MINUTES = 15                 # попередження за 15 хвилин
WARN_SECONDS = WARN_MINUTES * 60
RESTORE_GRACE_SECONDS = 5 * 60    # "ввімкнення" не надсилаємо, якщо пройшло більше 5 хв
MAX_CROSS_MIDNIGHT_HOURS = 12     # відключення "22-02" допустиме, "16-14" — ні

QUEUES = ["1.1", "1.2", "2.1", "2.2", "3.1", "3.2",
          "4.1", "4.2", "5.1", "5.2", "6.1", "6.2"]
QUEUE_RE = re.compile(r"^[1-6]\.[1-2]$")

HTTP_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "uk,en;q=0.8",
    "Cache-Control": "no-cache",
}

db: Database | None = None
dp = Dispatcher()

Interval = tuple  # (початок: datetime, кінець: datetime)


# ======================================================================
# Допоміжні функції часу
# ======================================================================
def now_kyiv() -> datetime:
    return datetime.now(TZ)


def elapsed(start: datetime, end: datetime) -> timedelta:
    """Справжній проміжок часу (коректний і при переході на літній/зимовий час)."""
    return end.astimezone(UTC) - start.astimezone(UTC)


def _u(dt: datetime) -> datetime:
    return dt.astimezone(UTC)


def local_key(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%Y-%m-%dT%H:%M")


def _plural(n: int, one: str, few: str, many: str) -> str:
    n = abs(int(n))
    if 11 <= n % 100 <= 14:
        return many
    if n % 10 == 1:
        return one
    if 2 <= n % 10 <= 4:
        return few
    return many


def format_duration(value) -> str:
    """timedelta або секунди -> '2 години', '1 година 30 хвилин', '30 хвилин'."""
    seconds = value.total_seconds() if isinstance(value, timedelta) else float(value)
    total_minutes = int(round(seconds / 60))
    hours, minutes = divmod(total_minutes, 60)
    parts = []
    if hours:
        parts.append(f"{hours} {_plural(hours, 'година', 'години', 'годин')}")
    if minutes:
        parts.append(f"{minutes} {_plural(minutes, 'хвилина', 'хвилини', 'хвилин')}")
    return " ".join(parts) if parts else "0 хвилин"


def fmt_when(dt: datetime, now: datetime) -> str:
    """'14:00', а якщо це не сьогодні — '14:00 (15.11)'."""
    local = dt.astimezone(TZ)
    text = f"{local:%H:%M}"
    if local.date() != now.astimezone(TZ).date():
        text += f" ({local:%d.%m})"
    return text


def day_label(d: date, today: date) -> str:
    return {0: " (сьогодні)", 1: " (завтра)", -1: " (вчора)"}.get((d - today).days, "")


def day_bounds(d: date) -> tuple:
    start = datetime(d.year, d.month, d.day, tzinfo=TZ)
    nxt = d + timedelta(days=1)
    end = datetime(nxt.year, nxt.month, nxt.day, tzinfo=TZ)
    return start, end


def fmt_range(start: datetime, end: datetime, day_end: datetime) -> str:
    s = start.astimezone(TZ)
    e = end.astimezone(TZ)
    end_txt = "24:00" if _u(end) == _u(day_end) else f"{e:%H:%M}"
    return f"{s:%H:%M}–{end_txt}"


# ======================================================================
# Інтервали: об'єднання та підрахунок за добу
# ======================================================================
def merge_intervals(intervals) -> list:
    """Об'єднує інтервали, що перетинаються або торкаються (22-24 + 00-02 = 22-02)."""
    items = sorted(((s, e) for s, e in intervals if _u(e) > _u(s)),
                   key=lambda p: _u(p[0]))
    merged: list = []
    for s, e in items:
        if merged and _u(s) <= _u(merged[-1][1]):
            if _u(e) > _u(merged[-1][1]):
                merged[-1] = (merged[-1][0], e)
        else:
            merged.append((s, e))
    return merged


@dataclass
class DayView:
    outages: list        # [(початок, кінець)] обрізані межами доби
    lights: list         # періоди зі світлом за графіком
    outage_seconds: float
    light_seconds: float


def day_view(events, d: date) -> DayView:
    """Розбір доби `d`: відключення, періоди зі світлом, підсумкові тривалості."""
    ds, de = day_bounds(d)
    outages = []
    for s, e in merge_intervals(events):
        cs = s if _u(s) > _u(ds) else ds
        ce = e if _u(e) < _u(de) else de
        if _u(ce) > _u(cs):
            outages.append((cs, ce))

    lights = []
    cursor = ds
    for s, e in outages:
        if _u(s) > _u(cursor):
            lights.append((cursor, s))
        cursor = e
    if _u(de) > _u(cursor):
        lights.append((cursor, de))

    out_sec = sum(elapsed(s, e).total_seconds() for s, e in outages)
    day_sec = elapsed(ds, de).total_seconds()
    return DayView(outages, lights, out_sec, day_sec - out_sec)


# ======================================================================
# ПАРСЕР ТЕКСТУ ГРАФІКА
# ======================================================================
_QTOKEN = r"[1-6][.,][1-2]"
QTOKEN_RE = re.compile(_QTOKEN)

# Рядок з чергою (або кількома чергами): "Черга 2.1: 14-16", "🔌 2.1 — 14:00-16:00",
# "1.1, 1.2: 00-04", "2.1" (час — на наступних рядках).
QUEUE_LINE_RE = re.compile(
    r"^[^\w]*"
    r"(?:(?:під)?черг[аиіуо]*\s*[№#]?\s*)?"
    rf"({_QTOKEN}(?:\s*(?:,|;|/|\+|&|та|і|й|и)\s*{_QTOKEN})*)"
    r"(?!\d)(?!\.\d)"
    r"\s*[:\-–—]?\s*(.*)$",
    re.IGNORECASE,
)

# 14-16, 14:00-16:00, 00-00:30, 22-24, 14.00–16.00, "з 14:00 до 16:00"
TIME_RANGE_RE = re.compile(
    r"(?<!\d)(?<!\d[.:/\-])"
    r"(\d{1,2})(?:[:.]([0-5]\d))?"
    r"\s*(?:[-–—−‒―]|\bдо\b)\s*"
    r"(\d{1,2})(?:[:.]([0-5]\d))?"
    r"(?!\d)(?![:.]\d)"
)

MONTHS = {
    "січня": 1, "лютого": 2, "березня": 3, "квітня": 4, "травня": 5, "червня": 6,
    "липня": 7, "серпня": 8, "вересня": 9, "жовтня": 10, "листопада": 11, "грудня": 12,
}
FULL_DATE_RE = re.compile(r"(?<![\d.])(\d{1,2})\.(\d{1,2})\.(\d{4}|\d{2})(?!\d)")
SHORT_DATE_RE = re.compile(r"(?<![\d.])(\d{1,2})\.(\d{2})(?![\d.]\d)(?!\d)")
MONTH_NAME_RE = re.compile(
    r"(?<!\d)(\d{1,2})\s+(" + "|".join(MONTHS) + r")(?:\s+(\d{4}))?",
    re.IGNORECASE,
)

_SPACES_RE = re.compile(r"[ -   　 ]")
_INVISIBLE_RE = re.compile(r"[​‌‍⁠﻿]")


def normalize_text(text: str) -> str:
    text = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    text = _SPACES_RE.sub(" ", text)
    text = _INVISIBLE_RE.sub("", text)
    return text.strip()


def normalize_queue(q: str) -> str:
    return q.replace(",", ".")


def _valid_date(d: int, m: int, y: int):
    try:
        return date(y, m, d)
    except ValueError:
        return None


def _infer_year(d: int, m: int, ref: date):
    best = None
    for y in (ref.year - 1, ref.year, ref.year + 1):
        cand = _valid_date(d, m, y)
        if cand and (best is None or abs((cand - ref).days) < abs((best - ref).days)):
            best = cand
    return best


def _find_date(text: str, ref: date, allow_short: bool):
    """Шукає дату графіка в тексті. Перевага — даті після слова «на» («графік на 14.11»)."""
    found = []  # (пріоритет, позиція, дата)

    def add(pos, dt):
        if not dt:
            return
        before = text[max(0, pos - 12):pos].lower()
        preferred = before.endswith("на ") and not before.endswith("станом на ")
        found.append((0 if preferred else 1, pos, dt))

    for m in FULL_DATE_RE.finditer(text):
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        add(m.start(), _valid_date(d, mo, y + 2000 if y < 100 else y))
    for m in MONTH_NAME_RE.finditer(text):
        d, mo = int(m.group(1)), MONTHS[m.group(2).lower()]
        add(m.start(), _valid_date(d, mo, int(m.group(3))) if m.group(3)
            else _infer_year(d, mo, ref))
    if allow_short:
        for m in SHORT_DATE_RE.finditer(text):
            add(m.start(), _infer_year(int(m.group(1)), int(m.group(2)), ref))

    return min(found)[2] if found else None


def parse_time_ranges(text: str, day: date) -> list:
    """Усі проміжки часу з фрагмента тексту -> [(початок, кінець)] з часовим поясом Kyiv."""
    base = datetime(day.year, day.month, day.day, tzinfo=TZ)
    result = []
    for m in TIME_RANGE_RE.finditer(text):
        sh, sm, eh, em = m.groups()
        sh, sm, eh, em = int(sh), int(sm or 0), int(eh), int(em or 0)

        if sh > 23 or eh > 24 or (eh == 24 and em != 0):
            continue

        start = base.replace(hour=sh, minute=sm)
        if eh == 24:
            end = base + timedelta(days=1)
        else:
            end = base.replace(hour=eh, minute=em)

        if _u(end) == _u(start):
            continue
        if _u(end) < _u(start):  # перехід через північ: "22-02"
            end = end + timedelta(days=1)
            if elapsed(start, end) > timedelta(hours=MAX_CROSS_MIDNIGHT_HOURS):
                continue

        result.append((start, end))
    return result


@dataclass
class ParsedSchedule:
    schedule_date: date
    date_source: str      # "text" — дату знайдено в тексті, "post_date" — взято дату публікації
    queues: dict          # {"2.1": [(початок, кінець), ...]}  (відключень може не бути)


def parse_schedule_text(text: str, posted_at: datetime | None = None):
    """
    Розпізнає графік у тексті допису. Повертає ParsedSchedule або None,
    якщо в тексті немає жодної черги.
    """
    text = normalize_text(text)
    if not text:
        return None

    entries: list = []    # [(список черг, фрагмент із часом)]
    order: list = []      # черги в порядку появи
    header: list = []     # рядки до першої черги (там зазвичай дата)
    active: list = []
    seen_queue = False

    for raw in text.split("\n"):
        line = raw.strip()
        if not line:
            continue

        m = QUEUE_LINE_RE.match(line)
        if m:
            seen_queue = True
            active = [normalize_queue(q) for q in QTOKEN_RE.findall(m.group(1))]
            for q in active:
                if q not in order:
                    order.append(q)
            rest = m.group(2).strip()
            if rest:
                entries.append((list(active), rest))
            continue

        if not seen_queue:
            header.append(line)
        elif active and TIME_RANGE_RE.search(line):
            entries.append((list(active), line))
        else:
            active = []

    if not order:
        return None

    ref = posted_at.astimezone(TZ).date() if posted_at else now_kyiv().date()
    d = _find_date("\n".join(header), ref, allow_short=True)
    source = "text"
    if d is None:
        d = _find_date(text, ref, allow_short=False)
    if d is None:
        d, source = ref, "post_date"

    queues = {q: [] for q in order}
    for qs, fragment in entries:
        for iv in parse_time_ranges(fragment, d):
            for q in qs:
                queues[q].append(iv)
    for q in queues:
        queues[q] = merge_intervals(queues[q])

    return ParsedSchedule(d, source, queues)


# ======================================================================
# ЧИТАННЯ ПУБЛІЧНОЇ СТОРІНКИ КАНАЛУ
# ======================================================================
@dataclass(frozen=True)
class ChannelPost:
    post_id: int
    posted_at: datetime | None
    text: str


class FetchError(Exception):
    pass


def parse_channel_html(html: str, channel: str) -> list:
    """HTML сторінки t.me/s/<канал> -> список дописів (за зростанням номера)."""
    soup = BeautifulSoup(html, "html.parser")
    want = channel.lower()
    posts = []

    for wrap in soup.select("div.tgme_widget_message[data-post]"):
        name, _, num = (wrap.get("data-post") or "").partition("/")
        if name.lower() != want or not num.isdigit():
            continue

        posted = None
        time_tag = wrap.select_one("a.tgme_widget_message_date time[datetime]")
        if time_tag:
            try:
                posted = datetime.fromisoformat(time_tag["datetime"])
            except ValueError:
                posted = None

        text = ""
        for el in wrap.select("div.tgme_widget_message_text"):
            classes = el.get("class") or []
            if "js-message_reply_text" in classes or el.find_parent(
                    class_="tgme_widget_message_reply"):
                continue  # це цитата допису, на який відповідають
            for br in el.find_all("br"):
                br.replace_with("\n")
            text = el.get_text()
            break

        posts.append(ChannelPost(int(num), posted, normalize_text(text)))

    posts.sort(key=lambda p: p.post_id)
    return posts


async def fetch_channel_html(session: aiohttp.ClientSession, channel: str) -> str:
    url = f"https://t.me/s/{channel}"
    try:
        async with session.get(url, headers=HTTP_HEADERS, allow_redirects=True) as resp:
            if resp.status != 200:
                raise FetchError(f"HTTP {resp.status}")
            if "/s/" not in resp.url.path:
                raise FetchError(
                    "Telegram перенаправив запит: канал не віддає публічний перегляд /s/")
            return await resp.text()
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        raise FetchError(f"мережева помилка: {exc!r}") from exc


# ======================================================================
# ОБРОБКА ДОПИСІВ -> БАЗА
# ======================================================================
@dataclass
class ProcessStats:
    new: int = 0
    edited: int = 0
    unchanged: int = 0
    no_text: int = 0
    schedules: int = 0
    partial: int = 0


def rebuild_schedule_date(database: Database, channel: str, schedule_date: str) -> bool:
    """
    Записує в базу графік на дату з АКТУАЛЬНОГО допису (найновіший із повним графіком).
    Якщо такого допису немає — нічого не видаляє (останній збережений графік лишається).
    """
    winner = database.get_winning_post(channel, schedule_date)
    if not winner:
        return False
    posted = None
    if winner["posted_at"]:
        try:
            posted = datetime.fromisoformat(winner["posted_at"])
        except ValueError:
            posted = None
    parsed = parse_schedule_text(winner["text"], posted)
    if not parsed or parsed.schedule_date.isoformat() != schedule_date:
        return False
    database.replace_day_schedule(
        schedule_date, channel, winner["post_id"], parsed.queues,
        source_date=winner["posted_at"],
    )
    return True


def process_posts(database: Database, posts, channel: str,
                  min_queues: int = MIN_QUEUES) -> ProcessStats:
    stats = ProcessStats()
    affected: set = set()

    for post in sorted(posts, key=lambda p: p.post_id):
        if not post.text:
            stats.no_text += 1   # допис без тексту (наприклад, лише картинка)
            continue

        text_hash = hashlib.sha256(post.text.encode("utf-8")).hexdigest()
        existing = database.get_post(channel, post.post_id)
        if existing and existing["text_hash"] == text_hash:
            stats.unchanged += 1
            continue

        parsed = parse_schedule_text(post.text, post.posted_at)
        is_schedule = bool(parsed and len(parsed.queues) >= min_queues)
        sdate = parsed.schedule_date.isoformat() if parsed else None

        database.upsert_post(
            channel, post.post_id,
            post.posted_at.astimezone(TZ).isoformat(timespec="seconds")
            if post.posted_at else None,
            text_hash, post.text, sdate,
            len(parsed.queues) if parsed else 0, is_schedule,
        )

        if existing:
            stats.edited += 1
            if existing["schedule_date"]:
                affected.add(existing["schedule_date"])
        else:
            stats.new += 1

        if is_schedule:
            stats.schedules += 1
            affected.add(sdate)
            if parsed.date_source != "text":
                log.warning("Допис %s: дату не знайдено в тексті, взято дату публікації %s",
                            post.post_id, sdate)
        elif parsed:
            stats.partial += 1
            log.warning(
                "Допис %s розпізнано лише частково (%s з %s черг) — НЕ вважається повним "
                "графіком і ігнорується. Якщо це помилка, перевірте формат: python bot.py --check",
                post.post_id, len(parsed.queues), min_queues)

    for d in sorted(affected):
        rebuild_schedule_date(database, channel, d)

    return stats


def diff_snapshots(before: dict, after: dict, today_iso: str) -> dict:
    """Які черги/дати змінилися: {черга: [(дата, це_новий_графік), ...]} (лише сьогодні й далі)."""
    changes: dict = {}
    for (queue, d), events in after.items():
        if d < today_iso:
            continue
        old = before.get((queue, d))
        if old != events:
            changes.setdefault(queue, []).append((d, old is None))
    for items in changes.values():
        items.sort()
    return changes


# ======================================================================
# ФОРМУВАННЯ ТЕКСТІВ
# ======================================================================
def load_queue_events(queue: str, now: datetime, days_back: int = 2) -> list:
    """Усі збережені відключення черги (з кількох дат), об'єднані в безперервні проміжки."""
    since = (now.astimezone(TZ).date() - timedelta(days=days_back)).isoformat()
    ivs = []
    for r in db.get_global_events(queue, since):
        try:
            ivs.append((datetime.fromisoformat(r["start_at"]),
                        datetime.fromisoformat(r["end_at"])))
        except ValueError:
            log.warning("Пошкоджений запис графіка: %s", r)
    return merge_intervals(ivs)


def render_day_block(events, d: date, today: date) -> str:
    view = day_view(events, d)
    ds, de = day_bounds(d)
    lines = [f"📅 {d:%d.%m.%Y}{day_label(d, today)}"]
    if not view.outages:
        lines.append("🟢 Відключень за графіком немає.")
        return "\n".join(lines)
    for s, e in view.outages:
        lines.append(f"🔴 {fmt_range(s, e, de)} · {format_duration(elapsed(s, e))}")
    lines.append("")
    lines.append(f"🔴 Без світла: {format_duration(view.outage_seconds)}.")
    lines.append(f"🟢 Зі світлом за графіком: {format_duration(view.light_seconds)}.")
    return "\n".join(lines)


def render_calc_block(events, d: date, today: date) -> str:
    view = day_view(events, d)
    ds, de = day_bounds(d)
    lines = [f"📊 Графік на {d:%d.%m.%Y}{day_label(d, today)}", ""]
    if not view.outages:
        lines.append("🟢 Відключень за графіком немає — світло всю добу.")
        return "\n".join(lines)

    lines.append("🔴 Відключення:")
    for i, (s, e) in enumerate(view.outages, 1):
        lines.append(f"  {i}) {fmt_range(s, e, de)} — {format_duration(elapsed(s, e))}")
    lines.append("")
    lines.append("🟢 Світло за графіком:")
    for i, (s, e) in enumerate(view.lights, 1):
        lines.append(f"  {i}) {fmt_range(s, e, de)} — {format_duration(elapsed(s, e))}")
    lines.append("")
    lines.append(f"🔴 Без світла: {format_duration(view.outage_seconds)}.")
    lines.append(f"🟢 Зі світлом за графіком: {format_duration(view.light_seconds)}.")
    return "\n".join(lines)


def _dates_to_show(queue: str, today: date) -> tuple:
    """Дати для показу: сьогодні й пізніше; якщо їх немає — остання відома (позначається застарілою)."""
    upcoming = db.get_schedule_dates(queue, today.isoformat())[:3]
    if upcoming:
        return [date.fromisoformat(x) for x in upcoming], False
    latest = db.get_latest_schedule_date(queue)
    if latest:
        return [date.fromisoformat(latest)], True
    return [], False


NO_SCHEDULE_TEXT = ("Поки графіка для цієї черги немає. Бот надішле його автоматично, "
                    "щойно він з'явиться на каналі.")
PLANNED_NOTE = "ℹ️ Це плановий графік, а не підтвердження фактичної наявності світла."


def build_schedule_message(queue: str, now: datetime) -> str:
    today = now.astimezone(TZ).date()
    dates, stale = _dates_to_show(queue, today)
    if not dates:
        return NO_SCHEDULE_TEXT
    events = load_queue_events(queue, now)
    blocks = [render_day_block(events, d, today) for d in dates]
    head = f"⚡ Графік для черги {queue}"
    if stale:
        head += "\n⚠️ Новий графік ще не опубліковано — показано останній відомий."
    return head + "\n\n" + "\n\n".join(blocks) + "\n\n" + PLANNED_NOTE


def build_calc_message(queue: str, now: datetime) -> str:
    today = now.astimezone(TZ).date()
    dates, stale = _dates_to_show(queue, today)
    if not dates:
        return NO_SCHEDULE_TEXT
    events = load_queue_events(queue, now)
    head = f"🔌 Черга {queue}"
    if stale:
        head += "\n⚠️ Новий графік ще не опубліковано — показано останній відомий."
    return head + "\n\n" + "\n\n".join(render_calc_block(events, d, today) for d in dates)


def build_next_message(queue: str, now: datetime) -> str:
    events = load_queue_events(queue, now)
    lines = []

    current = next((iv for iv in events if _u(iv[0]) <= _u(now) < _u(iv[1])), None)
    if current:
        lines.append(f"🔴 За графіком зараз світла немає — до {fmt_when(current[1], now)}.")
        lines.append("")

    upcoming = next((iv for iv in events if _u(iv[0]) > _u(now)), None)
    if not upcoming:
        if current:
            lines.append("Подальших відключень у графіку не знайдено.")
            return "\n".join(lines)
        return "Майбутніх відключень у графіку не знайдено."

    s, e = upcoming
    lines += [
        "⏰ Наступне планове відключення",
        "",
        f"🔌 Черга: {queue}",
        f"🕒 {fmt_when(s, now)} — {fmt_when(e, now)}",
        f"⏳ Початок через {format_duration(elapsed(now, s))}",
        f"⌛ Тривалість: {format_duration(elapsed(s, e))}",
    ]
    return "\n".join(lines)


def build_warning_text(queue: str, start: datetime, end: datetime,
                       seconds_left: float, now: datetime) -> str:
    minutes_left = max(1, int(round(seconds_left / 60)))
    return (
        f"⚠️ УВАГА! Через {minutes_left} {_plural(minutes_left, 'хвилину', 'хвилини', 'хвилин')} "
        f"планове відключення світла.\n\n"
        f"🔌 Ваша черга: {queue}\n"
        f"🕒 Початок відключення: {fmt_when(start, now)}\n"
        f"⏳ Орієнтовна тривалість: {format_duration(elapsed(start, end))}"
    )


def build_restore_text(queue: str, start: datetime, end: datetime, now: datetime) -> str:
    return (
        "🟢 За графіком електроенергія має з'явитися.\n\n"
        f"🔌 Ваша черга: {queue}\n"
        f"🕒 Планове ввімкнення: {fmt_when(end, now)}\n"
        f"⏱ Без світла були: {format_duration(elapsed(start, end))}\n\n"
        "ℹ️ Це плановий час за графіком, а не підтвердження фактичного "
        "відновлення електроенергії."
    )


def build_update_message(queue: str, items: list, now: datetime) -> str:
    today = now.astimezone(TZ).date()
    events = load_queue_events(queue, now)
    blocks = [render_day_block(events, date.fromisoformat(d), today) for d, _ in items]
    return (
        f"🔄 Оновлено графік для твоєї черги {queue}\n\n"
        + "\n\n".join(blocks)
        + "\n\n🔔 Сповіщення про відключення перераховано за новим графіком."
    )


# ======================================================================
# КЛАВІАТУРИ
# ======================================================================
def queue_keyboard(current: str | None = None):
    kb = InlineKeyboardBuilder()
    for q in QUEUES:
        kb.button(text=f"✅ {q}" if q == current else q, callback_data=f"setq:{q}")
    kb.adjust(2, 2, 2, 2, 2, 2)
    return kb.as_markup()


def main_menu():
    kb = InlineKeyboardBuilder()
    for title, data in [
        ("⚡ Мій графік", "schedule"),
        ("🔌 Моя черга", "queue"),
        ("⏰ Наступне відключення", "next"),
        ("📊 Підрахунок годин", "calc"),
        ("🔔 Сповіщення", "notifications"),
        ("🕘 Історія", "history"),
    ]:
        kb.button(text=title, callback_data=data)
    kb.adjust(2, 2, 2)
    return kb.as_markup()


# ======================================================================
# ВІДПРАВКА ПОВІДОМЛЕНЬ
# ======================================================================
async def safe_send(bot, user_id: int, text: str, **kwargs) -> bool:
    """Надсилає повідомлення; помилки одного користувача не зупиняють роботу бота."""
    for _ in range(2):
        try:
            await bot.send_message(user_id, text, **kwargs)
            return True
        except TelegramRetryAfter as exc:
            await asyncio.sleep(float(exc.retry_after) + 1)
        except TelegramForbiddenError:
            db.set_blocked(user_id, True)
            log.info("Користувач %s заблокував бота — сповіщення вимкнено для нього", user_id)
            return False
        except TelegramAPIError as exc:
            log.warning("Помилка Telegram для користувача %s: %s", user_id, exc)
            return False
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log.warning("Мережева помилка при надсиланні користувачу %s: %r", user_id, exc)
            return False
    return False


async def notify_schedule_changes(bot, changes: dict, now: datetime) -> int:
    """Надсилає оновлений графік ЛИШЕ користувачам тих черг, чий графік справді змінився."""
    sent = 0
    for queue, items in changes.items():
        users = db.get_users_by_queue(queue)
        if not users:
            continue
        text = build_update_message(queue, items, now)
        for user in users:
            if await safe_send(bot, user["id"], text):
                db.add_history(user["id"], f"Оновлено графік для черги {queue}")
                sent += 1
            await asyncio.sleep(0.05)
    return sent


async def process_notifications(bot, now: datetime | None = None) -> int:
    """
    Попередження за 15 хвилин і повідомлення про планове ввімкнення.
    Розраховується щоразу заново з актуального графіка в базі, тому:
      * після заміни графіка старі майбутні сповіщення просто зникають;
      * після простою бота застарілі сповіщення не надсилаються.
    """
    now = now or now_kyiv()
    delivered = 0

    for queue in QUEUES:
        events = load_queue_events(queue, now)
        users = None

        for start, end in events:
            to_start = elapsed(now, start).total_seconds()
            since_end = elapsed(end, now).total_seconds()

            if 0 < to_start <= WARN_SECONDS:
                kind = "before15"
                key = f"{queue}|{local_key(start)}"   # залежить лише від початку
                text = build_warning_text(queue, start, end, to_start, now)
            elif 0 <= since_end <= RESTORE_GRACE_SECONDS:
                kind = "restore"
                key = f"{queue}|{local_key(start)}|{local_key(end)}"
                text = build_restore_text(queue, start, end, now)
            else:
                continue

            if users is None:
                users = db.get_users_by_queue(queue)

            for user in users:
                if db.was_notified(user["id"], key, kind):
                    continue
                if await safe_send(bot, user["id"], text):
                    db.mark_notified(user["id"], key, kind)
                    db.add_history(
                        user["id"],
                        f"Попередження про відключення {fmt_when(start, now)}"
                        if kind == "before15"
                        else f"Планове ввімкнення {fmt_when(end, now)}")
                    delivered += 1
                await asyncio.sleep(0.05)

    return delivered


# ======================================================================
# ОДНА ПЕРЕВІРКА КАНАЛУ
# ======================================================================
async def scrape_once(bot, session, now: datetime | None = None) -> ProcessStats:
    now = now or now_kyiv()
    html = await fetch_channel_html(session, CHANNEL)
    posts = parse_channel_html(html, CHANNEL)
    if not posts:
        raise FetchError(
            "на сторінці не знайдено жодного допису (канал закритий або Telegram змінив розмітку)")
    return await apply_posts(bot, posts, now)


async def apply_posts(bot, posts, now: datetime) -> ProcessStats:
    today_iso = now.astimezone(TZ).date().isoformat()
    since = (now.astimezone(TZ).date() - timedelta(days=2)).isoformat()

    before = db.get_schedule_snapshot(since)
    stats = process_posts(db, posts, CHANNEL)
    after = db.get_schedule_snapshot(since)

    changes = diff_snapshots(before, after, today_iso)
    if changes:
        log.info("Графік змінився для черг: %s", ", ".join(sorted(changes)))
        await notify_schedule_changes(bot, changes, now)

    db.set_meta("last_poll_ok", now.isoformat(timespec="seconds"))
    db.set_meta("last_poll_error", "")
    if stats.new or stats.edited:
        log.info("Канал оброблено: нових %s, змінених %s, графіків %s, без змін %s",
                 stats.new, stats.edited, stats.schedules, stats.unchanged)
    return stats


# ======================================================================
# КОМАНДИ ТА КНОПКИ
# ======================================================================
def apply_queue_choice(uid: int, queue: str) -> str:
    db.ensure_user(uid)
    db.set_blocked(uid, False)
    db.set_queue(uid, queue)
    db.add_history(uid, f"Вибрано чергу {queue}")
    return f"✅ Чергу збережено: {queue}.\n\n" + build_schedule_message(queue, now_kyiv())


def _user_queue(uid: int):
    user = db.get_user(uid)
    return user["queue"] if user and user["queue"] else None


NEED_QUEUE_TEXT = "Спочатку обери свою чергу:"


@dp.message(CommandStart())
async def cmd_start(message: Message):
    uid = message.from_user.id
    db.ensure_user(uid)
    db.set_blocked(uid, False)
    current = _user_queue(uid)

    await message.answer(
        "⚡ Бот графіків відключення світла.\n\n"
        "Я автоматично читаю графіки з каналу та попереджаю тебе за 15 хвилин "
        "до планового відключення.\n\n"
        "ℹ️ Планове ввімкнення — це час за графіком, а не підтвердження "
        "фактичного відновлення світла.\n\n"
        + (f"Твоя поточна черга: {current}. Щоб змінити — обери нову:"
           if current else "Обери свою чергу:"),
        reply_markup=queue_keyboard(current),
    )
    if current:
        await message.answer("Меню:", reply_markup=main_menu())


@dp.message(Command("queue"))
async def cmd_queue(message: Message):
    parts = (message.text or "").split(maxsplit=1)
    uid = message.from_user.id

    if len(parts) == 2:  # старий спосіб: /queue 2.1
        queue = parts[1].strip().replace(",", ".")
        if not QUEUE_RE.fullmatch(queue):
            return await message.answer("Невірний формат. Приклад: /queue 2.1")
        return await message.answer(apply_queue_choice(uid, queue), reply_markup=main_menu())

    db.ensure_user(uid)
    await message.answer("Обери свою чергу:", reply_markup=queue_keyboard(_user_queue(uid)))


@dp.message(F.text.regexp(r"^\s*[1-6][.,][1-2]\s*$"))
async def text_queue(message: Message):
    queue = normalize_queue((message.text or "").strip())
    await message.answer(apply_queue_choice(message.from_user.id, queue),
                         reply_markup=main_menu())


@dp.message(Command("channel"))
async def cmd_channel(message: Message):
    await message.answer(f"📡 Джерело графіків: https://t.me/{CHANNEL}\n"
                         "Бот читає його автоматично, нічого пересилати не потрібно.")


@dp.message(Command("notify"))
async def cmd_notify(message: Message):
    parts = (message.text or "").split(maxsplit=1)
    uid = message.from_user.id

    if len(parts) != 2 or parts[1].strip().lower() not in ("on", "off"):
        user = db.get_user(uid)
        state = "увімкнені" if user and user["notifications"] else "вимкнені"
        return await message.answer(
            f"🔔 Сповіщення зараз {state}.\nВикористання: /notify on або /notify off")

    enabled = parts[1].strip().lower() == "on"
    db.set_notifications(uid, enabled)
    await message.answer("🔔 Сповіщення увімкнено." if enabled else "🔕 Сповіщення вимкнено.")


@dp.message(Command("status"))
async def cmd_status(message: Message):
    user = db.get_user(message.from_user.id)
    if not user:
        return await message.answer("Спочатку введи /start")

    last_ok = db.get_meta("last_poll_ok")
    last_err = db.get_meta("last_poll_error")
    lines = [
        f"🔌 Черга: {user['queue'] or 'не обрана'}",
        f"🔔 Сповіщення: {'увімкнені' if user['notifications'] else 'вимкнені'}",
        f"📡 Джерело: https://t.me/{CHANNEL}",
        "🕒 Остання успішна перевірка каналу: "
        + (last_ok[:16].replace("T", " ") if last_ok else "ще не було"),
    ]
    if user["queue"]:
        latest = db.get_latest_schedule_date(user["queue"])
        lines.append("📅 Останній отриманий графік: "
                     + (date.fromisoformat(latest).strftime("%d.%m.%Y") if latest else "ще немає"))
    if last_err:
        lines.append(f"⚠️ Остання помилка читання каналу: {last_err}")
    await message.answer("\n".join(lines))


@dp.callback_query(F.data.startswith("setq:"))
async def cb_set_queue(call: CallbackQuery):
    queue = (call.data or "")[5:]
    if not QUEUE_RE.fullmatch(queue):
        return await call.answer("Невірна черга", show_alert=True)

    text = apply_queue_choice(call.from_user.id, queue)
    try:
        await call.message.edit_reply_markup(reply_markup=queue_keyboard(queue))
    except TelegramAPIError:
        pass
    await call.message.answer(text, reply_markup=main_menu())
    await call.answer("Чергу збережено")


@dp.callback_query(F.data == "queue")
async def cb_queue(call: CallbackQuery):
    await call.message.answer("Обери свою чергу:",
                              reply_markup=queue_keyboard(_user_queue(call.from_user.id)))
    await call.answer()


@dp.callback_query(F.data == "schedule")
async def cb_schedule(call: CallbackQuery):
    queue = _user_queue(call.from_user.id)
    if not queue:
        await call.message.answer(NEED_QUEUE_TEXT, reply_markup=queue_keyboard())
    else:
        await call.message.answer(build_schedule_message(queue, now_kyiv()))
    await call.answer()


@dp.callback_query(F.data == "next")
async def cb_next(call: CallbackQuery):
    queue = _user_queue(call.from_user.id)
    if not queue:
        await call.message.answer(NEED_QUEUE_TEXT, reply_markup=queue_keyboard())
    else:
        await call.message.answer(build_next_message(queue, now_kyiv()))
    await call.answer()


@dp.callback_query(F.data == "calc")
async def cb_calc(call: CallbackQuery):
    queue = _user_queue(call.from_user.id)
    if not queue:
        await call.message.answer(NEED_QUEUE_TEXT, reply_markup=queue_keyboard())
    else:
        await call.message.answer(build_calc_message(queue, now_kyiv()))
    await call.answer()


@dp.callback_query(F.data == "notifications")
async def cb_notifications(call: CallbackQuery):
    user = db.get_user(call.from_user.id)
    new_value = not bool(user and user["notifications"])
    db.set_notifications(call.from_user.id, new_value)
    await call.message.answer(
        "🔔 Сповіщення увімкнено." if new_value else "🔕 Сповіщення вимкнено.")
    await call.answer()


@dp.callback_query(F.data == "history")
async def cb_history(call: CallbackQuery):
    rows = db.get_history(call.from_user.id, 10)
    await call.message.answer(
        "🕘 Останні події:\n" + "\n".join(rows) if rows else "Історія поки порожня.")
    await call.answer()


# ======================================================================
# ФОНОВІ ЦИКЛИ
# ======================================================================
async def scraper_loop(bot):
    timeout = aiohttp.ClientTimeout(total=25)
    last_prune = None
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            delay = SCRAPE_SECONDS
            try:
                await scrape_once(bot, session)
            except asyncio.CancelledError:
                raise
            except FetchError as exc:
                log.warning("Не вдалося прочитати канал: %s (повторю за кілька секунд)", exc)
                db.set_meta("last_poll_error", f"{now_kyiv():%d.%m %H:%M} {exc}")
                delay = min(SCRAPE_SECONDS, 20)
            except Exception as exc:
                log.exception("Неочікувана помилка при читанні каналу")
                db.set_meta("last_poll_error", f"{now_kyiv():%d.%m %H:%M} {exc!r}")
                delay = min(SCRAPE_SECONDS, 20)

            try:
                today = now_kyiv().date()
                if last_prune != today:
                    db.prune_posts(60)
                    db.prune_sent(14)
                    last_prune = today
            except Exception:
                log.exception("Не вдалося очистити старі записи")

            await asyncio.sleep(delay)


async def notifier_loop(bot):
    while True:
        try:
            await process_notifications(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Помилка циклу сповіщень")
        await asyncio.sleep(NOTIFY_TICK_SECONDS)


async def start_health_server():
    """
    Необов'язково: якщо задано PORT, відкриває простий HTTP-відповідач "OK".
    Деякі безкоштовні хостинги вимагають відкритий порт, а зовнішній сервіс
    моніторингу може пінгувати його, щоб сервіс не засинав.
    """
    port = os.getenv("PORT", "").strip()
    if not port.isdigit():
        return None
    from aiohttp import web

    async def handler(_request):
        return web.json_response({
            "status": "ok",
            "last_poll_ok": db.get_meta("last_poll_ok"),
        })

    app = web.Application()
    app.router.add_get("/", handler)
    app.router.add_get("/health", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(port)).start()
    log.info("Health-сервер запущено на порту %s", port)
    return runner


def setup_logging():
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    handlers: list = [logging.StreamHandler()]
    try:
        handlers.append(RotatingFileHandler(
            BASE_DIR / "bot.log", maxBytes=1_000_000, backupCount=3, encoding="utf-8"))
    except OSError:
        pass  # на деяких хостингах диск лише для читання — тоді лог тільки в консоль
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
        force=True,
    )


async def main():
    global db
    setup_logging()

    if not BOT_TOKEN:
        raise SystemExit("У файлі .env не вказано BOT_TOKEN (див. .env.example)")

    db = Database(DB_PATH)
    log.info("База даних: %s; канал: %s", DB_PATH, CHANNEL)

    bot = Bot(BOT_TOKEN)
    await bot.delete_webhook(drop_pending_updates=False)
    health = await start_health_server()

    tasks = [
        asyncio.create_task(scraper_loop(bot), name="scraper"),
        asyncio.create_task(notifier_loop(bot), name="notifier"),
    ]
    try:
        await dp.start_polling(bot)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if health:
            await health.cleanup()
        await bot.session.close()


# ======================================================================
# Службові режими для перевірки (без токена бота)
# ======================================================================
def _describe(parsed: ParsedSchedule | None, min_queues: int) -> list:
    if not parsed:
        return ["   → черг у тексті не знайдено (це не графік)"]
    lines = [
        f"   → дата графіка: {parsed.schedule_date:%d.%m.%Y} "
        f"({'знайдена в тексті' if parsed.date_source == 'text' else 'ВЗЯТА З ДАТИ ПУБЛІКАЦІЇ'})",
        f"   → розпізнано черг: {len(parsed.queues)} (потрібно {min_queues}) → "
        + ("ПОВНИЙ ГРАФІК ✅" if len(parsed.queues) >= min_queues else "неповний, буде проігноровано ❌"),
    ]
    for q in QUEUES:
        if q in parsed.queues:
            ivs = parsed.queues[q]
            txt = ", ".join(f"{s:%H:%M}-{e:%H:%M}" for s, e in ivs) or "відключень немає"
            lines.append(f"      {q}: {txt}")
    missing = [q for q in QUEUES if q not in parsed.queues]
    if missing:
        lines.append(f"      не знайдено: {', '.join(missing)}")
    return lines


def cli_check():
    async def run():
        timeout = aiohttp.ClientTimeout(total=25)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            html = await fetch_channel_html(session, CHANNEL)
        posts = parse_channel_html(html, CHANNEL)
        print(f"Канал: https://t.me/s/{CHANNEL}")
        print(f"Знайдено дописів на сторінці: {len(posts)}\n")
        if not posts:
            print("Дописів немає: канал може не віддавати публічний перегляд.")
            return
        for post in posts[-8:]:
            when = post.posted_at.astimezone(TZ).strftime("%d.%m.%Y %H:%M") if post.posted_at else "?"
            print(f"Допис #{post.post_id} · {when} · {len(post.text)} символів")
            if not post.text:
                print("   → тексту немає (можливо, графік у вигляді картинки — такі бот прочитати не може)")
            else:
                print("   текст: " + post.text[:120].replace("\n", " ⏎ "))
                for line in _describe(parse_schedule_text(post.text, post.posted_at), MIN_QUEUES):
                    print(line)
            print()
    try:
        asyncio.run(run())
    except FetchError as exc:
        print(f"ПОМИЛКА: {exc}")
        sys.exit(1)


def cli_parse_file(path: str):
    text = Path(path).read_text(encoding="utf-8")
    for line in _describe(parse_schedule_text(text, now_kyiv()), MIN_QUEUES):
        print(line)


if __name__ == "__main__":
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    ap = argparse.ArgumentParser(description="Бот графіків відключення світла")
    ap.add_argument("--check", action="store_true",
                    help="прочитати публічну сторінку каналу й показати, що розпізнано (без запуску бота)")
    ap.add_argument("--parse-file", metavar="ФАЙЛ",
                    help="розпізнати графік із текстового файлу (скопіюйте туди текст допису)")
    args = ap.parse_args()

    if args.check:
        cli_check()
    elif args.parse_file:
        cli_parse_file(args.parse_file)
    else:
        asyncio.run(main())
