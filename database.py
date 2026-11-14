"""
База даних бота графіків відключення світла (SQLite).

Що важливо знати:
  * Усі старі таблиці (users, events, sent, history, global_events) ЗБЕРІГАЮТЬСЯ.
    Нічого не видаляється й не перейменовується. Нові можливості додаються
    новими таблицями та новими стовпцями (безпечна міграція).
  * Перед першою міграцією існуючого файлу автоматично створюється копія:
    <файл бази>.backup-before-v2
  * Графіки зберігаються централізовано в таблиці global_events
    (один запис = одне відключення однієї черги), а не для кожного користувача.
  * Кожен користувач має власну чергу та власне налаштування сповіщень (users).
"""

import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

log = logging.getLogger("lightbot.db")

TZ = ZoneInfo("Europe/Kyiv")
SCHEMA_VERSION = 2


def _now_iso():
    return datetime.now(TZ).isoformat(timespec="seconds")


class Database:
    def __init__(self, path):
        self.path = str(path)
        self._migrate()

    # ------------------------------------------------------------------
    # З'єднання
    # ------------------------------------------------------------------
    def connect(self):
        c = sqlite3.connect(self.path, timeout=30)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA busy_timeout = 30000")
        return c

    @contextmanager
    def _session(self):
        """Транзакція: commit при успіху, rollback при помилці, з'єднання завжди закривається."""
        c = self.connect()
        try:
            yield c
            c.commit()
        except Exception:
            c.rollback()
            raise
        finally:
            c.close()

    # ------------------------------------------------------------------
    # Створення таблиць і безпечна міграція
    # ------------------------------------------------------------------
    def _migrate(self):
        path = Path(self.path)
        is_new_file = (not path.exists()) or path.stat().st_size == 0

        with self._session() as c:
            version = c.execute("PRAGMA user_version").fetchone()[0]

        if not is_new_file and version < SCHEMA_VERSION:
            self._backup()

        with self._session() as c:
            # --- таблиці, що вже існували (створюються тільки якщо їх немає) ---
            c.execute("""
                CREATE TABLE IF NOT EXISTS users(
                    id INTEGER PRIMARY KEY,
                    queue TEXT,
                    channel TEXT,
                    notifications INTEGER NOT NULL DEFAULT 1
                )""")
            c.execute("""
                CREATE TABLE IF NOT EXISTS events(
                    channel TEXT NOT NULL,
                    queue TEXT NOT NULL,
                    event_key TEXT NOT NULL,
                    start_at TEXT NOT NULL,
                    end_at TEXT NOT NULL,
                    source_date TEXT,
                    PRIMARY KEY(channel, queue, event_key)
                )""")
            c.execute("""
                CREATE TABLE IF NOT EXISTS sent(
                    user_id INTEGER NOT NULL,
                    event_key TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    PRIMARY KEY(user_id, event_key, kind)
                )""")
            c.execute("""
                CREATE TABLE IF NOT EXISTS history(
                    user_id INTEGER NOT NULL,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )""")
            c.execute("""
                CREATE TABLE IF NOT EXISTS global_events(
                    queue TEXT NOT NULL,
                    event_key TEXT NOT NULL,
                    start_at TEXT NOT NULL,
                    end_at TEXT NOT NULL,
                    source_date TEXT,
                    channel TEXT NOT NULL,
                    PRIMARY KEY(queue, event_key)
                )""")

            # --- нові таблиці ---
            # Усі оброблені дописи каналу (щоб не обробляти один допис двічі
            # і щоб помічати редагування за зміною хешу тексту).
            c.execute("""
                CREATE TABLE IF NOT EXISTS posts(
                    channel TEXT NOT NULL,
                    post_id INTEGER NOT NULL,
                    posted_at TEXT,
                    text_hash TEXT NOT NULL,
                    text TEXT NOT NULL,
                    schedule_date TEXT,
                    queues_found INTEGER NOT NULL DEFAULT 0,
                    is_schedule INTEGER NOT NULL DEFAULT 0,
                    first_seen_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(channel, post_id)
                )""")
            # Для яких черг і на які дати графік відомий (навіть якщо відключень немає).
            c.execute("""
                CREATE TABLE IF NOT EXISTS schedule_days(
                    queue TEXT NOT NULL,
                    schedule_date TEXT NOT NULL,
                    post_id INTEGER,
                    channel TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(queue, schedule_date)
                )""")
            c.execute("""
                CREATE TABLE IF NOT EXISTS meta(
                    key TEXT PRIMARY KEY,
                    value TEXT
                )""")

            # --- нові стовпці у старих таблицях ---
            self._add_column(c, "users", "blocked", "INTEGER NOT NULL DEFAULT 0")
            self._add_column(c, "users", "created_at", "TEXT")
            self._add_column(c, "global_events", "schedule_date", "TEXT")
            self._add_column(c, "global_events", "post_id", "INTEGER")
            self._add_column(c, "global_events", "updated_at", "TEXT")
            self._add_column(c, "sent", "sent_at", "TEXT")

            # Старі записи (з часів каналу-посередника) отримують дату графіка
            # з початку відключення. Їх post_id порожній, тому нові сповіщення
            # на їх основі НЕ надсилаються, але самі дані збережені.
            c.execute("""
                UPDATE global_events
                SET schedule_date = substr(start_at, 1, 10)
                WHERE schedule_date IS NULL
            """)

            c.execute("CREATE INDEX IF NOT EXISTS idx_ge_queue_date "
                      "ON global_events(queue, schedule_date)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_posts_date "
                      "ON posts(channel, schedule_date, is_schedule)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_users_queue "
                      "ON users(queue)")

            c.execute(f"PRAGMA user_version = {int(SCHEMA_VERSION)}")

    @staticmethod
    def _add_column(c, table, column, ddl):
        cols = {r["name"] for r in c.execute(f"PRAGMA table_info({table})")}
        if column not in cols:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    def _backup(self):
        backup_path = f"{self.path}.backup-before-v{SCHEMA_VERSION}"
        if Path(backup_path).exists():
            return
        try:
            src = sqlite3.connect(self.path)
            dst = sqlite3.connect(backup_path)
            try:
                src.backup(dst)
            finally:
                dst.close()
                src.close()
            log.info("Створено резервну копію бази: %s", backup_path)
        except Exception:
            log.exception("Не вдалося створити резервну копію бази")

    # ------------------------------------------------------------------
    # Користувачі
    # ------------------------------------------------------------------
    def ensure_user(self, uid):
        with self._session() as c:
            c.execute(
                "INSERT OR IGNORE INTO users(id, created_at) VALUES(?, ?)",
                (uid, _now_iso()),
            )

    def get_user(self, uid):
        with self._session() as c:
            r = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
            return dict(r) if r else None

    def set_queue(self, uid, queue):
        self.ensure_user(uid)
        with self._session() as c:
            c.execute("UPDATE users SET queue=? WHERE id=?", (queue, uid))

    def set_channel(self, uid, channel):
        # Залишено для сумісності зі старою версією; у новій схемі не потрібно.
        self.ensure_user(uid)
        with self._session() as c:
            c.execute("UPDATE users SET channel=? WHERE id=?", (channel, uid))

    def set_notifications(self, uid, enabled):
        self.ensure_user(uid)
        with self._session() as c:
            c.execute(
                "UPDATE users SET notifications=? WHERE id=?",
                (int(bool(enabled)), uid),
            )

    def set_blocked(self, uid, blocked):
        """Позначає, що користувач заблокував бота (щоб не слати йому повідомлення)."""
        with self._session() as c:
            c.execute(
                "UPDATE users SET blocked=? WHERE id=?",
                (int(bool(blocked)), uid),
            )

    def get_users_with_settings(self):
        """Усі користувачі з вибраною чергою (незалежно від того, чи вже був графік)."""
        with self._session() as c:
            rows = c.execute(
                "SELECT * FROM users WHERE queue IS NOT NULL AND blocked=0"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_users_by_queue(self, queue, only_notifying=True):
        sql = "SELECT * FROM users WHERE queue=? AND blocked=0"
        if only_notifying:
            sql += " AND notifications=1"
        with self._session() as c:
            return [dict(r) for r in c.execute(sql, (queue,)).fetchall()]

    # ------------------------------------------------------------------
    # Дописи каналу
    # ------------------------------------------------------------------
    def get_post(self, channel, post_id):
        with self._session() as c:
            r = c.execute(
                "SELECT * FROM posts WHERE channel=? AND post_id=?",
                (channel, post_id),
            ).fetchone()
            return dict(r) if r else None

    def upsert_post(self, channel, post_id, posted_at, text_hash, text,
                    schedule_date, queues_found, is_schedule):
        now = _now_iso()
        with self._session() as c:
            c.execute(
                """
                INSERT INTO posts(channel, post_id, posted_at, text_hash, text,
                                  schedule_date, queues_found, is_schedule,
                                  first_seen_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(channel, post_id) DO UPDATE SET
                    posted_at=excluded.posted_at,
                    text_hash=excluded.text_hash,
                    text=excluded.text,
                    schedule_date=excluded.schedule_date,
                    queues_found=excluded.queues_found,
                    is_schedule=excluded.is_schedule,
                    updated_at=excluded.updated_at
                """,
                (channel, post_id, posted_at, text_hash, text, schedule_date,
                 int(queues_found), int(bool(is_schedule)), now, now),
            )

    def get_winning_post(self, channel, schedule_date):
        """Актуальний допис із повним графіком на дату: найновіший (найбільший номер)."""
        with self._session() as c:
            r = c.execute(
                """
                SELECT * FROM posts
                WHERE channel=? AND schedule_date=? AND is_schedule=1
                ORDER BY post_id DESC LIMIT 1
                """,
                (channel, schedule_date),
            ).fetchone()
            return dict(r) if r else None

    def prune_posts(self, days=60):
        cutoff = (datetime.now(TZ) - timedelta(days=days)).isoformat(timespec="seconds")
        with self._session() as c:
            c.execute(
                "DELETE FROM posts WHERE COALESCE(posted_at, first_seen_at) < ?",
                (cutoff,),
            )

    # ------------------------------------------------------------------
    # Графіки (централізовано для всіх користувачів)
    # ------------------------------------------------------------------
    def replace_day_schedule(self, schedule_date, channel, post_id, queues,
                             source_date=None):
        """
        Замінює графік на одну дату для кожної черги зі словника `queues`:
            {"2.1": [(start_dt, end_dt), ...], ...}
        Усе в одній транзакції. Графіки інших дат і черг, яких немає у `queues`,
        не змінюються.
        """
        now = _now_iso()
        with self._session() as c:
            for queue, events in queues.items():
                c.execute(
                    """
                    DELETE FROM global_events
                    WHERE queue=? AND schedule_date=? AND post_id IS NOT NULL
                    """,
                    (queue, schedule_date),
                )
                for start, end in events:
                    key = (f"{queue}|{start:%Y-%m-%dT%H:%M}|{end:%Y-%m-%dT%H:%M}")
                    c.execute(
                        """
                        INSERT OR REPLACE INTO global_events
                        (queue, event_key, start_at, end_at, source_date, channel,
                         schedule_date, post_id, updated_at)
                        VALUES(?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            queue,
                            key,
                            start.isoformat(timespec="minutes"),
                            end.isoformat(timespec="minutes"),
                            source_date,
                            channel,
                            schedule_date,
                            post_id,
                            now,
                        ),
                    )
                c.execute(
                    """
                    INSERT OR REPLACE INTO schedule_days
                    (queue, schedule_date, post_id, channel, updated_at)
                    VALUES(?,?,?,?,?)
                    """,
                    (queue, schedule_date, post_id, channel, now),
                )

    def save_global_events(self, channel, queue, events, source_date=None,
                           post_id=None):
        """
        Сумісність зі старою версією: зберігає список подій однієї черги.
        Події групуються за датою початку й замінюють графік саме цієї дати
        (інші дати не видаляються).
        """
        by_date = {}
        for e in events:
            by_date.setdefault(e["start"].date().isoformat(), []).append(
                (e["start"], e["end"])
            )
        for d, items in by_date.items():
            self.replace_day_schedule(d, channel, post_id if post_id is not None else 0,
                                      {queue: items}, source_date)

    def get_global_events(self, queue, since_date=None):
        """Відключення черги, отримані з каналу (старі записи без post_id не враховуються)."""
        sql = ("SELECT * FROM global_events "
               "WHERE queue=? AND post_id IS NOT NULL")
        params = [queue]
        if since_date:
            sql += " AND schedule_date >= ?"
            params.append(since_date)
        sql += " ORDER BY schedule_date, start_at"
        with self._session() as c:
            return [dict(r) for r in c.execute(sql, params).fetchall()]

    def get_schedule_dates(self, queue, since_date=None):
        sql = "SELECT schedule_date FROM schedule_days WHERE queue=?"
        params = [queue]
        if since_date:
            sql += " AND schedule_date >= ?"
            params.append(since_date)
        sql += " ORDER BY schedule_date"
        with self._session() as c:
            return [r["schedule_date"] for r in c.execute(sql, params).fetchall()]

    def get_latest_schedule_date(self, queue):
        with self._session() as c:
            r = c.execute(
                "SELECT MAX(schedule_date) AS d FROM schedule_days WHERE queue=?",
                (queue,),
            ).fetchone()
            return r["d"] if r and r["d"] else None

    def get_schedule_snapshot(self, since_date):
        """
        Знімок поточних графіків: {(черга, дата): ((початок, кінець), ...)}.
        Використовується, щоб визначити, чи справді графік змінився.
        """
        snapshot = {}
        with self._session() as c:
            for r in c.execute(
                "SELECT queue, schedule_date FROM schedule_days WHERE schedule_date >= ?",
                (since_date,),
            ):
                snapshot[(r["queue"], r["schedule_date"])] = []
            for r in c.execute(
                """
                SELECT queue, schedule_date, start_at, end_at FROM global_events
                WHERE post_id IS NOT NULL AND schedule_date >= ?
                """,
                (since_date,),
            ):
                snapshot.setdefault((r["queue"], r["schedule_date"]), []).append(
                    (r["start_at"], r["end_at"])
                )
        return {k: tuple(sorted(v)) for k, v in snapshot.items()}

    def get_global_channel(self):
        with self._session() as c:
            row = c.execute(
                "SELECT channel FROM posts ORDER BY post_id DESC LIMIT 1"
            ).fetchone()
            if row:
                return row["channel"]
            row = c.execute(
                "SELECT channel FROM global_events ORDER BY source_date DESC LIMIT 1"
            ).fetchone()
            return row["channel"] if row else None

    def get_all_global_schedules(self):
        result = {}
        with self._session() as c:
            rows = c.execute(
                """
                SELECT * FROM global_events
                WHERE post_id IS NOT NULL
                ORDER BY queue, schedule_date, start_at
                """
            ).fetchall()
            for row in rows:
                item = dict(row)
                result.setdefault(item["queue"], []).append(item)
        return result

    # --- старі "особисті" події (збережено для сумісності, у новій логіці не потрібні) ---
    def save_events(self, channel, queue, events, source_date=None):
        with self._session() as c:
            for e in events:
                key = f"{queue}|{e['start'].isoformat()}|{e['end'].isoformat()}"
                c.execute(
                    """
                    INSERT OR REPLACE INTO events
                    (channel, queue, event_key, start_at, end_at, source_date)
                    VALUES(?,?,?,?,?,?)
                    """,
                    (
                        channel,
                        queue,
                        key,
                        e["start"].isoformat(timespec="minutes"),
                        e["end"].isoformat(timespec="minutes"),
                        source_date,
                    ),
                )

    def get_events(self, channel, queue):
        with self._session() as c:
            rows = c.execute(
                "SELECT * FROM events WHERE channel=? AND queue=? ORDER BY start_at",
                (channel, queue),
            ).fetchall()
            if rows:
                return [dict(r) for r in rows]
        return self.get_global_events(queue)

    # ------------------------------------------------------------------
    # Захист від повторних сповіщень
    # ------------------------------------------------------------------
    def was_notified(self, uid, key, kind):
        with self._session() as c:
            return c.execute(
                "SELECT 1 FROM sent WHERE user_id=? AND event_key=? AND kind=?",
                (uid, key, kind),
            ).fetchone() is not None

    def mark_notified(self, uid, key, kind):
        with self._session() as c:
            c.execute(
                "INSERT OR IGNORE INTO sent(user_id, event_key, kind, sent_at) "
                "VALUES(?,?,?,?)",
                (uid, key, kind, _now_iso()),
            )

    def prune_sent(self, days=14):
        cutoff = (datetime.now(TZ) - timedelta(days=days)).isoformat(timespec="seconds")
        with self._session() as c:
            c.execute(
                "DELETE FROM sent WHERE sent_at IS NOT NULL AND sent_at < ?",
                (cutoff,),
            )

    # ------------------------------------------------------------------
    # Історія
    # ------------------------------------------------------------------
    def add_history(self, uid, message):
        with self._session() as c:
            c.execute(
                "INSERT INTO history(user_id, message, created_at) VALUES(?,?,?)",
                (uid, message, _now_iso()),
            )

    def get_history(self, uid, limit=10):
        with self._session() as c:
            rows = c.execute(
                """
                SELECT message, created_at FROM history
                WHERE user_id=?
                ORDER BY rowid DESC
                LIMIT ?
                """,
                (uid, limit),
            ).fetchall()
            return [f"• {r['created_at'][:16]} — {r['message']}" for r in rows]

    # ------------------------------------------------------------------
    # Службові значення (стан останньої перевірки каналу тощо)
    # ------------------------------------------------------------------
    def set_meta(self, key, value):
        with self._session() as c:
            c.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, None if value is None else str(value)),
            )

    def get_meta(self, key, default=None):
        with self._session() as c:
            r = c.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return r["value"] if r and r["value"] is not None else default
