import os
import io
import csv
import logging
import sqlite3
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from contextlib import closing

from flask import Flask, request, jsonify, redirect, Response, render_template
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo,
    InputFile,
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ============================================================
# CONFIG
# ============================================================
BOT_TOKEN = os.getenv("8225104783:AAGsMLrMPYHm9lreO54-MiAZfuT0EfuV8IY", "")
BASE_URL = os.getenv("BASE_URL", "https://warehouse-mini-app.onrender.com").rstrip("/")
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("PORT", "8080"))
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "481903f396246a735d26ceebbb2a2190")
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "America/New_York")
DB_PATH = os.getenv("DB_PATH", "orders.db")
DEFAULT_AUTO_EXPORT_HOUR = int(os.getenv("DEFAULT_AUTO_EXPORT_HOUR", "21"))

TZ = ZoneInfo(APP_TIMEZONE)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("warehouse_app")

app = Flask(__name__)
scheduler = BackgroundScheduler(timezone=APP_TIMEZONE)

telegram_app = None
if BOT_TOKEN:
    telegram_app = Application.builder().token(BOT_TOKEN).build()
else:
    logger.warning("BOT_TOKEN is not configured. Telegram bot features will be disabled.")

# ============================================================
# DATABASE
# ============================================================
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    created_date TEXT NOT NULL,
    telegram_user_id INTEGER,
    telegram_username TEXT,
    technician_name TEXT,
    bp_number TEXT NOT NULL,
    xg1v4 INTEGER NOT NULL DEFAULT 0,
    xi6 INTEGER NOT NULL DEFAULT 0,
    xid INTEGER NOT NULL DEFAULT 0,
    xct2 INTEGER NOT NULL DEFAULT 0,
    ddr INTEGER NOT NULL DEFAULT 0,
    notes TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS admin_users (
    telegram_user_id INTEGER PRIMARY KEY,
    added_at TEXT NOT NULL
);
"""


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(get_db()) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
        ensure_setting("auto_export_hour", str(DEFAULT_AUTO_EXPORT_HOUR))


def ensure_setting(key: str, value: str):
    with closing(get_db()) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)",
            (key, value),
        )
        conn.commit()


def set_setting(key: str, value: str):
    with closing(get_db()) as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        conn.commit()


def get_setting(key: str, default=None):
    with closing(get_db()) as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def add_admin_user(user_id: int):
    now = datetime.now(TZ).isoformat()
    with closing(get_db()) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO admin_users(telegram_user_id, added_at) VALUES(?, ?)",
            (user_id, now),
        )
        conn.commit()


def is_admin(user_id: int) -> bool:
    with closing(get_db()) as conn:
        row = conn.execute(
            "SELECT telegram_user_id FROM admin_users WHERE telegram_user_id=?",
            (user_id,),
        ).fetchone()
        return row is not None


def save_order(payload: dict):
    now = datetime.now(TZ)
    with closing(get_db()) as conn:
        conn.execute(
            """
            INSERT INTO orders (
                created_at, created_date, telegram_user_id, telegram_username,
                technician_name, bp_number, xg1v4, xi6, xid, xct2, ddr, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now.isoformat(),
                now.date().isoformat(),
                payload.get("telegram_user_id"),
                payload.get("telegram_username", ""),
                payload.get("technician_name", ""),
                payload["bp_number"],
                int(payload.get("xg1v4", 0)),
                int(payload.get("xi6", 0)),
                int(payload.get("xid", 0)),
                int(payload.get("xct2", 0)),
                int(payload.get("ddr", 0)),
                payload.get("notes", ""),
            ),
        )
        conn.commit()


def fetch_orders_for_day(day_iso: str):
    with closing(get_db()) as conn:
        rows = conn.execute(
            "SELECT * FROM orders WHERE created_date=? ORDER BY created_at DESC",
            (day_iso,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_daily_stats(day_iso: str):
    with closing(get_db()) as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_orders,
                COALESCE(SUM(xg1v4), 0) AS xg1v4,
                COALESCE(SUM(xi6), 0) AS xi6,
                COALESCE(SUM(xid), 0) AS xid,
                COALESCE(SUM(xct2), 0) AS xct2,
                COALESCE(SUM(ddr), 0) AS ddr
            FROM orders
            WHERE created_date=?
            """,
            (day_iso,),
        ).fetchone()

        by_tech = conn.execute(
            """
            SELECT
                COALESCE(technician_name, '') AS technician_name,
                bp_number,
                COUNT(*) AS orders,
                COALESCE(SUM(xg1v4), 0) AS xg1v4,
                COALESCE(SUM(xi6), 0) AS xi6,
                COALESCE(SUM(xid), 0) AS xid,
                COALESCE(SUM(xct2), 0) AS xct2,
                COALESCE(SUM(ddr), 0) AS ddr
            FROM orders
            WHERE created_date=?
            GROUP BY technician_name, bp_number
            ORDER BY technician_name, bp_number
            """,
            (day_iso,),
        ).fetchall()

        return {
            "date": day_iso,
            "summary": dict(row) if row else {},
            "by_technician": [dict(r) for r in by_tech],
        }


def build_daily_csv_bytes(day_iso: str) -> bytes:
    orders = fetch_orders_for_day(day_iso)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "created_at",
        "telegram_user_id",
        "telegram_username",
        "technician_name",
        "bp_number",
        "xg1v4",
        "xi6",
        "xid",
        "xct2",
        "ddr",
        "notes",
    ])
    for row in orders:
        writer.writerow([
            row["created_at"],
            row["telegram_user_id"],
            row["telegram_username"],
            row["technician_name"],
            row["bp_number"],
            row["xg1v4"],
            row["xi6"],
            row["xid"],
            row["xct2"],
            row["ddr"],
            row["notes"],
        ])
    return output.getvalue().encode("utf-8-sig")


def stats_to_text(day_iso: str) -> str:
    stats = get_daily_stats(day_iso)
    s = stats["summary"]
    lines = [
        f"📊 Daily Stats - {day_iso}",
        f"Total orders: {s.get('total_orders', 0)}",
        f"XG1v4: {s.get('xg1v4', 0)}",
        f"XI6: {s.get('xi6', 0)}",
        f"XID: {s.get('xid', 0)}",
        f"XCT2: {s.get('xct2', 0)}",
        f"DDR: {s.get('ddr', 0)}",
        "",
        "By technician:",
    ]
    if not stats["by_technician"]:
        lines.append("- No orders for this date.")
    else:
        for row in stats["by_technician"]:
            tech = row["technician_name"] or "Unknown"
            lines.append(
                f"- {tech} | BP {row['bp_number']} | Orders {row['orders']} | "
                f"XG1v4 {row['xg1v4']} | XI6 {row['xi6']} | XID {row['xid']} | "
                f"XCT2 {row['xct2']} | DDR {row['ddr']}"
            )
    return "\n".join(lines)


# ============================================================
# TELEGRAM BOT
# ============================================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    keyboard = [[
        InlineKeyboardButton(
            text="Open Order App",
            web_app=WebAppInfo(
                url=f"{BASE_URL}/webapp?uid={user.id}&name={user.full_name}&username={user.username or ''}"
            ),
        )
    ]]
    await update.message.reply_text(
        "Open the app to submit warehouse requests.",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Unauthorized. You are not an admin.")
        return

    hour = get_setting("auto_export_hour", str(DEFAULT_AUTO_EXPORT_HOUR))
    msg = (
        "🔐 Admin Panel\n\n"
        "Commands:\n"
        "/stats - show today's stats\n"
        "/export - export today's CSV here in Telegram\n"
        "/exportdate YYYY-MM-DD - export any date\n"
        "/sethour HH - set automatic daily export hour (0-23)\n"
        f"\nCurrent automatic export hour: {hour}:00 ({APP_TIMEZONE})"
    )
    await update.message.reply_text(msg)


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Unauthorized.")
        return
    today = datetime.now(TZ).date().isoformat()
    await update.message.reply_text(stats_to_text(today))


async def export_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Unauthorized.")
        return
    today = datetime.now(TZ).date().isoformat()
    await send_daily_export_to_chat(context, update.effective_chat.id, today)


async def export_date_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Unauthorized.")
        return

    if not context.args:
        await update.message.reply_text("Use: /exportdate YYYY-MM-DD")
        return

    day_iso = context.args[0]
    try:
        datetime.strptime(day_iso, "%Y-%m-%d")
    except ValueError:
        await update.message.reply_text("Invalid date format. Use YYYY-MM-DD")
        return

    await send_daily_export_to_chat(context, update.effective_chat.id, day_iso)


async def set_hour_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Unauthorized.")
        return

    if not context.args:
        await update.message.reply_text("Use: /sethour HH")
        return

    try:
        hour = int(context.args[0])
        if hour < 0 or hour > 23:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Hour must be a number from 0 to 23.")
        return

    set_setting("auto_export_hour", str(hour))
    reschedule_auto_export(hour)
    await update.message.reply_text(
        f"✅ Automatic daily export set to {hour:02d}:00 ({APP_TIMEZONE})."
    )


async def grant_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Use: /grantadmin YOUR_ADMIN_TOKEN")
        return

    supplied = context.args[0]
    if supplied != ADMIN_TOKEN:
        await update.message.reply_text("Invalid admin token.")
        return

    user = update.effective_user
    add_admin_user(user.id)
    await update.message.reply_text("✅ Admin access granted to your Telegram account.")


async def send_daily_export_to_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int, day_iso: str):
    csv_bytes = build_daily_csv_bytes(day_iso)
    caption = stats_to_text(day_iso)
    input_file = InputFile(io.BytesIO(csv_bytes), filename=f"orders_{day_iso}.csv")
    await context.bot.send_document(
        chat_id=chat_id,
        document=input_file,
        caption=caption[:1024],
    )


async def auto_export_job():
    if telegram_app is None:
        return

    today = datetime.now(TZ).date().isoformat()
    with closing(get_db()) as conn:
        admins = conn.execute("SELECT telegram_user_id FROM admin_users").fetchall()

    if not admins:
        logger.info("No admin users registered. Skipping auto export.")
        return

    for row in admins:
        try:
            csv_bytes = build_daily_csv_bytes(today)
            caption = stats_to_text(today)
            input_file = InputFile(io.BytesIO(csv_bytes), filename=f"orders_{today}.csv")
            await telegram_app.bot.send_document(
                chat_id=row["telegram_user_id"],
                document=input_file,
                caption=caption[:1024],
            )
        except Exception as exc:
            logger.exception(
                "Failed sending auto export to admin %s: %s",
                row["telegram_user_id"],
                exc,
            )


def run_auto_export_job():
    if telegram_app is None:
        return
    try:
        import asyncio
        asyncio.run(auto_export_job())
    except Exception as exc:
        logger.exception("Auto export job failed: %s", exc)


def reschedule_auto_export(hour: int):
    if scheduler.get_job("daily_export"):
        scheduler.remove_job("daily_export")

    scheduler.add_job(
        func=run_auto_export_job,
        trigger="cron",
        id="daily_export",
        hour=hour,
        minute=0,
        replace_existing=True,
    )
    logger.info("Auto export scheduled at %02d:00 %s", hour, APP_TIMEZONE)


def register_handlers():
    if telegram_app is None:
        return
    telegram_app.add_handler(CommandHandler("start", start_command))
    telegram_app.add_handler(CommandHandler("admin", admin_command))
    telegram_app.add_handler(CommandHandler("stats", stats_command))
    telegram_app.add_handler(CommandHandler("export", export_command))
    telegram_app.add_handler(CommandHandler("exportdate", export_date_command))
    telegram_app.add_handler(CommandHandler("sethour", set_hour_command))
    telegram_app.add_handler(CommandHandler("grantadmin", grant_admin_command))


def run_bot():
    if telegram_app is None:
        logger.info("Telegram bot not started because BOT_TOKEN is missing.")
        return
    register_handlers()
    telegram_app.run_polling(close_loop=False)


# ============================================================
# WEB ROUTES
# ============================================================
@app.get("/")
def root():
    ensure_app_started()
    return redirect("/webapp")


@app.get("/webapp")
def webapp_page():
    ensure_app_started()
    return render_template("request_form.html")


@app.post("/api/orders")
def create_order():
    ensure_app_started()

    data = request.get_json(silent=True) or {}
    bp_number = str(data.get("bp_number", "")).strip()

    if not bp_number:
        return jsonify({"error": "BP Number is required."}), 400

    payload = {
        "telegram_user_id": data.get("telegram_user_id"),
        "telegram_username": data.get("telegram_username", ""),
        "technician_name": str(data.get("technician_name", "")).strip(),
        "bp_number": bp_number,
        "xg1v4": int(data.get("xg1v4", 0) or 0),
        "xi6": int(data.get("xi6", 0) or 0),
        "xid": int(data.get("xid", 0) or 0),
        "xct2": int(data.get("xct2", 0) or 0),
        "ddr": int(data.get("ddr", 0) or 0),
        "notes": str(data.get("notes", "")).strip(),
    }

    save_order(payload)
    return jsonify({"ok": True, "message": "Order submitted successfully."})


@app.get("/admin/stats")
def admin_stats_api():
    ensure_app_started()

    token = request.args.get("token", "")
    if token != ADMIN_TOKEN:
        return jsonify({"error": "Unauthorized"}), 401

    day_iso = request.args.get("date") or datetime.now(TZ).date().isoformat()
    return jsonify(get_daily_stats(day_iso))


@app.get("/admin/export")
def admin_export_api():
    ensure_app_started()

    token = request.args.get("token", "")
    if token != ADMIN_TOKEN:
        return jsonify({"error": "Unauthorized"}), 401

    day_iso = request.args.get("date") or datetime.now(TZ).date().isoformat()
    csv_bytes = build_daily_csv_bytes(day_iso)

    return Response(
        csv_bytes,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=orders_{day_iso}.csv"},
    )


# ============================================================
# APP STARTUP
# ============================================================
_started = False
_start_lock = threading.Lock()


def setup_scheduler():
    hour = int(get_setting("auto_export_hour", str(DEFAULT_AUTO_EXPORT_HOUR)))
    if not scheduler.running:
        scheduler.start()
    reschedule_auto_export(hour)


def start_background_services():
    if telegram_app is not None:
        bot_thread = threading.Thread(target=run_bot, daemon=True)
        bot_thread.start()
        logger.info("Telegram bot thread started.")
    else:
        logger.info("Telegram bot disabled.")

    setup_scheduler()
    logger.info("Scheduler started.")


def ensure_app_started():
    global _started
    if _started:
        return

    with _start_lock:
        if _started:
            return

        init_db()
        start_background_services()
        _started = True
        logger.info("Application startup completed.")


ensure_app_started()

if __name__ == "__main__":
    logger.info("Web app starting on %s:%s", APP_HOST, APP_PORT)
    logger.info("BASE_URL=%s", BASE_URL)
    app.run(host=APP_HOST, port=APP_PORT, debug=False, use_reloader=False)
