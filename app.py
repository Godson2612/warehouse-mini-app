import os
import io
import csv
import sqlite3
import logging
import threading
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from contextlib import closing

from flask import Flask, request, jsonify, redirect, Response, render_template
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import Application, CommandHandler, ContextTypes

# ============================================================
# CONFIG
# ============================================================
TECH_BOT_TOKEN = os.getenv(
    "BOT_TOKEN",
    "8225104783:AAGsMLrMPYHm9lreO54-MiAZfuT0EfuV8IY",
)
ADMIN_BOT_TOKEN = os.getenv(
    "ADMIN_BOT_TOKEN",
    "8798395520:AAGadGCNtPmgXUv_eUfdQmyfVz57JygDYdc",
)
BASE_URL = os.getenv("BASE_URL", "https://warehouse-mini-app.onrender.com").rstrip("/")
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("PORT", "8080"))
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "America/New_York")
DB_PATH = os.getenv("DB_PATH", "orders.db")
SECRET_KEY = os.getenv("SECRET_KEY", "warehouse-secret-key")
ADMIN_ACCESS_TOKEN = os.getenv("ADMIN_TOKEN", "481903f396246a735d26ceebbb2a2190")

TZ = ZoneInfo(APP_TIMEZONE)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("warehouse_app")

app = Flask(__name__)
app.secret_key = SECRET_KEY

tech_bot_app = None
if TECH_BOT_TOKEN:
    tech_bot_app = Application.builder().token(TECH_BOT_TOKEN).build()

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
    tech_id TEXT NOT NULL,
    bp_number TEXT NOT NULL,

    xb3 INTEGER NOT NULL DEFAULT 0,
    xb6 INTEGER NOT NULL DEFAULT 0,
    xb7 INTEGER NOT NULL DEFAULT 0,
    xb8 INTEGER NOT NULL DEFAULT 0,
    xb10 INTEGER NOT NULL DEFAULT 0,

    xg1 INTEGER NOT NULL DEFAULT 0,
    xg1_4k INTEGER NOT NULL DEFAULT 0,
    xg2 INTEGER NOT NULL DEFAULT 0,
    xid INTEGER NOT NULL DEFAULT 0,
    xi6 INTEGER NOT NULL DEFAULT 0,
    xer10 INTEGER NOT NULL DEFAULT 0,
    onu INTEGER NOT NULL DEFAULT 0,

    screen INTEGER NOT NULL DEFAULT 0,
    battery INTEGER NOT NULL DEFAULT 0,
    sensor INTEGER NOT NULL DEFAULT 0,
    camera INTEGER NOT NULL DEFAULT 0,

    extra_item_name TEXT NOT NULL DEFAULT '',
    extra_item_qty INTEGER NOT NULL DEFAULT 0,

    notes TEXT NOT NULL DEFAULT ''
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

DEFAULT_SETTINGS = {
    "limit_modems_total": "12",
    "limit_dvr_total": "5",
    "limit_xg2": "5",
    "limit_xid": "12",
    "limit_xi6": "12",
    "limit_xer10": "2",
    "limit_onu": "2",
    "limit_screen": "5",
    "limit_battery": "50",
    "limit_sensor": "50",
    "limit_camera": "50",
    "limit_extra_item": "10",
    "technician_message": "",
    "technician_message_active_until": "",
}


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(get_db()) as conn:
        conn.executescript(SCHEMA_SQL)
        for key, value in DEFAULT_SETTINGS.items():
            conn.execute(
                "INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)",
                (key, value),
            )
        conn.commit()


def get_setting(key: str, default: str = "") -> str:
    with closing(get_db()) as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def set_setting(key: str, value: str):
    with closing(get_db()) as conn:
        conn.execute(
            """
            INSERT INTO settings(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )
        conn.commit()


def get_limits():
    return {
        "modems_total": int(get_setting("limit_modems_total", "12")),
        "dvr_total": int(get_setting("limit_dvr_total", "5")),
        "xg2": int(get_setting("limit_xg2", "5")),
        "xid": int(get_setting("limit_xid", "12")),
        "xi6": int(get_setting("limit_xi6", "12")),
        "xer10": int(get_setting("limit_xer10", "2")),
        "onu": int(get_setting("limit_onu", "2")),
        "screen": int(get_setting("limit_screen", "5")),
        "battery": int(get_setting("limit_battery", "50")),
        "sensor": int(get_setting("limit_sensor", "50")),
        "camera": int(get_setting("limit_camera", "50")),
        "extra_item": int(get_setting("limit_extra_item", "10")),
    }


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


def parse_int(value, default=0):
    try:
        return max(0, int(str(value).strip() or default))
    except Exception:
        return default


def get_end_of_today_iso() -> str:
    now = datetime.now(TZ)
    end_of_day = now.replace(hour=23, minute=59, second=59, microsecond=0)
    return end_of_day.isoformat()


def get_active_technician_message() -> str:
    message = get_setting("technician_message", "").strip()
    active_until = get_setting("technician_message_active_until", "").strip()

    if not message or not active_until:
        return ""

    try:
        expires_at = datetime.fromisoformat(active_until)
    except Exception:
        set_setting("technician_message", "")
        set_setting("technician_message_active_until", "")
        return ""

    now = datetime.now(TZ)
    if now > expires_at:
        set_setting("technician_message", "")
        set_setting("technician_message_active_until", "")
        return ""

    return message


def save_order(payload: dict):
    now = datetime.now(TZ)
    with closing(get_db()) as conn:
        conn.execute(
            """
            INSERT INTO orders (
                created_at, created_date, telegram_user_id, telegram_username,
                tech_id, bp_number,
                xb3, xb6, xb7, xb8, xb10,
                xg1, xg1_4k, xg2, xid, xi6, xer10, onu,
                screen, battery, sensor, camera,
                extra_item_name, extra_item_qty, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now.isoformat(),
                now.date().isoformat(),
                payload.get("telegram_user_id"),
                payload.get("telegram_username", ""),
                payload["tech_id"],
                payload["bp_number"],
                payload.get("xb3", 0),
                payload.get("xb6", 0),
                payload.get("xb7", 0),
                payload.get("xb8", 0),
                payload.get("xb10", 0),
                payload.get("xg1", 0),
                payload.get("xg1_4k", 0),
                payload.get("xg2", 0),
                payload.get("xid", 0),
                payload.get("xi6", 0),
                payload.get("xer10", 0),
                payload.get("onu", 0),
                payload.get("screen", 0),
                payload.get("battery", 0),
                payload.get("sensor", 0),
                payload.get("camera", 0),
                payload.get("extra_item_name", ""),
                payload.get("extra_item_qty", 0),
                payload.get("notes", ""),
            ),
        )
        conn.commit()


def fetch_recent_orders(limit=20):
    with closing(get_db()) as conn:
        rows = conn.execute(
            """
            SELECT * FROM orders
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def fetch_orders_for_day(day_iso: str):
    with closing(get_db()) as conn:
        rows = conn.execute(
            "SELECT * FROM orders WHERE created_date=? ORDER BY created_at DESC",
            (day_iso,),
        ).fetchall()
        return [dict(r) for r in rows]


def build_daily_csv_bytes(day_iso: str) -> bytes:
    orders = fetch_orders_for_day(day_iso)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "created_at",
        "telegram_user_id",
        "telegram_username",
        "tech_id",
        "bp_number",
        "xb3",
        "xb6",
        "xb7",
        "xb8",
        "xb10",
        "xg1",
        "xg1_4k",
        "xg2",
        "xid",
        "xi6",
        "xer10",
        "onu",
        "screen",
        "battery",
        "sensor",
        "camera",
        "extra_item_name",
        "extra_item_qty",
        "notes",
    ])
    for row in orders:
        writer.writerow([
            row["created_at"],
            row["telegram_user_id"],
            row["telegram_username"],
            row["tech_id"],
            row["bp_number"],
            row["xb3"],
            row["xb6"],
            row["xb7"],
            row["xb8"],
            row["xb10"],
            row["xg1"],
            row["xg1_4k"],
            row["xg2"],
            row["xid"],
            row["xi6"],
            row["xer10"],
            row["onu"],
            row["screen"],
            row["battery"],
            row["sensor"],
            row["camera"],
            row["extra_item_name"],
            row["extra_item_qty"],
            row["notes"],
        ])
    return output.getvalue().encode("utf-8-sig")


def validate_payload(payload: dict):
    limits = get_limits()

    tech_id = payload["tech_id"].strip()
    bp_number = payload["bp_number"].strip()

    if not tech_id:
        return "Tech ID is required."
    if not bp_number:
        return "BP Number is required."

    modem_total = payload["xb3"] + payload["xb6"] + payload["xb7"] + payload["xb8"] + payload["xb10"]
    dvr_total = payload["xg1"] + payload["xg1_4k"]

    if modem_total > limits["modems_total"]:
        return f"Modem category limit reached. Maximum allowed is {limits['modems_total']}."
    if dvr_total > limits["dvr_total"]:
        return f"DVR category limit reached. Maximum allowed is {limits['dvr_total']}."
    if payload["xg2"] > limits["xg2"]:
        return f"XG2 limit reached. Maximum allowed is {limits['xg2']}."
    if payload["xid"] > limits["xid"]:
        return f"XID limit reached. Maximum allowed is {limits['xid']}."
    if payload["xi6"] > limits["xi6"]:
        return f"XI6 limit reached. Maximum allowed is {limits['xi6']}."
    if payload["xer10"] > limits["xer10"]:
        return f"XER10 limit reached. Maximum allowed is {limits['xer10']}."
    if payload["onu"] > limits["onu"]:
        return f"ONU limit reached. Maximum allowed is {limits['onu']}."
    if payload["screen"] > limits["screen"]:
        return f"Screen limit reached. Maximum allowed is {limits['screen']}."
    if payload["battery"] > limits["battery"]:
        return f"Battery limit reached. Maximum allowed is {limits['battery']}."
    if payload["sensor"] > limits["sensor"]:
        return f"Sensor limit reached. Maximum allowed is {limits['sensor']}."
    if payload["camera"] > limits["camera"]:
        return f"Camera limit reached. Maximum allowed is {limits['camera']}."

    if payload["extra_item_qty"] > 0 and not payload["extra_item_name"].strip():
        return "Please enter the item name for Add Item."

    if payload["extra_item_qty"] > limits["extra_item"]:
        return f"Additional item limit reached. Maximum allowed is {limits['extra_item']}."

    grand_total = (
        modem_total
        + dvr_total
        + payload["xg2"]
        + payload["xid"]
        + payload["xi6"]
        + payload["xer10"]
        + payload["onu"]
        + payload["screen"]
        + payload["battery"]
        + payload["sensor"]
        + payload["camera"]
        + payload["extra_item_qty"]
    )
    if grand_total <= 0:
        return "Please add at least one equipment item."

    return None


# ============================================================
# TECH BOT
# ============================================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    keyboard = [[
        InlineKeyboardButton(
            text="Open Order App",
            web_app=WebAppInfo(
                url=f"{BASE_URL}/webapp?uid={user.id}&username={user.username or ''}"
            ),
        )
    ]]
    await update.message.reply_text(
        "Open the app to submit equipment requests.",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


def register_tech_handlers():
    if tech_bot_app is None:
        return
    tech_bot_app.add_handler(CommandHandler("start", start_command))


def run_tech_bot():
    if tech_bot_app is None:
        logger.info("Tech bot disabled.")
        return
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        register_tech_handlers()
        logger.info("Tech bot started.")
        tech_bot_app.run_polling(close_loop=False)
    except Exception as exc:
        logger.exception("Tech bot failed: %s", exc)


# ============================================================
# WEB ROUTES
# ============================================================
@app.get("/")
def root():
    return redirect("/webapp")


@app.get("/health")
def health():
    return jsonify({"ok": True})


@app.get("/webapp")
def webapp_page():
    limits = get_limits()
    technician_message = get_active_technician_message()
    popup_message = (
        "Badge is required to pick up equipment. "
        "The warehouse closes at 9:00 AM. "
        "Please check your buffer first. "
        f"Maximum allowed quantities: "
        f"Modems {limits['modems_total']} total, "
        f"XI6 {limits['xi6']}, "
        f"XID {limits['xid']}, "
        f"XG2 {limits['xg2']}, "
        f"DVR {limits['dvr_total']} total, "
        f"XER10 {limits['xer10']}, "
        f"ONU {limits['onu']}, "
        f"Screen {limits['screen']}, "
        f"Additional Item {limits['extra_item']}."
    )
    return render_template(
        "request_form.html",
        limits=limits,
        popup_message=popup_message,
        technician_message=technician_message,
    )


@app.post("/create-order")
def create_order():
    payload = {
        "telegram_user_id": request.form.get("telegram_user_id"),
        "telegram_username": request.form.get("telegram_username", "").strip(),
        "tech_id": request.form.get("tech_id", "").strip(),
        "bp_number": request.form.get("bp_number", "").strip(),

        "xb3": parse_int(request.form.get("xb3")),
        "xb6": parse_int(request.form.get("xb6")),
        "xb7": parse_int(request.form.get("xb7")),
        "xb8": parse_int(request.form.get("xb8")),
        "xb10": parse_int(request.form.get("xb10")),

        "xg1": parse_int(request.form.get("xg1")),
        "xg1_4k": parse_int(request.form.get("xg1_4k")),
        "xg2": parse_int(request.form.get("xg2")),
        "xid": parse_int(request.form.get("xid")),
        "xi6": parse_int(request.form.get("xi6")),
        "xer10": parse_int(request.form.get("xer10")),
        "onu": parse_int(request.form.get("onu")),

        "screen": parse_int(request.form.get("screen")),
        "battery": parse_int(request.form.get("battery")),
        "sensor": parse_int(request.form.get("sensor")),
        "camera": parse_int(request.form.get("camera")),

        "extra_item_name": request.form.get("extra_item_name", "").strip(),
        "extra_item_qty": parse_int(request.form.get("extra_item_qty")),
        "notes": request.form.get("notes", "").strip(),
    }

    error = validate_payload(payload)
    if error:
        limits = get_limits()
        technician_message = get_active_technician_message()
        popup_message = (
            "Badge is required to pick up equipment. "
            "The warehouse closes at 9:00 AM. "
            "Please check your buffer first. "
            f"Maximum allowed quantities: "
            f"Modems {limits['modems_total']} total, "
            f"XI6 {limits['xi6']}, "
            f"XID {limits['xid']}, "
            f"XG2 {limits['xg2']}, "
            f"DVR {limits['dvr_total']} total, "
            f"XER10 {limits['xer10']}, "
            f"ONU {limits['onu']}, "
            f"Screen {limits['screen']}, "
            f"Additional Item {limits['extra_item']}."
        )
        return render_template(
            "request_form.html",
            limits=limits,
            popup_message=popup_message,
            technician_message=technician_message,
            form_data=payload,
            form_error=error,
        ), 400

    save_order(payload)

    return render_template(
        "success.html",
        confirmation_message="Your request was sent to WH successfully.",
        bp_number=payload["bp_number"],
    )


@app.get("/admin/export")
def admin_export_api():
    token = request.args.get("token", "")
    if token != ADMIN_ACCESS_TOKEN:
        return jsonify({"error": "Unauthorized"}), 401

    day_iso = request.args.get("date") or datetime.now(TZ).date().isoformat()
    csv_bytes = build_daily_csv_bytes(day_iso)
    return Response(
        csv_bytes,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=orders_{day_iso}.csv"},
    )


# ============================================================
# STARTUP
# ============================================================
_started = False
_start_lock = threading.Lock()


def ensure_app_started():
    global _started
    if _started:
        return
    with _start_lock:
        if _started:
            return
        init_db()

        if tech_bot_app is not None:
            bot_thread = threading.Thread(target=run_tech_bot, daemon=True)
            bot_thread.start()

        _started = True
        logger.info("Application startup completed.")


ensure_app_started()

if __name__ == "__main__":
    logger.info("Web app starting on %s:%s", APP_HOST, APP_PORT)
    logger.info("BASE_URL=%s", BASE_URL)
    app.run(host=APP_HOST, port=APP_PORT, debug=False, use_reloader=False)
