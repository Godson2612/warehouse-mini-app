import os
import io
import csv
import sqlite3
import logging
import threading
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from contextlib import closing

from flask import Flask, request, jsonify, redirect, Response, render_template
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo,
    InputFile,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BotCommand,
)
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

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
admin_bot_app = None

if TECH_BOT_TOKEN:
    tech_bot_app = Application.builder().token(TECH_BOT_TOKEN).build()

if ADMIN_BOT_TOKEN:
    admin_bot_app = Application.builder().token(ADMIN_BOT_TOKEN).concurrent_updates(True).build()

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


def get_active_message_info():
    message = get_setting("technician_message", "").strip()
    active_until = get_setting("technician_message_active_until", "").strip()

    if not message or not active_until:
        return "", ""

    try:
        expires_at = datetime.fromisoformat(active_until)
    except Exception:
        set_setting("technician_message", "")
        set_setting("technician_message_active_until", "")
        return "", ""

    now = datetime.now(TZ)
    if now > expires_at:
        set_setting("technician_message", "")
        set_setting("technician_message_active_until", "")
        return "", ""

    return message, active_until


def get_active_technician_message() -> str:
    message, _ = get_active_message_info()
    return message


def set_technician_message(message: str):
    active_until = get_end_of_today_iso()
    set_setting("technician_message", message)
    set_setting("technician_message_active_until", active_until)


def clear_technician_message():
    set_setting("technician_message", "")
    set_setting("technician_message_active_until", "")


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


def fetch_orders_since(start_date_iso: str):
    with closing(get_db()) as conn:
        rows = conn.execute(
            "SELECT * FROM orders WHERE created_date>=? ORDER BY created_at DESC",
            (start_date_iso,),
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
async def tech_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    tech_bot_app.add_handler(CommandHandler("start", tech_start_command))


def run_tech_bot():
    if tech_bot_app is None:
        logger.info("Tech bot disabled.")
        return
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        register_tech_handlers()
        logger.info("Tech bot started.")
        tech_bot_app.run_polling(
            close_loop=False,
            drop_pending_updates=True,
            stop_signals=None,
        )
    except Exception as exc:
        logger.exception("Tech bot failed: %s", exc)


# ============================================================
# ADMIN BOT
# ============================================================
SETTABLE_LIMITS = {
    "limit_modems_total": "Modems Total",
    "limit_dvr_total": "DVR Total",
    "limit_xg2": "XG2",
    "limit_xid": "XID",
    "limit_xi6": "XI6",
    "limit_xer10": "XER10",
    "limit_onu": "ONU",
    "limit_screen": "Screen",
    "limit_battery": "Battery",
    "limit_sensor": "Sensor",
    "limit_camera": "Camera",
    "limit_extra_item": "Additional Item",
}


def admin_home_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton("Home"), KeyboardButton("View Orders")],
            [KeyboardButton("Export Orders"), KeyboardButton("View Statistics")],
            [KeyboardButton("Message for Technicians"), KeyboardButton("Set Max Equipment")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        is_persistent=True,
    )


def admin_main_menu():
    message, _ = get_active_message_info()
    msg_status = "Active" if message else "Inactive"

    return InlineKeyboardMarkup([
        [InlineKeyboardButton("View Orders", callback_data="view_orders")],
        [InlineKeyboardButton("Export Orders", callback_data="export_orders")],
        [InlineKeyboardButton("View Statistics", callback_data="view_stats")],
        [InlineKeyboardButton(f"Message for Technicians ({msg_status})", callback_data="message_menu")],
        [InlineKeyboardButton("Set Max Equipment", callback_data="set_limits")],
        [InlineKeyboardButton("Refresh", callback_data="refresh_menu")],
    ])


def admin_message_menu():
    message, _ = get_active_message_info()
    rows = []
    if message:
        rows.append([InlineKeyboardButton("Edit Message", callback_data="edit_message")])
        rows.append([InlineKeyboardButton("Delete Message", callback_data="delete_message")])
    else:
        rows.append([InlineKeyboardButton("Create Message", callback_data="create_message")])

    rows.append([InlineKeyboardButton("Back", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def admin_limits_menu():
    rows = []
    current_row = []
    for key, label in SETTABLE_LIMITS.items():
        current_value = get_setting(key, "0")
        current_row.append(
            InlineKeyboardButton(f"{label}: {current_value}", callback_data=f"limit::{key}")
        )
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    rows.append([InlineKeyboardButton("Back", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def admin_message_status_text():
    message, active_until = get_active_message_info()
    if not message:
        return "No active technician message."

    expires = datetime.fromisoformat(active_until).astimezone(TZ)
    return (
        "Current technician message:\n\n"
        f"{message}\n\n"
        f"Active until: {expires.strftime('%Y-%m-%d %I:%M:%S %p')}"
    )


def equipment_totals(rows):
    keys = [
        "xb3", "xb6", "xb7", "xb8", "xb10",
        "xg1", "xg1_4k", "xg2", "xid", "xi6",
        "xer10", "onu", "screen", "battery", "sensor", "camera",
        "extra_item_qty",
    ]
    totals = {k: 0 for k in keys}
    for row in rows:
        for k in keys:
            totals[k] += int(row.get(k, 0) or 0)
    return totals


def weekly_stats_text():
    start_date = (datetime.now(TZ).date() - timedelta(days=6)).isoformat()
    rows = fetch_orders_since(start_date)

    totals = equipment_totals(rows)

    tech_visits = {}
    for row in rows:
        tech = row.get("tech_id") or "Unknown"
        tech_visits[tech] = tech_visits.get(tech, 0) + 1

    top_equipment = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    top_techs = sorted(tech_visits.items(), key=lambda x: x[1], reverse=True)

    lines = [
        "📊 Weekly Statistics",
        f"Range: {start_date} to {datetime.now(TZ).date().isoformat()}",
        "",
        "Most requested equipment:",
    ]

    shown_any = False
    for key, value in top_equipment:
        if value > 0:
            lines.append(f"- {key.upper()}: {value}")
            shown_any = True
    if not shown_any:
        lines.append("- No orders this week.")

    lines.append("")
    lines.append("Technician WH visits this week:")
    if top_techs:
        for tech, visits in top_techs:
            lines.append(f"- {tech}: {visits}")
    else:
        lines.append("- No visits recorded.")

    return "\n".join(lines)


def recent_orders_text():
    rows = fetch_recent_orders(20)
    if not rows:
        return "No recent orders found."

    lines = ["📦 Real-time Orders (latest 20)", ""]
    for row in rows:
        items = []
        for key in [
            "xb3", "xb6", "xb7", "xb8", "xb10",
            "xg1", "xg1_4k", "xg2", "xid", "xi6",
            "xer10", "onu", "screen", "battery", "sensor", "camera"
        ]:
            qty = int(row.get(key, 0) or 0)
            if qty > 0:
                items.append(f"{key.upper()} {qty}")

        extra_name = (row.get("extra_item_name") or "").strip()
        extra_qty = int(row.get("extra_item_qty", 0) or 0)
        if extra_qty > 0:
            items.append(f"{extra_name or 'ADDITIONAL ITEM'} {extra_qty}")

        item_text = ", ".join(items) if items else "No items"
        lines.append(
            f"- {row['created_at'][:19]} | Tech ID {row['tech_id']} | BP {row['bp_number']} | {item_text}"
        )

    return "\n".join(lines)


def build_excel_summary(day_iso: str) -> bytes:
    rows = fetch_orders_for_day(day_iso)
    output = io.BytesIO()

    try:
        from openpyxl import Workbook
    except Exception:
        csv_bytes = build_daily_csv_bytes(day_iso)
        output.write(csv_bytes)
        output.seek(0)
        return output.getvalue()

    wb = Workbook()

    ws_summary = wb.active
    ws_summary.title = "Summary by Tech"

    ws_summary.append([
        "Tech ID",
        "BP Number",
        "Orders",
        "XB3", "XB6", "XB7", "XB8", "XB10",
        "XG1", "XG1_4K", "XG2", "XID", "XI6",
        "XER10", "ONU", "Screen", "Battery", "Sensor", "Camera",
        "Extra Item", "Extra Qty",
    ])

    grouped = {}
    for row in rows:
        key = (row["tech_id"], row["bp_number"])
        if key not in grouped:
            grouped[key] = {
                "orders": 0,
                "xb3": 0, "xb6": 0, "xb7": 0, "xb8": 0, "xb10": 0,
                "xg1": 0, "xg1_4k": 0, "xg2": 0, "xid": 0, "xi6": 0,
                "xer10": 0, "onu": 0, "screen": 0, "battery": 0, "sensor": 0, "camera": 0,
                "extra_item_name": "",
                "extra_item_qty": 0,
            }

        grouped[key]["orders"] += 1
        for field in [
            "xb3", "xb6", "xb7", "xb8", "xb10",
            "xg1", "xg1_4k", "xg2", "xid", "xi6",
            "xer10", "onu", "screen", "battery", "sensor", "camera",
        ]:
            grouped[key][field] += int(row.get(field, 0) or 0)

        if row.get("extra_item_name"):
            grouped[key]["extra_item_name"] = row["extra_item_name"]
        grouped[key]["extra_item_qty"] += int(row.get("extra_item_qty", 0) or 0)

    for (tech_id, bp_number), data in grouped.items():
        ws_summary.append([
            tech_id,
            bp_number,
            data["orders"],
            data["xb3"], data["xb6"], data["xb7"], data["xb8"], data["xb10"],
            data["xg1"], data["xg1_4k"], data["xg2"], data["xid"], data["xi6"],
            data["xer10"], data["onu"], data["screen"], data["battery"], data["sensor"], data["camera"],
            data["extra_item_name"], data["extra_item_qty"],
        ])

    ws_raw = wb.create_sheet("Raw Orders")
    ws_raw.append([
        "created_at",
        "tech_id",
        "bp_number",
        "xb3", "xb6", "xb7", "xb8", "xb10",
        "xg1", "xg1_4k", "xg2", "xid", "xi6",
        "xer10", "onu", "screen", "battery", "sensor", "camera",
        "extra_item_name", "extra_item_qty", "notes"
    ])
    for row in rows:
        ws_raw.append([
            row["created_at"],
            row["tech_id"],
            row["bp_number"],
            row["xb3"], row["xb6"], row["xb7"], row["xb8"], row["xb10"],
            row["xg1"], row["xg1_4k"], row["xg2"], row["xid"], row["xi6"],
            row["xer10"], row["onu"], row["screen"], row["battery"], row["sensor"], row["camera"],
            row["extra_item_name"], row["extra_item_qty"], row["notes"]
        ])

    wb.save(output)
    output.seek(0)
    return output.getvalue()


async def admin_safe_answer(query):
    try:
        await query.answer()
    except BadRequest as exc:
        if "Query is too old" in str(exc) or "query id is invalid" in str(exc):
            logger.warning("Ignored expired admin callback query.")
            return
        raise


async def admin_safe_edit_message(query, text, reply_markup=None):
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except BadRequest as exc:
        if "Message is not modified" in str(exc):
            return
        raise


async def admin_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled admin bot error: %s", context.error)


async def admin_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if is_admin(user.id):
        await update.message.reply_text(
            "Admin bot ready.",
            reply_markup=admin_home_keyboard(),
        )
        await update.message.reply_text(
            "Select an option below.",
            reply_markup=admin_main_menu(),
        )
        return

    await update.message.reply_text(
        "Admin authorization required.\nUse:\n/authorize YOUR_ADMIN_TOKEN",
        reply_markup=admin_home_keyboard(),
    )


async def admin_authorize_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Use: /authorize YOUR_ADMIN_TOKEN")
        return

    if context.args[0] != ADMIN_ACCESS_TOKEN:
        await update.message.reply_text("Invalid admin token.")
        return

    user = update.effective_user
    add_admin_user(user.id)
    await update.message.reply_text(
        "Admin access granted.",
        reply_markup=admin_home_keyboard(),
    )
    await update.message.reply_text(
        "Select an option below.",
        reply_markup=admin_main_menu(),
    )


async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await admin_safe_answer(query)

    user = query.from_user
    if not is_admin(user.id):
        await admin_safe_edit_message(query, "Unauthorized.")
        return

    data = query.data

    if data in ("refresh_menu", "back_main"):
        await admin_safe_edit_message(query, "Select an option below.", reply_markup=admin_main_menu())
        return

    if data == "view_orders":
        await admin_safe_edit_message(query, recent_orders_text(), reply_markup=admin_main_menu())
        return

    if data == "view_stats":
        await admin_safe_edit_message(query, weekly_stats_text(), reply_markup=admin_main_menu())
        return

    if data == "export_orders":
        day_iso = datetime.now(TZ).date().isoformat()
        file_bytes = build_excel_summary(day_iso)
        filename = f"orders_{day_iso}.xlsx"
        if file_bytes[:2] != b"PK":
            filename = f"orders_{day_iso}.csv"

        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=InputFile(io.BytesIO(file_bytes), filename=filename),
            caption=f"Export for {day_iso}",
        )
        await admin_safe_edit_message(query, "Export sent.", reply_markup=admin_main_menu())
        return

    if data == "message_menu":
        await admin_safe_edit_message(query, admin_message_status_text(), reply_markup=admin_message_menu())
        return

    if data == "create_message":
        context.user_data["awaiting"] = "technician_message_create"
        await admin_safe_edit_message(
            query,
            "Send the new message for technicians now.\nIt will expire automatically today at 11:59 PM.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="message_menu")]])
        )
        return

    if data == "edit_message":
        context.user_data["awaiting"] = "technician_message_edit"
        await admin_safe_edit_message(
            query,
            "Send the updated message for technicians now.\nIt will expire automatically today at 11:59 PM.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="message_menu")]])
        )
        return

    if data == "delete_message":
        clear_technician_message()
        context.user_data.pop("awaiting", None)
        await admin_safe_edit_message(query, "Technician message deleted.", reply_markup=admin_main_menu())
        return

    if data == "set_limits":
        await admin_safe_edit_message(query, "Select the equipment limit to update:", reply_markup=admin_limits_menu())
        return

    if data.startswith("limit::"):
        setting_key = data.split("::", 1)[1]
        context.user_data["awaiting"] = f"limit::{setting_key}"
        label = SETTABLE_LIMITS.get(setting_key, setting_key)
        await admin_safe_edit_message(
            query,
            f"Send the new value for {label}.\nAllowed range: 0 to 50.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="set_limits")]])
        )
        return


async def admin_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        return

    text = (update.message.text or "").strip()
    awaiting = context.user_data.get("awaiting")

    if text == "Home":
        context.user_data.pop("awaiting", None)
        await update.message.reply_text("Select an option below.", reply_markup=admin_main_menu())
        return

    if text == "View Orders":
        await update.message.reply_text(recent_orders_text(), reply_markup=admin_main_menu())
        return

    if text == "Export Orders":
        day_iso = datetime.now(TZ).date().isoformat()
        file_bytes = build_excel_summary(day_iso)
        filename = f"orders_{day_iso}.xlsx"
        if file_bytes[:2] != b"PK":
            filename = f"orders_{day_iso}.csv"

        await update.message.reply_document(
            document=InputFile(io.BytesIO(file_bytes), filename=filename),
            caption=f"Export for {day_iso}",
        )
        await update.message.reply_text("Select an option below.", reply_markup=admin_main_menu())
        return

    if text == "View Statistics":
        await update.message.reply_text(weekly_stats_text(), reply_markup=admin_main_menu())
        return

    if text == "Message for Technicians":
        await update.message.reply_text(admin_message_status_text(), reply_markup=admin_message_menu())
        return

    if text == "Set Max Equipment":
        await update.message.reply_text("Select the equipment limit to update:", reply_markup=admin_limits_menu())
        return

    if not awaiting:
        await update.message.reply_text("Select an option below.", reply_markup=admin_main_menu())
        return

    if awaiting in ("technician_message_create", "technician_message_edit"):
        set_technician_message(text)
        context.user_data.pop("awaiting", None)
        _, active_until = get_active_message_info()
        expires = datetime.fromisoformat(active_until).astimezone(TZ).strftime("%Y-%m-%d %I:%M:%S %p")
        await update.message.reply_text(
            f"Technician message saved.\nActive until: {expires}",
            reply_markup=admin_main_menu(),
        )
        return

    if awaiting.startswith("limit::"):
        try:
            value = int(text)
            if value < 0 or value > 50:
                raise ValueError
        except Exception:
            await update.message.reply_text("Enter a number from 0 to 50.")
            return

        setting_key = awaiting.split("::", 1)[1]
        set_setting(setting_key, str(value))
        context.user_data.pop("awaiting", None)
        label = SETTABLE_LIMITS.get(setting_key, setting_key)
        await update.message.reply_text(
            f"{label} updated to {value}.",
            reply_markup=admin_main_menu(),
        )
        return


async def admin_post_init(application: Application):
    await application.bot.set_my_commands([
        BotCommand("start", "Open admin menu"),
        BotCommand("authorize", "Authorize admin access"),
    ])


def register_admin_handlers():
    if admin_bot_app is None:
        return
    admin_bot_app.post_init = admin_post_init
    admin_bot_app.add_handler(CommandHandler("start", admin_start_command))
    admin_bot_app.add_handler(CommandHandler("authorize", admin_authorize_command))
    admin_bot_app.add_handler(CallbackQueryHandler(admin_callback_handler))
    admin_bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, admin_text_handler))
    admin_bot_app.add_error_handler(admin_error_handler)


def run_admin_bot():
    if admin_bot_app is None:
        logger.info("Admin bot disabled.")
        return
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        register_admin_handlers()
        logger.info("Admin bot started.")
        admin_bot_app.run_polling(
            close_loop=False,
            drop_pending_updates=True,
            stop_signals=None,
        )
    except Exception as exc:
        logger.exception("Admin bot failed: %s", exc)


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
            tech_thread = threading.Thread(target=run_tech_bot, daemon=True)
            tech_thread.start()

        if admin_bot_app is not None:
            admin_thread = threading.Thread(target=run_admin_bot, daemon=True)
            admin_thread.start()

        _started = True
        logger.info("Application startup completed.")


ensure_app_started()

if __name__ == "__main__":
    logger.info("Web app starting on %s:%s", APP_HOST, APP_PORT)
    logger.info("BASE_URL=%s", BASE_URL)
    app.run(host=APP_HOST, port=APP_PORT, debug=False, use_reloader=False)
