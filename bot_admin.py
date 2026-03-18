import os
import io
import sqlite3
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from contextlib import closing

from openpyxl import Workbook
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
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
ADMIN_BOT_TOKEN = os.getenv(
    "ADMIN_BOT_TOKEN",
    "8798395520:AAGadGCNtPmgXUv_eUfdQmyfVz57JygDYdc",
)
ADMIN_ACCESS_TOKEN = os.getenv("ADMIN_TOKEN", "481903f396246a735d26ceebbb2a2190")
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "America/New_York")
DB_PATH = os.getenv("DB_PATH", "orders.db")

TZ = ZoneInfo(APP_TIMEZONE)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("admin_bot")

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


# ============================================================
# DB HELPERS
# ============================================================
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


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


def fetch_recent_orders(limit=20):
    with closing(get_db()) as conn:
        rows = conn.execute(
            "SELECT * FROM orders ORDER BY created_at DESC LIMIT ?",
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


# ============================================================
# REPORTS
# ============================================================
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
        f"📊 Weekly Statistics",
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

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output.getvalue()


# ============================================================
# UI
# ============================================================
def main_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("View Orders", callback_data="view_orders")],
        [InlineKeyboardButton("Export Orders", callback_data="export_orders")],
        [InlineKeyboardButton("View Statistics", callback_data="view_stats")],
        [InlineKeyboardButton("Message for Technicians", callback_data="message_techs")],
        [InlineKeyboardButton("Set Max Equipment", callback_data="set_limits")],
        [InlineKeyboardButton("Refresh", callback_data="refresh_menu")],
    ])


def limits_menu():
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


# ============================================================
# HANDLERS
# ============================================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if is_admin(user.id):
        await update.message.reply_text("Admin bot ready.", reply_markup=main_menu())
        return

    await update.message.reply_text(
        "Admin authorization required.\nUse:\n/authorize YOUR_ADMIN_TOKEN"
    )


async def authorize_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Use: /authorize YOUR_ADMIN_TOKEN")
        return

    if context.args[0] != ADMIN_ACCESS_TOKEN:
        await update.message.reply_text("Invalid admin token.")
        return

    user = update.effective_user
    add_admin_user(user.id)
    await update.message.reply_text("Admin access granted.", reply_markup=main_menu())


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user = query.from_user
    if not is_admin(user.id):
        await query.edit_message_text("Unauthorized.")
        return

    data = query.data

    if data == "refresh_menu" or data == "back_main":
        await query.edit_message_text("Admin bot ready.", reply_markup=main_menu())
        return

    if data == "view_orders":
        await query.edit_message_text(recent_orders_text(), reply_markup=main_menu())
        return

    if data == "view_stats":
        await query.edit_message_text(weekly_stats_text(), reply_markup=main_menu())
        return

    if data == "export_orders":
        day_iso = datetime.now(TZ).date().isoformat()
        file_bytes = build_excel_summary(day_iso)
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=InputFile(io.BytesIO(file_bytes), filename=f"orders_{day_iso}.xlsx"),
            caption=f"Excel export for {day_iso}",
        )
        await query.edit_message_text("Export sent.", reply_markup=main_menu())
        return

    if data == "message_techs":
        context.user_data["awaiting"] = "technician_message"
        await query.edit_message_text(
            "Send the message for technicians now.\nIt will appear after the first Accept popup.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="back_main")]])
        )
        return

    if data == "set_limits":
        await query.edit_message_text(
            "Select the equipment limit to update:",
            reply_markup=limits_menu(),
        )
        return

    if data.startswith("limit::"):
        setting_key = data.split("::", 1)[1]
        context.user_data["awaiting"] = f"limit::{setting_key}"
        label = SETTABLE_LIMITS.get(setting_key, setting_key)
        await query.edit_message_text(
            f"Send the new value for {label}.\nAllowed range: 0 to 50.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="set_limits")]])
        )
        return


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        return

    awaiting = context.user_data.get("awaiting")
    if not awaiting:
        await update.message.reply_text("Use the buttons below.", reply_markup=main_menu())
        return

    text = (update.message.text or "").strip()

    if awaiting == "technician_message":
        set_setting("technician_message", text)
        context.user_data.pop("awaiting", None)
        await update.message.reply_text("Technician message updated.", reply_markup=main_menu())
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
            reply_markup=main_menu(),
        )
        return


def main():
    application = Application.builder().token(ADMIN_BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("authorize", authorize_command))
    application.add_handler(CallbackQueryHandler(callback_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    logger.info("Admin bot started.")
    application.run_polling()


if __name__ == "__main__":
    main()