import os
import io
import csv
import json
import sqlite3
import logging
import threading
from datetime import datetime, date
from zoneinfo import ZoneInfo
from contextlib import closing

from flask import Flask, request, jsonify, redirect, Response
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
BOT_TOKEN = os.getenv("BOT_TOKEN", "8225104783:AAGsMLrMPYHm9lreO54-MiAZfuT0EfuV8IY")
BASE_URL = os.getenv("BASE_URL", "https://your-domain-or-tunnel-url")
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("PORT", "8080"))
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "CHANGE_THIS_ADMIN_TOKEN")
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "America/New_York")
DB_PATH = os.getenv("DB_PATH", "orders.db")
DEFAULT_AUTO_EXPORT_HOUR = int(os.getenv("DEFAULT_AUTO_EXPORT_HOUR", "21"))

TZ = ZoneInfo(APP_TIMEZONE)
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("cpe_app")

if BOT_TOKEN.startswith("PUT_"):
    logger.warning("BOT_TOKEN is not configured yet.")

app = Flask(__name__)
scheduler = BackgroundScheduler(timezone=APP_TIMEZONE)
telegram_app = Application.builder().token(BOT_TOKEN).build()

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
    keyboard = [
        [
            InlineKeyboardButton(
                text="Open Order App",
                web_app=WebAppInfo(url=f"{BASE_URL}/webapp?uid={user.id}&name={user.full_name}&username={user.username or ''}"),
            )
        ]
    ]
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
    await context.bot.send_document(chat_id=chat_id, document=input_file, caption=caption[:1024])


async def auto_export_job(context: ContextTypes.DEFAULT_TYPE):
    today = datetime.now(TZ).date().isoformat()
    with closing(get_db()) as conn:
        admins = conn.execute("SELECT telegram_user_id FROM admin_users").fetchall()

    if not admins:
        logger.info("No admin users registered. Skipping auto export.")
        return

    for row in admins:
        try:
            await send_daily_export_to_chat(context, row["telegram_user_id"], today)
        except Exception as exc:
            logger.exception("Failed sending auto export to admin %s: %s", row["telegram_user_id"], exc)


def reschedule_auto_export(hour: int):
    if scheduler.get_job("daily_export"):
        scheduler.remove_job("daily_export")

    scheduler.add_job(
        func=lambda: telegram_app.create_task(auto_export_job(telegram_app.bot)),
        trigger="cron",
        id="daily_export",
        hour=hour,
        minute=0,
        replace_existing=True,
    )
    logger.info("Auto export scheduled at %02d:00 %s", hour, APP_TIMEZONE)


# ============================================================
# WEB APP UI
# ============================================================
HTML_PAGE = """
<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>Warehouse Request</title>
  <script src=\"https://telegram.org/js/telegram-web-app.js\"></script>
  <style>
    :root {
      --bg: #0f172a;
      --card: #111827;
      --muted: #94a3b8;
      --text: #f8fafc;
      --line: #1f2937;
      --accent: #2563eb;
      --accent2: #1d4ed8;
      --ok: #16a34a;
      --danger: #dc2626;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Arial, sans-serif;
      background: linear-gradient(180deg, #0b1220, #111827);
      color: var(--text);
      padding: 18px;
    }
    .wrap {
      max-width: 720px;
      margin: 0 auto;
    }
    .card {
      background: rgba(17,24,39,.98);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 10px 30px rgba(0,0,0,.25);
    }
    h1 {
      font-size: 22px;
      margin: 0 0 8px;
    }
    p.small { color: var(--muted); margin-top: 0; }
    .field { margin-bottom: 14px; }
    label {
      display: block;
      margin-bottom: 7px;
      font-weight: 700;
      font-size: 14px;
    }
    input, textarea {
      width: 100%;
      padding: 14px;
      border-radius: 12px;
      border: 1px solid #334155;
      background: #0b1220;
      color: #fff;
      font-size: 16px;
      outline: none;
    }
    textarea { min-height: 88px; resize: vertical; }
    .qty-row {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 12px;
      align-items: center;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 14px;
      margin-bottom: 10px;
      background: #0b1220;
    }
    .qty-controls {
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .qty-btn {
      border: none;
      width: 42px;
      height: 42px;
      border-radius: 12px;
      font-size: 24px;
      color: white;
      background: var(--accent);
      cursor: pointer;
    }
    .qty-btn:active { background: var(--accent2); }
    .qty-value {
      min-width: 28px;
      text-align: center;
      font-size: 20px;
      font-weight: 700;
    }
    .submit-btn {
      width: 100%;
      padding: 15px;
      border: none;
      border-radius: 14px;
      background: linear-gradient(90deg, var(--accent), var(--accent2));
      color: #fff;
      font-size: 17px;
      font-weight: 700;
      cursor: pointer;
      margin-top: 8px;
    }
    .status {
      margin-top: 14px;
      padding: 12px;
      border-radius: 12px;
      display: none;
      font-weight: 700;
    }
    .ok { background: rgba(22,163,74,.16); border: 1px solid rgba(22,163,74,.45); color: #bbf7d0; }
    .err { background: rgba(220,38,38,.16); border: 1px solid rgba(220,38,38,.45); color: #fecaca; }

    .modal-bg {
      position: fixed;
      inset: 0;
      background: rgba(0,0,0,.7);
      display: flex;
      align-items: center;
      justify-content: center;
      z-index: 9999;
      padding: 20px;
    }
    .modal {
      width: 100%;
      max-width: 520px;
      background: #111827;
      border: 1px solid #334155;
      border-radius: 20px;
      padding: 20px;
      box-shadow: 0 20px 50px rgba(0,0,0,.45);
    }
    .modal h2 { margin-top: 0; }
    .modal p { color: #dbeafe; line-height: 1.5; }
    .modal button {
      width: 100%;
      margin-top: 12px;
      padding: 14px;
      border: none;
      border-radius: 14px;
      background: linear-gradient(90deg, var(--accent), var(--accent2));
      color: white;
      font-weight: 700;
      font-size: 16px;
      cursor: pointer;
    }
    .hidden { display: none !important; }
  </style>
</head>
<body>
  <div id=\"disclaimerModal\" class=\"modal-bg\">
    <div class=\"modal\">
      <h2>Important Notice</h2>
      <p>
        This tool is for authorized internal warehouse requests only. Verify all quantities before submitting.
        By continuing, you confirm the order information is accurate and business-related.
      </p>
      <button onclick=\"acceptDisclaimer()\">Accept and Continue</button>
    </div>
  </div>

  <div class=\"wrap\">
    <div class=\"card\">
      <h1>Warehouse Request</h1>
      <p class=\"small\">Professional internal order entry</p>

      <div class=\"field\">
        <label for=\"techName\">Technician Name</label>
        <input id=\"techName\" placeholder=\"Enter technician name\" />
      </div>

      <div class=\"field\">
        <label for=\"bpNumber\">BP Number</label>
        <input id=\"bpNumber\" inputmode=\"numeric\" placeholder=\"Enter BP number\" />
      </div>

      <div id=\"qtyList\"></div>

      <div class=\"field\">
        <label for=\"notes\">Notes (optional)</label>
        <textarea id=\"notes\" placeholder=\"Extra details if needed\"></textarea>
      </div>

      <button class=\"submit-btn\" onclick=\"submitOrder()\">Submit Order</button>
      <div id=\"status\" class=\"status\"></div>
    </div>
  </div>

  <script>
    const tg = window.Telegram?.WebApp;
    if (tg) {
      tg.ready();
      tg.expand();
    }

    const items = [
      { key: 'xg1v4', label: 'XG1v4' },
      { key: 'xi6', label: 'XI6' },
      { key: 'xid', label: 'XID' },
      { key: 'xct2', label: 'XCT2' },
      { key: 'ddr', label: 'DDR' },
    ];

    const counts = {
      xg1v4: 0,
      xi6: 0,
      xid: 0,
      xct2: 0,
      ddr: 0,
    };

    const qtyList = document.getElementById('qtyList');
    items.forEach(item => {
      const row = document.createElement('div');
      row.className = 'qty-row';
      row.innerHTML = `
        <div><strong>${item.label}</strong></div>
        <div class=\"qty-controls\">
          <button type=\"button\" class=\"qty-btn\" onclick=\"changeQty('${item.key}', -1)\">−</button>
          <div id=\"val_${item.key}\" class=\"qty-value\">0</div>
          <button type=\"button\" class=\"qty-btn\" onclick=\"changeQty('${item.key}', 1)\">+</button>
        </div>
      `;
      qtyList.appendChild(row);
    });

    function acceptDisclaimer() {
      localStorage.setItem('warehouse_disclaimer_ok', '1');
      document.getElementById('disclaimerModal').classList.add('hidden');
    }

    if (localStorage.getItem('warehouse_disclaimer_ok') === '1') {
      document.getElementById('disclaimerModal').classList.add('hidden');
    }

    function changeQty(key, delta) {
      counts[key] = Math.max(0, (counts[key] || 0) + delta);
      document.getElementById(`val_${key}`).textContent = counts[key];
    }

    function showStatus(message, type) {
      const el = document.getElementById('status');
      el.textContent = message;
      el.className = `status ${type}`;
      el.style.display = 'block';
    }

    const bpInput = document.getElementById('bpNumber');
    bpInput.addEventListener('keydown', function(e) {
      if (e.key === 'Enter') {
        e.preventDefault();
        bpInput.blur();
        document.activeElement?.blur?.();
      }
    });

    async function submitOrder() {
      const techName = document.getElementById('techName').value.trim();
      const bpNumber = document.getElementById('bpNumber').value.trim();
      const notes = document.getElementById('notes').value.trim();

      if (!bpNumber) {
        showStatus('BP Number is required.', 'err');
        return;
      }

      const payload = {
        technician_name: techName,
        bp_number: bpNumber,
        notes,
        ...counts,
      };

      if (tg?.initDataUnsafe?.user) {
        payload.telegram_user_id = tg.initDataUnsafe.user.id;
        payload.telegram_username = tg.initDataUnsafe.user.username || '';
      }

      try {
        const res = await fetch('/api/orders', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Failed to submit order.');

        showStatus('Order submitted successfully. Your request was sent to the Warehouse.', 'ok');
        document.getElementById('notes').value = '';
        items.forEach(item => {
          counts[item.key] = 0;
          document.getElementById(`val_${item.key}`).textContent = '0';
        });
      } catch (err) {
        showStatus(err.message || 'Unexpected error.', 'err');
      }
    }
  </script>
</body>
</html>
"""


@app.get("/")
def root():
    return redirect("/webapp")


@app.get("/webapp")
def webapp_page():
    return Response(HTML_PAGE, mimetype="text/html")


@app.post("/api/orders")
def create_order():
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
    token = request.args.get("token", "")
    if token != ADMIN_TOKEN:
        return jsonify({"error": "Unauthorized"}), 401
    day_iso = request.args.get("date") or datetime.now(TZ).date().isoformat()
    return jsonify(get_daily_stats(day_iso))


@app.get("/admin/export")
def admin_export_api():
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
def setup_scheduler():
    hour = int(get_setting("auto_export_hour", str(DEFAULT_AUTO_EXPORT_HOUR)))
    if not scheduler.running:
        scheduler.start()
    reschedule_auto_export(hour)


def register_handlers():
    telegram_app.add_handler(CommandHandler("start", start_command))
    telegram_app.add_handler(CommandHandler("admin", admin_command))
    telegram_app.add_handler(CommandHandler("stats", stats_command))
    telegram_app.add_handler(CommandHandler("export", export_command))
    telegram_app.add_handler(CommandHandler("exportdate", export_date_command))
    telegram_app.add_handler(CommandHandler("sethour", set_hour_command))
    telegram_app.add_handler(CommandHandler("grantadmin", grant_admin_command))


def run_bot():
    register_handlers()
    telegram_app.run_polling(close_loop=False)


def run_web():
    app.run(host=APP_HOST, port=APP_PORT, debug=False, use_reloader=False)


def main():
    init_db()
    setup_scheduler()

    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()

    logger.info("Web app starting on %s:%s", APP_HOST, APP_PORT)
    logger.info("BASE_URL=%s", BASE_URL)
    run_web()


if __name__ == "__main__":
    main()
