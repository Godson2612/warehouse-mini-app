from flask import Flask, render_template, request, redirect, url_for, session
import sqlite3
from datetime import datetime
import os
import json

app = Flask(__name__)
app.secret_key = "change-this-secret-key"

DB_PATH = "database.db"

LIMITS = {
    "modems_total": 12,
    "xi6": 12,
    "xid": 12,
    "xg2": 5,
    "dvr": 5,
    "onu": 2
}

ITEM_FIELDS = [
    "xb3", "xb6", "xb7", "xb8", "xb10",
    "xi6", "xid", "xg2", "dvr",
    "onu", "xer10",
    "camera", "battery", "sensor", "screen",
    "extra_qty"
]

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            telegram_user_id TEXT,
            telegram_username TEXT,
            telegram_name TEXT,
            bp_number TEXT NOT NULL,
            xb3 INTEGER DEFAULT 0,
            xb6 INTEGER DEFAULT 0,
            xb7 INTEGER DEFAULT 0,
            xb8 INTEGER DEFAULT 0,
            xb10 INTEGER DEFAULT 0,
            xi6 INTEGER DEFAULT 0,
            xid INTEGER DEFAULT 0,
            xg2 INTEGER DEFAULT 0,
            dvr INTEGER DEFAULT 0,
            onu INTEGER DEFAULT 0,
            xer10 INTEGER DEFAULT 0,
            camera INTEGER DEFAULT 0,
            battery INTEGER DEFAULT 0,
            sensor INTEGER DEFAULT 0,
            screen INTEGER DEFAULT 0,
            extra_qty INTEGER DEFAULT 0,
            extra_note TEXT DEFAULT '',
            raw_telegram_data TEXT DEFAULT ''
        )
    """)
    conn.commit()
    conn.close()

@app.before_request
def setup():
    init_db()

@app.route("/")
def index():
    session.clear()
    return render_template("disclaimer.html", limits=LIMITS)

@app.route("/accept", methods=["POST"])
def accept():
    session["accepted_disclaimer"] = True
    return redirect(url_for("request_form"))

@app.route("/request")
def request_form():
    if not session.get("accepted_disclaimer"):
        return redirect(url_for("index"))
    return render_template("request_form.html", limits=LIMITS)

def to_int(value):
    try:
        return max(0, int(value))
    except:
        return 0

def validate_order(data):
    errors = []

    bp_number = data.get("bp_number", "").strip()
    if not bp_number:
        errors.append("BP Number is required.")

    xb3 = to_int(data.get("xb3"))
    xb6 = to_int(data.get("xb6"))
    xb7 = to_int(data.get("xb7"))
    xb8 = to_int(data.get("xb8"))
    xb10 = to_int(data.get("xb10"))
    xi6 = to_int(data.get("xi6"))
    xid = to_int(data.get("xid"))
    xg2 = to_int(data.get("xg2"))
    dvr = to_int(data.get("dvr"))
    onu = to_int(data.get("onu"))
    xer10 = to_int(data.get("xer10"))
    camera = to_int(data.get("camera"))
    battery = to_int(data.get("battery"))
    sensor = to_int(data.get("sensor"))
    screen = to_int(data.get("screen"))
    extra_qty = to_int(data.get("extra_qty"))

    modems_total = xb3 + xb6 + xb7 + xb8 + xb10
    if modems_total > LIMITS["modems_total"]:
        errors.append(f"Total modems cannot exceed {LIMITS['modems_total']}.")

    if xi6 > LIMITS["xi6"]:
        errors.append(f"XI6 cannot exceed {LIMITS['xi6']}.")

    if xid > LIMITS["xid"]:
        errors.append(f"XID cannot exceed {LIMITS['xid']}.")

    if xg2 > LIMITS["xg2"]:
        errors.append(f"XG2 cannot exceed {LIMITS['xg2']}.")

    if dvr > LIMITS["dvr"]:
        errors.append(f"DVR cannot exceed {LIMITS['dvr']}.")

    if onu > LIMITS["onu"]:
        errors.append(f"ONU cannot exceed {LIMITS['onu']}.")

    total_requested = (
        modems_total + xi6 + xid + xg2 + dvr + onu + xer10 +
        camera + battery + sensor + screen + extra_qty
    )

    if total_requested <= 0:
        errors.append("You must request at least one item.")

    cleaned = {
        "bp_number": bp_number,
        "xb3": xb3, "xb6": xb6, "xb7": xb7, "xb8": xb8, "xb10": xb10,
        "xi6": xi6, "xid": xid, "xg2": xg2, "dvr": dvr,
        "onu": onu, "xer10": xer10,
        "camera": camera, "battery": battery, "sensor": sensor, "screen": screen,
        "extra_qty": extra_qty,
        "extra_note": data.get("extra_note", "").strip()
    }

    return errors, cleaned

@app.route("/submit", methods=["POST"])
def submit():
    if not session.get("accepted_disclaimer"):
        return redirect(url_for("index"))

    errors, cleaned = validate_order(request.form)
    if errors:
        return render_template("request_form.html", limits=LIMITS, errors=errors, form=request.form)

    telegram_user_id = request.form.get("telegram_user_id", "").strip()
    telegram_username = request.form.get("telegram_username", "").strip()
    telegram_name = request.form.get("telegram_name", "").strip()
    raw_telegram_data = request.form.get("raw_telegram_data", "").strip()

    conn = get_db()
    conn.execute("""
        INSERT INTO orders (
            created_at, telegram_user_id, telegram_username, telegram_name, bp_number,
            xb3, xb6, xb7, xb8, xb10,
            xi6, xid, xg2, dvr,
            onu, xer10,
            camera, battery, sensor, screen,
            extra_qty, extra_note, raw_telegram_data
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        telegram_user_id, telegram_username, telegram_name, cleaned["bp_number"],
        cleaned["xb3"], cleaned["xb6"], cleaned["xb7"], cleaned["xb8"], cleaned["xb10"],
        cleaned["xi6"], cleaned["xid"], cleaned["xg2"], cleaned["dvr"],
        cleaned["onu"], cleaned["xer10"],
        cleaned["camera"], cleaned["battery"], cleaned["sensor"], cleaned["screen"],
        cleaned["extra_qty"], cleaned["extra_note"], raw_telegram_data
    ))
    conn.commit()
    conn.close()

    return render_template("success.html", bp_number=cleaned["bp_number"])

@app.route("/admin/orders")
def admin_orders():
    conn = get_db()
    rows = conn.execute("SELECT * FROM orders ORDER BY id DESC").fetchall()
    conn.close()

    html = """
    <html><head><title>Orders</title>
    <style>
    body{font-family:Arial,sans-serif;padding:24px;background:#f4f7fb}
    .card{background:#fff;padding:16px;margin-bottom:12px;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,.08)}
    </style></head><body>
    <h1>Submitted Orders</h1>
    """
    for r in rows:
        html += f"""
        <div class='card'>
            <strong>Order #{r['id']}</strong><br>
            Created: {r['created_at']}<br>
            BP Number: {r['bp_number']}<br>
            Telegram: {r['telegram_name']} (@{r['telegram_username']})<br>
            XB3:{r['xb3']} | XB6:{r['xb6']} | XB7:{r['xb7']} | XB8:{r['xb8']} | XB10:{r['xb10']}<br>
            XI6:{r['xi6']} | XID:{r['xid']} | XG2:{r['xg2']} | DVR:{r['dvr']}<br>
            ONU:{r['onu']} | XER10:{r['xer10']}<br>
            CAMERA:{r['camera']} | BATTERY:{r['battery']} | SENSOR:{r['sensor']} | SCREEN:{r['screen']}<br>
            EXTRA QTY:{r['extra_qty']} | EXTRA NOTE:{r['extra_note']}
        </div>
        """
    html += "</body></html>"
    return html

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)