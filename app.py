from __future__ import annotations

import csv
import hashlib
import html
import io
import os
import smtplib
from datetime import date, datetime, timedelta, timezone
from email.message import EmailMessage
from zoneinfo import ZoneInfo

from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_sqlalchemy import SQLAlchemy

APP_TIMEZONE = os.getenv("APP_TIMEZONE", "America/New_York")
TZ = ZoneInfo(APP_TIMEZONE)


def normalize_database_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg://", 1)
    if url.startswith("postgresql://") and "+psycopg" not in url:
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


DATABASE_URL = normalize_database_url(
    os.getenv("DATABASE_URL", "sqlite:///warehouse_local.db")
)

app = Flask(
    __name__,
    template_folder="template",
    static_folder="statics",
    static_url_path="/static",
)

app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "change-this-secret-key")
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

LIMITS = {
    "modems_total": 12,
    "xi6": 12,
    "xid": 12,
    "xg2": 5,
    "dvr": 5,
    "onu": 2,
}

QTY_FIELDS = [
    "xb3",
    "xb6",
    "xb7",
    "xb8",
    "xb10",
    "xi6",
    "xid",
    "xg2",
    "dvr",
    "onu",
    "xer10",
    "camera",
    "battery",
    "sensor",
    "screen",
    "extra_qty",
]

EXPORT_EMAIL_TO = os.getenv("EXPORT_EMAIL_TO", "lisiyluis90@gmail.com")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
EXPORT_ENDPOINT_TOKEN = os.getenv("EXPORT_ENDPOINT_TOKEN", "")


class Order(db.Model):
    __tablename__ = "orders"

    id = db.Column(db.Integer, primary_key=True)
    created_at_utc = db.Column(
        db.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )
    business_date = db.Column(db.Date, nullable=False, index=True)

    telegram_user_id = db.Column(db.String(80), nullable=True)
    telegram_username = db.Column(db.String(120), nullable=True)
    telegram_name = db.Column(db.String(200), nullable=True)
    raw_telegram_data = db.Column(db.Text, nullable=True)

    bp_number = db.Column(db.String(120), nullable=False, index=True)

    xb3 = db.Column(db.Integer, nullable=False, default=0)
    xb6 = db.Column(db.Integer, nullable=False, default=0)
    xb7 = db.Column(db.Integer, nullable=False, default=0)
    xb8 = db.Column(db.Integer, nullable=False, default=0)
    xb10 = db.Column(db.Integer, nullable=False, default=0)
    xi6 = db.Column(db.Integer, nullable=False, default=0)
    xid = db.Column(db.Integer, nullable=False, default=0)
    xg2 = db.Column(db.Integer, nullable=False, default=0)
    dvr = db.Column(db.Integer, nullable=False, default=0)
    onu = db.Column(db.Integer, nullable=False, default=0)
    xer10 = db.Column(db.Integer, nullable=False, default=0)
    camera = db.Column(db.Integer, nullable=False, default=0)
    battery = db.Column(db.Integer, nullable=False, default=0)
    sensor = db.Column(db.Integer, nullable=False, default=0)
    screen = db.Column(db.Integer, nullable=False, default=0)
    extra_qty = db.Column(db.Integer, nullable=False, default=0)
    extra_note = db.Column(db.String(255), nullable=False, default="")

    def created_at_et(self) -> datetime:
        dt = self.created_at_utc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(TZ)


class ExportLog(db.Model):
    __tablename__ = "export_logs"

    business_date = db.Column(db.Date, primary_key=True)
    last_hash = db.Column(db.String(64), nullable=False, default="")
    last_sent_at_utc = db.Column(db.DateTime(timezone=True), nullable=True)
    send_count = db.Column(db.Integer, nullable=False, default=0)


def empty_form_values() -> dict:
    values = {field: 0 for field in QTY_FIELDS}
    values["bp_number"] = ""
    values["extra_note"] = ""
    return values


def et_now() -> datetime:
    return datetime.now(TZ)


def current_business_date(now_et: datetime | None = None) -> date:
    now_et = now_et or et_now()
    if now_et.hour < 3:
        return (now_et - timedelta(days=1)).date()
    return now_et.date()


def in_export_window(now_et: datetime | None = None) -> bool:
    now_et = now_et or et_now()
    return now_et.hour >= 21 or now_et.hour < 3


def parse_non_negative_int(value: str | None) -> int:
    try:
        return max(0, int((value or "0").strip()))
    except (TypeError, ValueError, AttributeError):
        return 0


def validate_order_form(form_data) -> tuple[list[str], dict]:
    cleaned = empty_form_values()
    cleaned["bp_number"] = (form_data.get("bp_number") or "").strip()
    cleaned["extra_note"] = (form_data.get("extra_note") or "").strip()

    for field in QTY_FIELDS:
        cleaned[field] = parse_non_negative_int(form_data.get(field))

    errors = []

    if not cleaned["bp_number"]:
        errors.append("BP Number is required.")

    modems_total = (
        cleaned["xb3"]
        + cleaned["xb6"]
        + cleaned["xb7"]
        + cleaned["xb8"]
        + cleaned["xb10"]
    )

    if modems_total > LIMITS["modems_total"]:
        errors.append("Total modems cannot exceed 12.")

    if cleaned["xi6"] > LIMITS["xi6"]:
        errors.append("XI6 cannot exceed 12.")

    if cleaned["xid"] > LIMITS["xid"]:
        errors.append("XID cannot exceed 12.")

    if cleaned["xg2"] > LIMITS["xg2"]:
        errors.append("XG2 cannot exceed 5.")

    if cleaned["dvr"] > LIMITS["dvr"]:
        errors.append("DVR cannot exceed 5.")

    if cleaned["onu"] > LIMITS["onu"]:
        errors.append("ONU cannot exceed 2.")

    if cleaned["extra_qty"] > 0 and not cleaned["extra_note"]:
        errors.append("Please describe the additional equipment.")

    total_selected = sum(cleaned[field] for field in QTY_FIELDS)
    if total_selected <= 0:
        errors.append("Please select at least one item.")

    return errors, cleaned


def get_orders_for_business_date(business_date: date) -> list[Order]:
    return (
        Order.query.filter_by(business_date=business_date)
        .order_by(Order.created_at_utc.asc(), Order.id.asc())
        .all()
    )


def build_export_data(business_date: date) -> tuple[str, str, str, int]:
    orders = get_orders_for_business_date(business_date)

    headers = [
        "Submitted ET",
        "Order ID",
        "BP Number",
        "Technician Name",
        "Telegram Username",
        "Telegram User ID",
        "XB3",
        "XB6",
        "XB7",
        "XB8",
        "XB10",
        "XI6",
        "XID",
        "XG2",
        "DVR",
        "ONU",
        "XER10",
        "CAMERA",
        "BATTERY",
        "SENSOR",
        "SCREEN",
        "ADDITIONAL QTY",
        "ADDITIONAL NOTE",
    ]

    totals = {field: 0 for field in QTY_FIELDS}
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)

    html_rows = []

    for order in orders:
        created_et = order.created_at_et().strftime("%Y-%m-%d %I:%M %p")
        writer.writerow(
            [
                created_et,
                order.id,
                order.bp_number,
                order.telegram_name or "",
                order.telegram_username or "",
                order.telegram_user_id or "",
                order.xb3,
                order.xb6,
                order.xb7,
                order.xb8,
                order.xb10,
                order.xi6,
                order.xid,
                order.xg2,
                order.dvr,
                order.onu,
                order.xer10,
                order.camera,
                order.battery,
                order.sensor,
                order.screen,
                order.extra_qty,
                order.extra_note,
            ]
        )

        for field in QTY_FIELDS:
            totals[field] += int(getattr(order, field) or 0)

        html_rows.append(
            f"""
            <tr>
              <td style="padding:10px;border-bottom:1px solid #e7edf5;">{html.escape(created_et)}</td>
              <td style="padding:10px;border-bottom:1px solid #e7edf5;">{html.escape(order.bp_number)}</td>
              <td style="padding:10px;border-bottom:1px solid #e7edf5;">{html.escape(order.telegram_name or "-")}</td>
              <td style="padding:10px;border-bottom:1px solid #e7edf5;">
                XB3 {order.xb3}, XB6 {order.xb6}, XB7 {order.xb7}, XB8 {order.xb8}, XB10 {order.xb10},
                XI6 {order.xi6}, XID {order.xid}, XG2 {order.xg2}, DVR {order.dvr}, ONU {order.onu},
                XER10 {order.xer10}, CAMERA {order.camera}, BATTERY {order.battery}, SENSOR {order.sensor},
                SCREEN {order.screen}, EXTRA {order.extra_qty}
                {f" ({html.escape(order.extra_note)})" if order.extra_note else ""}
              </td>
            </tr>
            """
        )

    csv_content = output.getvalue()
    subject = f"Warehouse Orders Export - {business_date.isoformat()}"

    summary_html = f"""
    <html>
      <body style="font-family:Arial,Helvetica,sans-serif;background:#f5f7fb;color:#122033;padding:24px;">
        <div style="max-width:980px;margin:0 auto;background:#ffffff;border-radius:18px;padding:24px;box-shadow:0 12px 30px rgba(18,32,51,0.08);">
          <h2 style="margin:0 0 8px;">Warehouse Orders Export</h2>
          <p style="margin:0 0 18px;color:#556579;">Business date: <strong>{business_date.isoformat()}</strong></p>

          <div style="background:#f7faff;border:1px solid #dfe8f5;border-radius:14px;padding:14px 16px;margin-bottom:18px;">
            <p style="margin:0 0 10px;"><strong>Total orders:</strong> {len(orders)}</p>
            <p style="margin:0;line-height:1.7;">
              <strong>Modems:</strong> {totals['xb3'] + totals['xb6'] + totals['xb7'] + totals['xb8'] + totals['xb10']}<br>
              <strong>XI6:</strong> {totals['xi6']} |
              <strong>XID:</strong> {totals['xid']} |
              <strong>XG2:</strong> {totals['xg2']} |
              <strong>DVR:</strong> {totals['dvr']} |
              <strong>ONU:</strong> {totals['onu']}<br>
              <strong>XER10:</strong> {totals['xer10']} |
              <strong>CAMERA:</strong> {totals['camera']} |
              <strong>BATTERY:</strong> {totals['battery']} |
              <strong>SENSOR:</strong> {totals['sensor']} |
              <strong>SCREEN:</strong> {totals['screen']} |
              <strong>ADDITIONAL:</strong> {totals['extra_qty']}
            </p>
          </div>

          <table style="width:100%;border-collapse:collapse;font-size:14px;">
            <thead>
              <tr>
                <th align="left" style="padding:10px;border-bottom:2px solid #d7e2f0;">Submitted ET</th>
                <th align="left" style="padding:10px;border-bottom:2px solid #d7e2f0;">BP Number</th>
                <th align="left" style="padding:10px;border-bottom:2px solid #d7e2f0;">Technician</th>
                <th align="left" style="padding:10px;border-bottom:2px solid #d7e2f0;">Order Breakdown</th>
              </tr>
            </thead>
            <tbody>
              {''.join(html_rows) if html_rows else '<tr><td colspan="4" style="padding:10px;">No orders found.</td></tr>'}
            </tbody>
          </table>

          <p style="margin-top:18px;color:#6b7b8f;">A CSV attachment with the full breakdown is included.</p>
        </div>
      </body>
    </html>
    """

    return csv_content, subject, summary_html, len(orders)


def send_export_email(subject: str, html_body: str, filename: str, csv_content: str) -> tuple[bool, str]:
    if not SMTP_USERNAME or not SMTP_PASSWORD:
        return False, "SMTP credentials are missing."

    message = EmailMessage()
    message["From"] = SMTP_USERNAME
    message["To"] = EXPORT_EMAIL_TO
    message["Subject"] = subject
    message.set_content(
        "Warehouse export attached. Open this email in HTML to view the formatted summary."
    )
    message.add_alternative(html_body, subtype="html")
    message.add_attachment(
        csv_content.encode("utf-8"),
        maintype="text",
        subtype="csv",
        filename=filename,
    )

    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
            smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
            smtp.send_message(message)
        return True, "Export email sent."
    except Exception as exc:
        return False, f"Email send failed: {exc}"


def maybe_send_export(force: bool = False) -> tuple[bool, str]:
    now_local = et_now()

    if not force and not in_export_window(now_local):
        return False, "Outside export window."

    business_date = current_business_date(now_local)
    csv_content, subject, html_body, order_count = build_export_data(business_date)

    if order_count == 0:
        return False, "No orders to export."

    payload_hash = hashlib.sha256(csv_content.encode("utf-8")).hexdigest()
    log = db.session.get(ExportLog, business_date)

    if log and log.last_hash == payload_hash and not force:
        return False, "No changes since the last export."

    ok, message = send_export_email(
        subject=subject,
        html_body=html_body,
        filename=f"warehouse_orders_{business_date.isoformat()}.csv",
        csv_content=csv_content,
    )
    if not ok:
        return False, message

    if not log:
        log = ExportLog(business_date=business_date)
        db.session.add(log)

    log.last_hash = payload_hash
    log.last_sent_at_utc = datetime.now(timezone.utc)
    log.send_count = int(log.send_count or 0) + 1
    db.session.commit()

    return True, f"{message} {order_count} orders included."


def require_token(expected_token: str) -> None:
    provided = request.args.get("token") or request.headers.get("X-Admin-Token", "")
    if not expected_token or provided != expected_token:
        abort(403)


@app.after_request
def add_cache_headers(response):
    if request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "public, max-age=300"
    else:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return response


@app.route("/")
def index():
    session["accepted_rules"] = False
    return render_template("disclaimer.html", limits=LIMITS)


@app.route("/accept", methods=["POST"])
def accept():
    session["accepted_rules"] = True
    return redirect(url_for("request_form"))


@app.route("/request")
def request_form():
    if not session.get("accepted_rules"):
        return redirect(url_for("index"))
    return render_template(
        "request_form.html",
        limits=LIMITS,
        errors=[],
        form_values=empty_form_values(),
    )


@app.route("/submit", methods=["POST"])
def submit_order():
    if not session.get("accepted_rules"):
        return redirect(url_for("index"))

    errors, cleaned = validate_order_form(request.form)
    if errors:
        return render_template(
            "request_form.html",
            limits=LIMITS,
            errors=errors,
            form_values=cleaned,
        )

    order = Order(
        business_date=current_business_date(),
        telegram_user_id=(request.form.get("telegram_user_id") or "").strip(),
        telegram_username=(request.form.get("telegram_username") or "").strip(),
        telegram_name=(request.form.get("telegram_name") or "").strip(),
        raw_telegram_data=(request.form.get("raw_telegram_data") or "").strip(),
        bp_number=cleaned["bp_number"],
        xb3=cleaned["xb3"],
        xb6=cleaned["xb6"],
        xb7=cleaned["xb7"],
        xb8=cleaned["xb8"],
        xb10=cleaned["xb10"],
        xi6=cleaned["xi6"],
        xid=cleaned["xid"],
        xg2=cleaned["xg2"],
        dvr=cleaned["dvr"],
        onu=cleaned["onu"],
        xer10=cleaned["xer10"],
        camera=cleaned["camera"],
        battery=cleaned["battery"],
        sensor=cleaned["sensor"],
        screen=cleaned["screen"],
        extra_qty=cleaned["extra_qty"],
        extra_note=cleaned["extra_note"],
    )

    db.session.add(order)
    db.session.commit()

    export_note = ""
    if in_export_window():
        sent, status = maybe_send_export(force=False)
        export_note = status if sent else ""

    return render_template(
        "success.html",
        bp_number=cleaned["bp_number"],
        confirmation_message="Your request was sent to the Warehouse.",
        export_note=export_note,
    )


@app.route("/admin/orders")
def admin_orders():
    require_token(ADMIN_TOKEN)
    business_date_raw = request.args.get("date")
    if business_date_raw:
        selected_date = date.fromisoformat(business_date_raw)
    else:
        selected_date = current_business_date()

    orders = get_orders_for_business_date(selected_date)
    return render_template(
        "admin_orders.html",
        selected_date=selected_date.isoformat(),
        orders=orders,
    )


@app.route("/admin/export/check")
def admin_export_check():
    require_token(EXPORT_ENDPOINT_TOKEN)
    force = request.args.get("force") == "1"
    sent, status = maybe_send_export(force=force)
    return jsonify(
        {
            "ok": True,
            "sent": sent,
            "status": status,
            "business_date": current_business_date().isoformat(),
            "now_et": et_now().isoformat(),
        }
    )


@app.route("/admin/export/csv")
def admin_export_csv():
    require_token(ADMIN_TOKEN)
    business_date_raw = request.args.get("date")
    selected_date = (
        date.fromisoformat(business_date_raw)
        if business_date_raw
        else current_business_date()
    )
    csv_content, _, _, _ = build_export_data(selected_date)
    return Response(
        csv_content,
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=warehouse_orders_{selected_date.isoformat()}.csv"
        },
    )


with app.app_context():
    db.create_all()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
