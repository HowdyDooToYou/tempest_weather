import math
import os
import re
import smtplib
import sqlite3
from datetime import datetime, timezone
from email.message import EmailMessage

ALERT_STATE_TABLE = "alert_state"
ALERT_CONFIG_TABLE = "alert_config"
DEFAULT_SMTP_HOST = "smtp.gmail.com"
DEFAULT_SMTP_PORT = 587
DEFAULT_SMTP_CREDENTIAL_TARGET = "TempestWeatherSMTP"


def _now_epoch() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _clean_str(value) -> str:
    if not value:
        return ""
    return str(value).strip()


def _env_flag(name: str, default: str) -> bool:
    return os.getenv(name, default).lower() in ("1", "true", "yes", "on")


def _read_windows_credential(target: str) -> tuple[str | None, str | None]:
    if os.name != "nt" or not target:
        return None, None
    try:
        import ctypes
        from ctypes import wintypes
    except Exception:
        return None, None

    class CREDENTIAL(ctypes.Structure):
        _fields_ = [
            ("Flags", wintypes.DWORD),
            ("Type", wintypes.DWORD),
            ("TargetName", wintypes.LPWSTR),
            ("Comment", wintypes.LPWSTR),
            ("LastWritten", wintypes.FILETIME),
            ("CredentialBlobSize", wintypes.DWORD),
            ("CredentialBlob", ctypes.c_void_p),
            ("Persist", wintypes.DWORD),
            ("AttributeCount", wintypes.DWORD),
            ("Attributes", ctypes.c_void_p),
            ("TargetAlias", wintypes.LPWSTR),
            ("UserName", wintypes.LPWSTR),
        ]

    advapi32 = ctypes.WinDLL("Advapi32", use_last_error=True)
    cred_read = advapi32.CredReadW
    cred_read.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(ctypes.POINTER(CREDENTIAL)),
    ]
    cred_read.restype = wintypes.BOOL
    cred_free = advapi32.CredFree
    cred_free.argtypes = [ctypes.c_void_p]
    cred_free.restype = None

    cred_ptr = ctypes.POINTER(CREDENTIAL)()
    if not cred_read(target, 1, 0, ctypes.byref(cred_ptr)):
        return None, None
    try:
        cred = cred_ptr.contents
        username = cred.UserName
        password = ""
        if cred.CredentialBlobSize and cred.CredentialBlob:
            blob = ctypes.string_at(cred.CredentialBlob, cred.CredentialBlobSize)
            try:
                password = blob.decode("utf-16-le")
            except UnicodeDecodeError:
                password = blob.decode("utf-8", errors="ignore")
            password = password.rstrip("\x00")
        return username or None, password or None
    finally:
        cred_free(cred_ptr)


def _load_smtp_credentials() -> None:
    env_username = _clean_str(os.getenv("SMTP_USERNAME"))
    env_password = os.getenv("SMTP_PASSWORD")
    if env_username and env_password:
        return
    target = _clean_str(os.getenv("SMTP_CRED_TARGET")) or DEFAULT_SMTP_CREDENTIAL_TARGET
    username, password = _read_windows_credential(target)
    if username and not env_username:
        os.environ["SMTP_USERNAME"] = username
    if password and not env_password:
        os.environ["SMTP_PASSWORD"] = password


def ensure_alert_state_table(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ALERT_STATE_TABLE} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.commit()


def load_alert_state(db_path: str) -> dict:
    ensure_alert_state_table(db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(f"SELECT key, value FROM {ALERT_STATE_TABLE}").fetchall()
    state = {key: value == "1" for key, value in rows}
    state.setdefault("freeze_sent", False)
    state.setdefault("deep_freeze_sent", False)
    return state


def save_alert_state(db_path: str, updates: dict) -> None:
    if not updates:
        return
    ensure_alert_state_table(db_path)
    now_epoch = _now_epoch()
    with sqlite3.connect(db_path) as conn:
        for key, value in updates.items():
            conn.execute(
                f"""
                INSERT OR REPLACE INTO {ALERT_STATE_TABLE} (key, value, updated_at)
                VALUES (?, ?, ?)
                """,
                (key, "1" if value else "0", now_epoch),
            )
        conn.commit()


def ensure_alert_config_table(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ALERT_CONFIG_TABLE} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.commit()


def load_alert_config(db_path: str) -> tuple[dict, int | None]:
    ensure_alert_config_table(db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT key, value, updated_at FROM {ALERT_CONFIG_TABLE}"
        ).fetchall()
    config = {}
    updated_at = None
    for key, value, updated in rows:
        config[key] = value
        if updated_at is None or updated > updated_at:
            updated_at = updated
    return config, updated_at


def save_alert_config(db_path: str, updates: dict) -> tuple[list[str], list[str]]:
    if not updates:
        return [], []
    ensure_alert_config_table(db_path)
    now_epoch = _now_epoch()
    saved_keys = []
    cleared_keys = []
    with sqlite3.connect(db_path) as conn:
        for key, value in updates.items():
            clean_value = _clean_str(value)
            if clean_value:
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO {ALERT_CONFIG_TABLE} (key, value, updated_at)
                    VALUES (?, ?, ?)
                    """,
                    (key, clean_value, now_epoch),
                )
                saved_keys.append(key)
            else:
                conn.execute(
                    f"DELETE FROM {ALERT_CONFIG_TABLE} WHERE key = ?",
                    (key,),
                )
                cleared_keys.append(key)
        conn.commit()
    return saved_keys, cleared_keys


def delete_alert_config(db_path: str, keys: list[str]) -> None:
    if not keys:
        return
    ensure_alert_config_table(db_path)
    with sqlite3.connect(db_path) as conn:
        for key in keys:
            conn.execute(
                f"DELETE FROM {ALERT_CONFIG_TABLE} WHERE key = ?",
                (key,),
            )
        conn.commit()


def resolve_alert_recipients(db_path: str, overrides: dict | None = None) -> tuple[str | None, str | None]:
    overrides = overrides or {}
    email_override = _clean_str(overrides.get("alert_email_to"))
    sms_override = _clean_str(overrides.get("alert_sms_to"))
    if email_override or sms_override:
        return email_override or None, sms_override or None

    config, _ = load_alert_config(db_path)
    email_to = _clean_str(config.get("alert_email_to"))
    sms_to = _clean_str(config.get("alert_sms_to"))
    if not email_to:
        email_to = _clean_str(os.getenv("ALERT_EMAIL_TO"))
    if not sms_to:
        sms_to = _clean_str(os.getenv("VERIZON_SMS_TO"))
    return email_to or None, sms_to or None


def get_email_config(
    overrides: dict | None = None,
    to_address: str | None = None,
    return_error: bool = False,
) -> tuple[dict | None, str | None] | dict | None:
    overrides = overrides or {}
    host = os.getenv("SMTP_HOST") or DEFAULT_SMTP_HOST
    port_value = os.getenv("SMTP_PORT") or str(DEFAULT_SMTP_PORT)
    try:
        port = int(port_value)
    except ValueError:
        port = DEFAULT_SMTP_PORT
    username = _clean_str(overrides.get("smtp_username"))
    password = overrides.get("smtp_password")
    if not username or not password:
        _load_smtp_credentials()
    if not username:
        username = _clean_str(os.getenv("SMTP_USERNAME"))
    if not password:
        password = os.getenv("SMTP_PASSWORD")
    from_address = _clean_str(overrides.get("smtp_from")) or _clean_str(os.getenv("ALERT_EMAIL_FROM")) or username
    to_address = _clean_str(to_address) or _clean_str(os.getenv("ALERT_EMAIL_TO"))
    use_tls = _env_flag("SMTP_USE_TLS", "true")
    use_ssl = _env_flag("SMTP_USE_SSL", "false")
    if not username or not password or not from_address:
        message = "Email auth missing (SMTP env vars or Windows Credential Manager)."
        return (None, message) if return_error else None
    config = {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "from_address": from_address,
        "to_address": to_address,
        "use_tls": use_tls,
        "use_ssl": use_ssl,
    }
    return (config, None) if return_error else config


def send_email(
    subject: str,
    body: str,
    to_address: str | None = None,
    overrides: dict | None = None,
    return_error: bool = False,
) -> tuple[bool, str | None] | bool:
    config, config_error = get_email_config(
        overrides=overrides,
        to_address=to_address,
        return_error=True,
    )
    if not config:
        message = config_error or "Email configuration missing."
        return (False, message) if return_error else False
    recipient = _clean_str(to_address) or config["to_address"]
    if not recipient:
        message = "Recipient missing (alert recipient email or ALERT_EMAIL_TO)."
        return (False, message) if return_error else False
    message = EmailMessage()
    message["Subject"] = subject or ""
    message["From"] = config["from_address"]
    message["To"] = recipient
    message.set_content(body)
    try:
        if config["use_ssl"]:
            server = smtplib.SMTP_SSL(config["host"], config["port"], timeout=10)
        else:
            server = smtplib.SMTP(config["host"], config["port"], timeout=10)
        with server:
            server.ehlo()
            if not config["use_ssl"] and config["use_tls"]:
                server.starttls()
                server.ehlo()
            server.login(config["username"], config["password"])
            server.send_message(message)
        return (True, None) if return_error else True
    except Exception as exc:
        return (False, str(exc)) if return_error else False


def get_verizon_sms_address(raw_number: str | None) -> str | None:
    digits = re.sub(r"\D", "", raw_number or "")
    if not digits:
        return None
    return f"{digits}@vtext.com"


def send_verizon_sms(
    message: str,
    sms_number: str | None = None,
    overrides: dict | None = None,
    return_error: bool = False,
) -> tuple[bool, str | None] | bool:
    sms_address = get_verizon_sms_address(sms_number) if sms_number else None
    if not sms_address:
        sms_address = get_verizon_sms_address(os.getenv("VERIZON_SMS_TO"))
    if not sms_address:
        message_text = "Verizon SMS number missing (override or VERIZON_SMS_TO)."
        return (False, message_text) if return_error else False
    return send_email(
        "Tempest Alert",
        message,
        to_address=sms_address,
        overrides=overrides,
        return_error=return_error,
    )


def determine_freeze_alerts(temp_f: float, state: dict) -> tuple[list[dict], dict]:
    alerts = []
    reset_updates = {}
    if temp_f is None:
        return alerts, reset_updates
    try:
        temp_f = float(temp_f)
    except (TypeError, ValueError):
        return alerts, reset_updates
    if math.isnan(temp_f):
        return alerts, reset_updates
    freeze_sent = state.get("freeze_sent", False)
    deep_freeze_sent = state.get("deep_freeze_sent", False)
    if temp_f > float(os.getenv("FREEZE_RESET_F", "34")):
        if freeze_sent or deep_freeze_sent:
            reset_updates = {"freeze_sent": False, "deep_freeze_sent": False}
        return alerts, reset_updates
    if temp_f <= float(os.getenv("DEEP_FREEZE_F", "18")) and not deep_freeze_sent:
        alerts.append({
            "title": "Deep Freeze Advisory",
            "state_updates": {"deep_freeze_sent": True, "freeze_sent": True},
        })
    elif temp_f <= float(os.getenv("FREEZE_WARNING_F", "32")) and not freeze_sent:
        alerts.append({
            "title": "Freeze Warning",
            "state_updates": {"freeze_sent": True},
        })
    return alerts, reset_updates


def format_local_time(dt_value) -> str:
    if dt_value is None:
        return "--"
    try:
        return dt_value.strftime("%I:%M %p").lstrip("0")
    except Exception:
        return "--"


def build_freeze_alert_message(title: str, temp_f: float, when_local) -> str:
    time_text = format_local_time(when_local)
    return f"{title}: Tempest {temp_f:.1f} F at {time_text}."
