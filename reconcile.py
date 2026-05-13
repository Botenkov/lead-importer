#!/usr/bin/env python3
"""
reconcile.py — ежедневная сверка Google Sheets ↔ Bitrix24.

Что делает:
  1. Читает все вкладки (Kitchen New, Ormari, Kitchen май)
  2. Для каждой строки со статусом CREATED за последние N дней
     проверяет — действительно ли лид существует в Bitrix24
  3. Если фантом (CREATED в таблице, нет в Bitrix) → очищает статус.
     Lead-importer пересоздаст лид в следующем cron tick.
  4. Опционально шлёт отчёт в Telegram.

Безопасность:
  - НИКОГДА не создаёт и не удаляет лидов в Bitrix24.
  - НИКОГДА не трогает строки старше LOOKBACK_DAYS.
  - НИКОГДА не трогает строки со статусом отличным от "CREATED".

Деплой на Railway:
  - Отдельный Cron сервис в том же проекте, что и lead-importer
  - Команда:  python reconcile.py
  - Расписание:  0 3 * * *  (03:00 UTC = 06:00 МСК ежедневно)
  - ENV vars: BITRIX_WEBHOOK, GOOGLE_CREDENTIALS_JSON, SPREADSHEET_ID
              опц.: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, RECONCILE_LOOKBACK_DAYS
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta

import requests
import gspread
from google.oauth2.service_account import Credentials


# ─── Конфигурация из ENV ────────────────────────────────────────────────────
BITRIX_WEBHOOK    = os.environ["BITRIX_WEBHOOK"]
GOOGLE_CREDS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]
SPREADSHEET_ID    = os.environ.get(
    "SPREADSHEET_ID",
    "1cOvC_x4jpdArbtPuFnmtXgTk4KiWfpauyw4OIv7UPxA",
)

LOOKBACK_DAYS = int(os.environ.get("RECONCILE_LOOKBACK_DAYS", "7"))

# Telegram — опционально, если оба заданы
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TG_CHAT  = os.environ.get("TELEGRAM_CHAT_ID")

# Пауза между запросами к Bitrix24 (rate limit ~2/сек)
BITRIX_DELAY = 0.4


# ─── Логирование ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("reconcile")


# ─── Bitrix API ─────────────────────────────────────────────────────────────
def bitrix_call(method: str, params: dict) -> dict:
    """
    Делает POST к Bitrix24 REST API. Параметры flatten'ятся в form-encoded
    (Bitrix так ожидает вложенные структуры).
    Поднимает RuntimeError если в ответе error (Bitrix может вернуть
    HTTP 200 с error в JSON).
    """
    flat = {}

    def flatten(obj, prefix=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                flatten(v, f"{prefix}[{k}]" if prefix else k)
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                flatten(v, f"{prefix}[{i}]")
        else:
            flat[prefix] = "" if obj is None else str(obj)

    flatten(params)

    r = requests.post(BITRIX_WEBHOOK + method, data=flat, timeout=15)
    r.raise_for_status()
    data = r.json()

    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(
            f"Bitrix '{method}': {data.get('error')} — {data.get('error_description')}"
        )
    return data


def lead_exists_in_bitrix(email: str, phone: str) -> bool:
    """
    Проверяет существование лида в Bitrix24 по email и/или телефону через
    crm.duplicate.findbycomm — это родной механизм Bitrix для дедупликации.

    Структура ответа Bitrix непоследовательная:
      Дубли есть: {"result": {"LEAD": [id1, id2]}}    ← dict
      Дублей нет: {"result": []}                       ← list
    Поэтому проверяем isinstance перед .get().
    """

    def _check(comm_type: str, value: str) -> bool:
        if not value:
            return False
        r = bitrix_call("crm.duplicate.findbycomm", {
            "type":        comm_type,
            "values":      [value],
            "entity_type": "LEAD",
        })
        result = r.get("result")
        if isinstance(result, dict) and result.get("LEAD"):
            return True
        return False

    if email and _check("EMAIL", email):
        return True
    time.sleep(BITRIX_DELAY)

    if phone and _check("PHONE", phone):
        return True
    time.sleep(BITRIX_DELAY)

    return False


# ─── Парсинг ────────────────────────────────────────────────────────────────
def parse_phone(raw: str) -> str:
    """Удаляет префикс 'p:' и обрезает пробелы."""
    if not raw:
        return ""
    p = raw.strip()
    if p.startswith("p:"):
        p = p[2:]
    return p.strip()


def parse_lead_date(raw: str):
    """
    Парсит '2026-05-09T07:52:27-05:00' → naive datetime (UTC игнор для отсечки).
    Возвращает None если не получилось — такая строка попадёт в проверку.
    """
    if not raw:
        return None
    try:
        # достаточно даты + часов для отсечки по дням
        return datetime.strptime(raw[:19], "%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


# ─── Telegram ───────────────────────────────────────────────────────────────
def telegram_notify(text: str) -> None:
    """Шлёт уведомление в Telegram если настроены TG_TOKEN и TG_CHAT."""
    if not (TG_TOKEN and TG_CHAT):
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data={"chat_id": TG_CHAT, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram упал: {e}")


# ─── Основная логика ────────────────────────────────────────────────────────
TABS = [
    {
        "name":            "Kitchen New",
        "range":           "A2:R",
        "status_col_idx":  17,   # R (0-based)
        "email_col_idx":   14,   # O
        "phone_col_idx":   16,   # Q
        "date_col_idx":    1,    # B
    },
    {
        "name":            "Ormari",
        "range":           "A2:S",
        "status_col_idx":  18,   # S
        "email_col_idx":   15,   # P
        "phone_col_idx":   17,   # R
        "date_col_idx":    1,    # B
    },
    {
        "name":            "kitchen Май",
        "range":           "A2:T",
        "status_col_idx":  19,   # T (0-based) — статус
        "email_col_idx":   16,   # Q
        "phone_col_idx":   18,   # S
        "date_col_idx":    1,    # B
    },
]


def main() -> int:
    log.info("=" * 60)
    log.info(f"reconcile.py — старт, lookback {LOOKBACK_DAYS} дней")
    log.info("=" * 60)

    # Auth Google
    creds = Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDS_JSON),
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    cutoff = datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)

    total_checked  = 0
    total_phantoms = 0
    phantom_details = []  # (tab, row, email)

    for cfg in TABS:
        tab = cfg["name"]
        log.info(f"\n── Сверка вкладки: {tab} ──")

        try:
            sheet = spreadsheet.worksheet(tab)
            rows = sheet.get(cfg["range"])
        except Exception as e:
            log.error(f"  Не смог открыть вкладку {tab}: {e}")
            continue

        log.info(f"  Прочитано строк: {len(rows)}")

        clear_rows = []

        for i, row in enumerate(rows):
            sheet_row = i + 2  # данные с строки 2

            # Только строки со статусом CREATED
            status = row[cfg["status_col_idx"]] if len(row) > cfg["status_col_idx"] else ""
            if status != "CREATED":
                continue

            # Только строки в окне LOOKBACK_DAYS
            date_raw = row[cfg["date_col_idx"]] if len(row) > cfg["date_col_idx"] else ""
            d = parse_lead_date(date_raw)
            if d and d < cutoff:
                continue

            # Берём email и phone
            email = (row[cfg["email_col_idx"]] if len(row) > cfg["email_col_idx"] else "").strip().lower()
            phone = parse_phone(row[cfg["phone_col_idx"]] if len(row) > cfg["phone_col_idx"] else "")

            total_checked += 1

            try:
                exists = lead_exists_in_bitrix(email, phone)
            except Exception as e:
                log.error(f"  Не смог проверить row {sheet_row} ({email}): {e}")
                continue

            if not exists:
                log.warning(f"  ⚠ ФАНТОМ: {tab} row {sheet_row} — {email} ({phone})")
                clear_rows.append(sheet_row)
                phantom_details.append((tab, sheet_row, email))
                total_phantoms += 1

        # Очистка фантомных строк (status → "")
        if clear_rows:
            log.info(f"  Очищаю {len(clear_rows)} строк во вкладке {tab}")
            status_col_1based = cfg["status_col_idx"] + 1
            for sheet_row in clear_rows:
                try:
                    sheet.update_cell(sheet_row, status_col_1based, "")
                    time.sleep(0.3)
                except Exception as e:
                    log.error(f"  Не смог очистить row {sheet_row}: {e}")

    # ── Итог ────────────────────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info(f"ИТОГ: проверено {total_checked} строк CREATED, фантомов: {total_phantoms}")
    log.info("=" * 60)

    # Telegram отчёт
    if total_phantoms > 0:
        msg = f"⚠️ *Lead-importer reconcile*\n\nНайдено и очищено *{total_phantoms}* фантомов:\n\n"
        for tab, row, email in phantom_details[:20]:
            msg += f"• `{tab}` row {row}: {email}\n"
        if len(phantom_details) > 20:
            msg += f"\n…и ещё {len(phantom_details) - 20}"
        msg += "\n\nlead-importer пересоздаст их в следующем cron tick."
        telegram_notify(msg)
    elif os.environ.get("RECONCILE_DAILY_PING") == "1":
        telegram_notify(f"✅ Reconcile OK: проверено {total_checked} строк, фантомов нет.")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log.exception(f"reconcile.py упал: {e}")
        # Алерт о падении самого reconcile
        telegram_notify(f"🚨 *reconcile.py упал*\n\n```\n{e}\n```")
        sys.exit(1)
