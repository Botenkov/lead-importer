"""
lead_importer.py
────────────────────────────────────────────────────────────────────────────
Импорт лидов из Google Sheets → Bitrix24.

Читает вкладки "Kitchen New", "Ormari" и "Kitchen май", проверяет дубли через
Bitrix24 API, создаёт новые лиды и отмечает каждую строку в таблице:
  CREATED   — лид успешно создан И ВЕРИФИЦИРОВАН в Bitrix24
  DUPLICATE — уже существует в Bitrix24
  ERROR: …  — что-то пошло не так (текст ошибки)

Назначение операторов в ChatApp вынесено в отдельный сервис (chat-assigner).

Переменные окружения (Railway / .env):
  GOOGLE_CREDENTIALS_JSON  — JSON сервисного аккаунта Google (одной строкой)
  BITRIX_WEBHOOK           — URL вебхука Bitrix24
────────────────────────────────────────────────────────────────────────────
"""

import os
import re
import json
import time
import logging
from datetime import datetime, timezone

import gspread
from google.oauth2.service_account import Credentials
import requests

# ─── Настройка логирования ──────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Конфигурация ───────────────────────────────────────────────────────────

SPREADSHEET_ID  = "1cOvC_x4jpdArbtPuFnmtXgTk4KiWfpauyw4OIv7UPxA"
BITRIX_WEBHOOK  = os.environ.get(
    "BITRIX_WEBHOOK",
    "https://tekstura.bitrix24.com/rest/1/acrfkolvy6hhyum9/"
)

# Пауза между запросами к Bitrix24 — чтобы не словить rate limit
BITRIX_DELAY = 0.4  # секунды

# SOURCE_ID по платформе
SOURCE_MAP = {
    "ig": "UC_M3JOUQ",  # Instagram
    "fb": "UC_EXNWD9",  # Facebook
}

LEAD_STATUS_ID = "UC_6TAZVN"

# ─── Маппинги для Kitchen май ───────────────────────────────────────────────
# Поля новой формы (Kitchen 2.0): бюджет + флаг "нужна техника"

# UF_CRM_1778484294109 (money, "число|EUR")
# Берём верхнюю границу диапазона — так уже было у руками заведённых лидов
BUDGET_MAP = {
    "do_4.000€":     "4000|EUR",
    "4.000–6.000€":  "6000|EUR",
    "6.000–9.000€":  "9000|EUR",
    "9.000€+":       "10000|EUR",
    # "još_nisam_siguran" → не передаём (см. parse_budget)
}

# UF_CRM_1778484348218 (boolean, "Запрос на технику")
# Если "samo_kuhinja" — точно нет; если "kuhinja_+_tehnika" — точно да; иначе не передаём.
TEHNIKA_MAP = {
    "samo_kuhinja":           0,
    "kuhinja_+_tehnika":      1,
    # "još_nisam_siguran_/_sigurna" → None (не передаём)
}

UF_BUDGET  = "UF_CRM_1778484294109"
UF_TEHNIKA = "UF_CRM_1778484348218"


def parse_budget(raw: str) -> str | None:
    """Превращает 'do_4.000€' / '4.000–6.000€' / ... в '4000|EUR'. Иначе None."""
    return BUDGET_MAP.get(raw.strip()) if raw else None


def parse_tehnika(raw: str) -> int | None:
    """0 / 1 для известных значений, None для 'не уверен' и прочего."""
    return TEHNIKA_MAP.get(raw.strip()) if raw else None


# ─── Google Sheets: авторизация ─────────────────────────────────────────────

def get_gspread_client():
    """
    Создаёт авторизованный клиент gspread.
    Читает credentials из переменной окружения GOOGLE_CREDENTIALS_JSON.
    """
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise EnvironmentError(
            "Переменная окружения GOOGLE_CREDENTIALS_JSON не задана. "
            "Добавь её в Railway → Variables."
        )
    # Убираем переносы строк — Railway иногда сохраняет переменную в многострочном формате
    creds_json_clean = creds_json.strip().replace("\r\n", "").replace("\r", "")
    creds_dict = json.loads(creds_json_clean)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)


# ─── Bitrix24: вспомогательные функции ─────────────────────────────────────

def bitrix_call(method: str, params: dict) -> dict:
    """
    Делает POST-запрос к Bitrix24 REST API.
    Параметры передаются как form-encoded (Bitrix24 так ожидает вложенные поля).

    КРИТИЧНО: Bitrix24 при логических ошибках (rate limit, auth, невалидный фильтр)
    возвращает HTTP 200 + JSON {"error": "...", "error_description": "..."}.
    Эта функция ВСЕГДА raise при наличии error в ответе — иначе скрипт думает
    что всё ок и пишет CREATED без реального создания лида.
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
    response = requests.post(BITRIX_WEBHOOK + method, data=flat, timeout=15)
    response.raise_for_status()
    data = response.json()

    # Bitrix может вернуть ошибку в JSON при HTTP 200 — обрабатываем явно
    if isinstance(data, dict) and data.get("error"):
        err_code = data.get("error", "")
        err_desc = data.get("error_description", "")
        raise RuntimeError(f"Bitrix API '{method}' error: {err_code} — {err_desc}")

    return data


def is_duplicate(email: str, phone: str) -> bool:
    """
    Поиск дублей через crm.duplicate.findbycomm — это родной метод Bitrix24
    для поиска по средствам связи. Возвращает точные совпадения.

    Структура ответа Bitrix24 непоследовательная:
      Дубли есть: {"result": {"LEAD": [id1, id2]}}    ← dict
      Дублей нет: {"result": []}                       ← list (sic!)
    Поэтому проверяем isinstance перед .get()
    """
    def _check(comm_type: str, value: str) -> bool:
        r = bitrix_call("crm.duplicate.findbycomm", {
            "type":        comm_type,
            "values":      [value],
            "entity_type": "LEAD",
        })
        result = r.get("result")
        if isinstance(result, dict) and result.get("LEAD"):
            log.info(f"  Дубль по {comm_type} ({value}): {result['LEAD']}")
            return True
        return False

    if email:
        if _check("EMAIL", email):
            return True
        time.sleep(BITRIX_DELAY)

    if phone:
        if _check("PHONE", phone):
            return True
        time.sleep(BITRIX_DELAY)

    return False


def verify_lead_exists(lead_id) -> bool:
    """
    Проверяет что лид реально существует в Bitrix24 после создания.
    Защита от фантомного CREATED — если crm.lead.add вернул что-то странное,
    crm.lead.get не найдёт лид и мы поднимем ERROR.
    """
    try:
        r = bitrix_call("crm.lead.get", {"id": lead_id})
        result = r.get("result")
        if not result or str(result.get("ID")) != str(lead_id):
            return False
        return True
    except Exception as e:
        log.warning(f"  verify_lead_exists({lead_id}) упал: {e}")
        return False


def create_bitrix_lead(title, name, last_name, phone, email, source_id, comment,
                       assigned_by_id, extra_fields: dict | None = None):
    """
    Создаёт контакт + лид в Bitrix24, добавляет Viber-ссылку и верифицирует.
    Возвращает lead_id или бросает исключение при ошибке.

    extra_fields — дополнительные поля лида (например UF_CRM_*).

    Порядок:
      A. crm.contact.add → contact_id
      B. crm.lead.add (с CONTACT_IDS и Viber сразу) → lead_id
      C. crm.lead.get → верификация что лид реально создан

    Назначение оператора в ChatApp вынесено в отдельный сервис (chat-assigner).
    """

    # ── A. Создаём контакт ──────────────────────────────────────────────────
    contact_fields = {"NAME": name, "LAST_NAME": last_name}
    if email:
        contact_fields["EMAIL"] = [{"VALUE": email, "VALUE_TYPE": "WORK"}]
    if phone:
        contact_fields["PHONE"] = [{"VALUE": phone, "VALUE_TYPE": "WORK"}]

    r_contact = bitrix_call("crm.contact.add", {"fields": contact_fields})
    contact_id = r_contact.get("result")
    if not contact_id or not isinstance(contact_id, int):
        raise RuntimeError(
            f"crm.contact.add не вернул валидный ID. Ответ: {r_contact}"
        )
    log.info(f"  Контакт создан: ID={contact_id}")
    time.sleep(BITRIX_DELAY)

    # ── B. Создаём лид (с привязкой к контакту и Viber-ссылкой сразу) ──────
    lead_fields = {
        "TITLE":          title,
        "NAME":           name,
        "LAST_NAME":      last_name,
        "SOURCE_ID":      source_id,
        "STATUS_ID":      LEAD_STATUS_ID,
        "ASSIGNED_BY_ID": assigned_by_id,
        "COMMENTS":       comment,
        "CONTACT_IDS":    [contact_id],
    }
    if email:
        lead_fields["EMAIL"] = [{"VALUE": email, "VALUE_TYPE": "WORK"}]
    if phone:
        lead_fields["PHONE"]              = [{"VALUE": phone, "VALUE_TYPE": "WORK"}]
        lead_fields["IM"]                 = [{"VALUE_TYPE": "VIBER", "VALUE": phone}]
        lead_fields["UF_CRM_VIBER_LINK"]  = f"viber://chat?number={phone}"

    # Дополнительные кастомные поля (Kitchen май: бюджет, флаг техники)
    if extra_fields:
        for k, v in extra_fields.items():
            if v is not None:
                lead_fields[k] = v

    r_lead = bitrix_call("crm.lead.add", {"fields": lead_fields})
    lead_id = r_lead.get("result")
    if not lead_id or not isinstance(lead_id, int):
        raise RuntimeError(
            f"crm.lead.add не вернул валидный ID. Ответ: {r_lead}"
        )
    log.info(f"  Лид создан: ID={lead_id}")
    time.sleep(BITRIX_DELAY)

    # ── C. ВЕРИФИКАЦИЯ: лид реально существует? ────────────────────────────
    if not verify_lead_exists(lead_id):
        raise RuntimeError(
            f"crm.lead.add вернул ID={lead_id}, но crm.lead.get не подтверждает "
            f"существование. Контакт {contact_id} остался осиротевшим."
        )
    log.info(f"  Лид {lead_id} верифицирован ✓")
    time.sleep(BITRIX_DELAY)

    return lead_id


# ─── Вспомогательные функции парсинга ───────────────────────────────────────

# Паттерн для удаления "стилизованных" Unicode-символов типа 𝕯, 𝓜, 𝗞.
# Оставляем базовую латиницу, кириллицу и расширенную латиницу (диакритика).
UNICODE_JUNK = re.compile(
    r"[^\x20-\x7E\u00C0-\u024F\u0400-\u04FF]"
)


def clean_name(raw: str) -> tuple[str, str]:
    """
    Очищает имя от тяжёлого Unicode, разбивает на NAME и LAST_NAME.
    Если одно слово — LAST_NAME = "—".
    """
    cleaned = UNICODE_JUNK.sub("", raw).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if not cleaned:
        # Если после очистки ничего не осталось — оставляем оригинал
        cleaned = raw.strip()

    parts = cleaned.split(" ", 1)
    name      = parts[0]
    last_name = parts[1] if len(parts) > 1 else "—"
    return name, last_name


def parse_phone(raw: str) -> str | None:
    """
    Убирает префикс "p:" из значения телефона.
    Возвращает None если телефона нет ("0" или пусто).
    """
    phone = raw.replace("p:", "").strip()
    return None if (phone in ("", "0")) else phone


def get_assigned_by_id() -> int:
    """Определяет ответственного менеджера по дню недели."""
    weekday = datetime.now(timezone.utc).weekday()  # 0=Пн, 6=Вс
    return 28 if weekday < 5 else 30  # Пн–Пт=28, Сб–Вс=30


def get_source(platform: str) -> str:
    return SOURCE_MAP.get(platform.lower(), "UC_EXNWD9")


def platform_label(platform: str) -> str:
    return "Instagram" if platform.lower() == "ig" else "Facebook"


# ─── Обработка вкладки Kitchen New ─────────────────────────────────────────

def process_kitchen_row(row: list, row_index: int, sheet, assigned_by_id: int) -> str:
    """
    Обрабатывает одну строку из вкладки Kitchen New.
    Возвращает статус: "CREATED", "DUPLICATE", "SKIP", или "ERROR: ..."

    Индексы колонок (0-based):
      11=платформа, 12=размер, 13=timeline, 14=email, 15=имя, 16=телефон, 17=статус
    """
    # Расширяем строку до нужной длины (некоторые строки короче)
    while len(row) < 18:
        row.append("")

    status_val = row[17].strip()
    if status_val:
        return "SKIP"  # уже обработана

    email     = row[14].strip()
    raw_name  = row[15].strip()
    raw_phone = row[16].strip()
    platform  = row[11].strip()

    # Данные для комментария
    ad       = row[3].strip()
    adset    = row[5].strip()
    size     = row[12].strip()
    timeline = row[13].strip()
    date     = row[1].strip()

    phone = parse_phone(raw_phone)

    # Дедупликация (через crm.duplicate.findbycomm — единственный надёжный способ)
    if is_duplicate(email, phone):
        return "DUPLICATE"

    # Парсинг имени
    name, last_name = clean_name(raw_name)
    plat_label = platform_label(platform)
    src_id     = get_source(platform)

    title = (
        f"{name} {last_name} — Kitchen ({plat_label})"
        if last_name != "—"
        else f"{name} — Kitchen ({plat_label})"
    )
    comment = (
        f"Tab: Kitchen New | Ad: {ad} | Size: {size} | "
        f"Timeline: {timeline} | Adset: {adset} | Date: {date}"
    )

    log.info(f"  → Создаю Kitchen лид: {title}")
    lead_id = create_bitrix_lead(title, name, last_name, phone, email, src_id, comment, assigned_by_id)
    log.info(f"  ✓ Kitchen New → лид создан ID={lead_id}: {title}")
    return "CREATED"


# ─── Обработка вкладки Ormari ───────────────────────────────────────────────

def process_ormari_row(row: list, row_index: int, sheet, assigned_by_id: int) -> str:
    """
    Обрабатывает одну строку из вкладки Ormari.
    Возвращает статус.

    Индексы колонок (0-based):
      11=платформа, 12=тип шкафа, 13=ширина, 14=timeline,
      15=email, 16=имя, 17=телефон, 18=статус
    """
    while len(row) < 19:
        row.append("")

    status_val = row[18].strip()
    if status_val:
        return "SKIP"  # уже обработана

    email     = row[15].strip()
    raw_name  = row[16].strip()
    raw_phone = row[17].strip()
    platform  = row[11].strip()

    # Данные для комментария
    ad       = row[3].strip()
    adset    = row[5].strip()
    wardrobe = row[12].strip()
    size     = row[13].strip()
    timeline = row[14].strip()
    date     = row[1].strip()

    phone = parse_phone(raw_phone)

    # Дедупликация
    if is_duplicate(email, phone):
        return "DUPLICATE"

    # Парсинг имени
    name, last_name = clean_name(raw_name)
    plat_label = platform_label(platform)
    src_id     = get_source(platform)

    title = (
        f"{name} {last_name} — Ormari ({plat_label})"
        if last_name != "—"
        else f"{name} — Ormari ({plat_label})"
    )
    comment = (
        f"Tab: Ormari | Ad: {ad} | Wardrobe: {wardrobe} | Size: {size} | "
        f"Timeline: {timeline} | Adset: {adset} | Date: {date}"
    )

    log.info(f"  → Создаю Ormari лид: {title}")
    lead_id = create_bitrix_lead(title, name, last_name, phone, email, src_id, comment, assigned_by_id)
    log.info(f"  ✓ Ormari → лид создан ID={lead_id}: {title}")
    return "CREATED"


# ─── Обработка вкладки Kitchen май ─────────────────────────────────────────

def process_kitchen_may_row(row: list, row_index: int, sheet, assigned_by_id: int) -> str:
    """
    Обрабатывает одну строку из вкладки Kitchen май (форма Kitchen 2.0).
    Возвращает статус.

    Индексы колонок (0-based) — 20 колонок (A-T):
      11=платформа (ig/fb)
      12=da_li_imate_plan         (есть план стана: da/ne)
      13=kada_planirate           (когда планирует начать)
      14=budget                   (бюджет диапазон) → UF_CRM_1778484294109
      15=kuhinja_ili_tehnika      (нужна ли техника)→ UF_CRM_1778484348218
      16=email
      17=имя
      18=телефон
      19=статус
    """
    while len(row) < 20:
        row.append("")

    status_val = row[19].strip()
    if status_val:
        return "SKIP"  # уже обработана

    email     = row[16].strip()
    raw_name  = row[17].strip()
    raw_phone = row[18].strip()
    platform  = row[11].strip()

    # Данные для комментария + UF поля
    ad             = row[3].strip()
    adset          = row[5].strip()
    has_plan       = row[12].strip()
    timeline       = row[13].strip()
    budget_raw     = row[14].strip()
    tehnika_raw    = row[15].strip()
    date           = row[1].strip()

    phone = parse_phone(raw_phone)

    # Дедупликация
    if is_duplicate(email, phone):
        return "DUPLICATE"

    # Парсинг имени
    name, last_name = clean_name(raw_name)
    plat_label = platform_label(platform)
    src_id     = get_source(platform)

    title = (
        f"{name} {last_name} — Kitchen ({plat_label})"
        if last_name != "—"
        else f"{name} — Kitchen ({plat_label})"
    )
    comment = (
        f"Tab: Kitchen май | Ad: {ad} | Plan: {has_plan} | Timeline: {timeline} | "
        f"Budget: {budget_raw} | Tehnika: {tehnika_raw} | Adset: {adset} | Date: {date}"
    )

    # Доп. поля (UF_CRM_*) — только если распарсились в известное значение
    extra_fields = {}
    budget_value = parse_budget(budget_raw)
    if budget_value is not None:
        extra_fields[UF_BUDGET] = budget_value

    tehnika_value = parse_tehnika(tehnika_raw)
    if tehnika_value is not None:
        extra_fields[UF_TEHNIKA] = tehnika_value

    log.info(f"  → Создаю Kitchen май лид: {title} | budget={budget_value} tehnika={tehnika_value}")
    lead_id = create_bitrix_lead(
        title, name, last_name, phone, email, src_id, comment, assigned_by_id,
        extra_fields=extra_fields,
    )
    log.info(f"  ✓ Kitchen май → лид создан ID={lead_id}: {title}")
    return "CREATED"


# ─── Главная функция ─────────────────────────────────────────────────────────

def run():
    log.info("=" * 60)
    log.info("Запуск импорта лидов")

    # Определяем ответственного
    assigned_by_id = get_assigned_by_id()
    weekday_name = datetime.now(timezone.utc).strftime("%A")
    log.info(f"День: {weekday_name}, ASSIGNED_BY_ID={assigned_by_id}")

    # Подключаемся к Google Sheets
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    # Счётчики
    total_created   = 0
    total_duplicate = 0
    total_error     = 0

    # ── Конфигурация вкладок ─────────────────────────────────────────────
    tabs = [
        {
            "name":        "Kitchen New",
            "range":       "A2:R500",
            "status_col":  18,  # Колонка R (1-indexed для gspread)
            "processor":   process_kitchen_row,
        },
        {
            "name":        "Ormari",
            "range":       "A2:S500",
            "status_col":  19,  # Колонка S (1-indexed для gspread)
            "processor":   process_ormari_row,
        },
        {
            "name":        "Kitchen май",
            "range":       "A2:T500",
            "status_col":  20,  # Колонка T (1-indexed для gspread)
            "processor":   process_kitchen_may_row,
        },
    ]

    for tab_cfg in tabs:
        tab_name   = tab_cfg["name"]
        status_col = tab_cfg["status_col"]
        processor  = tab_cfg["processor"]

        log.info(f"\n── Обработка вкладки: {tab_name} ──")

        # Открываем лист
        try:
            sheet = spreadsheet.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            log.warning(f"  Вкладка '{tab_name}' не найдена — пропускаем.")
            continue

        rows  = sheet.get(tab_cfg["range"])

        if not rows:
            log.info(f"  Вкладка {tab_name}: данных нет, пропускаем.")
            continue

        log.info(f"  Прочитано строк: {len(rows)}")

        for i, row in enumerate(rows):
            # row_number в таблице = i + 2 (данные начинаются со строки 2)
            sheet_row = i + 2

            try:
                status = processor(row, sheet_row, sheet, assigned_by_id)

                if status == "SKIP":
                    continue  # Строка уже обработана — молча пропускаем

                # Записываем статус в таблицу
                sheet.update_cell(sheet_row, status_col, status)

                if status == "CREATED":
                    total_created += 1
                elif status == "DUPLICATE":
                    total_duplicate += 1

            except Exception as e:
                err_msg = f"ERROR: {str(e)[:80]}"
                log.error(f"  Строка {sheet_row}: {err_msg}")
                try:
                    sheet.update_cell(sheet_row, status_col, err_msg)
                except Exception:
                    pass  # Не можем записать ошибку — просто логируем
                total_error += 1

            # Небольшая пауза между строками — вежливо к API
            time.sleep(0.1)

    # ── Итоговый отчёт ───────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("ИТОГ:")
    log.info(f"  Создано лидов:    {total_created}")
    log.info(f"  Дублей пропущено: {total_duplicate}")
    log.info(f"  Ошибок:           {total_error}")
    log.info("=" * 60)


# ─── Точка входа ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run()
