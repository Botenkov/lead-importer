"""
lead_importer.py
────────────────────────────────────────────────────────────────────────────
Импорт лидов из Google Sheets → Bitrix24.

Читает вкладки "Kitchen New", "Ormari" и "Kitchen май", проверяет дубли через
Bitrix24 API, создаёт новые лиды и отмечает каждую строку в таблице:
  CREATED:ID           — лид успешно создан И ВЕРИФИЦИРОВАН в Bitrix24
  VIBER_PENDING:ID     — лид создан, но Viber отложен до рабочего времени (8–20)
  DUPLICATE            — уже существует в Bitrix24
  ERROR: …             — что-то пошло не так (текст ошибки)

Назначение операторов в ChatApp вынесено в отдельный сервис (chat-assigner).

Переменные окружения (Railway / .env):
  GOOGLE_CREDENTIALS_JSON  — JSON сервисного аккаунта Google (одной строкой)
  BITRIX_WEBHOOK           — URL вебхука Bitrix24
  WAZZUP_API_KEY           — API ключ WazzUp для отправки Viber
────────────────────────────────────────────────────────────────────────────
"""

import os
import re
import json
import time
import random
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


# ─── Viber / WazzUp ─────────────────────────────────────────────────────────

WAZZUP_API_KEY          = os.environ.get("WAZZUP_API_KEY", "")
WAZZUP_VIBER_CHANNEL_ID = "f0911dd4-a6b5-48ad-b39f-f9b47c171277"

# Задержка между сообщениями — случайная, чтобы выглядело по-человечески
VIBER_DELAY_MIN        = 30   # секунды
VIBER_DELAY_MAX        = 45

# Рабочие часы для отправки Viber (ночью не беспокоим клиентов)
VIBER_WORK_HOURS_START = 8
VIBER_WORK_HOURS_END   = 20

# Шаблоны сообщений (сербский язык, утверждены носителем, 5 вариантов)
# {name} заменяется на имя клиента при отправке
VIBER_TEMPLATES = [
    (
        "Zdravo, {name}! 👋 Tvoja prijava je primljena. "
        "Naš menadžer će te kontaktirati između 8:00 i 20:00. "
        "Ako ti odgovara neko drugo vreme, ili ti je lakše da nastavimo komunikaciju "
        "putem poruka, slobodno nam piši 💬 "
        "Mere, fotografije prostora ili primer kuhinje koja ti se dopada možeš poslati odmah "
        "— to će nam pomoći da brže pripremimo ponudu. "
        "Tim Tekstura Buro 🏠"
    ),
    (
        "Dobar dan, {name}! "
        "Hvala na interesovanju za Tekstura Buro ✨ "
        "Tvoja prijava je primljena, a naš menadžer će te kontaktirati između 8:00 i 20:00. "
        "Ako ti odgovara određeno vreme za poziv, ili želiš da nastavimo komunikaciju "
        "putem poruka, samo nam napiši — prilagodićemo se 💬"
    ),
    (
        "Zdravo, {name}! 💛 "
        "Hvala ti na interesovanju za Tekstura Buro. "
        "Tvoja prijava je primljena — naš menadžer će te kontaktirati između 8:00 i 20:00. "
        "Ako ti više odgovara određeno vreme za poziv ili komunikacija putem poruka, "
        "slobodno nam piši. "
        "Takođe, možeš nam odmah poslati mere, fotografije prostora ili render/primer "
        "kuhinje koja ti se dopada — rado ćemo pogledati 📐"
    ),
    (
        "Dobar dan, {name}! "
        "Tim Tekstura Buro je primio tvoju prijavu 🤍 "
        "Naš menadžer će te kontaktirati između 8:00 i 20:00. "
        "Ako ti više odgovara neko drugo vreme ili komunikacija putem poruka, "
        "možemo nastaviti ovde 💬 "
        "Fotografije prostora, mere ili primer kuhinje koja ti se dopada "
        "slobodno možeš poslati odmah."
    ),
    (
        "Zdravo, {name}! 🌿 "
        "Tvoja prijava je primljena i već je kod našeg tima. "
        "Menadžer će te kontaktirati između 8:00 i 20:00, "
        "a ako ti odgovara neko drugo vreme, slobodno nam napiši. "
        "Ako ti je lakše da komuniciramo putem poruka, tu smo 💬 "
        "Mere, fotografije prostora, render ili primer kuhinje koja ti se dopada "
        "možeš poslati odmah — tako možemo brže da krenemo sa pripremom ponude ✨"
    ),
]


def is_working_hours() -> bool:
    """Проверяет, рабочее ли сейчас время (8:00–20:00) для отправки Viber."""
    hour = datetime.now().hour
    return VIBER_WORK_HOURS_START <= hour < VIBER_WORK_HOURS_END


def get_viber_text(name: str) -> str:
    """Выбирает случайный шаблон и подставляет имя клиента."""
    template = random.choice(VIBER_TEMPLATES)
    return template.format(name=name if name else "")


def send_viber_wazzup(phone: str, name: str, lead_id: str) -> str:
    """
    Отправляет сообщение в Viber через WazzUp API.
    Возвращает messageId при успехе, бросает Exception при ошибке.

    crmMessageId = "lead_{lead_id}" — WazzUp игнорирует повторный запрос
    с тем же ID в течение 60 сек, что защищает от случайных дублей.
    """
    chat_id = phone.lstrip("+").replace(" ", "").replace("-", "")

    payload = {
        "channelId":    WAZZUP_VIBER_CHANNEL_ID,
        "chatType":     "viber",
        "chatId":       chat_id,
        "crmMessageId": f"lead_{lead_id}",
        "text":         get_viber_text(name),
    }

    resp = requests.post(
        "https://api.wazzup24.com/v3/message",
        headers={
            "Authorization": f"Bearer {WAZZUP_API_KEY}",
            "Content-Type":  "application/json",
        },
        json=payload,
        timeout=10,
    )

    data = resp.json()
    if resp.status_code != 201:
        raise Exception(f"WazzUp error {resp.status_code}: {data}")

    return data.get("messageId", "unknown")


def process_viber_queue(viber_queue: list):
    """
    Отправляет Viber-сообщения из очереди с задержкой 30–45 сек между каждым.
    Ошибка отправки одного лида не останавливает очередь — просто логируется.

    viber_queue = [{"lead_id": str, "phone": str, "name": str}, ...]
    """
    if not viber_queue:
        return

    log.info(f"[VIBER] Очередь отправки: {len(viber_queue)} сообщений")

    for idx, item in enumerate(viber_queue):
        try:
            msg_id = send_viber_wazzup(
                phone   = item["phone"],
                name    = item["name"],
                lead_id = str(item["lead_id"]),
            )
            log.info(f"[VIBER] ✅ Отправлено: лид {item['lead_id']}, msgId={msg_id}")
        except Exception as e:
            log.warning(f"[VIBER] ❌ Ошибка лид {item['lead_id']}: {e}")

        # Пауза между сообщениями — случайная, чтобы не выглядело как бот
        if idx < len(viber_queue) - 1:
            delay = random.randint(VIBER_DELAY_MIN, VIBER_DELAY_MAX)
            log.info(f"[VIBER] Пауза {delay} сек...")
            time.sleep(delay)


def process_pending_viber(spreadsheet, tabs_cfg: list):
    """
    Находит строки со статусом VIBER_PENDING:{lead_id} во всех вкладках
    и отправляет им Viber (только в рабочее время).

    Вызывается в самом начале run() — подбирает ночные лиды утром.
    После успешной отправки статус меняется на CREATED:{lead_id}.
    """
    if not is_working_hours():
        log.info("[VIBER] Нерабочее время — отложенные лиды не обрабатываем.")
        return

    pending_queue = []  # [{lead_id, phone, name, sheet, row_index, status_col, lead_id_str}, ...]

    for tab_cfg in tabs_cfg:
        tab_name   = tab_cfg["name"]
        phone_col  = tab_cfg["phone_col"]   # 0-based индекс колонки телефона
        name_col   = tab_cfg["name_col"]    # 0-based индекс колонки имени
        status_col = tab_cfg["status_col"]  # 1-based для gspread.update_cell

        try:
            sheet = spreadsheet.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            continue

        rows = sheet.get(tab_cfg["range"])
        if not rows:
            continue

        for i, row in enumerate(rows):
            sheet_row  = i + 2
            # Дополняем строку если она короче ожидаемой
            while len(row) < status_col:
                row.append("")

            status_val = row[status_col - 1].strip()  # -1 потому что status_col 1-based

            # Ищем статус вида "VIBER_PENDING:1234"
            if not status_val.startswith("VIBER_PENDING:"):
                continue

            lead_id_str = status_val.replace("VIBER_PENDING:", "").strip()
            phone_raw   = row[phone_col].strip() if len(row) > phone_col else ""
            name_raw    = row[name_col].strip()  if len(row) > name_col  else ""

            phone = phone_raw.lstrip("p:+").replace(" ", "").replace("-", "")
            name  = name_raw.split(" ")[0] if name_raw else ""  # только имя (без фамилии)

            if not phone:
                log.warning(f"[VIBER] PENDING row {sheet_row} в '{tab_name}': нет телефона — пропускаем")
                continue

            pending_queue.append({
                "lead_id":   lead_id_str,
                "phone":     phone,
                "name":      name,
                "sheet":     sheet,
                "sheet_row": sheet_row,
                "status_col": status_col,
            })

    if not pending_queue:
        log.info("[VIBER] Отложенных ночных лидов нет.")
        return

    log.info(f"[VIBER] Найдено {len(pending_queue)} отложенных ночных лидов — отправляем")

    for idx, item in enumerate(pending_queue):
        try:
            msg_id = send_viber_wazzup(
                phone   = item["phone"],
                name    = item["name"],
                lead_id = item["lead_id"],
            )
            # Успешно — меняем статус с VIBER_PENDING на CREATED
            item["sheet"].update_cell(
                item["sheet_row"],
                item["status_col"],
                f"CREATED:{item['lead_id']}"
            )
            log.info(f"[VIBER] ✅ Ночной лид {item['lead_id']} отправлен, msgId={msg_id}")
        except Exception as e:
            log.warning(f"[VIBER] ❌ Ошибка ночного лида {item['lead_id']}: {e}")

        if idx < len(pending_queue) - 1:
            delay = random.randint(VIBER_DELAY_MIN, VIBER_DELAY_MAX)
            log.info(f"[VIBER] Пауза {delay} сек...")
            time.sleep(delay)


# ─── Формат статуса в Google Sheets ─────────────────────────────────────────

CREATED_WITH_ID      = re.compile(r"^CREATED:\d+$")
VIBER_PENDING_WITH_ID = re.compile(r"^VIBER_PENDING:\d+$")

def is_our_processed_status(status_val: str) -> bool:
    """
    Возвращает True если статус — наша запись (строку можно пропустить).
    Просто 'CREATED' без ID считаем подозрительным и переобрабатываем.
    """
    s = (status_val or "").strip()
    if not s:
        return False
    if CREATED_WITH_ID.match(s):          # CREATED:1234
        return True
    if VIBER_PENDING_WITH_ID.match(s):    # VIBER_PENDING:1234
        return True
    if s == "DUPLICATE":
        return True
    if s.startswith("ERROR:") or s.startswith("ERROR "):
        return True
    return False

# ─── Маппинги для Kitchen май ───────────────────────────────────────────────

BUDGET_MAP = {
    "do_4.000€":     "4000|EUR",
    "4.000–6.000€":  "6000|EUR",
    "6.000–9.000€":  "9000|EUR",
    "9.000€+":       "10000|EUR",
}

TEHNIKA_MAP = {
    "samo_kuhinja":           0,
    "kuhinja_+_tehnika":      1,
}

UF_BUDGET  = "UF_CRM_1778484294109"
UF_TEHNIKA = "UF_CRM_1778484348218"


def parse_budget(raw: str) -> str | None:
    return BUDGET_MAP.get(raw.strip()) if raw else None


def parse_tehnika(raw: str) -> int | None:
    return TEHNIKA_MAP.get(raw.strip()) if raw else None


# ─── Google Sheets: авторизация ─────────────────────────────────────────────

def get_gspread_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise EnvironmentError(
            "Переменная окружения GOOGLE_CREDENTIALS_JSON не задана. "
            "Добавь её в Railway → Variables."
        )
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

    КРИТИЧНО: Bitrix24 при логических ошибках возвращает HTTP 200 + JSON
    {"error": "...", "error_description": "..."}. Всегда raise при наличии
    error в ответе — иначе скрипт думает что всё ок.
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

    if isinstance(data, dict) and data.get("error"):
        err_code = data.get("error", "")
        err_desc = data.get("error_description", "")
        raise RuntimeError(f"Bitrix API '{method}' error: {err_code} — {err_desc}")

    return data


def is_duplicate(email: str, phone: str) -> bool:
    """
    Поиск дублей через crm.duplicate.findbycomm.

    Структура ответа Bitrix24 непоследовательная:
      Дубли есть: {"result": {"LEAD": [id1, id2]}}    ← dict
      Дублей нет: {"result": []}                       ← list
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
    """Проверяет что лид реально существует в Bitrix24 после создания."""
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
    """

    # ── A. Создаём контакт ──────────────────────────────────────────────────
    contact_fields = {
        "NAME":           name,
        "LAST_NAME":      last_name,
        "ASSIGNED_BY_ID": assigned_by_id,
        "OPENED":         "Y",
    }
    if email:
        contact_fields["EMAIL"] = [{"VALUE": email, "VALUE_TYPE": "WORK"}]
    if phone:
        contact_fields["PHONE"] = [{"VALUE": phone, "VALUE_TYPE": "WORK"}]
        contact_fields["IM"]    = [{"VALUE": phone, "VALUE_TYPE": "VIBER"}]

    r_contact = bitrix_call("crm.contact.add", {"fields": contact_fields})
    contact_id = r_contact.get("result")
    if not contact_id or not isinstance(contact_id, int):
        raise RuntimeError(f"crm.contact.add не вернул валидный ID. Ответ: {r_contact}")
    log.info(f"  Контакт создан: ID={contact_id}")
    time.sleep(BITRIX_DELAY)

    # ── B. Создаём лид ──────────────────────────────────────────────────────
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
        lead_fields["PHONE"]             = [{"VALUE": phone, "VALUE_TYPE": "WORK"}]
        lead_fields["IM"]                = [{"VALUE_TYPE": "VIBER", "VALUE": phone}]
        lead_fields["UF_CRM_VIBER_LINK"] = f"viber://chat?number={phone}"

    if extra_fields:
        for k, v in extra_fields.items():
            if v is not None:
                lead_fields[k] = v

    r_lead = bitrix_call("crm.lead.add", {"fields": lead_fields})
    lead_id = r_lead.get("result")
    if not lead_id or not isinstance(lead_id, int):
        raise RuntimeError(f"crm.lead.add не вернул валидный ID. Ответ: {r_lead}")
    log.info(f"  Лид создан: ID={lead_id}")
    time.sleep(BITRIX_DELAY)

    # ── C. ВЕРИФИКАЦИЯ ──────────────────────────────────────────────────────
    if not verify_lead_exists(lead_id):
        raise RuntimeError(
            f"crm.lead.add вернул ID={lead_id}, но crm.lead.get не подтверждает "
            f"существование. Контакт {contact_id} остался осиротевшим."
        )
    log.info(f"  Лид {lead_id} верифицирован ✓")
    time.sleep(BITRIX_DELAY)

    return lead_id


# ─── Вспомогательные функции парсинга ───────────────────────────────────────

UNICODE_JUNK = re.compile(r"[^\x20-\x7E\u00C0-\u024F\u0400-\u04FF]")


def clean_name(raw: str) -> tuple[str, str]:
    cleaned = UNICODE_JUNK.sub("", raw).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if not cleaned:
        cleaned = raw.strip()
    parts     = cleaned.split(" ", 1)
    name      = parts[0]
    last_name = parts[1] if len(parts) > 1 else "—"
    return name, last_name


def parse_phone(raw: str) -> str | None:
    phone = raw.replace("p:", "").strip()
    return None if (phone in ("", "0")) else phone


def get_assigned_by_id() -> int:
    weekday = datetime.now(timezone.utc).weekday()
    return 28 if weekday < 5 else 30


def get_source(platform: str) -> str:
    return SOURCE_MAP.get(platform.lower(), "UC_EXNWD9")


def platform_label(platform: str) -> str:
    return "Instagram" if platform.lower() == "ig" else "Facebook"


# ─── Обработка вкладки Kitchen New ─────────────────────────────────────────

def process_kitchen_row(row: list, row_index: int, sheet, assigned_by_id: int) -> str:
    while len(row) < 18:
        row.append("")

    status_val = row[17].strip()
    if is_our_processed_status(status_val):
        return "SKIP"

    email     = row[14].strip()
    raw_name  = row[15].strip()
    raw_phone = row[16].strip()
    platform  = row[11].strip()

    ad       = row[3].strip()
    adset    = row[5].strip()
    size     = row[12].strip()
    timeline = row[13].strip()
    date     = row[1].strip()

    phone = parse_phone(raw_phone)

    if is_duplicate(email, phone):
        return "DUPLICATE"

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
    return f"CREATED:{lead_id}"


# ─── Обработка вкладки Ormari ───────────────────────────────────────────────

def process_ormari_row(row: list, row_index: int, sheet, assigned_by_id: int) -> str:
    while len(row) < 19:
        row.append("")

    status_val = row[18].strip()
    if is_our_processed_status(status_val):
        return "SKIP"

    email     = row[15].strip()
    raw_name  = row[16].strip()
    raw_phone = row[17].strip()
    platform  = row[11].strip()

    ad       = row[3].strip()
    adset    = row[5].strip()
    wardrobe = row[12].strip()
    size     = row[13].strip()
    timeline = row[14].strip()
    date     = row[1].strip()

    phone = parse_phone(raw_phone)

    if is_duplicate(email, phone):
        return "DUPLICATE"

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
    return f"CREATED:{lead_id}"


# ─── Обработка вкладки Kitchen май ─────────────────────────────────────────

def process_kitchen_may_row(row: list, row_index: int, sheet, assigned_by_id: int) -> str:
    while len(row) < 20:
        row.append("")

    status_val = row[19].strip()
    if is_our_processed_status(status_val):
        return "SKIP"

    email     = row[16].strip()
    raw_name  = row[17].strip()
    raw_phone = row[18].strip()
    platform  = row[11].strip()

    ad          = row[3].strip()
    adset       = row[5].strip()
    has_plan    = row[12].strip()
    timeline    = row[13].strip()
    budget_raw  = row[14].strip()
    tehnika_raw = row[15].strip()
    date        = row[1].strip()

    phone = parse_phone(raw_phone)

    if is_duplicate(email, phone):
        return "DUPLICATE"

    name, last_name = clean_name(raw_name)
    plat_label = platform_label(platform)
    src_id     = get_source(platform)

    title = (
        f"{name} {last_name} — Kitchen ({plat_label})"
        if last_name != "—"
        else f"{name} — Kitchen ({plat_label})"
    )
    comment = (
        f"Tab: kitchen Май | Ad: {ad} | Plan: {has_plan} | Timeline: {timeline} | "
        f"Budget: {budget_raw} | Tehnika: {tehnika_raw} | Adset: {adset} | Date: {date}"
    )

    extra_fields = {}
    budget_value = parse_budget(budget_raw)
    if budget_value is not None:
        extra_fields[UF_BUDGET] = budget_value

    tehnika_value = parse_tehnika(tehnika_raw)
    if tehnika_value is not None:
        extra_fields[UF_TEHNIKA] = tehnika_value

    log.info(f"  → Создаю kitchen Май лид: {title} | budget={budget_value} tehnika={tehnika_value}")
    lead_id = create_bitrix_lead(
        title, name, last_name, phone, email, src_id, comment, assigned_by_id,
        extra_fields=extra_fields,
    )
    log.info(f"  ✓ kitchen Май → лид создан ID={lead_id}: {title}")
    return f"CREATED:{lead_id}"


# ─── Главная функция ─────────────────────────────────────────────────────────

def run():
    log.info("=" * 60)
    log.info("Запуск импорта лидов")

    assigned_by_id = get_assigned_by_id()
    weekday_name   = datetime.now(timezone.utc).strftime("%A")
    log.info(f"День: {weekday_name}, ASSIGNED_BY_ID={assigned_by_id}")
    log.info(f"Рабочее время для Viber: {'ДА' if is_working_hours() else 'НЕТ (ночной режим)'}")

    gc          = get_gspread_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    # ── Конфигурация вкладок ─────────────────────────────────────────────
    # phone_col и name_col — 0-based индексы для извлечения данных при
    # добавлении в viber_queue и обработке VIBER_PENDING строк
    tabs = [
        {
            "name":       "Kitchen New",
            "range":      "A2:R500",
            "status_col": 18,   # колонка R (1-based для gspread)
            "processor":  process_kitchen_row,
            "phone_col":  16,   # колонка Q (0-based)
            "name_col":   15,   # колонка P (0-based)
        },
        {
            "name":       "Ormari",
            "range":      "A2:S500",
            "status_col": 19,   # колонка S
            "processor":  process_ormari_row,
            "phone_col":  17,   # колонка R (0-based)
            "name_col":   16,   # колонка Q (0-based)
        },
        {
            "name":       "kitchen Май",
            "range":      "A2:T500",
            "status_col": 20,   # колонка T
            "processor":  process_kitchen_may_row,
            "phone_col":  18,   # колонка S (0-based)
            "name_col":   17,   # колонка R (0-based)
        },
    ]

    # ── ШАГ 1: обработать ночные лиды (VIBER_PENDING) ────────────────────
    # Этот блок ищет лиды созданные ночью и шлёт им Viber утром.
    # В нерабочее время молча возвращается без действий.
    process_pending_viber(spreadsheet, tabs)

    # ── ШАГ 2: обработать новые строки из таблицы ────────────────────────
    total_created   = 0
    total_duplicate = 0
    total_error     = 0

    # Очередь Viber для лидов созданных в этом запуске (рабочее время)
    viber_queue = []

    for tab_cfg in tabs:
        tab_name   = tab_cfg["name"]
        status_col = tab_cfg["status_col"]
        processor  = tab_cfg["processor"]
        phone_col  = tab_cfg["phone_col"]
        name_col   = tab_cfg["name_col"]

        log.info(f"\n── Обработка вкладки: {tab_name} ──")

        try:
            sheet = spreadsheet.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            log.warning(f"  Вкладка '{tab_name}' не найдена — пропускаем.")
            continue

        rows = sheet.get(tab_cfg["range"])

        if not rows:
            log.info(f"  Вкладка {tab_name}: данных нет, пропускаем.")
            continue

        log.info(f"  Прочитано строк: {len(rows)}")

        for i, row in enumerate(rows):
            sheet_row = i + 2

            try:
                status = processor(row, sheet_row, sheet, assigned_by_id)

                if status == "SKIP":
                    continue

                if status.startswith("CREATED:"):
                    # Лид создан — решаем когда слать Viber
                    lead_id   = status.split(":")[1]
                    phone_raw = row[phone_col].strip() if len(row) > phone_col else ""
                    name_raw  = row[name_col].strip()  if len(row) > name_col  else ""

                    # clean_name убирает Unicode-мусор (𝓜, 𝗞 и т.п.) и нормализует пробелы,
                    # затем берём только первое слово — имя без фамилии для обращения в Viber
                    name, _ = clean_name(name_raw) if name_raw else ("", "")
                    phone = phone_raw.lstrip("p:+").replace(" ", "").replace("-", "")

                    if is_working_hours() and phone:
                        # Рабочее время — добавляем в очередь немедленной отправки
                        viber_queue.append({
                            "lead_id": lead_id,
                            "phone":   phone,
                            "name":    name,
                        })
                        sheet.update_cell(sheet_row, status_col, status)
                    elif phone:
                        # Ночь — откладываем до утра, пишем VIBER_PENDING
                        sheet.update_cell(sheet_row, status_col, f"VIBER_PENDING:{lead_id}")
                        log.info(f"  [VIBER] Ночной лид {lead_id} → VIBER_PENDING (отправим утром)")
                    else:
                        # Нет телефона — Viber невозможен, пишем обычный CREATED
                        sheet.update_cell(sheet_row, status_col, status)
                        log.warning(f"  [VIBER] Лид {lead_id} без телефона — Viber пропущен")

                    total_created += 1

                elif status == "DUPLICATE":
                    sheet.update_cell(sheet_row, status_col, status)
                    total_duplicate += 1

                else:
                    # Любой другой статус (например ERROR) — пишем как есть
                    sheet.update_cell(sheet_row, status_col, status)

            except Exception as e:
                err_msg = f"ERROR: {str(e)[:80]}"
                log.error(f"  Строка {sheet_row}: {err_msg}")
                try:
                    sheet.update_cell(sheet_row, status_col, err_msg)
                except Exception:
                    pass
                total_error += 1

            time.sleep(0.1)

    # ── ШАГ 3: отправить Viber очередь с задержкой 30–45 сек ─────────────
    # Viber шлём ПОСЛЕ того как все лиды записаны в Bitrix —
    # чтобы ошибка в очереди не мешала основному импорту.
    process_viber_queue(viber_queue)

    # ── Итоговый отчёт ───────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("ИТОГ:")
    log.info(f"  Создано лидов:    {total_created}")
    log.info(f"  Дублей пропущено: {total_duplicate}")
    log.info(f"  Ошибок:           {total_error}")
    log.info(f"  Viber отправлено: {len(viber_queue)}")
    log.info("=" * 60)


# ─── Точка входа ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run()
