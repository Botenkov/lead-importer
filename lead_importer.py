"""
lead_importer.py
────────────────────────────────────────────────────────────────────────────
Импорт лидов из Google Sheets → Bitrix24.

Читает вкладки "Kitchen New" и "Ormari", проверяет дубли через Bitrix24 API,
создаёт новые лиды и отмечает каждую строку в таблице:
  CREATED   — лид успешно создан
  DUPLICATE — уже существует в Bitrix24
  ERROR: …  — что-то пошло не так (текст ошибки)

После создания лида — назначает оператора в ChatApp по маппингу Bitrix ID.

Переменные окружения (Railway / .env):
  GOOGLE_CREDENTIALS_JSON  — JSON сервисного аккаунта Google (одной строкой)
  BITRIX_WEBHOOK           — URL вебхука Bitrix24
  CHATAPP_EMAIL            — email от кабинета ChatApp
  CHATAPP_PASSWORD         — пароль от кабинета ChatApp
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
BITRIX_DELAY = 0.3  # секунды

# SOURCE_ID по платформе
SOURCE_MAP = {
    "ig": "UC_M3JOUQ",  # Instagram
    "fb": "UC_EXNWD9",  # Facebook
}

LEAD_STATUS_ID = "UC_6TAZVN"

# ─── ChatApp конфигурация ────────────────────────────────────────────────────

CHATAPP_EMAIL      = os.environ.get("CHATAPP_EMAIL", "project@teksturaburo.com")
CHATAPP_PASSWORD   = os.environ.get("CHATAPP_PASSWORD", "3A8CMJU5Jmx!PPd")
CHATAPP_APP_ID     = "app_92851_1"
CHATAPP_LICENSE_ID = 76410
CHATAPP_MESSENGER  = "grWhatsApp"
CHATAPP_API        = "https://api.chatapp.online/v1"

# Маппинг: Bitrix ASSIGNED_BY_ID → ChatApp operator ID
# Dmitry Botenkov (ID 1) исключён
CHATAPP_OPERATOR_MAP = {
    28: 94104,  # Djordje Tomic
    30: 93980,  # Dmitrii Piskun
}

# Кэш токена — получаем один раз за запуск
_chatapp_token: str | None = None


def get_chatapp_token() -> str | None:
    """
    Получает accessToken от ChatApp API.
    Кэширует на время работы скрипта.
    """
    global _chatapp_token
    if _chatapp_token:
        return _chatapp_token
    try:
        r = requests.post(
            f"{CHATAPP_API}/tokens",
            json={
                "email":    CHATAPP_EMAIL,
                "password": CHATAPP_PASSWORD,
                "appId":    CHATAPP_APP_ID,
            },
            timeout=10,
        )
        r.raise_for_status()
        token = r.json().get("data", {}).get("accessToken")
        if token:
            _chatapp_token = token
            log.info("ChatApp: accessToken получен успешно")
        else:
            log.warning(f"ChatApp: accessToken не найден в ответе: {r.json()}")
        return _chatapp_token
    except Exception as e:
        log.warning(f"ChatApp: ошибка получения токена: {e}")
        return None


def assign_chatapp_operator(phone: str, bitrix_assigned_id: int) -> bool:
    """
    Назначает оператора в ChatApp для чата по номеру телефона.
    Возвращает True при успехе, False при ошибке.
    """
    operator_id = CHATAPP_OPERATOR_MAP.get(bitrix_assigned_id)
    if not operator_id:
        log.info(f"ChatApp: нет маппинга для Bitrix ID={bitrix_assigned_id}, пропускаем")
        return False

    if not phone:
        log.info("ChatApp: телефон не указан, пропускаем назначение оператора")
        return False

    token = get_chatapp_token()
    if not token:
        return False

    # chatId = номер телефона (только цифры)
    chat_id = re.sub(r"\D", "", phone)

    try:
        r = requests.put(
            f"{CHATAPP_API}/licenses/{CHATAPP_LICENSE_ID}"
            f"/messengers/{CHATAPP_MESSENGER}"
            f"/chats/{chat_id}/operator",
            headers={"Authorization": token},
            json={"operatorId": operator_id},
            timeout=10,
        )
        r.raise_for_status()
        log.info(
            f"ChatApp: оператор {operator_id} назначен для чата {chat_id} "
            f"(Bitrix ID={bitrix_assigned_id})"
        )
        return True
    except Exception as e:
        log.warning(f"ChatApp: ошибка назначения оператора для {chat_id}: {e}")
        return False


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
    return response.json()


def find_lead_by_email(email: str) -> bool:
    """Возвращает True, если в Bitrix24 уже есть лид с таким email."""
    if not email:
        return False
    result = bitrix_call("crm.lead.list", {"filter": {"EMAIL": email}, "select": ["ID"]})
    time.sleep(BITRIX_DELAY)
    return result.get("total", 0) > 0


def find_lead_by_phone(phone: str) -> bool:
    """Возвращает True, если в Bitrix24 уже есть лид с таким телефоном."""
    if not phone:
        return False
    result = bitrix_call("crm.lead.list", {"filter": {"PHONE": phone}, "select": ["ID"]})
    time.sleep(BITRIX_DELAY)
    return result.get("total", 0) > 0


def create_bitrix_lead(title, name, last_name, phone, email, source_id, comment, assigned_by_id):
    """
    Создаёт контакт + лид в Bitrix24, связывает их и добавляет Viber-ссылку.
    После создания — назначает оператора в ChatApp.
    Возвращает lead_id или бросает исключение при ошибке.

    Порядок: A → B → C → D → E
    """

    # A: создаём контакт
    contact_fields = {"NAME": name, "LAST_NAME": last_name}
    if email:
        contact_fields["EMAIL"] = [{"VALUE": email, "VALUE_TYPE": "WORK"}]
    if phone:
        contact_fields["PHONE"] = [{"VALUE": phone, "VALUE_TYPE": "WORK"}]

    r_contact = bitrix_call("crm.contact.add", {"fields": contact_fields})
    contact_id = r_contact.get("result")
    if not contact_id:
        raise RuntimeError(f"crm.contact.add вернул пустой result: {r_contact}")
    time.sleep(BITRIX_DELAY)

    # B: создаём лид
    lead_fields = {
        "TITLE":          title,
        "NAME":           name,
        "LAST_NAME":      last_name,
        "SOURCE_ID":      source_id,
        "STATUS_ID":      LEAD_STATUS_ID,
        "ASSIGNED_BY_ID": assigned_by_id,
        "COMMENTS":       comment,
    }
    if email:
        lead_fields["EMAIL"] = [{"VALUE": email, "VALUE_TYPE": "WORK"}]
    if phone:
        lead_fields["PHONE"] = [{"VALUE": phone, "VALUE_TYPE": "WORK"}]
        lead_fields["IM"]    = [{"VALUE_TYPE": "VIBER", "VALUE": phone}]

    r_lead = bitrix_call("crm.lead.add", {"fields": lead_fields})
    lead_id = r_lead.get("result")
    if not lead_id:
        raise RuntimeError(f"crm.lead.add вернул пустой result: {r_lead}")
    time.sleep(BITRIX_DELAY)

    # C: привязываем контакт к лиду (обязательно отдельным вызовом!)
    bitrix_call("crm.lead.update", {"id": lead_id, "fields": {"CONTACT_ID": contact_id}})
    time.sleep(BITRIX_DELAY)

    # D: добавляем Viber-ссылку
    if phone:
        bitrix_call("crm.lead.update", {
            "id":     lead_id,
            "fields": {"UF_CRM_VIBER_LINK": f"viber://chat?number={phone}"},
        })
        time.sleep(BITRIX_DELAY)

    # E: назначаем оператора в ChatApp
    if phone:
        assign_chatapp_operator(phone, assigned_by_id)

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

    # Дедупликация
    if find_lead_by_email(email):
        return "DUPLICATE"
    if phone and find_lead_by_phone(phone):
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

    lead_id = create_bitrix_lead(title, name, last_name, phone, email, src_id, comment, assigned_by_id)
    log.info(f"  Kitchen New → лид создан ID={lead_id}: {title}")
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
    if find_lead_by_email(email):
        return "DUPLICATE"
    if phone and find_lead_by_phone(phone):
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

    lead_id = create_bitrix_lead(title, name, last_name, phone, email, src_id, comment, assigned_by_id)
    log.info(f"  Ormari → лид создан ID={lead_id}: {title}")
    return "CREATED"


# ─── Главная функция ─────────────────────────────────────────────────────────

def run():
    log.info("=" * 60)
    log.info("Запуск импорта лидов")

    # Прогреваем ChatApp токен заранее
    get_chatapp_token()

    # Определяем ответственного
    assigned_by_id = get_assigned_by_id()
    weekday_name = datetime.now(timezone.utc).strftime("%A")
    log.info(f"День: {weekday_name}, ASSIGNED_BY_ID={assigned_by_id}")
    log.info(f"ChatApp operator: {CHATAPP_OPERATOR_MAP.get(assigned_by_id, 'нет маппинга')}")

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
    ]

    for tab_cfg in tabs:
        tab_name   = tab_cfg["name"]
        status_col = tab_cfg["status_col"]
        processor  = tab_cfg["processor"]

        log.info(f"\n── Обработка вкладки: {tab_name} ──")

        # Открываем лист
        sheet = spreadsheet.worksheet(tab_name)
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
