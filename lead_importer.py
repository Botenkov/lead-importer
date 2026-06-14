"""
lead_importer.py
────────────────────────────────────────────────────────────────────────────
Импорт лидов из Google Sheets → Bitrix24.

Читает вкладки "Kitchen New", "Ormari", "Kitchen май" и "Kitchen MAY-copy",
проверяет дубли через Bitrix24 API, создаёт новые лиды и отмечает каждую
строку в таблице:
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
from datetime import datetime, timezone, timedelta
from functools import partial
from zoneinfo import ZoneInfo

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

LEAD_STATUS_ID = "UC_SRP1D8"   # «New sale AI» — лиды ведёт Milica (бот 2296, worker_milica)
MILICA_ID = 2296

# WEB-лиды (с сайта) создаются прямо в Bitrix на человеке (resp 1). Забираем свежие на Milica + Viber-
# welcome, как лид-форму (решение Дмитрия 14.06). Источник ШУМНЫЙ (спам/мусор) — гард ниже. 0/false = выкл.
WEB_LEADS_ENABLED = os.environ.get("MILICA_WEB_LEADS_ENABLED", "true").lower() == "true"
WEB_LEADS_MAX_AGE_DAYS = int(os.environ.get("MILICA_WEB_LEADS_MAX_AGE_DAYS", "2"))  # старее — не приветствуем
_WEB_JUNK_NAMES = {"", "lead", "guest", "none", "test", "klijent", "klijenti"}

# Возврат известного клиента: Bitrix при возврате клиента (старая сделка закрыта/новый канал) плодит
# НОВЫЙ лид, хотя у контакта уже есть сделка. Это НЕ дубль-баг, а повторное обращение. Раньше парковали
# статусом «Повторный/дубль» — но обращение повисало без хозяина (все новые лиды идут на Milica).
# Решение Дмитрия 14.06: ВОЗВРАТ ОТДАЁМ ПРОШЛОМУ МЕНЕДЖЕРУ — лид+контакт на ответственного его последней
# сделки + стадия-колонка этого менеджера + дело «ответить», чтобы ничего не терялось. Milica при этом
# сама отходит (poller._owned_by_human: у контакта есть сделка человека → не лезет). 0/false = выкл.
DUP_CLEANUP_ENABLED = os.environ.get("MILICA_DUP_CLEANUP_ENABLED", "true").lower() == "true"
DUP_LEAD_STATUS = os.environ.get("MILICA_DUP_LEAD_STATUS", "11")        # «Повторный/дубль» (парковка)
DUP_CLEANUP_MAX_AGE_DAYS = int(os.environ.get("MILICA_DUP_MAX_AGE_DAYS", "3"))
MILICA_BOT_ID = 2296

def _parse_stage_map(raw: str) -> dict:
    out = {}
    for pair in (raw or "").split(","):
        pair = pair.strip()
        if ":" in pair:
            k, v = pair.split(":", 1)
            if k.strip().isdigit() and v.strip():
                out[int(k.strip())] = v.strip()
    return out

RETURN_ROUTING_ENABLED = os.environ.get("MILICA_RETURN_ROUTING_ENABLED", "true").lower() == "true"
# user_id прошлого менеджера → стадия-колонка лида («sale manager id N»). Дмитрий завёл стадии 14.06.
RETURN_STAGE_MAP = _parse_stage_map(
    os.environ.get("MILICA_RETURN_STAGE_MAP", "30:UC_98EDOH,28:UC_WLE3S8"))

# Email-лиды: входящее письмо на наш ящик → распределить лид в стадию-колонку этого ящика (Дмитрий 14.06).
# Ящик-получатель берём из bound email-активити (SETTINGS.EMAIL_META.__email). Только свежие NEW; стадия
# (ответственного/диалог не трогаем — email не Milica-канал). 0/false = выкл.
EMAIL_ROUTING_ENABLED = os.environ.get("MILICA_EMAIL_ROUTING_ENABLED", "true").lower() == "true"

def _parse_email_map(raw: str) -> dict:
    out = {}
    for pair in (raw or "").split(","):
        pair = pair.strip()
        if ":" in pair:
            k, v = pair.split(":", 1)
            if k.strip() and v.strip():
                out[k.strip().lower()] = v.strip()
    return out

EMAIL_STAGE_MAP = _parse_email_map(os.environ.get(
    "MILICA_EMAIL_STAGE_MAP", "sales@teksturaburo.com:UC_CYNTHP,custom@teksturaburo.com:UC_O9S4TO"))


def _looks_real_phone(raw: str) -> bool:
    """Гард против мусора WEB-источника: настоящий номер = код страны + ≥11 цифр (напр. +381601682333).
    Локальные/спам-номера вроде 8054002077 (10 цифр, без кода) отсекаются → таким лидом займётся человек."""
    p = re.sub(r"[ \-()]", "", (raw or "").replace("p:", "").strip())
    digits = re.sub(r"\D", "", p)
    if len(digits) < 11:
        return False
    return p.startswith("+") or digits.startswith("381") or digits.startswith("00381")


# ─── Viber / WazzUp ─────────────────────────────────────────────────────────

WAZZUP_API_KEY          = os.environ.get("WAZZUP_API_KEY", "")
WAZZUP_VIBER_CHANNEL_ID = "f0911dd4-a6b5-48ad-b39f-f9b47c171277"

# Задержка между Viber-сообщениями — случайная: и «по-человечески», и анти-бан Wazzup/Viber.
# 3–5 мин (решение Дмитрия 14.06): утренний батч ночных лидов в 8:00 нельзя слать пачкой — Viber
# банит за всплеск. Применяется к ОБОИМ циклам отправки (дневная очередь и утренний разбор pending).
VIBER_DELAY_MIN        = int(os.getenv("VIBER_DELAY_MIN", "180"))   # секунды (3 мин)
VIBER_DELAY_MAX        = int(os.getenv("VIBER_DELAY_MAX", "300"))   # секунды (5 мин)

# Часовой пояс Сербии (Railway работает в UTC — без явного TZ будет неверное время)
BELGRADE_TZ = ZoneInfo("Europe/Belgrade")

# Рабочие часы для отправки Viber (ночью не беспокоим клиентов)
VIBER_WORK_HOURS_START = 8
VIBER_WORK_HOURS_END   = 20

# ─── Распределение ответственного ───────────────────────────────────────────
# GO-LIVE Milica (12.06.2026): ВСЕ новые лиды — на Milica (бот 2296), она ведёт их
# в открытых линиях (worker_milica). Старое правило «Djordje 28 будни / Piskun 30
# выходные по времени отправки Viber» отключено приравниванием констант (функция
# get_responsible() осталась — откат = вернуть 28/30 в две строки ниже).
RESPONSIBLE_WEEKDAY = 2296   # Milica (была 28 — Djordje Tomic)
RESPONSIBLE_WEEKEND = 2296   # Milica (была 30 — Dmitrii Piskun)
FRIDAY_CUTOFF_HOUR  = 17

# Шаблоны сообщений (сербский язык, утверждены носителем, 5 вариантов)
# {name} заменяется на имя клиента при отправке
# Общие welcome (шкафы и прочая мебель; кухни — WELCOME_KITCHEN ниже).
# Ви-регистр (персирање) как у кухонных — Milica продолжает диалог на Ви, welcome обязан совпадать.
# Старые ty-шаблоны («Tvoja prijava… menadžer će te kontaktirati») убраны 12.06: «ты» + обещание
# звонка против нового флоу (упор на переписку, ведёт Milica) + упоминали кухню для шкафных лидов.
VIBER_TEMPLATES = [
    (
        "Zdravo, {name}! Ja sam Milica iz TeksturaBuro, hvala vam na upitu! "
        "Sa zadovoljstvom ću vam pomoći oko nameštaja po meri. Treba mi samo par detalja "
        "pa da vam pripremim okvirnu cenu. Mere, fotografije prostora ili primer koji vam se "
        "dopada možete poslati odmah. Predlažem da se dopisujemo ovde, a ako vam je draže "
        "da se čujemo telefonom, tu sam i za to."
    ),
    (
        "Pozdrav, {name}! Ja sam Milica iz TeksturaBuro. Hvala vam na interesovanju! "
        "Rado bih vam pomogla da dođemo do okvirne cene — postaviću par pitanja, neće "
        "oduzeti mnogo vremena. Ako imate mere, fotografije prostora ili primer koji vam se "
        "dopada, slobodno pošaljite odmah. Predlažem da se dopisujemo ovde, a ako biste "
        "želeli da se čujemo, samo recite — biće mi drago."
    ),
]


def is_working_hours() -> bool:
    """Проверяет рабочее ли время (8:00–20:00) по белградскому времени."""
    hour = datetime.now(BELGRADE_TZ).hour
    return VIBER_WORK_HOURS_START <= hour < VIBER_WORK_HOURS_END


# ─── Сегментированные приветствия Milica (КУХНИ) ────────────────────────────
# Ви-регистр (персирање), экавица, женский род. Сегментация по полю Tehnika из формы
# (есть только во вкладке «kitchen Май»; Kitchen New без Tehnika → ветка C). Ротация 1/2:
# вариант 1 — «hvala vam na upitu», вариант 2 — «hvala vam na interesovanju».
# Тексты валидированы носителем (сербский ресёрч 11.06). Шкафы (Ormari) — на общих VIBER_TEMPLATES.
WELCOME_KITCHEN = {
    "A": [  # samo_kuhinja — только кухня
        "Zdravo, {name}! Ja sam Milica iz TeksturaBuro, hvala vam na upitu! Sa zadovoljstvom ću vam pomoći oko kuhinje. Treba mi samo par detalja pa da vam pripremim okvirnu cenu. Predlažem da se dopisujemo ovde, a ako vam je draže da se čujemo telefonom, tu sam i za to.",
        "Pozdrav, {name}! Ja sam Milica iz TeksturaBuro. Hvala vam na interesovanju! Rado bih vam pomogla da dođemo do okvirne cene za vašu kuhinju — postaviću par pitanja, neće oduzeti mnogo vremena. Predlažem da se dopisujemo ovde, a ako biste želeli da se čujemo telefonom, samo recite — biće mi drago.",
    ],
    "B": [  # kuhinja_+_tehnika — кухня и техника
        "Zdravo, {name}! Ja sam Milica iz TeksturaBuro, hvala vam na upitu! Rado ću vam pomoći oko kuhinje, a po želji i oko bele tehnike, da sve bude u jednoj ponudi pa da imate potpunu sliku. Treba mi samo par detalja o kuhinji za okvirnu cenu. Što se bele tehnike tiče — mogu da vas posavetujem oko svakog tehničkog pitanja. Predlažem da se dopisujemo ovde, a ako vam je draže telefonom, tu sam i za to.",
        "Pozdrav, {name}! Ja sam Milica iz TeksturaBuro. Hvala vam na interesovanju! Sa zadovoljstvom ću vam pomoći i oko kuhinje i oko bele tehnike — mogu sve da objedinim u jednu ponudu, da odmah imate potpunu cenu. Par detalja o kuhinji — neće oduzeti mnogo vremena, a oko bele tehnike mogu da vas posavetujem oko svakog tehničkog pitanja. Predlažem da se dopisujemo ovde, a ako vam više odgovara da se čujemo, samo recite.",
    ],
    "C": [  # još_nisam_siguran / пусто / Kitchen New без поля Tehnika
        "Zdravo, {name}! Ja sam Milica iz TeksturaBuro, hvala vam na upitu! Sa zadovoljstvom ću vam pomoći oko kuhinje. Par detalja — pa da vam pripremim okvirnu cenu. Predlažem da se dopisujemo ovde, a ako vam je draže da se čujemo, tu sam i za to.",
        "Pozdrav, {name}! Ja sam Milica iz TeksturaBuro. Hvala vam na interesovanju! Rado bih vam pomogla da dođemo do okvirne cene za kuhinju — postaviću par pitanja, neće oduzeti mnogo vremena. Predlažem da se dopisujemo ovde, a ako biste želeli da se čujemo, samo recite — biće mi drago.",
    ],
}
WELCOME_NO_RUSH = "Bez žurbe — pripremiću vam ponudu da bude spremna za trenutak kada vam bude odgovaralo."
# Срок «не горит» → добавляем строку без спешки. TODO: сверить реальные значения Timeline в форме.
_NO_RUSH_RE = re.compile(r"kasnij|naredn|2\s*[–-]?\s*3|nekoliko\s*mesec", re.I)


def _kitchen_branch(tehnika_raw: str) -> str:
    t = (tehnika_raw or "").strip().lower()
    if t == "samo_kuhinja":
        return "A"
    if t == "kuhinja_+_tehnika":
        return "B"
    return "C"  # još_nisam_siguran / пусто / нет поля Tehnika


def pick_welcome(name: str, product: str = "kitchen", tehnika_raw: str = "", timeline: str = "") -> str:
    """Выбрать приветствие: кухни — сегментировано по Tehnika + ротация + строка «без спешки»;
    шкафы (wardrobe) — пока на общих VIBER_TEMPLATES (кухонные тексты не подходят)."""
    name = name or ""
    if product == "wardrobe":
        return random.choice(VIBER_TEMPLATES).format(name=name)
    text = random.choice(WELCOME_KITCHEN[_kitchen_branch(tehnika_raw)])
    if timeline and _NO_RUSH_RE.search(timeline):
        text = text + " " + WELCOME_NO_RUSH
    return text.format(name=name)


def get_viber_text(name: str, product: str = "kitchen", tehnika_raw: str = "", timeline: str = "") -> str:
    """Текст приветствия для клиента (по умолчанию — кухня, ветка C)."""
    return pick_welcome(name, product, tehnika_raw, timeline)


def send_viber_wazzup(phone: str, name: str, lead_id: str,
                      product: str = "kitchen", tehnika_raw: str = "", timeline: str = "") -> str:
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
        "text":         get_viber_text(name, product, tehnika_raw, timeline),
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
                phone       = item["phone"],
                name        = item["name"],
                lead_id     = str(item["lead_id"]),
                product     = item.get("product", "kitchen"),
                tehnika_raw = item.get("tehnika_raw", ""),
                timeline    = item.get("timeline", ""),
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
        product      = tab_cfg.get("product", "kitchen")     # kitchen | wardrobe (для выбора приветствия)
        timeline_col = tab_cfg.get("timeline_col")           # 0-based; для строки «без спешки»
        tehnika_col  = tab_cfg.get("tehnika_col")            # 0-based; только «kitchen Май»

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

            timeline_v = row[timeline_col].strip() if timeline_col is not None and len(row) > timeline_col else ""
            tehnika_v  = row[tehnika_col].strip()  if tehnika_col  is not None and len(row) > tehnika_col  else ""
            pending_queue.append({
                "lead_id":   lead_id_str,
                "phone":     phone,
                "name":      name,
                "sheet":     sheet,
                "sheet_row": sheet_row,
                "status_col": status_col,
                "product":     product,
                "tehnika_raw": tehnika_v,
                "timeline":    timeline_v,
            })

    if not pending_queue:
        log.info("[VIBER] Отложенных ночных лидов нет.")
        return

    log.info(f"[VIBER] Найдено {len(pending_queue)} отложенных ночных лидов — отправляем")

    for idx, item in enumerate(pending_queue):
        # АНТИ-ДУБЛЬ при перекрытии прогонов: батч с паузой 3–5 мин может длиться дольше интервала
        # крона → второй инстанс стартует и берёт ещё не отправленные лиды из своего снимка. Перечитываем
        # статус строки ПЕРЕД отправкой — если другой прогон уже отправил (CREATED), пропускаем (без
        # повторного welcome клиенту). Сбой чтения не блокирует отправку (деградируем безопасно).
        try:
            cur_status = (item["sheet"].cell(item["sheet_row"], item["status_col"]).value or "").strip()
        except Exception as e:
            cur_status = f"VIBER_PENDING:{item['lead_id']}"
            log.warning(f"[VIBER] не перечитал статус лида {item['lead_id']} ({e}) — отправляю")
        if not cur_status.startswith("VIBER_PENDING:"):
            log.info(f"[VIBER] Ночной лид {item['lead_id']} уже обработан ({cur_status}) — "
                     f"пропускаю (перекрытие прогонов)")
            continue
        try:
            msg_id = send_viber_wazzup(
                phone       = item["phone"],
                name        = item["name"],
                lead_id     = item["lead_id"],
                product     = item.get("product", "kitchen"),
                tehnika_raw = item.get("tehnika_raw", ""),
                timeline    = item.get("timeline", ""),
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


# ─── Расчёт ответственного ──────────────────────────────────────────────────

def get_send_time(created_at: datetime) -> datetime:
    """
    Возвращает время, когда клиенту реально уйдёт Viber.

    - Дневной лид (08:00–20:00)   → отправка сразу = created_at
    - Поздний вечер (20:00–24:00) → завтра в 08:00
    - Ночь / раннее утро (00:00–08:00) → сегодня в 08:00
    """
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=BELGRADE_TZ)

    h = created_at.hour

    # Дневной лид — отправка сразу
    if VIBER_WORK_HOURS_START <= h < VIBER_WORK_HOURS_END:
        return created_at

    # Ночной лид — утром в 08:00
    base = created_at + timedelta(days=1) if h >= VIBER_WORK_HOURS_END else created_at
    return base.replace(hour=VIBER_WORK_HOURS_START, minute=0, second=0, microsecond=0)


def get_responsible_id(now_belgrade: datetime | None = None) -> int:
    """
    Возвращает ID ответственного по правилу teksturaburo.

    Правило (Europe/Belgrade):
        Пн 08:00 — Пт 17:00 → 28 (Djordje Tomic)
        Пт 17:00 — Пн 08:00 → 30 (Dmitrii Piskun)

    Привязка — к моменту, когда клиент получит первое Viber-сообщение
    (для ночных лидов это утро следующего дня, не время создания лида).

    Параметр now_belgrade нужен только для тестов и переобработки старых лидов.
    """
    if now_belgrade is None:
        now_belgrade = datetime.now(BELGRADE_TZ)
    elif now_belgrade.tzinfo is None:
        now_belgrade = now_belgrade.replace(tzinfo=BELGRADE_TZ)

    send_time = get_send_time(now_belgrade)
    wd = send_time.weekday()   # 0=Пн ... 6=Вс
    hour = send_time.hour

    # Сб, Вс — целиком на Дмитрия
    if wd in (5, 6):
        return RESPONSIBLE_WEEKEND
    # Пятница после 17:00
    if wd == 4 and hour >= FRIDAY_CUTOFF_HOUR:
        return RESPONSIBLE_WEEKEND
    # Понедельник до 08:00 (редкий кейс — send_time почти всегда ≥ 08:00)
    if wd == 0 and hour < VIBER_WORK_HOURS_START:
        return RESPONSIBLE_WEEKEND
    return RESPONSIBLE_WEEKDAY


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


# ─── Обработка вкладки Kitchen май / Kitchen MAY-copy ──────────────────────
# Обе вкладки имеют идентичную структуру (это копии одной FB Lead Ads формы),
# поэтому используется один processor с параметром tab_label, который пишется
# в комментарий лида и в логи. Для Kitchen MAY-copy подключается через
# functools.partial в массиве tabs.

def process_kitchen_may_row(
    row: list,
    row_index: int,
    sheet,
    assigned_by_id: int,
    tab_label: str = "kitchen Май",
) -> str:
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
        f"Tab: {tab_label} | Ad: {ad} | Plan: {has_plan} | Timeline: {timeline} | "
        f"Budget: {budget_raw} | Tehnika: {tehnika_raw} | Adset: {adset} | Date: {date}"
    )

    extra_fields = {}
    budget_value = parse_budget(budget_raw)
    if budget_value is not None:
        extra_fields[UF_BUDGET] = budget_value

    tehnika_value = parse_tehnika(tehnika_raw)
    if tehnika_value is not None:
        extra_fields[UF_TEHNIKA] = tehnika_value

    log.info(f"  → Создаю {tab_label} лид: {title} | budget={budget_value} tehnika={tehnika_value}")
    lead_id = create_bitrix_lead(
        title, name, last_name, phone, email, src_id, comment, assigned_by_id,
        extra_fields=extra_fields,
    )
    log.info(f"  ✓ {tab_label} → лид создан ID={lead_id}: {title}")
    return f"CREATED:{lead_id}"


# ─── Главная функция ─────────────────────────────────────────────────────────

def _route_return_lead(ld: dict, cid: int, deals: list) -> str:
    """Возврат известного клиента → прошлому менеджеру. deals — сделки контакта (с ASSIGNED_BY_ID).
    «Тот кто общался» = ответственный последней ЧЕЛОВЕЧЕСКОЙ сделки (≠Milica). Действия:
    есть человек с заведённой стадией → лид+контакт на него + его стадия-колонка + дело «ответить»;
    человек без стадии → парковка + закрепляем за ним; только Milica/без менеджера → парковка «дубль».
    Возвращает короткую метку действия для лога."""
    lead_id = ld["ID"]
    humans = [d for d in deals if int(d.get("ASSIGNED_BY_ID") or 0) not in (0, MILICA_BOT_ID)]
    if RETURN_ROUTING_ENABLED and humans:
        last = max(humans, key=lambda d: int(d.get("ID") or 0))   # последняя сделка человека
        prior = int(last.get("ASSIGNED_BY_ID") or 0)
        deal_id = last.get("ID")
        if prior in RETURN_STAGE_MAP:                  # менеджер с колонкой → полноценная маршрутизация
            stage = RETURN_STAGE_MAP[prior]
            bitrix_call("crm.lead.update", {"id": lead_id,
                        "fields": {"ASSIGNED_BY_ID": prior, "STATUS_ID": stage}})
            bitrix_call("crm.contact.update", {"id": cid, "fields": {"ASSIGNED_BY_ID": prior}})
            try:
                bitrix_call("crm.activity.todo.add", {
                    "ownerTypeId": 1, "ownerId": int(lead_id), "responsibleId": prior,
                    "deadline": datetime.now(BELGRADE_TZ).replace(microsecond=0).isoformat(),
                    "title": "Возврат клиента — ответить в открытой линии",
                    "description": (f"Известный клиент написал снова (повторное обращение). Прошлая сделка "
                                    f"#{deal_id}. Ответьте ему в открытой линии — Milica этот диалог не ведёт."),
                    "pingOffsets": [0]})
            except Exception as e:
                log.warning(f"[RETURN] лид {lead_id}: дело не создано: {e}")
            return f"→ менеджеру {prior} (стадия {stage}, прошлая сделка #{deal_id})"
        # человек без заведённой стадии-колонки → паркуем, но закрепляем за ним (не теряем владельца)
        bitrix_call("crm.lead.update", {"id": lead_id,
                    "fields": {"STATUS_ID": DUP_LEAD_STATUS, "ASSIGNED_BY_ID": prior}})
        return f"→ «Повторный/дубль» (менеджер {prior} без стадии-колонки, сделка #{deal_id})"
    # только Milica-сделки / без менеджера → парковка (решение Дмитрия: возврат Milica-сделки не маршрутим)
    last = max(deals, key=lambda d: int(d.get("ID") or 0))
    bitrix_call("crm.lead.update", {"id": lead_id, "fields": {"STATUS_ID": DUP_LEAD_STATUS}})
    return f"→ «Повторный/дубль» (прошлая сделка Milica/без менеджера #{last.get('ID')})"


def _lead_session_manager(lead_id) -> int:
    """ID живого менеджера (из RETURN_STAGE_MAP), который ВЁЛ открытую линию этого лида — по автору
    активити-сессии IMOPENLINES_SESSION (берём последнюю с человеком-менеджером). Milica-сессии
    (author 2296) и без человека → 0. Сигнал «менеджер уже общался» для лидов БЕЗ контакта/сделки
    (кейс 4924: WhatsApp без контакта, 4 сессии вёл Piskun, висел в «Новый лид»). Решение Дмитрия 14.06."""
    try:
        acts = bitrix_call("crm.activity.list", {
            "filter": {"OWNER_TYPE_ID": 1, "OWNER_ID": int(lead_id), "PROVIDER_ID": "IMOPENLINES_SESSION"},
            "select": ["ID", "AUTHOR_ID"], "order": {"ID": "DESC"}}).get("result", []) or []
    except Exception as e:
        log.warning(f"[RETURN] сессии лида {lead_id} не получены: {e}")
        return 0
    for a in acts:
        au = int(a.get("AUTHOR_ID") or 0)
        if au in RETURN_STAGE_MAP:
            return au
    return 0


def _route_lead_to_manager(lead_id, cid, mgr: int) -> str:
    """Лид (без сделки) → колонка менеджера, который вёл его открытую линию + дело «проверить»."""
    stage = RETURN_STAGE_MAP[mgr]
    bitrix_call("crm.lead.update", {"id": lead_id, "fields": {"STATUS_ID": stage, "ASSIGNED_BY_ID": mgr}})
    if cid:
        bitrix_call("crm.contact.update", {"id": cid, "fields": {"ASSIGNED_BY_ID": mgr}})
    try:
        bitrix_call("crm.activity.todo.add", {
            "ownerTypeId": 1, "ownerId": int(lead_id), "responsibleId": mgr,
            "deadline": datetime.now(BELGRADE_TZ).replace(microsecond=0).isoformat(),
            "title": "Лид менеджера — проверить (висел в «Новый лид»)",
            "description": ("Открытую линию этого лида вёл ты (есть твой ответ в чате), но лид застрял в "
                            "«Новый лид». Перенесён в твою колонку — проверь/закрой/возобнови."),
            "pingOffsets": [0]})
    except Exception as e:
        log.warning(f"[RETURN] лид {lead_id}: дело не создано: {e}")
    return f"→ менеджеру {mgr} (стадия {stage}, вёл открытую линию)"


def cleanup_duplicate_leads() -> int:
    """Возврат/лид менеджера в «Новый лид» → в колонку менеджера, чтобы не повисло. Два сигнала:
    (1) у контакта свежего NEW-лида есть СДЕЛКА человека → прошлому менеджеру (лид+контакт+стадия+дело);
        Milica-сделка/без менеджера → парковка «Повторный/дубль».
    (2) лид БЕЗ сделки/контакта, но открытую линию вёл живой менеджер (28/30) → его колонка (кейс 4924).
    Milica сама отходит (poller._owned_by_human). Новый клиент (нет ни сделки, ни менеджер-сессии) — не
    трогаем. Решение Дмитрия 14.06."""
    if not DUP_CLEANUP_ENABLED:
        return 0
    since = (datetime.now(BELGRADE_TZ) - timedelta(days=DUP_CLEANUP_MAX_AGE_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        leads = bitrix_call("crm.lead.list", {
            "filter": {"STATUS_ID": "NEW", ">DATE_CREATE": since},
            "select": ["ID", "CONTACT_ID", "TITLE"]}).get("result", []) or []
    except Exception as e:
        log.error(f"[RETURN] список NEW-лидов не получен: {e}")
        return 0
    handled = 0
    for ld in leads:
        lead_id = ld["ID"]
        cid = ld.get("CONTACT_ID")
        try:
            # (1) возврат по СДЕЛКЕ контакта — надёжный сигнал
            if cid:
                deals = bitrix_call("crm.deal.list", {
                    "filter": {"CONTACT_ID": cid}, "select": ["ID", "ASSIGNED_BY_ID"]}).get("result", []) or []
                if deals:
                    action = _route_return_lead(ld, cid, deals)
                    log.info(f"[RETURN] лид {lead_id} (контакт {cid}) {action}")
                    handled += 1
                    continue
            # (2) лид без сделки, но открытую линию вёл живой менеджер → его колонка
            mgr = _lead_session_manager(lead_id)
            if mgr in RETURN_STAGE_MAP:
                action = _route_lead_to_manager(lead_id, cid, mgr)
                log.info(f"[RETURN] лид {lead_id} (сессию вёл менеджер {mgr}) {action}")
                handled += 1
        except Exception as e:
            log.error(f"[RETURN] лид {lead_id}: маршрутизация не удалась: {e}")
    return handled


def _lead_inbox(lead_id: int) -> str | None:
    """Ящик-получатель входящего email-лида: SETTINGS.EMAIL_META.__email из bound email-активити (TYPE_ID=4).
    Совпадение с известным ящиком в EMAIL_STAGE_MAP; предпочитаем входящие (DIRECTION=1). None — не наш/нет."""
    try:
        acts = bitrix_call("crm.activity.list", {
            "filter": {"OWNER_TYPE_ID": 1, "OWNER_ID": int(lead_id), "TYPE_ID": 4},
            "select": ["ID", "DIRECTION", "SETTINGS"], "order": {"ID": "ASC"}}).get("result", []) or []
    except Exception as e:
        log.warning(f"[EMAIL] активити лида {lead_id} не получены: {e}")
        return None
    fallback = None
    for a in acts:
        s = a.get("SETTINGS")
        if isinstance(s, str):
            try:
                s = json.loads(s)
            except Exception:
                s = {}
        em = (((s or {}).get("EMAIL_META") or {}).get("__email") or "").strip().lower()
        if em in EMAIL_STAGE_MAP:
            if str(a.get("DIRECTION")) == "1":          # входящее на наш ящик — точный сигнал
                return em
            fallback = fallback or em
    return fallback


def route_email_leads() -> int:
    """Свежий NEW-лид c SOURCE_ID=EMAIL → стадия-колонка ящика-получателя (sales@→email sales,
    custom@→email Custom). Только стадия (email не Milica-канал; ответственного/диалог не трогаем).
    Неизвестный ящик → не трогаем. Идёт ПОСЛЕ возврат-маршрутизации (та уводит возвраты из NEW). Дмитрий 14.06."""
    if not EMAIL_ROUTING_ENABLED or not EMAIL_STAGE_MAP:
        return 0
    since = (datetime.now(BELGRADE_TZ) - timedelta(days=DUP_CLEANUP_MAX_AGE_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        leads = bitrix_call("crm.lead.list", {
            "filter": {"STATUS_ID": "NEW", "SOURCE_ID": "EMAIL", ">DATE_CREATE": since},
            "select": ["ID", "TITLE"]}).get("result", []) or []
    except Exception as e:
        log.error(f"[EMAIL] список NEW email-лидов не получен: {e}")
        return 0
    n = 0
    for ld in leads:
        inbox = _lead_inbox(ld["ID"])
        if not inbox:
            continue                                    # ящик не распознан / не наш → оставляем как есть
        stage = EMAIL_STAGE_MAP[inbox]
        try:
            bitrix_call("crm.lead.update", {"id": ld["ID"], "fields": {"STATUS_ID": stage}})
            log.info(f"[EMAIL] лид {ld['ID']} → стадия {stage} (получатель {inbox})")
            n += 1
        except Exception as e:
            log.error(f"[EMAIL] лид {ld['ID']}: перевод в {stage} не удался: {e}")
    return n


def process_web_leads() -> int:
    """WEB-лиды (источник «Website», создаются прямо в Bitrix на человеке) → на Milica + Viber-welcome.
    Берём ТОЛЬКО свежие (STATUS=NEW, не старше WEB_LEADS_MAX_AGE_DAYS) и прошедшие гард качества
    (валидный телефон с кодом + нормальное имя) — источник шумный (спам/мусор отдаём человеку). После
    welcome переводим NEW→«New sale AI» + ответственный 2296 (это и дедуп: повторно не возьмём). Только
    рабочее время Viber (8–20 Белград; ночью пропускаем — утренний tick подхватит). Решение Дмитрия 14.06."""
    if not WEB_LEADS_ENABLED:
        return 0
    if not is_working_hours():
        log.info("[WEB] нерабочее время (8–20) — WEB-лиды до утра")
        return 0
    since = (datetime.now(BELGRADE_TZ) - timedelta(days=WEB_LEADS_MAX_AGE_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        leads = bitrix_call("crm.lead.list", {
            "filter": {"SOURCE_ID": "WEB", "STATUS_ID": "NEW", ">DATE_CREATE": since},
            "select": ["ID", "NAME", "PHONE", "ASSIGNED_BY_ID"],
            "order": {"DATE_CREATE": "ASC"}}).get("result", []) or []
    except Exception as e:
        log.error(f"[WEB] список WEB-лидов не получен: {e}")
        return 0
    if leads:
        log.info(f"[WEB] свежих WEB-лидов (NEW, ≤{WEB_LEADS_MAX_AGE_DAYS}д): {len(leads)}")
    sent = 0
    for ld in leads:
        lead_id = str(ld["ID"])
        ph = ld.get("PHONE") or []
        phone_raw = (ph[0].get("VALUE") if ph and isinstance(ph[0], dict) else "") or ""
        name, _ = clean_name(ld.get("NAME") or "")
        if name.lower() in _WEB_JUNK_NAMES or not _looks_real_phone(phone_raw):
            log.warning(f"[WEB] лид {lead_id} ПРОПУЩЕН гардом (имя='{name}', тел='{phone_raw}') — оставляю человеку")
            continue
        phone = phone_raw.lstrip("p:+").replace(" ", "").replace("-", "")
        # Viber СНАЧАЛА (если упал — лид остаётся NEW, ретрай в след. tick), затем claim+дедуп
        try:
            send_viber_wazzup(phone, name, lead_id, product="kitchen")
        except Exception as e:
            log.error(f"[WEB] лид {lead_id}: Viber не ушёл ({e}) — оставляю NEW, ретрай позже")
            continue
        try:
            bitrix_call("crm.lead.update", {"id": lead_id, "fields": {
                "ASSIGNED_BY_ID": MILICA_ID, "STATUS_ID": LEAD_STATUS_ID}})
        except Exception as e:
            log.error(f"[WEB] лид {lead_id}: Viber ушёл, но claim не удался ({e}) — возможен повтор")
        log.info(f"[WEB] лид {lead_id} → Milica + Viber-welcome ({name})")
        sent += 1
        time.sleep(random.uniform(30, 45))   # как process_viber_queue (анти-флуд)
    return sent


def run():
    log.info("=" * 60)
    log.info("Запуск импорта лидов")

    now_bel = datetime.now(BELGRADE_TZ)
    log.info(f"Время Belgrade: {now_bel:%A %Y-%m-%d %H:%M:%S}")
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
            "product":      "kitchen",
            "timeline_col": 13,   # колонка N (0-based) — Timeline; Tehnika в этой вкладке нет → ветка C
        },
        {
            "name":       "Ormari",
            "range":      "A2:S500",
            "status_col": 19,   # колонка S
            "processor":  process_ormari_row,
            "phone_col":  17,   # колонка R (0-based)
            "name_col":   16,   # колонка Q (0-based)
            "product":      "wardrobe",   # шкафы — кухонные тексты не подходят, идёт на общие VIBER_TEMPLATES
            "timeline_col": 14,   # колонка O (0-based) — Timeline
        },
        {
            "name":       "kitchen Май",
            "range":      "A2:T500",
            "status_col": 20,   # колонка T
            "processor":  process_kitchen_may_row,    # tab_label по умолчанию "kitchen Май"
            "phone_col":  18,   # колонка S (0-based)
            "name_col":   17,   # колонка R (0-based)
            "product":      "kitchen",
            "timeline_col": 13,   # колонка N (0-based) — Timeline
            "tehnika_col":  15,   # колонка P (0-based) — Tehnika (samo_kuhinja / kuhinja_+_tehnika / …)
        },
        {
            # Структура идентична kitchen Май — переиспользуем тот же processor
            # через functools.partial с другим tab_label для комментария лида.
            "name":       "Kitchen MAY-copy",
            "range":      "A2:T500",
            "status_col": 20,   # колонка T (lead_status)
            "processor":  partial(process_kitchen_may_row, tab_label="Kitchen MAY-copy"),
            "phone_col":  18,   # колонка S (0-based) — phone
            "name_col":   17,   # колонка R (0-based) — full_name
            "product":      "kitchen",   # идентична kitchen Май → та же сегментация A/B/C + no-rush
            "timeline_col": 13,
            "tehnika_col":  15,
        },
    ]

    # ── ШАГ 1: обработать ночные лиды (VIBER_PENDING) ────────────────────
    # Этот блок ищет лиды созданные ночью и шлёт им Viber утром.
    # В нерабочее время молча возвращается без действий.
    process_pending_viber(spreadsheet, tabs)

    # ── ШАГ 1.5: WEB-лиды (источник сайта, прямо в Bitrix) → на Milica + Viber-welcome ──
    try:
        n_web = process_web_leads()
        if n_web:
            log.info(f"  [WEB] поприветствовано WEB-лидов: {n_web}")
    except Exception as e:
        log.error(f"  [WEB] обработка WEB-лидов упала: {e}")

    # ── ШАГ 1.6: возврат известного клиента (у контакта есть сделка) → прошлому менеджеру / парковка ──
    try:
        n_ret = cleanup_duplicate_leads()
        if n_ret:
            log.info(f"  [RETURN] обработано возвратных лидов: {n_ret}")
    except Exception as e:
        log.error(f"  [RETURN] маршрутизация возвратов упала: {e}")

    # ── ШАГ 1.7: email-лиды (письмо на sales@/custom@) → стадия-колонка ящика ──
    try:
        n_mail = route_email_leads()
        if n_mail:
            log.info(f"  [EMAIL] распределено email-лидов по стадиям: {n_mail}")
    except Exception as e:
        log.error(f"  [EMAIL] распределение email-лидов упало: {e}")

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
        product      = tab_cfg.get("product", "kitchen")
        timeline_col = tab_cfg.get("timeline_col")
        tehnika_col  = tab_cfg.get("tehnika_col")

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
                # Ответственный считается ДЛЯ КАЖДОГО ЛИДА ОТДЕЛЬНО.
                # Это важно потому что:
                #   - cron может стартовать на границе пт 16:59 → пт 17:01;
                #     первые лиды должны уйти на 28, последние — на 30
                #   - в одном батче могут быть и дневные, и ночные лиды;
                #     ответственный считается на момент реальной отправки Viber
                assigned_by_id = get_responsible_id()

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

                    timeline_v = row[timeline_col].strip() if timeline_col is not None and len(row) > timeline_col else ""
                    tehnika_v  = row[tehnika_col].strip()  if tehnika_col  is not None and len(row) > tehnika_col  else ""

                    if is_working_hours() and phone:
                        # Рабочее время — добавляем в очередь немедленной отправки
                        viber_queue.append({
                            "lead_id":     lead_id,
                            "phone":       phone,
                            "name":        name,
                            "product":     product,
                            "tehnika_raw": tehnika_v,
                            "timeline":    timeline_v,
                        })
                        sheet.update_cell(sheet_row, status_col, status)
                        log.info(f"  [ASSIGN] Лид {lead_id} → ответственный {assigned_by_id}")
                    elif phone:
                        # Ночь — откладываем до утра, пишем VIBER_PENDING
                        sheet.update_cell(sheet_row, status_col, f"VIBER_PENDING:{lead_id}")
                        log.info(f"  [VIBER] Ночной лид {lead_id} → VIBER_PENDING (отправим утром), ответственный {assigned_by_id}")
                    else:
                        # Нет телефона — Viber невозможен, пишем обычный CREATED
                        sheet.update_cell(sheet_row, status_col, status)
                        log.warning(f"  [VIBER] Лид {lead_id} без телефона — Viber пропущен, ответственный {assigned_by_id}")

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
