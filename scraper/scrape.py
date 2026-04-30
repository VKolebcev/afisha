#!/usr/bin/env python3
"""
Theater Monitor Scraper — Playwright edition
Используем Playwright (настоящий Chromium) вместо requests,
чтобы обходить IP-фильтры и JS-загружаемый контент.

Реализованные парсеры:
  fomenki        — fomenki.ru (Мастерская Петра Фоменко)
  electrotheatre — electrotheatre.ru (Электротеатр Станиславский)
  todo           — заглушка

Установка:
  pip install -r requirements.txt
  playwright install chromium
"""

import json
import os
import re
import time
import traceback
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Browser, Page

# ─────────────────────────────────────────────
# КОНСТАНТЫ
# ─────────────────────────────────────────────

MONTHS_RU: dict[str, int] = {
    "янв": 1, "фев": 2, "мар": 3, "апр": 4,
    "май": 5, "мая": 5, "июн": 6, "июл": 7,
    "авг": 8, "сен": 9, "окт": 10, "ноя": 11, "дек": 12,
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
    "июня": 6, "июля": 7, "августа": 8, "сентября": 9,
    "октября": 10, "ноября": 11, "декабря": 12,
}

WEEKDAYS_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

# Глобальный браузер (инициализируется один раз в main)
_browser: Browser | None = None


# ─────────────────────────────────────────────
# PLAYWRIGHT HELPERS
# ─────────────────────────────────────────────

def get_browser() -> Browser:
    global _browser
    if _browser is None or not _browser.is_connected():
        raise RuntimeError("Browser not initialized. Call main() or init_browser().")
    return _browser


def fetch_page(url: str, wait_for: str = "networkidle", timeout: int = 30000) -> str:
    """
    Открывает URL в Playwright и возвращает HTML после рендера JS.
    wait_for: 'networkidle' | 'domcontentloaded' | 'load'
    """
    browser = get_browser()
    page: Page = browser.new_page(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        locale="ru-RU",
        timezone_id="Europe/Moscow",
    )
    try:
        page.set_extra_http_headers({
            "Accept-Language": "ru-RU,ru;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        page.goto(url, wait_until=wait_for, timeout=timeout)
        # Человекоподобная пауза
        page.wait_for_timeout(800)
        return page.content()
    except Exception as e:
        print(f"  ✗ Playwright GET {url}: {e}")
        return ""
    finally:
        page.close()


def soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


# ─────────────────────────────────────────────
# УТИЛИТЫ
# ─────────────────────────────────────────────

def today() -> date:
    return date.today()


def fmt(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def weekday_ru(d: date) -> str:
    return WEEKDAYS_RU[d.weekday()]


def parse_ru_date(text: str) -> date | None:
    """Парсит '1 апреля 2026', '1 апр', '01.04.2026', '2026-04-01'."""
    text = text.strip().lower()

    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return date(int(m[1]), int(m[2]), int(m[3]))

    m = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{2,4})", text)
    if m:
        y = int(m[3])
        return date(y if y > 100 else 2000 + y, int(m[2]), int(m[1]))

    m = re.search(r"(\d{1,2})\s+([а-яё]+)(?:\s+(\d{4}))?", text)
    if m:
        day = int(m[1])
        word = m[2]
        mon = next((v for k, v in MONTHS_RU.items() if word.startswith(k[:3])), None)
        if mon is None:
            return None
        year = int(m[3]) if m[3] else today().year
        try:
            d = date(year, mon, day)
            if not m[3] and d < today():
                d = date(year + 1, mon, day)
            return d
        except ValueError:
            return None
    return None


def base_result(cfg: dict) -> dict:
    return {
        "id":          cfg["id"],
        "name":        cfg["name"],
        "theater":     cfg.get("theater", ""),
        "description": cfg.get("description", ""),
        "image":       cfg.get("image", ""),
        "url":         cfg["url"],
        "available":   None,
        "price_min":   0,
        "price_max":   0,
        "currency":    "₽",
        "error":       None,
        "dates":       [],
    }


def error_result(cfg: dict, msg: str) -> dict:
    r = base_result(cfg)
    r["error"] = msg
    return r


def make_date_entry(
    d: date,
    time_str: str = "",
    available: bool = True,
    price_min: int = 0,
    price_max: int = 0,
    buy_url: str = "",
) -> dict:
    return {
        "date":      fmt(d),
        "time":      time_str,
        "weekday":   weekday_ru(d),
        "available": available,
        "price_min": price_min,
        "price_max": price_max,
        "buy_url":   buy_url,
    }


def finalize(result: dict, days_ahead: int = 90) -> dict:
    """Убирает прошедшие даты, считает доступность."""
    cutoff = today() + timedelta(days=days_ahead)
    future = [
        d for d in result["dates"]
        if today() <= date.fromisoformat(d["date"]) <= cutoff
    ]
    result["dates"] = sorted(future, key=lambda d: d["date"])

    avail = [d for d in future if d["available"]]
    result["available"] = len(avail) > 0

    prices = (
        [d["price_min"] for d in avail if d["price_min"]] +
        [d["price_max"] for d in avail if d["price_max"]]
    )
    if prices:
        result["price_min"] = min(prices)
        result["price_max"] = max(prices)

    return result


# ─────────────────────────────────────────────
# ПАРСЕР: fomenki.ru
# ─────────────────────────────────────────────
#
# Расписание: fomenki.ru/timetable/  и  /timetable/MM-YYYY/
#
# HTML-структура тайм-лайна (каждый спектакль в блоке):
#
#   <h2 class="...">1 ср</h2>          ← заголовок дня
#   <div class="performance-item">
#     <span class="time">19:00</span>
#     <a href="/performance/arcadia/">Аркадия</a>
#     <a href="/performance/arcadia/#170647">Состав</a>
#     <div class="venue">Новая сцена, Большой зал</div>
#   </div>
#
# По умолчанию в config указывается performance_slug
# (часть URL после /performance/).
#
# Ссылка на покупку = fomenki.ru/buy/
# Якорь #NNNNNN из ссылки «Состав» даёт прямой переход к нужному шоу.
# ─────────────────────────────────────────────

def parse_fomenki(cfg: dict) -> dict:
    opts = cfg.get("parser_options", {})
    slug = opts.get("performance_slug", "")
    if not slug:
        return error_result(cfg, "parser_options.performance_slug не задан")

    perf_path = f"/performance/{slug}/"
    buy_base  = "https://fomenki.ru/buy/"
    result    = base_result(cfg)

    # ── Описание и постер ──
    html = fetch_page(cfg["url"])
    if html:
        s = soup(html)
        for sel in [".performance__desc", ".description", "article .text p", ".b-text p"]:
            el = s.select_one(sel)
            if el and len(el.get_text(strip=True)) > 80:
                result["description"] = el.get_text(" ", strip=True)[:600]
                break
        if not result["image"]:
            for sel in [".performance__poster img", ".poster img",
                        "img[src*='performance']", "img[src*='upload']"]:
                img = s.select_one(sel)
                if img:
                    src = img.get("src", "")
                    result["image"] = src if src.startswith("http") else f"https://fomenki.ru{src}"
                    break

    # ── Расписание ──
    now = today()
    timetable_urls = ["https://fomenki.ru/timetable/"]
    for delta in range(1, 3):
        mon = (now.month + delta - 1) % 12 + 1
        yr  = now.year + (now.month + delta - 1) // 12
        timetable_urls.append(f"https://fomenki.ru/timetable/{mon:02d}-{yr}/")

    dates = []
    seen  = set()
    for url in timetable_urls:
        html = fetch_page(url)
        if html:
            dates.extend(_fomenki_parse_timetable(soup(html), perf_path, buy_base, seen))
        time.sleep(0.5)

    result["dates"] = dates
    return finalize(result)


def _fomenki_parse_timetable(
    s: BeautifulSoup, perf_path: str, buy_base: str, seen: set
) -> list[dict]:
    entries = []
    current_date: date | None = None

    # Стратегия: ищем h2 с датами + ближайшие ссылки на нужный спектакль
    # fomenki.ru кладёт каждый день в <section> или просто h2 + блоки после
    for tag in s.find_all(True):
        name = getattr(tag, "name", "")

        # Заголовок дня
        if name == "h2":
            text = tag.get_text(strip=True)
            # Убираем день недели
            clean = re.sub(r"\b(пн|вт|ср|чт|пт|сб|вс)\b", "", text, flags=re.I).strip()
            # Добавляем текущий месяц если только число
            if re.match(r"^\d{1,2}$", clean):
                t = today()
                try:
                    current_date = date(t.year, t.month, int(clean))
                    if current_date < today():
                        current_date = None
                except ValueError:
                    current_date = None
            else:
                current_date = parse_ru_date(clean)
            continue

        # Ищем ссылку на наш спектакль внутри текущего контейнера
        if current_date is None:
            continue
        if name not in ("div", "section", "article", "li", "ul", "table", "tr"):
            continue

        links = tag.find_all("a", href=True)
        perf_a  = None
        cast_a  = None
        for a in links:
            href = a["href"]
            if perf_path not in href:
                continue
            if "#" in href and href.count("#") == 1:
                cast_a = a
            else:
                perf_a = a

        if not (perf_a or cast_a):
            continue

        key = fmt(current_date)
        if key in seen:
            continue
        seen.add(key)

        # Время
        block_text = tag.get_text(" ")
        tm = re.search(r"\b(\d{1,2}:\d{2})\b", block_text)
        time_str = tm.group(1) if tm else ""

        # Buy URL
        anchor = ""
        if cast_a:
            parts = cast_a["href"].split("#")
            if len(parts) == 2:
                anchor = parts[1]
        buy_url = (buy_base + "#" + anchor) if anchor else buy_base

        entries.append(make_date_entry(current_date, time_str, True, buy_url=buy_url))

    return entries


# ─────────────────────────────────────────────
# ПАРСЕР: electrotheatre.ru
# ─────────────────────────────────────────────
#
# Афиша: electrotheatre.ru/playbill/
# Следующие месяцы: /playbill/?month=YYYY-MM
#
# После рендера JS структура страницы (упрощённо):
#
#   <div class="playbill-day">
#     <div class="playbill-day__date">25 апреля</div>
#     <div class="playbill-day__events">
#       <div class="event-card">
#         <div class="event-card__time">19:00</div>
#         <a class="event-card__title" href="/playbill/event.htm?id=XXXX">Название</a>
#         <a class="event-card__buy" href="...">Купить билеты</a>
#       </div>
#     </div>
#   </div>
#
# Сопоставление по match_name (подстрока в названии события).
# ─────────────────────────────────────────────

def parse_electrotheatre(cfg: dict) -> dict:
    opts       = cfg.get("parser_options", {})
    match_name = opts.get("match_name", "").strip().lower()
    if not match_name:
        return error_result(cfg, "parser_options.match_name не задан")

    result = base_result(cfg)
    base   = "https://electrotheatre.ru"

    # ── Описание и постер ──
    html = fetch_page(cfg["url"])
    if html:
        s = soup(html)
        for sel in [".spectacle-description", ".description", "article p", ".b-text"]:
            el = s.select_one(sel)
            if el and len(el.get_text(strip=True)) > 80:
                result["description"] = el.get_text(" ", strip=True)[:600]
                break
        if not result["image"]:
            for sel in [".spectacle-poster img", ".poster img", ".cover img",
                        "img[src*='spectacle']", "img[src*='upload']"]:
                img = s.select_one(sel)
                if img:
                    src = img.get("src", "")
                    result["image"] = src if src.startswith("http") else base + src
                    break

    # ── Расписание ──
    now = today()
    playbill_urls = [f"{base}/playbill/"]
    for delta in range(1, 3):
        mon = (now.month + delta - 1) % 12 + 1
        yr  = now.year + (now.month + delta - 1) // 12
        playbill_urls.append(f"{base}/playbill/?month={yr}-{mon:02d}")

    dates = []
    seen  = set()
    for url in playbill_urls:
        html = fetch_page(url, wait_for="networkidle")
        if html:
            dates.extend(_electrotheatre_parse(soup(html), match_name, base, seen))
        time.sleep(0.5)

    result["dates"] = dates
    return finalize(result)


def _electrotheatre_parse(
    s: BeautifulSoup, match_name: str, base: str, seen: set
) -> list[dict]:
    entries = []
    current_date: date | None = None

    # Паттерн 1: контейнеры дней (если JS отрендерил структуру)
    day_blocks = (
        s.select(".playbill-day, .day-block, [class*='playbill__day']") or
        s.select("section[data-date], div[data-date]")
    )

    if day_blocks:
        for block in day_blocks:
            # Дата из data-атрибута или текста
            date_str = (
                block.get("data-date", "") or
                _first_text(block.select_one("[class*='date'], [class*='Date']"))
            )
            d = parse_ru_date(date_str) if date_str else None
            if not d:
                continue
            entries.extend(_electro_scan_block(block, match_name, d, base, seen))
        return entries

    # Паттерн 2: линейный проход по тегам (fallback для плоской вёрстки)
    for tag in s.find_all(True):
        tname = getattr(tag, "name", "")

        # Детектор даты: <div> или <h2>/<h3> с текстом вида "25 апреля"
        if tname in ("h2", "h3", "div", "p", "span"):
            txt = tag.get_text(strip=True)
            if re.match(r"^\d{1,2}\s+[а-яА-Яё]+", txt) and len(txt) < 35:
                parsed = parse_ru_date(txt)
                if parsed:
                    current_date = parsed

        # Ищем событие рядом
        if current_date is None:
            continue
        if tname not in ("div", "article", "section", "li"):
            continue

        links = tag.find_all("a", href=True)
        for a in links:
            link_text = a.get_text(strip=True).lower()
            if match_name not in link_text:
                continue

            key = fmt(current_date)
            if key in seen:
                continue
            seen.add(key)

            parent_text = tag.get_text(" ")
            tm = re.search(r"\b(\d{1,2}:\d{2})\b", parent_text)
            time_str = tm.group(1) if tm else ""

            # Ищем кнопку купить рядом
            buy_a = tag.find("a", href=re.compile(r"buy|ticket|event|playbill"))
            href = (buy_a or a).get("href", "")
            buy_url = href if href.startswith("http") else base + href

            entries.append(make_date_entry(current_date, time_str, True, buy_url=buy_url))
            break

    return entries


def _electro_scan_block(
    block, match_name: str, d: date, base: str, seen: set
) -> list[dict]:
    entries = []
    for a in block.find_all("a", href=True):
        if match_name not in a.get_text(strip=True).lower():
            continue
        key = fmt(d)
        if key in seen:
            continue
        seen.add(key)

        parent = a.find_parent() or a
        tm = re.search(r"\b(\d{1,2}:\d{2})\b", parent.get_text(" "))
        time_str = tm.group(1) if tm else ""

        buy_a = block.find("a", string=re.compile(r"купить", re.I))
        href = (buy_a or a).get("href", "")
        buy_url = href if href.startswith("http") else base + href

        entries.append(make_date_entry(d, time_str, True, buy_url=buy_url))
        break
    return entries


def _first_text(el) -> str:
    return el.get_text(strip=True) if el else ""


# ─────────────────────────────────────────────
# ЗАГЛУШКА
# ─────────────────────────────────────────────

def parse_todo(cfg: dict) -> dict:
    r = base_result(cfg)
    r["error"] = "Парсер для этого театра ещё не реализован"
    return r


# ─────────────────────────────────────────────
# РЕЕСТР ПАРСЕРОВ
# ─────────────────────────────────────────────

PARSERS: dict = {
    "fomenki":        parse_fomenki,
    "electrotheatre": parse_electrotheatre,
    "todo":           parse_todo,
    # Следующие добавляются по одному:
    # "mayakovsky":   parse_mayakovsky,
    # "vakhtangov":   parse_vakhtangov,
    # "mxat":         parse_mxat,
    # "okolo":        parse_okolo,
    # "shalom":       parse_shalom,
    # "sreda21":      parse_sreda21,
    # "sovremennik":  parse_sovremennik,
    # "teatrdoc":     parse_teatrdoc,
    # "nations":      parse_nations,
    # "bronnaya":     parse_bronnaya,
    # "ermolova":     parse_ermolova,
    # "brodsky":      parse_brodsky,
}


# ─────────────────────────────────────────────
# ТОЧКА ВХОДА
# ─────────────────────────────────────────────

def main():
    global _browser

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(root, "config.json")
    output_path = os.path.join(root, "data", "productions.json")

    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)

    tz        = ZoneInfo(config.get("settings", {}).get("timezone", "Europe/Moscow"))
    days_ahead = config.get("settings", {}).get("days_ahead", 90)
    now_str   = datetime.now(tz).isoformat(timespec="seconds")

    with sync_playwright() as pw:
        _browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        productions = []
        for prod in config["productions"]:
            if "id" not in prod:
                continue  # пропускаем комментарии

            parser_key = prod.get("parser", "todo")
            parser_fn  = PARSERS.get(parser_key, parse_todo)

            label = "[TODO]" if parser_key == "todo" else f"[{parser_key}]"
            print(f"{label} {prod['name']} — {prod.get('theater', '')}")

            try:
                result = parser_fn(prod)
                result = finalize(result, days_ahead)
                n   = len(result["dates"])
                ok  = sum(1 for d in result["dates"] if d["available"])
                err = f" ⚠ {result['error']}" if result.get("error") else ""
                print(f"  → дат: {n}, доступно: {ok}{err}")
            except Exception as e:
                traceback.print_exc()
                result = error_result(prod, f"Ошибка: {e}")

            productions.append(result)

        _browser.close()

    output = {"last_updated": now_str, "productions": productions}
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    active = sum(1 for p in productions if p.get("parser") not in ("todo",) and not p.get("error"))
    print(f"\n✅ Готово: {len(productions)} постановок ({active} с данными) → {output_path}")


if __name__ == "__main__":
    main()