#!/usr/bin/env python3
"""
Theater Monitor Scraper — Playwright edition

Запуск:
  python scraper/scrape.py

Реализованные парсеры:
  fomenki        — fomenki.ru  (парсит страницу спектакля напрямую)
  electrotheatre — electrotheatre.ru
  todo           — заглушка
"""

import json
import os
import re
import sys
import time
import traceback
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Browser

_browser: Browser | None = None

# ─────────────────────────────────────────────
# PLAYWRIGHT
# ─────────────────────────────────────────────

def fetch(url: str, wait: str = "networkidle", timeout: int = 30_000) -> str:
    page = _browser.new_page(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        locale="ru-RU",
        timezone_id="Europe/Moscow",
    )
    page.set_extra_http_headers({
        "Accept-Language": "ru-RU,ru;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    try:
        page.goto(url, wait_until=wait, timeout=timeout)
        page.wait_for_timeout(800)
        return page.content()
    except Exception as e:
        print(f"  ✗ {url}: {e}")
        return ""
    finally:
        page.close()


def soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


# ─────────────────────────────────────────────
# ДАТЫ
# ─────────────────────────────────────────────

MONTHS_RU: dict[str, int] = {
    "янв":1,"фев":2,"мар":3,"апр":4,"май":5,"мая":5,
    "июн":6,"июл":7,"авг":8,"сен":9,"окт":10,"ноя":11,"дек":12,
    "января":1,"февраля":2,"марта":3,"апреля":4,
    "июня":6,"июля":7,"августа":8,"сентября":9,
    "октября":10,"ноября":11,"декабря":12,
}
WEEKDAYS_RU = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]


def today() -> date:
    return date.today()


def fmt(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def weekday_ru(d: date) -> str:
    return WEEKDAYS_RU[d.weekday()]


def parse_ru_date(text: str) -> date | None:
    """'30 апреля', '30 апреля 2026', '30.04.2026', '2026-04-30'"""
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


def parse_price(text: str) -> int:
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else 0


# ─────────────────────────────────────────────
# ОБЩИЕ ХЕЛПЕРЫ
# ─────────────────────────────────────────────

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
    d: date, time_str: str = "", available: bool = True,
    price_min: int = 0, price_max: int = 0, buy_url: str = "",
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
# Парсим страницу спектакля напрямую.
#
# Структура HTML (подтверждена из реального источника):
#
#   <meta property="og:image" content="https://fomenki.ru/f/performance/26523_c.png">
#   <div class="info">...Цена билетов от 1000 до 30000 руб....</div>
#   <div class="about">Описание спектакля...</div>
#
#   <div class="events-title">Ближайшие даты исполнения</div>
#   <div class="events">
#     <div class="event">
#       <p class="date">30 апреля, 19:00</p>
#       <p class="tickets">
#         <a href="/boxoffice/#28316352" class="btn lot-of">Купить билет</a>
#       </p>
#     </div>
#     ...
#   </div>
#
# Если билеты кончились — у события нет <a class="btn"> или есть другой класс.
# ─────────────────────────────────────────────

def parse_fomenki(cfg: dict) -> dict:
    html = fetch(cfg["url"])
    if not html:
        return error_result(cfg, "Не удалось загрузить страницу")

    s = soup(html)
    result = base_result(cfg)

    # ── Постер: og:image ──────────────────────────────────────
    og_img = s.select_one('meta[property="og:image"]')
    if og_img:
        result["image"] = og_img.get("content", "")

    # ── Описание ──────────────────────────────────────────────
    about = s.select_one(".about")
    if about:
        # Берём текст до цитат (первый блок)
        result["description"] = about.get_text(" ", strip=True)[:600]

    # ── Цены: "Цена билетов от 1000 до 30000 руб." ────────────
    info_el = s.select_one(".info")
    if info_el:
        info_text = info_el.get_text(" ", strip=True)
        pm = re.search(r"от\s*([\d\s]+?)\s*до\s*([\d\s]+?)\s*руб", info_text, re.I)
        if pm:
            result["price_min"] = parse_price(pm[1])
            result["price_max"] = parse_price(pm[2])

    # ── Даты: div.events > div.event ──────────────────────────
    dates = []
    for event_div in s.select(".events .event"):
        date_p  = event_div.select_one("p.date")
        buy_a   = event_div.select_one("a[href*='/boxoffice/']")

        if not date_p:
            continue

        # "30 апреля, 19:00" → дата + время
        raw = date_p.get_text(strip=True)
        parts = raw.split(",", 1)
        date_str = parts[0].strip()
        time_str = parts[1].strip() if len(parts) > 1 else ""

        d = parse_ru_date(date_str)
        if not d:
            continue

        if buy_a:
            href = buy_a.get("href", "")
            buy_url = href if href.startswith("http") else f"https://fomenki.ru{href}"
            available = True
        else:
            # Дата в расписании, но билеты ещё не открыты или распроданы
            buy_url = cfg["url"]
            available = False

        dates.append(make_date_entry(
            d, time_str, available,
            result["price_min"], result["price_max"],
            buy_url,
        ))

    result["dates"] = dates
    return finalize(result)


# ─────────────────────────────────────────────
# ПАРСЕР: electrotheatre.ru
# ─────────────────────────────────────────────
#
# Страница спектакля: electrotheatre.ru/repertoire/spectacle/NNNN
# Афиша: electrotheatre.ru/playbill/
#
# Постер ищем через og:image или первый <img> в основном блоке.
# Даты — через афишу (поиск по match_name).
# ─────────────────────────────────────────────

def parse_electrotheatre(cfg: dict) -> dict:
    opts       = cfg.get("parser_options", {})
    match_name = opts.get("match_name", "").strip().lower()
    if not match_name:
        return error_result(cfg, "parser_options.match_name не задан")

    result = base_result(cfg)
    base   = "https://electrotheatre.ru"

    # ── Постер и описание — со страницы спектакля ─────────────
    html = fetch(cfg["url"])
    if html:
        s = soup(html)

        # og:image — самый надёжный способ
        og = s.select_one('meta[property="og:image"]')
        if og:
            result["image"] = og.get("content", "")

        # Fallback: первый <img> в основном контенте
        if not result["image"]:
            for sel in ["main img", "article img", ".spectacle img", ".content img"]:
                img = s.select_one(sel)
                if img:
                    src = img.get("src") or img.get("data-src", "")
                    if src and "logo" not in src and "icon" not in src:
                        result["image"] = src if src.startswith("http") else base + src
                        break

        # Описание
        for sel in [".spectacle-description", ".description", "article p", ".b-text p"]:
            el = s.select_one(sel)
            if el and len(el.get_text(strip=True)) > 80:
                result["description"] = el.get_text(" ", strip=True)[:600]
                break

    # ── Расписание — из афиши за 3 месяца ─────────────────────
    now = today()
    playbill_urls = [f"{base}/playbill/"]
    for delta in range(1, 3):
        mon = (now.month + delta - 1) % 12 + 1
        yr  = now.year + (now.month + delta - 1) // 12
        playbill_urls.append(f"{base}/playbill/?month={yr}-{mon:02d}")

    dates = []
    seen  = set()

    for url in playbill_urls:
        html = fetch(url)
        if html:
            found = _electrotheatre_parse_playbill(soup(html), match_name, base, seen)
            dates.extend(found)
        time.sleep(0.5)

    result["dates"] = dates
    return finalize(result)


def _electrotheatre_parse_playbill(
    s: BeautifulSoup, match_name: str, base: str, seen: set
) -> list[dict]:
    """
    Парсит страницу афиши Электротеатра.
    Ищет события по подстроке match_name в названии.
    """
    entries = []
    current_date: date | None = None

    # Проходим по всем тегам страницы линейно
    for tag in s.find_all(True):
        name = getattr(tag, "name", "")

        # ── Детектор строки с датой ──────────────────────────
        # Электротеатр выводит даты как: "25 апреля", "Суббота", затем события
        if name in ("h3", "h2", "div", "p", "span", "strong", "b"):
            txt = tag.get_text(strip=True)
            # Паттерн: "DD месяц" или "DD месяц, Weekday"
            if re.match(r"^\d{1,2}\s+[а-яА-Яё]+", txt) and len(txt) < 40:
                parsed = parse_ru_date(txt)
                if parsed and parsed >= today():
                    current_date = parsed

        if current_date is None:
            continue

        # ── Ищем событие с нашим спектаклем ─────────────────
        if name not in ("div", "article", "section", "li", "tr"):
            continue

        for a in tag.find_all("a", href=True):
            link_text = a.get_text(strip=True).lower()
            if match_name not in link_text:
                continue

            key = fmt(current_date)
            if key in seen:
                continue
            seen.add(key)

            # Время: ищем в тексте родителя
            parent_text = tag.get_text(" ")
            tm = re.search(r"\b(\d{1,2}:\d{2})\b", parent_text)
            time_str = tm.group(1) if tm else ""

            # Ссылка «Купить билет» рядом с событием
            buy_a = tag.find("a", string=re.compile(r"купить", re.I))
            href = (buy_a or a).get("href", "")
            buy_url = href if href.startswith("http") else base + href

            entries.append(make_date_entry(current_date, time_str, True, buy_url=buy_url))
            break  # одна запись на дату

    return entries


# ─────────────────────────────────────────────
# ЗАГЛУШКА
# ─────────────────────────────────────────────

def parse_todo(cfg: dict) -> dict:
    r = base_result(cfg)
    r["error"] = "Парсер для этого театра ещё не реализован"
    return r


# ─────────────────────────────────────────────
# РЕЕСТР
# ─────────────────────────────────────────────

PARSERS: dict = {
    "fomenki":        parse_fomenki,
    "electrotheatre": parse_electrotheatre,
    "todo":           parse_todo,
}


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    global _browser

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(root, "config.json")
    output_path = os.path.join(root, "data", "productions.json")

    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)

    tz         = ZoneInfo(config.get("settings", {}).get("timezone", "Europe/Moscow"))
    days_ahead = config.get("settings", {}).get("days_ahead", 90)
    now_str    = datetime.now(tz).isoformat(timespec="seconds")

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
                continue

            parser_key = prod.get("parser", "todo")
            parser_fn  = PARSERS.get(parser_key, parse_todo)
            label = f"[{parser_key}]"
            print(f"{label} {prod['name']} — {prod.get('theater', '')}")

            try:
                result = parser_fn(prod)
                result = finalize(result, days_ahead)
                n   = len(result["dates"])
                ok  = sum(1 for d in result["dates"] if d["available"])
                err = f" ⚠ {result['error']}" if result.get("error") else ""
                print(f"  → дат: {n}, доступно: {ok}, постер: {'да' if result['image'] else 'нет'}{err}")
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