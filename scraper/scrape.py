#!/usr/bin/env python3
"""
Theater Monitor Scraper — Playwright edition

Запуск:
  python scraper/scrape.py

Реализованные парсеры:
  fomenki        — fomenki.ru  (парсит страницу спектакля напрямую)
  electrotheatre — electrotheatre.ru
  mxat           — mxat.ru (МХТ им. А.П. Чехова)
  vakhtangov    — vakhtangov.ru (Театр Вахтангова)
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
# Реальная структура (подтверждена из HTML):
#
# Страница /repertoire/spectacle/1182:
#   og:image -> https://electrotheatre.ru/static/pictures/12858.png
#   div.page_item.twocol div.about -> описание
#   Дат НЕТ — только в афише.
#
# Страница афиши /playbill/:
#   Ссылки на спектакль: href="/repertoire/spectacle.htm?id=1182"
#   Кнопка: <span class="js-unifd-trigger-link" data-unifd-performance-id="NNN">
#   Buy URL: https://electrotheatre.edinoepole.ru/performance/NNN
# ─────────────────────────────────────────────

EDINOEPOLE_BUY = "https://electrotheatre.edinoepole.ru/performance/{pid}"


def parse_electrotheatre(cfg: dict) -> dict:
    opts         = cfg.get("parser_options", {})
    spectacle_id = str(opts.get("spectacle_id", "")).strip()
    if not spectacle_id:
        m = re.search(r"/(\d+)/?$", cfg["url"])
        spectacle_id = m.group(1) if m else ""
    if not spectacle_id:
        return error_result(cfg, "Не удалось определить spectacle_id из URL")

    result = base_result(cfg)
    base   = "https://electrotheatre.ru"

    # ── Постер и описание ─────────────────────────────────────
    html = fetch(cfg["url"])
    if html:
        s = soup(html)
        og = s.select_one('meta[property="og:image"]')
        if og:
            src = og.get("content", "")
            result["image"] = src.replace("/pictures/", "/1600/")
        about = s.select_one(".page_item.twocol .about, .page_item .about")
        if about:
            result["description"] = about.get_text(" ", strip=True)[:600]

    # ── Расписание из афиши за 3 месяца ───────────────────────
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
            dates.extend(_electrotheatre_parse_playbill(soup(html), spectacle_id, base, seen))
        time.sleep(0.5)

    result["dates"] = dates
    return finalize(result)


def _electrotheatre_parse_playbill(s, spectacle_id: str, base: str, seen: set) -> list:
    """
    Находим все ссылки на наш спектакль в афише, поднимаемся к блоку события,
    извлекаем дату, время и ссылку на покупку через edinoepole.
    """
    entries = []
    pattern = re.compile(rf"spectacle(\.htm\?id=|/){spectacle_id}\b")
    spectacle_links = s.find_all("a", href=pattern)

    for link in spectacle_links:
        event_block = _find_event_block(link)
        if event_block is None:
            continue

        block_text = event_block.get_text(" ", strip=True)

        # Дата из блока события ("28 мая, Четверг, 19:00")
        d = _extract_date_from_block(event_block)
        if d is None or d < today():
            continue

        key = fmt(d)
        if key in seen:
            continue
        seen.add(key)

        tm = re.search(r"\b(\d{1,2}:\d{2})\b", block_text)
        time_str = tm.group(1) if tm else ""

        buy_span = event_block.find(attrs={"data-unifd-performance-id": True})
        if buy_span:
            pid = buy_span.get("data-unifd-performance-id", "")
            buy_url = EDINOEPOLE_BUY.format(pid=pid)
        else:
            buy_url = cfg["url"] if "cfg" in dir() else f"{base}/playbill/"

        entries.append(make_date_entry(d, time_str, True, buy_url=buy_url))

    return entries


def _find_event_block(link):
    """Поднимаемся по DOM до блока события."""
    node = link.parent
    for _ in range(8):
        if node is None:
            return None
        cls = " ".join(node.get("class", [])) if hasattr(node, "get") else ""
        if any(k in cls for k in ("item", "event", "card", "row", "playbill-item")):
            return node
        if getattr(node, "name", "") == "div" and node.get("class"):
            return node
        node = getattr(node, "parent", None)
    return None


def _extract_date_from_block(block) -> date | None:
    """Ищет дату вида 'DD месяц' в тексте блока или рядом с ним."""
    text = block.get_text(" ")
    m = re.search(r"\b(\d{1,2})\s+([а-яёА-ЯЁ]+)", text)
    if m:
        d = parse_ru_date(f"{m[1]} {m[2]}")
        if d:
            return d
    # Ищем заголовок дня выше по DOM
    node = block
    for _ in range(12):
        node = getattr(node, "previous_sibling", None)
        if node is None:
            break
        if not hasattr(node, "get_text"):
            continue
        txt = node.get_text(strip=True)
        if re.match(r"^\d{1,2}\s+[а-яА-Яё]+", txt) and len(txt) < 50:
            return parse_ru_date(txt)
    return None
# ─────────────────────────────────────────────
# ПАРСЕР: mxat.ru (МХТ им. А.П. Чехова)
# ─────────────────────────────────────────────
#
# Реальная структура (подтверждена из HTML):
#
# Страница спектакля:
#   <h1>Игра в «Городки»</h1>
#   <div id="about">...<div class="x-prose">Описание...</div></div>
#   <dl>...<dt>Цена билета:</dt><dd>от 2000 ₽ до 11500 ₽</dd>...</dl>
#   <div id="tickets">
#     <time datetime="2026-05-21 19:00">21 мая, Чт 19:00</time>
#     <a href="https://spa.profticket.ru/customer/54/shows/162/8249/">Купить билет</a>
#   </div>
#   Галерея: <img src="..."> в галерее
# ─────────────────────────────────────────────

def parse_mxat(cfg: dict) -> dict:
    html = fetch(cfg["url"])
    if not html:
        return error_result(cfg, "Не удалось загрузить страницу")

    s = soup(html)
    result = base_result(cfg)

    # ── Название ───────────────────────────────────────────
    h1 = s.select_one("h1")
    if h1:
        result["name"] = h1.get_text(strip=True)

    # ── Описание ───────────────────────────────────────────
    about = s.select_one("#about .x-prose")
    if not about:
        about = s.select_one(".x-prose")
    if about:
        # Очищаем от HTML тегов
        text = about.get_text(" ", strip=True)
        result["description"] = text[:600]

    # ── Постер ─────────────────────────────────────────────
    # Ищем в галерее первое изображение
    gallery_img = s.select_one("#gallery img, .mxat-gallery img")
    if gallery_img:
        src = gallery_img.get("src", "") or gallery_img.get("data-src", "")
        if src:
            result["image"] = src

    # ── Цены ───────────────────────────────────────────────
    price_dd = s.find("dd", string=re.compile(r"Цена билета:", re.I))
    if price_dd:
        price_text = price_dd.get_text(strip=True)
        pm = re.search(r"от\s*([\d\s]+?)\s*до\s*([\d\s]+?)\s*₽", price_text, re.I)
        if pm:
            result["price_min"] = parse_price(pm[1])
            result["price_max"] = parse_price(pm[2])

    # ── Даты ───────────────────────────────────────────────
    dates = []
    tickets_section = s.select_one("#tickets")
    if tickets_section:
        for time_el in tickets_section.find_all("time", datetime=True):
            dt = time_el.get("datetime", "")
            # Формат: "2026-05-21 19:00"
            m = re.search(r"(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})", dt)
            if m:
                d = date(int(m[1]), int(m[2]), int(m[3]))
                time_str = f"{m[4]}:{m[5]}"
                
                # Проверяем доступность билетов.
                # Билеты продаются через JS-попап на странице спектакля,
                # поэтому buy_url всегда указывает на страницу спектакля.
                parent = time_el.find_parent("div", class_="grid")
                buy_a = parent.find("a", href=True) if parent else None
                available = buy_a is not None and "Купить билет" in buy_a.get_text()
                buy_url = cfg["url"] if available else ""

                if d >= today():
                    dates.append(make_date_entry(
                        d, time_str, available,
                        result["price_min"], result["price_max"],
                        buy_url,
                    ))

    result["dates"] = dates
    return finalize(result)


# ─────────────────────────────────────────────
# ПАРСЕР: vakhtangov.ru (Театр Вахтангова)
# ─────────────────────────────────────────────
#
# Реальная структура (подтверждена из HTML):
#
# Страница спектакля:
#   <header class="cover" id="cover" style="background-image: url(...)">
#   <h1>Ночь перед Рождеством</h1>
#   <section class="ugc"><blockquote>Описание...</blockquote></section>
#   <section class="stage-and-tickets">
#     <ul class="show-afisha">
#       <li>
#         <p class="info"><span class="date">16 мая, суббота, 14:00</span></p>
#         <ul class="btn-list"><li><a href="/tickets/buy/?...">Купить билеты</a></li></ul>
#       </li>
#     </ul>
#     <p>Билеты: от 3300 до 4800 руб.</p>
#   </section>
# ─────────────────────────────────────────────

def parse_vakhtangov(cfg: dict) -> dict:
    html = fetch(cfg["url"])
    if not html:
        return error_result(cfg, "Не удалось загрузить страницу")

    s = soup(html)
    result = base_result(cfg)

    # ── Название ───────────────────────────────────────────
    h1 = s.select_one("h1")
    if h1:
        result["name"] = h1.get_text(strip=True)

    # ── Описание ───────────────────────────────────────────
    ugc = s.select_one("section.ugc")
    if ugc:
        # Берём текст из blockquote или всего блока
        blockquote = ugc.select_one("blockquote")
        if blockquote:
            text = blockquote.get_text(" ", strip=True)
        else:
            text = ugc.get_text(" ", strip=True)
        result["description"] = text[:600]

    # ── Постер ─────────────────────────────────────────────
    cover = s.select_one("#cover")
    if cover:
        # Ищем background-image в стиле
        style = cover.get("style", "")
        m = re.search(r"background-image:\s*url\(([^)]+)\)", style)
        if m:
            result["image"] = m.group(1)
        else:
            # Ищем в медиа галерее
            gallery_img = s.select_one(".thumbs-gallery img")
            if gallery_img:
                result["image"] = gallery_img.get("src", "")

    # ── Цены ───────────────────────────────────────────────
    price_p = s.find(string=re.compile(r"Билеты:.*руб", re.I))
    if price_p:
        price_text = price_p.strip() if isinstance(price_p, str) else price_p.get_text(strip=True)
        pm = re.search(r"от\s*([\d\s]+?)\s*до\s*([\d\s]+?)\s*руб", price_text, re.I)
        if pm:
            result["price_min"] = parse_price(pm[1])
            result["price_max"] = parse_price(pm[2])

    # ── Даты ───────────────────────────────────────────────
    dates = []
    afisha = s.select_one("ul.show-afisha")
    if afisha:
        for li in afisha.find_all("li"):
            # Ищем дату и время
            info = li.select_one("p.info")
            if not info:
                continue

            date_span = info.select_one("span.date")
            time_span = info.select_one("span.time")
            
            if not date_span:
                continue

            date_text = date_span.get_text(strip=True)
            time_text = time_span.get_text(strip=True) if time_span else ""

            d = parse_ru_date(date_text)
            if not d:
                continue

            # Проверяем доступность билетов
            buy_a = li.select_one("a.js-buy-tickets-btn")
            if buy_a:
                href = buy_a.get("href", "")
                buy_url = f"https://vakhtangov.ru{href}" if href.startswith("/") else href
                available = "Купить билеты" in buy_a.get_text()
            else:
                buy_url = cfg["url"]
                available = False

            if d >= today():
                dates.append(make_date_entry(
                    d, time_text, available,
                    result["price_min"], result["price_max"],
                    buy_url,
                ))

    result["dates"] = dates
    return finalize(result)


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
    "mxat":           parse_mxat,
    "vakhtangov":     parse_vakhtangov,
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