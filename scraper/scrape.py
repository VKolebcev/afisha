#!/usr/bin/env python3
"""
Theater Monitor Scraper
Читает config.json, скрапит страницы театров, пишет data/productions.json
"""

import json
import os
import re
import time
import traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

WEEKDAYS_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


def get(url: str, timeout=15) -> BeautifulSoup | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  ✗ GET {url}: {e}")
        return None


def parse_price(text: str) -> int:
    """Извлекает число из строки вида '1 500 ₽' → 1500"""
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else 0


def fmt_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


# ──────────────────────────────────────────────
# ПАРСЕРЫ ПОД КОНКРЕТНЫЕ САЙТЫ
# ──────────────────────────────────────────────

def parse_mxat(config: dict) -> dict:
    """МХТ им. Чехова — mxat.ru"""
    url = config["url"]
    soup = get(url)
    if not soup:
        return error_result(config, "Не удалось загрузить страницу")

    result = base_result(config)

    # Описание
    desc_el = soup.select_one(".performance-detail__description, .b-performance__desc")
    if desc_el:
        result["description"] = desc_el.get_text(" ", strip=True)[:500]

    # Постер
    img_el = soup.select_one(".performance-detail__poster img, .b-performance__poster img")
    if img_el:
        result["image"] = img_el.get("src", "")

    # Расписание
    dates = []
    for row in soup.select(".schedule-table__row, .b-afisha__item"):
        try:
            date_el = row.select_one(".schedule-table__date, .b-afisha__date")
            time_el = row.select_one(".schedule-table__time, .b-afisha__time")
            price_el = row.select_one(".schedule-table__price, .b-afisha__price")
            buy_el = row.select_one("a[href*='buy'], a[href*='ticket'], .btn-buy")
            sold_el = row.select_one(".sold-out, .b-afisha__sold")

            if not date_el:
                continue

            raw_date = date_el.get_text(strip=True)
            dt = parse_russian_date(raw_date)
            if not dt:
                continue

            price_text = price_el.get_text(strip=True) if price_el else ""
            prices = [parse_price(p) for p in re.split(r"[–—-]", price_text) if re.search(r"\d", p)]
            p_min = min(prices) if prices else 0
            p_max = max(prices) if prices else 0

            avail = sold_el is None and buy_el is not None
            buy_url = ""
            if buy_el:
                href = buy_el.get("href", "")
                buy_url = href if href.startswith("http") else f"https://mxat.ru{href}"

            dates.append({
                "date": fmt_date(dt),
                "time": time_el.get_text(strip=True) if time_el else "19:00",
                "weekday": WEEKDAYS_RU[dt.weekday()],
                "available": avail,
                "price_min": p_min,
                "price_max": p_max,
                "buy_url": buy_url,
            })
        except Exception:
            continue

    result["dates"] = sorted(dates, key=lambda d: d["date"])
    return enrich(result)


def parse_kassir(config: dict) -> dict:
    """kassir.ru"""
    url = config["url"]
    soup = get(url)
    if not soup:
        return error_result(config, "Не удалось загрузить страницу")

    result = base_result(config)

    # Попытка найти описание и постер
    desc_el = soup.select_one(".spectacle-description, .event-description, [class*='description']")
    if desc_el:
        result["description"] = desc_el.get_text(" ", strip=True)[:500]

    img_el = soup.select_one(".spectacle-poster img, .event-poster img, [class*='poster'] img")
    if img_el:
        result["image"] = img_el.get("src", "")

    dates = []
    for row in soup.select(".session-row, .event-row, [class*='session']"):
        try:
            date_el = row.select_one("[class*='date']")
            time_el = row.select_one("[class*='time']")
            price_el = row.select_one("[class*='price']")
            buy_el = row.select_one("a[href*='basket'], a[href*='buy'], button[class*='buy']")

            if not date_el:
                continue

            dt = parse_russian_date(date_el.get_text(strip=True))
            if not dt:
                continue

            price_text = price_el.get_text(strip=True) if price_el else ""
            prices = [parse_price(p) for p in re.split(r"[–—-]", price_text) if re.search(r"\d", p)]

            avail = buy_el is not None
            href = buy_el.get("href", "") if buy_el else ""
            buy_url = href if href.startswith("http") else f"https://www.kassir.ru{href}"

            dates.append({
                "date": fmt_date(dt),
                "time": time_el.get_text(strip=True) if time_el else "",
                "weekday": WEEKDAYS_RU[dt.weekday()],
                "available": avail,
                "price_min": min(prices) if prices else 0,
                "price_max": max(prices) if prices else 0,
                "buy_url": buy_url if avail else "",
            })
        except Exception:
            continue

    result["dates"] = sorted(dates, key=lambda d: d["date"])
    return enrich(result)


def parse_generic(config: dict) -> dict:
    """
    Универсальный парсер: ищет JSON-LD, потом пытается вытащить даты эвристически.
    Поддерживает кастомные CSS-селекторы в config:
      "selectors": {
        "date": ".my-date-class",
        "time": ".my-time-class",
        "price": ".my-price",
        "buy": "a.buy-btn",
        "description": ".desc",
        "image": ".poster img"
      }
    """
    url = config["url"]
    soup = get(url)
    if not soup:
        return error_result(config, "Не удалось загрузить страницу")

    result = base_result(config)
    sel = config.get("selectors", {})

    # JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0]
            if data.get("@type") in ("TheaterEvent", "Event", "MusicEvent"):
                result["description"] = result["description"] or data.get("description", "")[:500]
                if not result["image"]:
                    img = data.get("image", "")
                    result["image"] = img[0] if isinstance(img, list) else img
        except Exception:
            pass

    # Описание / изображение по CSS
    desc_el = soup.select_one(sel.get("description", ".description, .about, [class*='desc']"))
    if desc_el and not result["description"]:
        result["description"] = desc_el.get_text(" ", strip=True)[:500]

    img_el = soup.select_one(sel.get("image", "[class*='poster'] img, [class*='cover'] img"))
    if img_el and not result["image"]:
        result["image"] = img_el.get("src", "")

    # Расписание
    date_sel = sel.get("date", "[class*='date'], [class*='Date']")
    time_sel = sel.get("time", "[class*='time'], [class*='Time']")
    price_sel = sel.get("price", "[class*='price'], [class*='Price']")
    buy_sel = sel.get("buy", "a[href*='buy'], a[href*='ticket'], a[href*='basket']")

    dates = []
    # Пробуем найти строки расписания
    row_selectors = [
        "[class*='session']", "[class*='schedule']",
        "[class*='afisha']", "[class*='event-item']",
    ]
    rows = []
    for rs in row_selectors:
        rows = soup.select(rs)
        if len(rows) > 1:
            break

    for row in rows:
        try:
            date_el = row.select_one(date_sel)
            time_el = row.select_one(time_sel)
            price_el = row.select_one(price_sel)
            buy_el = row.select_one(buy_sel)

            if not date_el:
                continue
            dt = parse_russian_date(date_el.get_text(strip=True))
            if not dt:
                continue

            price_text = price_el.get_text(strip=True) if price_el else ""
            prices = [parse_price(p) for p in re.split(r"[–—\-]", price_text) if re.search(r"\d", p)]

            avail = buy_el is not None
            href = buy_el.get("href", "") if buy_el else ""
            if href and not href.startswith("http"):
                from urllib.parse import urljoin
                href = urljoin(url, href)

            dates.append({
                "date": fmt_date(dt),
                "time": time_el.get_text(strip=True) if time_el else "",
                "weekday": WEEKDAYS_RU[dt.weekday()],
                "available": avail,
                "price_min": min(prices) if prices else 0,
                "price_max": max(prices) if prices else 0,
                "buy_url": href if avail else "",
            })
        except Exception:
            continue

    result["dates"] = sorted(dates, key=lambda d: d["date"])
    return enrich(result)


# ──────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ──────────────────────────────────────────────

MONTHS_RU = {
    "янв": 1, "фев": 2, "мар": 3, "апр": 4, "май": 5, "мая": 5,
    "июн": 6, "июл": 7, "авг": 8, "сен": 9, "окт": 10, "ноя": 11, "дек": 12,
}


def parse_russian_date(text: str) -> datetime | None:
    """Парсит '10 мая 2026', '10 мая', '10.05.2026', '2026-05-10' и т.п."""
    text = text.strip().lower()
    now = datetime.now()

    # ISO
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return datetime(int(m[1]), int(m[2]), int(m[3]))

    # DD.MM.YYYY или DD.MM.YY
    m = re.search(r"(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{2,4})", text)
    if m:
        y = int(m[3])
        if y < 100:
            y += 2000
        return datetime(y, int(m[2]), int(m[1]))

    # DD мес YYYY или DD мес
    m = re.search(r"(\d{1,2})\s+([а-я]+)(?:\s+(\d{4}))?", text)
    if m:
        day = int(m[1])
        mon_str = m[2][:3]
        mon = next((v for k, v in MONTHS_RU.items() if mon_str.startswith(k[:3])), None)
        if not mon:
            return None
        year = int(m[3]) if m[3] else now.year
        try:
            dt = datetime(year, mon, day)
            # Если дата уже прошла и год не указан — берём следующий год
            if not m[3] and dt < now - timedelta(days=1):
                dt = datetime(year + 1, mon, day)
            return dt
        except ValueError:
            return None

    return None


def base_result(config: dict) -> dict:
    return {
        "id": config["id"],
        "name": config["name"],
        "theater": config.get("theater", ""),
        "description": config.get("description", ""),
        "image": config.get("image", ""),
        "url": config["url"],
        "available": None,
        "price_min": 0,
        "price_max": 0,
        "currency": "₽",
        "error": None,
        "dates": [],
    }


def error_result(config: dict, msg: str) -> dict:
    r = base_result(config)
    r["error"] = msg
    return r


def enrich(result: dict) -> dict:
    """Вычисляет доступность и ценовой диапазон из списка дат."""
    future_dates = [
        d for d in result["dates"]
        if d["date"] >= fmt_date(datetime.now())
    ]
    result["dates"] = future_dates

    available_dates = [d for d in future_dates if d["available"]]
    result["available"] = len(available_dates) > 0

    all_prices = [d["price_min"] for d in available_dates if d["price_min"]] + \
                 [d["price_max"] for d in available_dates if d["price_max"]]
    if all_prices:
        result["price_min"] = min(all_prices)
        result["price_max"] = max(all_prices)

    return result


PARSERS = {
    "mxat": parse_mxat,
    "kassir": parse_kassir,
    "generic": parse_generic,
}


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(root, "config.json")
    output_path = os.path.join(root, "data", "productions.json")

    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)

    tz = ZoneInfo(config.get("settings", {}).get("timezone", "Europe/Moscow"))
    now_str = datetime.now(tz).isoformat(timespec="seconds")

    productions = []
    for prod in config["productions"]:
        parser_key = prod.get("parser", "generic")
        parser_fn = PARSERS.get(parser_key, parse_generic)
        print(f"⏳ [{parser_key}] {prod['name']} ({prod['url']})")
        try:
            result = parser_fn(prod)
            n_dates = len(result["dates"])
            n_avail = sum(1 for d in result["dates"] if d["available"])
            print(f"  ✓ дат: {n_dates}, доступно: {n_avail}, цены: {result['price_min']}–{result['price_max']} ₽")
        except Exception as e:
            traceback.print_exc()
            result = error_result(prod, str(e))
        productions.append(result)
        time.sleep(1.5)  # вежливая пауза между запросами

    output = {
        "last_updated": now_str,
        "productions": productions,
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Записано {len(productions)} постановок → {output_path}")


if __name__ == "__main__":
    main()