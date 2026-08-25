#!/usr/bin/env python3
"""Fetch last 12 completed weeks of Stores ODR data and refresh data.js."""
from __future__ import annotations

import json
import os
import ssl
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
UA_MONTH = {
    1: "січ", 2: "лют", 3: "бер", 4: "квіт", 5: "трав", 6: "черв",
    7: "лип", 8: "серп", 9: "вер", 10: "жовт", 11: "лист", 12: "груд",
}
EN_MONTH = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}
CHART_BRANDS = [
    "VARUS", "KOPIYKA", "SANTIM", "RUKAVYCHKA", "TAISTRA",
    "KOPIYKA MINI", "CAFE RYNOK", "LOKO", "HOP HEY",
]
TABLE_A_BRANDS = [
    "VARUS", "LOKO", "HOP HEY", "KOPIYKA", "TAISTRA", "CAFE RYNOK",
    "RUKAVYCHKA", "SANTIM", "KOPIYKA MINI", "RODYNNA KOVBASKA",
    "ANRI-PHARM", "BRSM", "NO TABOO",
]
COUNTRY_META = {
    "ro": {"ua": "Румунія", "en": "Romania"},
    "ee": {"ua": "Естонія", "en": "Estonia"},
    "lt": {"ua": "Литва", "en": "Lithuania"},
    "ua": {"ua": "Україна", "en": "Ukraine"},
}


def _load_dotenv():
    candidates = [
        SCRIPT_DIR / ".env",
        SCRIPT_DIR.parent / "VARUS" / ".env",
        SCRIPT_DIR.parent / "Stores-internal-weekly-report" / ".env",
    ]
    for env_file in candidates:
        if not env_file.exists():
            continue
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
        break


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"Missing {name}", file=sys.stderr)
        sys.exit(1)
    return value


_load_dotenv()
HOST = _require("DATABRICKS_HOST").replace("https://", "")
TOKEN = _require("DATABRICKS_TOKEN")
WID = _require("DATABRICKS_WAREHOUSE_ID")
_CTX = ssl.create_default_context()
if os.environ.get("DATABRICKS_TLS_NO_VERIFY", "").strip().lower() in ("1", "true", "yes"):
    _CTX.check_hostname = False
    _CTX.verify_mode = ssl.CERT_NONE


WINDOW = """
  p.delivery_vertical LIKE 'store_%'
  AND b.order_state = 'delivered'
  AND b.basket_item_is_dish = true
  AND b.order_created_date >= DATE_ADD(DATE_TRUNC('week', CURRENT_DATE()), -84)
  AND b.order_created_date < DATE_TRUNC('week', CURRENT_DATE())
"""
BRAND = """CASE WHEN p.brand_name IN ('KOPIYKA','KOPIYKA MINI','SANTIM')
            THEN p.brand_name ELSE COALESCE(NULLIF(p.group_name,''), p.brand_name) END"""
GROUP = "COALESCE(NULLIF(p.group_name,''), p.brand_name)"


def run(sql: str):
    body = json.dumps({
        "warehouse_id": WID,
        "statement": sql,
        "wait_timeout": "50s",
        "format": "JSON_ARRAY",
    }).encode()
    req = urllib.request.Request(
        f"https://{HOST}/api/2.0/sql/statements",
        data=body,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    data = json.load(urllib.request.urlopen(req, context=_CTX))
    sid = data["statement_id"]
    while data["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        poll = urllib.request.Request(
            f"https://{HOST}/api/2.0/sql/statements/{sid}",
            headers={"Authorization": f"Bearer {TOKEN}"},
        )
        data = json.load(urllib.request.urlopen(poll, context=_CTX))
    if data["status"]["state"] != "SUCCEEDED":
        raise SystemExit("Query failed: " + json.dumps(data.get("status")))
    cols = [c["name"] for c in data["manifest"]["schema"]["columns"]]
    rows = data.get("result", {}).get("data_array") or []
    out = []
    for row in rows:
        rec = {}
        for col, val in zip(cols, row):
            rec[col] = None if val is None else val
        out.append(rec)
    return out


def fnum(v, nd=1):
    if v is None or v == "":
        return None
    return round(float(v), nd)


def fint(v):
    if v is None or v == "":
        return 0
    return int(float(v))


def as_date(v) -> date:
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    text = str(v)[:10]
    return datetime.strptime(text, "%Y-%m-%d").date()


def week_label(d: date, lang: str) -> str:
    months = UA_MONTH if lang == "ua" else EN_MONTH
    return f"{d.day} {months[d.month]}"


def long_date(d: date, lang: str) -> str:
    months_ua = {
        1: "січня", 2: "лютого", 3: "березня", 4: "квітня", 5: "травня", 6: "червня",
        7: "липня", 8: "серпня", 9: "вересня", 10: "жовтня", 11: "листопада", 12: "грудня",
    }
    months_en = {
        1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
        7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December",
    }
    if lang == "ua":
        return f"{d.day} {months_ua[d.month]} {d.year}"
    return f"{d.day} {months_en[d.month]} {d.year}"


def status_of(avg, last, delta):
    if avg is None:
        return "watch"
    if avg >= 30 or (last or 0) >= 40:
        return "crit"
    if avg >= 20 or (last or 0) >= 28:
        return "high"
    if avg < 10:
        return "good"
    if (delta or 0) <= -3:
        return "watch"
    return "watch"


def partner_copy(row):
    b, avg, last, delta, qty, repl, wt, orepl = (
        row["b"], row["avg"], row["last"], row["d"], row["qty"], row["repl"], row["wt"], row["orepl"]
    )
    dtxt = f"{delta:+.1f}"
    ua_problem, ua_action, en_problem, en_action = "", "", "", ""
    if b == "VARUS":
        ua_problem = (
            f"Визначає ринковий ODR: найбільший grocery-обсяг, середній ODR {avg:.1f}%, "
            f"останній тиждень {last:.1f}% ({dtxt} п.п. за 12 тижнів). Quantity {qty:.1f}% + "
            f"order replacement {orepl:.1f}%."
        )
        ua_action = (
            "1) Прибрати пакети з quantity defect. 2) Deep-dive найгірших магазинів. "
            "3) Daily OOS sync по напоях. 4) Ціль ODR <35% за 8 тижнів."
        )
        en_problem = (
            f"Sets market ODR: largest grocery volume, average ODR {avg:.1f}%, "
            f"latest week {last:.1f}% ({dtxt} pp over 12 weeks). Quantity {qty:.1f}% plus "
            f"order replacement {orepl:.1f}%."
        )
        en_action = (
            "1) Remove bags from quantity defect. 2) Deep-dive the worst stores. "
            "3) Daily OOS sync on beverages. 4) Target ODR <35% within 8 weeks."
        )
    elif b == "LOKO":
        ua_problem = (
            f"Найкращий великий партнер: середній ODR {avg:.1f}%, замін немає. "
            f"Останній тиждень {last:.1f}% ({dtxt} п.п.)."
        )
        ua_action = "Зафіксувати процеси наявності як playbook для grocery. Тримати ODR <7%."
        en_problem = (
            f"Best large partner: average ODR {avg:.1f}%, no replacements. "
            f"Latest week {last:.1f}% ({dtxt} pp)."
        )
        en_action = "Document availability processes as a grocery playbook. Keep ODR below 7%."
    elif b == "HOP HEY":
        ua_problem = (
            f"ODR низький і стабільний (сер. {avg:.1f}%, ост. {last:.1f}%), "
            f"але order replacement {orepl:.1f}%."
        )
        ua_action = "Не чіпати ODR. Перевірити каталог і правила замін."
        en_problem = (
            f"ODR is low and stable (avg {avg:.1f}%, last {last:.1f}%), "
            f"but order replacement is {orepl:.1f}%."
        )
        en_action = "Leave ODR alone. Review the catalog and substitution rules."
    elif b == "KOPIYKA":
        ua_problem = (
            f"Високий grocery ODR: сер. {avg:.1f}%, останній тиждень {last:.1f}% ({dtxt} п.п.). "
            f"Quantity {qty:.1f}%, order replacement {orepl:.1f}%."
        )
        ua_action = "Weekly ops review з AM. Root-cause по OOS у напоях і снеках. Ціль <28%."
        en_problem = (
            f"High grocery ODR: avg {avg:.1f}%, latest week {last:.1f}% ({dtxt} pp). "
            f"Quantity {qty:.1f}%, order replacement {orepl:.1f}%."
        )
        en_action = "Weekly ops review with the AM. Root-cause OOS in beverages and snacks. Target <28%."
    elif b == "TAISTRA":
        ua_problem = f"Прогрес {dtxt} п.п. (перший тиждень → {last:.1f}%). Quantity {qty:.1f}%."
        ua_action = "Закріпити прогрес і розібрати кейс як приклад для grocery."
        en_problem = f"Improvement {dtxt} pp (first week → {last:.1f}%). Quantity {qty:.1f}%."
        en_action = "Lock in the progress and use the case as a grocery reference."
    elif b == "CAFE RYNOK":
        ua_problem = (
            f"Помірний ODR (сер. {avg:.1f}%, ост. {last:.1f}%). Item replacement {repl:.1f}%, "
            f"order replacement {orepl:.1f}%."
        )
        ua_action = "Catalog audit: чи replacement реальний, чи артефакт меню."
        en_problem = (
            f"Moderate ODR (avg {avg:.1f}%, last {last:.1f}%). Item replacement {repl:.1f}%, "
            f"order replacement {orepl:.1f}%."
        )
        en_action = "Catalog audit: confirm whether replacements are real or a menu artefact."
    elif b == "RUKAVYCHKA":
        ua_problem = f"ODR сер. {avg:.1f}%, останній тиждень {last:.1f}% ({dtxt} п.п.). Quantity {qty:.1f}%."
        ua_action = "Продовжити дисципліну наявності. Порівняти процеси з TAISTRA."
        en_problem = f"ODR avg {avg:.1f}%, latest week {last:.1f}% ({dtxt} pp). Quantity {qty:.1f}%."
        en_action = "Keep availability discipline. Compare processes with TAISTRA."
    elif b == "SANTIM":
        ua_problem = (
            f"Високий ODR: сер. {avg:.1f}%, останній тиждень {last:.1f}% ({dtxt} п.п.). "
            f"Quantity {qty:.1f}%, weight {wt:.1f}%."
        )
        ua_action = "Спільний review з KOPIYKA. Точність вагових позицій плюс OOS-контроль."
        en_problem = (
            f"High ODR: avg {avg:.1f}%, latest week {last:.1f}% ({dtxt} pp). "
            f"Quantity {qty:.1f}%, weight {wt:.1f}%."
        )
        en_action = "Joint review with KOPIYKA. Weighted-item accuracy plus OOS control."
    elif b == "KOPIYKA MINI":
        ua_problem = (
            f"Волатильний mini-формат: сер. {avg:.1f}%, останній тиждень {last:.1f}% ({dtxt} п.п.)."
        )
        ua_action = "Стабілізувати наявність у mini-форматі, не дати підтягнутись до KOPIYKA."
        en_problem = (
            f"Volatile mini format: avg {avg:.1f}%, latest week {last:.1f}% ({dtxt} pp)."
        )
        en_action = "Stabilise mini-format availability; do not let it drift to KOPIYKA's level."
    elif b == "RODYNNA KOVBASKA":
        ua_problem = (
            f"Найгірші item-метрики: quantity {qty:.1f}%, weight {wt:.1f}%, ODR сер. {avg:.1f}%."
        )
        ua_action = "Пріоритет — вагові SKU. Не масштабувати, доки ODR >30%."
        en_problem = (
            f"Worst item-level metrics: quantity {qty:.1f}%, weight {wt:.1f}%, ODR avg {avg:.1f}%."
        )
        en_action = "Priority is weighted SKUs. Do not scale while ODR stays above 30%."
    elif b == "ANRI-PHARM":
        ua_problem = f"Аптечний формат: ODR сер. {avg:.1f}%, останній тиждень {last:.1f}%."
        ua_action = "Тримати поточний рівень, окремих дій не потрібно."
        en_problem = f"Pharmacy format: ODR avg {avg:.1f}%, latest week {last:.1f}%."
        en_action = "Hold the current level; no dedicated actions needed."
    elif b == "BRSM":
        ua_problem = f"Малий обсяг, ODR сер. {avg:.1f}%, останній тиждень {last:.1f}% ({dtxt} п.п.)."
        ua_action = "Моніторинг. Якщо ODR стабільно >10% — перевірка наявності на АЗС-форматі."
        en_problem = f"Small volume, ODR avg {avg:.1f}%, latest week {last:.1f}% ({dtxt} pp)."
        en_action = "Monitor. If ODR stays above 10%, run an availability check on the fuel-station format."
    else:
        ua_problem = f"Малий обсяг. ODR сер. {avg:.1f}%, останній тиждень {last:.1f}% ({dtxt} п.п.)."
        ua_action = "Низький пріоритет, підтримувати рівень."
        en_problem = f"Small volume. ODR avg {avg:.1f}%, latest week {last:.1f}% ({dtxt} pp)."
        en_action = "Low priority; maintain the current level."
    row["problem_ua"] = ua_problem
    row["action_ua"] = ua_action
    row["problem_en"] = en_problem
    row["action_en"] = en_action
    return row


def mean(vals):
    vals = [v for v in vals if v is not None]
    return round(sum(vals) / len(vals), 1) if vals else 0.0


def fetch():
    print("Fetching market weekly…")
    market = run(f"""
      SELECT CAST(DATE_TRUNC('week', b.order_created_date) AS DATE) AS week,
        COUNT(DISTINCT b.order_id) AS orders,
        ROUND(COUNT(DISTINCT CASE WHEN b.has_item_quantity_adjustment_with_eater_impact
          OR b.has_item_weighted_adjustment_with_eater_impact
          OR b.has_item_price_adjustment_with_price_increase THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS odr,
        ROUND(COUNT(DISTINCT CASE WHEN b.is_item_replacement THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS repl,
        ROUND(COUNT(DISTINCT CASE WHEN b.has_item_quantity_adjustment_with_eater_impact THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS qty,
        ROUND(COUNT(DISTINCT CASE WHEN b.has_item_weighted_adjustment_with_eater_impact THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS weight,
        ROUND(COUNT(DISTINCT CASE WHEN b.has_item_price_adjustment_with_price_increase THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS price
      FROM main.ng_delivery.dim_basket_item_delivery b
      JOIN main.ng_delivery.dim_provider_v2 p ON b.provider_id = p.provider_id
      WHERE p.country_code = 'ua' AND {WINDOW}
      GROUP BY 1 ORDER BY 1
    """)

    print("Fetching defect volumes…")
    volumes = run(f"""
      SELECT CAST(DATE_TRUNC('week', b.order_created_date) AS DATE) AS week,
        SUM(CASE WHEN b.has_item_quantity_adjustment_with_eater_impact THEN 1 ELSE 0 END) AS qty,
        SUM(CASE WHEN b.has_item_weighted_adjustment THEN 1 ELSE 0 END) AS weight,
        SUM(CASE WHEN b.has_item_price_adjustment THEN 1 ELSE 0 END) AS price
      FROM main.ng_delivery.dim_basket_item_delivery b
      JOIN main.ng_delivery.dim_provider_v2 p ON b.provider_id = p.provider_id
      WHERE p.country_code = 'ua' AND {WINDOW}
      GROUP BY 1 ORDER BY 1
    """)

    print("Fetching brand weekly ODR…")
    brand_weeks = run(f"""
      SELECT CAST(DATE_TRUNC('week', b.order_created_date) AS DATE) AS week,
        {BRAND} AS brand,
        ROUND(COUNT(DISTINCT CASE WHEN b.has_item_quantity_adjustment_with_eater_impact
          OR b.has_item_weighted_adjustment_with_eater_impact
          OR b.has_item_price_adjustment_with_price_increase THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS odr
      FROM main.ng_delivery.dim_basket_item_delivery b
      JOIN main.ng_delivery.dim_provider_v2 p ON b.provider_id = p.provider_id
      WHERE p.country_code = 'ua' AND {WINDOW}
        AND {BRAND} IN ({",".join("'" + b.replace("'", "''") + "'" for b in CHART_BRANDS + TABLE_A_BRANDS)})
      GROUP BY 1, 2 ORDER BY 1, 2
    """)

    print("Fetching partner item metrics…")
    partners = run(f"""
      WITH x AS (
        SELECT {BRAND} AS brand, b.order_id, b.basket_item_state,
          b.has_item_quantity_adjustment_with_eater_impact AS qty_d,
          b.has_item_weighted_adjustment_with_eater_impact AS wt_d,
          b.has_item_price_adjustment_with_price_increase AS price_d,
          b.is_item_replacement AS repl_d,
          CAST(DATE_TRUNC('week', b.order_created_date) AS DATE) AS week
        FROM main.ng_delivery.dim_basket_item_delivery b
        JOIN main.ng_delivery.dim_provider_v2 p ON b.provider_id = p.provider_id
        WHERE p.country_code = 'ua' AND {WINDOW}
      )
      SELECT brand,
        SUM(CASE WHEN basket_item_state = 'active' THEN 1 ELSE 0 END) AS items,
        ROUND(SUM(CASE WHEN qty_d THEN 1 ELSE 0 END) * 100.0
          / NULLIF(SUM(CASE WHEN basket_item_state = 'active' THEN 1 ELSE 0 END), 0), 1) AS qty_item,
        ROUND(SUM(CASE WHEN repl_d THEN 1 ELSE 0 END) * 100.0
          / NULLIF(SUM(CASE WHEN basket_item_state = 'active' THEN 1 ELSE 0 END), 0), 1) AS repl_item,
        ROUND(SUM(CASE WHEN wt_d THEN 1 ELSE 0 END) * 100.0
          / NULLIF(SUM(CASE WHEN basket_item_state = 'active' THEN 1 ELSE 0 END), 0), 1) AS wt_item,
        ROUND(COUNT(DISTINCT CASE WHEN qty_d OR wt_d OR price_d THEN order_id END) * 100.0
          / NULLIF(COUNT(DISTINCT order_id), 0), 1) AS odr_avg,
        ROUND(COUNT(DISTINCT CASE WHEN week = DATE_ADD(DATE_TRUNC('week', CURRENT_DATE()), -7)
          AND (qty_d OR wt_d OR price_d) THEN order_id END) * 100.0
          / NULLIF(COUNT(DISTINCT CASE WHEN week = DATE_ADD(DATE_TRUNC('week', CURRENT_DATE()), -7)
          THEN order_id END), 0), 1) AS odr_last,
        ROUND(COUNT(DISTINCT CASE WHEN week = DATE_ADD(DATE_TRUNC('week', CURRENT_DATE()), -84)
          AND (qty_d OR wt_d OR price_d) THEN order_id END) * 100.0
          / NULLIF(COUNT(DISTINCT CASE WHEN week = DATE_ADD(DATE_TRUNC('week', CURRENT_DATE()), -84)
          THEN order_id END), 0), 1) AS odr_first,
        ROUND(COUNT(DISTINCT CASE WHEN repl_d THEN order_id END) * 100.0
          / NULLIF(COUNT(DISTINCT order_id), 0), 1) AS orepl
      FROM x GROUP BY 1
    """)

    print("Fetching top-15…")
    top15 = run(f"""
      SELECT {GROUP} AS brand, COUNT(DISTINCT b.order_id) AS orders,
        ROUND(COUNT(DISTINCT CASE WHEN b.has_item_quantity_adjustment_with_eater_impact
          OR b.has_item_weighted_adjustment_with_eater_impact
          OR b.has_item_price_adjustment_with_price_increase THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS odr,
        ROUND(COUNT(DISTINCT CASE WHEN b.has_item_quantity_adjustment_with_eater_impact THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS qty,
        ROUND(COUNT(DISTINCT CASE WHEN b.has_item_weighted_adjustment_with_eater_impact THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS weight,
        ROUND(COUNT(DISTINCT CASE WHEN b.has_item_price_adjustment_with_price_increase THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS price,
        ROUND(COUNT(DISTINCT CASE WHEN b.is_item_replacement THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS repl
      FROM main.ng_delivery.dim_basket_item_delivery b
      JOIN main.ng_delivery.dim_provider_v2 p ON b.provider_id = p.provider_id
      WHERE p.country_code = 'ua' AND {WINDOW}
      GROUP BY 1 ORDER BY orders DESC LIMIT 15
    """)

    print("Fetching countries…")
    countries = run(f"""
      SELECT p.country_code, COUNT(DISTINCT b.order_id) AS orders,
        ROUND(COUNT(DISTINCT CASE WHEN b.has_item_quantity_adjustment_with_eater_impact
          OR b.has_item_weighted_adjustment_with_eater_impact
          OR b.has_item_price_adjustment_with_price_increase THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS odr,
        ROUND(COUNT(DISTINCT CASE WHEN b.has_item_quantity_adjustment_with_eater_impact THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS qty,
        ROUND(COUNT(DISTINCT CASE WHEN b.has_item_weighted_adjustment_with_eater_impact THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS weight,
        ROUND(COUNT(DISTINCT CASE WHEN b.has_item_price_adjustment_with_price_increase THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS price,
        ROUND(COUNT(DISTINCT CASE WHEN b.is_item_replacement THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS repl
      FROM main.ng_delivery.dim_basket_item_delivery b
      JOIN main.ng_delivery.dim_provider_v2 p ON b.provider_id = p.provider_id
      WHERE p.country_code IN ('ua','ro','ee','lt') AND {WINDOW}
      GROUP BY 1 ORDER BY orders DESC
    """)

    print("Fetching country top-5…")
    country_top = run(f"""
      WITH base AS (
        SELECT p.country_code, {GROUP} AS brand, b.order_id,
          b.has_item_quantity_adjustment_with_eater_impact
            OR b.has_item_weighted_adjustment_with_eater_impact
            OR b.has_item_price_adjustment_with_price_increase AS defect,
          b.is_item_replacement AS repl
        FROM main.ng_delivery.dim_basket_item_delivery b
        JOIN main.ng_delivery.dim_provider_v2 p ON b.provider_id = p.provider_id
        WHERE p.country_code IN ('ua','ro','ee','lt') AND {WINDOW}
      ), ranked AS (
        SELECT country_code, brand, COUNT(DISTINCT order_id) AS orders,
          ROUND(COUNT(DISTINCT CASE WHEN defect THEN order_id END) * 100.0
            / NULLIF(COUNT(DISTINCT order_id), 0), 1) AS odr,
          ROUND(COUNT(DISTINCT CASE WHEN repl THEN order_id END) * 100.0
            / NULLIF(COUNT(DISTINCT order_id), 0), 1) AS repl,
          ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY COUNT(DISTINCT order_id) DESC) AS rn
        FROM base GROUP BY 1, 2
      )
      SELECT * FROM ranked WHERE rn <= 5 ORDER BY country_code, rn
    """)

    print("Fetching VARUS stores…")
    stores = run(f"""
      SELECT COALESCE(p.provider_address, p.provider_name) AS store,
        SUM(CASE WHEN b.basket_item_state = 'active' THEN 1 ELSE 0 END) AS items,
        ROUND(SUM(CASE WHEN b.has_item_quantity_adjustment_with_eater_impact THEN 1 ELSE 0 END) * 100.0
          / NULLIF(SUM(CASE WHEN b.basket_item_state = 'active' THEN 1 ELSE 0 END), 0), 1) AS qty,
        ROUND(SUM(CASE WHEN b.is_item_replacement THEN 1 ELSE 0 END) * 100.0
          / NULLIF(SUM(CASE WHEN b.basket_item_state = 'active' THEN 1 ELSE 0 END), 0), 1) AS repl
      FROM main.ng_delivery.dim_basket_item_delivery b
      JOIN main.ng_delivery.dim_provider_v2 p ON b.provider_id = p.provider_id
      WHERE p.country_code = 'ua' AND {GROUP} = 'VARUS' AND {WINDOW}
      GROUP BY 1
      HAVING SUM(CASE WHEN b.basket_item_state = 'active' THEN 1 ELSE 0 END) >= 500
      ORDER BY (SUM(CASE WHEN b.has_item_quantity_adjustment_with_eater_impact THEN 1 ELSE 0 END)
        + SUM(CASE WHEN b.is_item_replacement THEN 1 ELSE 0 END)) * 1.0
        / NULLIF(SUM(CASE WHEN b.basket_item_state = 'active' THEN 1 ELSE 0 END), 0) DESC
      LIMIT 8
    """)

    print("Fetching categories…")
    cats = run(f"""
      SELECT COALESCE(c.name, 'Uncategorised') AS cat,
        SUM(CASE WHEN b.basket_item_state = 'active' THEN 1 ELSE 0 END) AS items,
        ROUND(SUM(CASE WHEN b.has_item_quantity_adjustment_with_eater_impact THEN 1 ELSE 0 END) * 100.0
          / NULLIF(SUM(CASE WHEN b.basket_item_state = 'active' THEN 1 ELSE 0 END), 0), 1) AS qty,
        ROUND(SUM(CASE WHEN b.is_item_replacement THEN 1 ELSE 0 END) * 100.0
          / NULLIF(SUM(CASE WHEN b.basket_item_state = 'active' THEN 1 ELSE 0 END), 0), 1) AS repl,
        SUM(CASE WHEN b.has_item_quantity_adjustment_with_eater_impact THEN 1 ELSE 0 END)
          + SUM(CASE WHEN b.is_item_replacement THEN 1 ELSE 0 END) AS defects
      FROM main.ng_delivery.dim_basket_item_delivery b
      JOIN main.ng_delivery.dim_provider_v2 p ON b.provider_id = p.provider_id
      LEFT JOIN main.ng_delivery.etl_delivery_sct_category c ON b.sct_category_id = c.id
      WHERE p.country_code = 'ua' AND {WINDOW}
      GROUP BY 1
      ORDER BY defects DESC
      LIMIT 10
    """)

    print("Fetching period totals…")
    totals = run(f"""
      SELECT COUNT(DISTINCT b.order_id) AS orders,
        ROUND(COUNT(DISTINCT CASE WHEN b.has_item_quantity_adjustment_with_eater_impact
          OR b.has_item_weighted_adjustment_with_eater_impact
          OR b.has_item_price_adjustment_with_price_increase THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS odr,
        ROUND(COUNT(DISTINCT CASE WHEN b.is_item_replacement THEN b.order_id END)
          * 100.0 / NULLIF(COUNT(DISTINCT b.order_id), 0), 1) AS repl
      FROM main.ng_delivery.dim_basket_item_delivery b
      JOIN main.ng_delivery.dim_provider_v2 p ON b.provider_id = p.provider_id
      WHERE p.country_code = 'ua' AND {WINDOW}
    """)[0]

    return {
        "market": market, "volumes": volumes, "brand_weeks": brand_weeks,
        "partners": partners, "top15": top15, "countries": countries,
        "country_top": country_top, "stores": stores, "cats": cats, "totals": totals,
    }


def build(raw):
    market = sorted(raw["market"], key=lambda r: as_date(r["week"]))
    volumes = {as_date(r["week"]): r for r in raw["volumes"]}
    weeks = [as_date(r["week"]) for r in market]
    first, last = weeks[0], weeks[-1]
    last_odr = fnum(market[-1]["odr"])
    first_odr = fnum(market[0]["odr"])
    last_orders = fint(market[-1]["orders"])
    first_orders = fint(market[0]["orders"])
    prev_odr = fnum(market[-2]["odr"]) if len(market) > 1 else last_odr
    prev_orders = fint(market[-2]["orders"]) if len(market) > 1 else last_orders

    qty_vol = [fint(volumes[w]["qty"]) for w in weeks]
    wt_vol = [fint(volumes[w]["weight"]) for w in weeks]
    price_vol = [fint(volumes[w]["price"]) for w in weeks]
    vol_sum = sum(qty_vol) + sum(wt_vol) + sum(price_vol)
    qty_share = round(100.0 * sum(qty_vol) / vol_sum, 1) if vol_sum else 0
    wt_share = round(100.0 * sum(wt_vol) / vol_sum, 1) if vol_sum else 0
    price_share = round(100.0 * sum(price_vol) / vol_sum, 1) if vol_sum else 0

    brand_map = defaultdict(dict)
    for r in raw["brand_weeks"]:
        brand_map[r["brand"]][as_date(r["week"])] = fnum(r["odr"])
    brand_odr = {
        b: [brand_map[b].get(w) for w in weeks]
        for b in CHART_BRANDS
        if b in brand_map
    }

    pmap = {r["brand"]: r for r in raw["partners"]}
    partners = []
    for name in TABLE_A_BRANDS:
        r = pmap.get(name)
        if not r:
            continue
        avg, last_v, first_v = fnum(r["odr_avg"]), fnum(r["odr_last"]), fnum(r["odr_first"])
        delta = round((last_v or 0) - (first_v or 0), 1)
        row = {
            "b": name,
            "items": fint(r["items"]),
            "avg": avg or 0,
            "last": last_v or 0,
            "d": delta,
            "qty": fnum(r["qty_item"]) or 0,
            "repl": fnum(r["repl_item"]) or 0,
            "wt": fnum(r["wt_item"]) or 0,
            "orepl": fnum(r["orepl"]) or 0,
            "st": status_of(avg, last_v, delta),
        }
        partners.append(partner_copy(row))

    dbx15 = []
    for r in raw["top15"]:
        dbx15.append([
            r["brand"], fint(r["orders"]), fnum(r["odr"]) or 0, fnum(r["qty"]) or 0,
            fnum(r["weight"]) or 0, fnum(r["price"]) or 0, fnum(r["repl"]) or 0,
        ])

    top_by_cc = defaultdict(list)
    for r in raw["country_top"]:
        label = r["brand"]
        if "BOLT MARKET" in (label or "").upper():
            label = "BOLT MARKET (1P)"
        top_by_cc[r["country_code"]].append([
            label, fint(r["orders"]), fnum(r["odr"]) or 0, fnum(r["repl"]) or 0,
        ])

    countries = []
    for i, r in enumerate(raw["countries"], 1):
        cc = r["country_code"]
        tops = top_by_cc.get(cc, [])
        top5 = mean([x[2] for x in tops])
        top5x = mean([x[2] for x in tops if "1P" not in x[0]])
        countries.append({
            "rank": i,
            "code": cc,
            "name_ua": COUNTRY_META[cc]["ua"],
            "name_en": COUNTRY_META[cc]["en"],
            "orders": fint(r["orders"]),
            "odr": fnum(r["odr"]) or 0,
            "qty": fnum(r["qty"]) or 0,
            "wt": fnum(r["weight"]) or 0,
            "price": fnum(r["price"]) or 0,
            "repl": fnum(r["repl"]) or 0,
            "top5": top5,
            "top5x": top5x,
            "partners": tops,
        })

    stores = []
    for r in raw["stores"]:
        stores.append([r["store"] or "—", fint(r["items"]), fnum(r["qty"]) or 0, fnum(r["repl"]) or 0])

    cats = []
    for r in raw["cats"]:
        cats.append([
            r["cat"] or "Uncategorised", fint(r["items"]),
            fnum(r["qty"]) or 0, fnum(r["repl"]) or 0, fint(r["defects"]),
        ])

    varus = next((p for p in partners if p["b"] == "VARUS"), None)
    kop = next((p for p in partners if p["b"] == "KOPIYKA"), None)
    santim = next((p for p in partners if p["b"] == "SANTIM"), None)
    taistra = next((p for p in partners if p["b"] == "TAISTRA"), None)
    ruk = next((p for p in partners if p["b"] == "RUKAVYCHKA"), None)
    mini = next((p for p in partners if p["b"] == "KOPIYKA MINI"), None)
    varus_dbx = next((x for x in dbx15 if x[0] == "VARUS"), None)
    ua = next(c for c in countries if c["code"] == "ua")
    peak_odr = max(fnum(r["odr"]) or 0 for r in market)
    last_is_peak = abs(last_odr - peak_odr) < 0.05
    price_peak_i = max(range(len(price_vol)), key=lambda i: price_vol[i])
    price_peak_week = weeks[price_peak_i]
    price_peak_n = price_vol[price_peak_i]

    odr_avg = fnum(raw["totals"]["odr"])
    repl_avg = fnum(raw["totals"]["repl"])
    dpp_market = round(last_odr - first_odr, 1)

    def kfmt(n):
        return f"{n / 1000:.1f}k"

    findings = {
        "ua": [
            {
                "h": "1. Тренд за 12 тижнів негативний, минулий тиждень — стабілізація на піку" if abs(last_odr - prev_odr) <= 0.5
                else ("1. Тренд погіршується" if last_odr > prev_odr else "1. Минулий тиждень трохи кращий, але рівень лишається високим"),
                "p": (
                    f"ODR ринку зріс з <b>{first_odr:.1f}%</b> ({week_label(first, 'ua')}) до "
                    f"<b>{last_odr:.1f}%</b> ({week_label(last, 'ua')}) — це {dpp_market:+.1f} п.п. за 12 тижнів. "
                    f"Минулий тиждень: {last_odr:.1f}% проти {prev_odr:.1f}% тижнем раніше, обсяг "
                    f"{kfmt(last_orders)} замовлень (було {kfmt(prev_orders)})."
                ),
            },
            {
                "h": "2. Проблема = grocery, а не Stores у цілому",
                "p": (
                    f"<b>VARUS</b> тримає ODR <b>{varus['avg']:.1f}%</b> (ост. тиждень {varus['last']:.1f}%) "
                    f"і сам по собі визначає ринковий рівень. "
                    f"<b>KOPIYKA</b> {kop['last']:.1f}% ({kop['d']:+.1f} п.п.), "
                    f"<b>SANTIM</b> {santim['last']:.1f}% ({santim['d']:+.1f} п.п.)"
                    + (f", <b>KOPIYKA MINI</b> {mini['last']:.1f}% ({mini['d']:+.1f} п.п.)" if mini else "")
                    + ". Алкогольні мережі в ТОП-15 лишаються на <b>0%</b>."
                ),
            },
            {
                "h": "3. Є партнери з реальним прогресом — їх процеси треба тиражувати",
                "p": (
                    f"<b>TAISTRA</b> {taistra['d']:+.1f} п.п. (ост. {taistra['last']:.1f}%), "
                    f"<b>RUKAVYCHKA</b> {ruk['d']:+.1f} п.п. (ост. {ruk['last']:.1f}%). "
                    "ODR керований на рівні партнера без змін у продукті."
                ),
            },
            {
                "h": "4. Стрибок price defect 10 серпня виглядає як артефакт, не клієнтський біль",
                "p": (
                    f"На тижні {week_label(price_peak_week, 'ua')} price-дефектів було {price_peak_n} шт., "
                    f"минулого тижня — {price_vol[-1]} шт. Quantity лишається головним драйвером "
                    f"({qty_share:.1f}% усіх item defects). Пакети та OOS на напоях треба чистити окремо від KPI."
                ),
            },
        ],
        "en": [
            {
                "h": "1. The 12-week trend is still up; last week stabilised near the peak" if abs(last_odr - prev_odr) <= 0.5
                else ("1. The trend is still worsening" if last_odr > prev_odr else "1. Last week improved slightly, but the level stays high"),
                "p": (
                    f"Market ODR grew from <b>{first_odr:.1f}%</b> ({week_label(first, 'en')}) to "
                    f"<b>{last_odr:.1f}%</b> ({week_label(last, 'en')}) — {dpp_market:+.1f} pp over 12 weeks. "
                    f"Last week: {last_odr:.1f}% vs {prev_odr:.1f}% the week before, on {kfmt(last_orders)} orders "
                    f"(previously {kfmt(prev_orders)})."
                ),
            },
            {
                "h": "2. The problem is grocery, not Stores as a whole",
                "p": (
                    f"<b>VARUS</b> holds ODR at <b>{varus['avg']:.1f}%</b> (latest week {varus['last']:.1f}%) "
                    f"and sets the market level. "
                    f"<b>KOPIYKA</b> {kop['last']:.1f}% ({kop['d']:+.1f} pp), "
                    f"<b>SANTIM</b> {santim['last']:.1f}% ({santim['d']:+.1f} pp)"
                    + (f", <b>KOPIYKA MINI</b> {mini['last']:.1f}% ({mini['d']:+.1f} pp)" if mini else "")
                    + ". Alcohol chains in the top-15 stay at <b>0%</b>."
                ),
            },
            {
                "h": "3. Some partners genuinely improved — their processes should be replicated",
                "p": (
                    f"<b>TAISTRA</b> {taistra['d']:+.1f} pp (latest {taistra['last']:.1f}%), "
                    f"<b>RUKAVYCHKA</b> {ruk['d']:+.1f} pp (latest {ruk['last']:.1f}%). "
                    "ODR is manageable at partner level without product changes."
                ),
            },
            {
                "h": "4. The 10 Aug price-defect spike looks like an accounting artefact",
                "p": (
                    f"Price defects peaked at {price_peak_n} items in the week of {week_label(price_peak_week, 'en')} "
                    f"and were {price_vol[-1]} last week. Quantity remains the main driver "
                    f"({qty_share:.1f}% of item defects). Bags and beverage OOS should be cleaned up separately from the KPI."
                ),
            },
        ],
    }

    ro = next(c for c in countries if c["code"] == "ro")
    worst_qty = max(dbx15, key=lambda x: x[3])
    worst_wt = max(partners, key=lambda x: x["wt"])

    return {
        "generated": date.today().isoformat(),
        "period": {
            "start": first.isoformat(),
            "end": last.isoformat(),
            "label_ua": f"{week_label(first, 'ua')} — {week_label(last, 'ua')} {last.year} (12 повних тижнів)",
            "label_en": f"{week_label(first, 'en')} — {week_label(last, 'en')} {last.year} (12 completed weeks)",
            "footer_ua": f"дані станом на {long_date(last + timedelta(days=6), 'ua')} (12 повних тижнів)",
            "footer_en": f"data as of {long_date(last + timedelta(days=6), 'en')} (12 completed weeks)",
            "last_week_ua": week_label(last, "ua"),
            "last_week_en": week_label(last, "en"),
        },
        "kpis": {
            "odr_avg": odr_avg,
            "odr_last": last_odr,
            "repl": repl_avg,
            "qty_share": qty_share,
            "last_is_peak": last_is_peak,
        },
        "weeks_ua": [week_label(w, "ua") for w in weeks],
        "weeks_en": [week_label(w, "en") for w in weeks],
        "defect_types": {"qty": qty_vol, "weight": wt_vol, "price": price_vol},
        "shares": {"qty": qty_share, "weight": wt_share, "price": price_share},
        "order_level": {
            "qty": fnum(market[-1]["qty"]),
            "qty_avg": round(sum(fnum(r["qty"]) or 0 for r in market) / len(market), 1),
            "weight": fnum(raw["market"] and mean([fnum(r["weight"]) for r in market])),
            "price": fnum(mean([fnum(r["price"]) for r in market])),
            "ua_qty": ua["qty"],
            "ua_wt": ua["wt"],
            "ua_price": ua["price"],
            "varus_qty": varus_dbx[3] if varus_dbx else None,
            "kop_qty": next((x[3] for x in dbx15 if x[0] == "KOPIYKA"), None),
            "worst_wt_brand": worst_wt["b"],
            "worst_wt": worst_wt["wt"],
            "ro_price": ro["price"],
        },
        "market": {
            "odr": [fnum(r["odr"]) for r in market],
            "repl": [fnum(r["repl"]) for r in market],
            "orders": [fint(r["orders"]) for r in market],
        },
        "brand_odr": brand_odr,
        "partners": partners,
        "dbx15": dbx15,
        "stores": stores,
        "cats": cats,
        "countries": countries,
        "findings": findings,
        "zero_odr": sum(1 for x in dbx15 if x[2] == 0),
    }


def write_data(payload):
    path = SCRIPT_DIR / "data.js"
    path.write_text(
        "window.ODR_DATA = " + json.dumps(payload, ensure_ascii=False, indent=2) + ";\n",
        encoding="utf-8",
    )
    print(f"Wrote {path}")


def main():
    raw = fetch()
    payload = build(raw)
    write_data(payload)
    print(
        f"Period {payload['period']['label_en']}: "
        f"ODR avg {payload['kpis']['odr_avg']}% last {payload['kpis']['odr_last']}%"
    )


if __name__ == "__main__":
    main()
