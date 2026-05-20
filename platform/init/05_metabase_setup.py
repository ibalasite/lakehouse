#!/usr/bin/env python3
"""
05_metabase_setup.py
--------------------
Automates Metabase initial configuration via the Metabase REST API.

Steps:
  1. Wait for Metabase to be ready
  2. Complete the onboarding setup (creates admin account)
  3. Add MySQL database connection (lakehouse_cache)
  4. Create a collection "問題單分析"
  5. Create 5 saved questions (cards) covering key KPIs
  6. Create dashboard "客服問題單日報" containing all 5 cards
  7. Print the dashboard URL

Usage:
    python3 05_metabase_setup.py
    python3 05_metabase_setup.py --metabase-url http://localhost:3000

All credentials are read from environment variables (or use defaults
appropriate for the local docker-compose stack).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from typing import Any

import requests

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[metabase-setup] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
MB_URL = os.environ.get("METABASE_URL", "http://localhost:3000")
ADMIN_EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@local.com")
ADMIN_PASSWORD = os.environ["METABASE_ADMIN_PASSWORD"]
ADMIN_FIRST = "Admin"
ADMIN_LAST = "User"
SITE_NAME = "Lakehouse 客服分析"

MYSQL_HOST = os.environ.get("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_DB = os.environ.get("MYSQL_DATABASE", "lakehouse_cache")
MYSQL_USER = os.environ.get("MYSQL_USER", "lakehouse")
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]


# ── HTTP helpers ──────────────────────────────────────────────────────────────

class MetabaseClient:
    def __init__(self, base_url: str) -> None:
        self.base = base_url.rstrip("/") + "/api"
        self.session: requests.Session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"

    def _raise(self, resp: requests.Response, action: str) -> None:
        if not resp.ok:
            log.error("%s failed: HTTP %s — %s", action, resp.status_code, resp.text[:400])
            sys.exit(1)

    def get(self, path: str) -> Any:
        return self.session.get(f"{self.base}{path}", timeout=15)

    def post(self, path: str, payload: dict, timeout: int = 120) -> Any:
        return self.session.post(f"{self.base}{path}", json=payload, timeout=timeout)

    def put(self, path: str, payload: dict) -> Any:
        return self.session.put(f"{self.base}{path}", json=payload, timeout=30)

    def set_token(self, token: str) -> None:
        self.session.headers["X-Metabase-Session"] = token


# ── Wait for readiness ────────────────────────────────────────────────────────

def wait_for_metabase(client: MetabaseClient, timeout: int = 300) -> None:
    log.info("Waiting for Metabase at %s (up to %ds) …", client.base, timeout)
    deadline = time.monotonic() + timeout
    attempt = 0
    while time.monotonic() < deadline:
        try:
            r = client.get("/health")
            if r.ok and r.json().get("status") in ("ok", "metabase-ok"):
                log.info("Metabase is ready.")
                return
        except Exception:
            pass
        attempt += 1
        if attempt % 6 == 1:
            log.info("  Still waiting … attempt %d", attempt)
        time.sleep(5)
    log.error("Metabase did not become ready within %ds.", timeout)
    sys.exit(1)


# ── Onboarding setup ──────────────────────────────────────────────────────────

def get_setup_token(client: MetabaseClient) -> str | None:
    r = client.get("/session/properties")
    client._raise(r, "GET /session/properties")
    token = r.json().get("setup-token")
    if not token:
        log.info("No setup-token present — Metabase is already configured.")
    return token


def run_setup(client: MetabaseClient, setup_token: str) -> str | None:
    log.info("Running first-time Metabase setup …")
    payload = {
        "token": setup_token,
        "prefs": {
            "site_name": SITE_NAME,
            "site_locale": "zh",
            "allow_tracking": False,
        },
        "user": {
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD,
            "password_confirm": ADMIN_PASSWORD,
            "first_name": ADMIN_FIRST,
            "last_name": ADMIN_LAST,
            "site_name": SITE_NAME,
        },
        "database": None,
    }
    r = client.post("/setup", payload)
    if r.status_code == 403:
        log.info("Setup already completed (403 on /setup) — will login instead.")
        return None
    client._raise(r, "POST /setup")
    token = r.json().get("id") or r.json().get("token")
    log.info("Setup complete. Session token obtained.")
    return token


def login(client: MetabaseClient) -> str:
    log.info("Logging in as %s …", ADMIN_EMAIL)
    r = client.post("/session", {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    client._raise(r, "POST /session")
    token = r.json()["id"]
    client.set_token(token)
    log.info("Logged in.")
    return token


# ── Database connection ────────────────────────────────────────────────────────

def add_mysql_database(client: MetabaseClient) -> int:
    # Return existing db id if already added
    r = client.get("/database")
    if r.ok:
        for db in r.json().get("data", r.json() if isinstance(r.json(), list) else []):
            if db.get("engine") == "mysql" and db.get("name") == "Lakehouse Cache (MySQL)":
                log.info("MySQL database already exists with id=%d.", db["id"])
                return db["id"]

    log.info("Adding MySQL database connection (lakehouse_cache) …")
    payload = {
        "engine": "mysql",
        "name": "Lakehouse Cache (MySQL)",
        "details": {
            "host": MYSQL_HOST,
            "port": MYSQL_PORT,
            "dbname": MYSQL_DB,
            "user": MYSQL_USER,
            "password": MYSQL_PASSWORD,
            "ssl": False,
            "tunnel-enabled": False,
            "additional-options": "allowPublicKeyRetrieval=true&useSSL=false",
        },
        "auto_run_queries": True,
        "is_on_demand": False,
        "is_full_sync": True,
    }
    r = client.post("/database", payload)
    client._raise(r, "POST /database")
    db_id = r.json()["id"]
    log.info("Database added with id=%d.", db_id)
    return db_id


def wait_for_sync(client: MetabaseClient, db_id: int, timeout: int = 120) -> None:
    """Wait until Metabase finishes syncing the database schema."""
    log.info("Waiting for database sync (db_id=%d) …", db_id)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = client.get(f"/database/{db_id}")
        if r.ok:
            state = r.json().get("initial_sync_status", "incomplete")
            if state == "complete":
                log.info("Database sync complete.")
                return
        time.sleep(5)
    log.warning("Database sync did not complete within %ds — continuing anyway.", timeout)


def get_table_id(client: MetabaseClient, db_id: int, table_name: str) -> int | None:
    r = client.get(f"/database/{db_id}/metadata")
    if not r.ok:
        return None
    for table in r.json().get("tables", []):
        if table["name"] == table_name:
            return table["id"]
    return None


# ── Collection ────────────────────────────────────────────────────────────────

def create_collection(client: MetabaseClient) -> int:
    r = client.get("/collection")
    if r.ok:
        cols = r.json() if isinstance(r.json(), list) else r.json().get("data", [])
        for col in cols:
            if col.get("name") == "問題單分析":
                log.info("Collection '問題單分析' already exists with id=%d.", col["id"])
                return col["id"]
    log.info("Creating collection '問題單分析' …")
    r = client.post("/collection", {
        "name": "問題單分析",
        "description": "客服問題單 KPI 儀表板與分析報表",
        "color": "#509EE3",
    })
    client._raise(r, "POST /collection")
    cid = r.json()["id"]
    log.info("Collection created with id=%d.", cid)
    return cid


# ── Saved questions (cards) ───────────────────────────────────────────────────

def create_card(
    client: MetabaseClient,
    collection_id: int,
    name: str,
    sql: str,
    db_id: int,
    description: str = "",
    display: str = "table",
    visualization_settings: dict | None = None,
) -> int:
    # Check if card already exists — update viz settings if it does
    r = client.get(f"/card?collection_id={collection_id}")
    if r.ok:
        for card in (r.json() if isinstance(r.json(), list) else r.json().get("data", [])):
            if card.get("name") == name:
                cid = card["id"]
                log.info("  Card '%s' already exists (id=%d) — updating display.", name, cid)
                client.put(f"/card/{cid}", {
                    "display": display,
                    "visualization_settings": visualization_settings or {},
                })
                return cid
    payload = {
        "name": name,
        "description": description,
        "display": display,
        "visualization_settings": visualization_settings or {},
        "dataset_query": {
            "type": "native",
            "database": db_id,
            "native": {"query": sql},
        },
        "collection_id": collection_id,
    }
    r = client.post("/card", payload)
    client._raise(r, f"POST /card ({name})")
    cid = r.json()["id"]
    log.info("  Card '%s' created with id=%d (display=%s).", name, cid, display)
    return cid


def create_all_cards(
    client: MetabaseClient,
    collection_id: int,
    db_id: int,
) -> list[int]:
    # Each entry: (name, sql, description, display, visualization_settings)
    cards = [
        (
            "每日問題單量趨勢",
            """
SELECT
  date_sk,
  SUM(total_tickets)         AS 總問題單數,
  SUM(resolved_tickets)      AS 已結案數,
  ROUND(AVG(pct_resolved),1) AS 結案率_pct
FROM cache_ticket_daily
GROUP BY date_sk
ORDER BY date_sk DESC
LIMIT 90
            """.strip(),
            "過去90天每日問題單趨勢，含結案率",
            "line",
            {
                "graph.dimensions":        ["date_sk"],
                "graph.metrics":           ["總問題單數", "已結案數"],
                "graph.x_axis.title_text": "日期",
                "graph.y_axis.title_text": "問題單數",
                "graph.show_values":       False,
                "series_settings": {
                    "總問題單數": {"color": "#509EE3", "display": "line"},
                    "已結案數":   {"color": "#88BF4D", "display": "line"},
                },
            },
        ),
        (
            "各子站問題單分布",
            """
SELECT
  CASE catsub_id
    WHEN 1 THEN '購物(台灣)'  WHEN 2 THEN '購物(香港)'
    WHEN 3 THEN '拍賣(台灣)'  WHEN 4 THEN '超市(台灣)'
    WHEN 5 THEN '旅遊(台灣)'  WHEN 6 THEN '票券(台灣)'
    WHEN 7 THEN '金融(台灣)'  WHEN 8 THEN '企業(台灣)'
    WHEN 9 THEN '廣告(台灣)'  ELSE '其他'
  END                                       AS 子站名稱,
  SUM(total_tickets)                        AS 總問題單數,
  ROUND(SUM(pct_resolved)/COUNT(*),1)       AS 平均結案率
FROM cache_ticket_daily
GROUP BY catsub_id
ORDER BY 總問題單數 DESC
            """.strip(),
            "各子站問題單量與結案率（全期）",
            "bar",
            {
                "graph.dimensions":        ["子站名稱"],
                "graph.metrics":           ["總問題單數"],
                "graph.x_axis.title_text": "子站",
                "graph.y_axis.title_text": "問題單數",
                "graph.show_values":       True,
                "series_settings": {"總問題單數": {"color": "#509EE3"}},
            },
        ),
        (
            "SLA達標分析",
            """
SELECT
  CASE prblm_perform_id
    WHEN 1  THEN '急件 (8hr)'
    WHEN 2  THEN '特急 (4hr)'
    WHEN 99 THEN '一般 (24hr)'
    ELSE    '未分類'
  END                                        AS SLA類型,
  SUM(total_tickets)                         AS 總問題單數,
  SUM(within_sla_tickets)                    AS 時效內問題單數,
  ROUND(SUM(pct_within_sla)/COUNT(*),1)      AS 平均達標率_pct
FROM cache_ticket_daily
GROUP BY prblm_perform_id
ORDER BY prblm_perform_id
            """.strip(),
            "各SLA等級達標率分析（全期）",
            "row",
            {
                "graph.dimensions":        ["SLA類型"],
                "graph.metrics":           ["平均達標率_pct"],
                "graph.x_axis.title_text": "達標率 (%)",
                "graph.show_values":       True,
                "series_settings": {"平均達標率_pct": {"color": "#88BF4D"}},
            },
        ),
        (
            "客訴問題單比例趨勢",
            """
SELECT
  date_sk,
  SUM(total_tickets)    AS 總問題單數,
  SUM(complain_tickets) AS 客訴問題單數,
  ROUND(
    100.0 * SUM(complain_tickets) / NULLIF(SUM(total_tickets),0), 2
  )                     AS 客訴比例_pct
FROM cache_ticket_daily
GROUP BY date_sk
ORDER BY date_sk DESC
LIMIT 60
            """.strip(),
            "過去60天每日客訴問題單比例",
            "area",
            {
                "graph.dimensions":        ["date_sk"],
                "graph.metrics":           ["客訴比例_pct"],
                "graph.x_axis.title_text": "日期",
                "graph.y_axis.title_text": "客訴比例 (%)",
                "graph.show_values":       False,
                "series_settings": {"客訴比例_pct": {"color": "#EF8C8C"}},
            },
        ),
        (
            "平均回覆與結案時效",
            """
SELECT
  date_sk,
  ROUND(AVG(avg_response_hours),1)   AS 平均回覆時效_hr,
  ROUND(AVG(avg_resolution_hours),1) AS 平均結案時效_hr,
  ROUND(AVG(pct_one_shot),1)         AS 一次結案率_pct
FROM cache_ticket_daily
WHERE avg_response_hours IS NOT NULL
GROUP BY date_sk
ORDER BY date_sk DESC
LIMIT 30
            """.strip(),
            "平均回覆時效、結案時效及一次結案率趨勢",
            "line",
            {
                "graph.dimensions":        ["date_sk"],
                "graph.metrics":           ["平均回覆時效_hr", "平均結案時效_hr"],
                "graph.x_axis.title_text": "日期",
                "graph.y_axis.title_text": "小時",
                "graph.show_values":       False,
                "series_settings": {
                    "平均回覆時效_hr": {"color": "#F9CF48", "display": "line"},
                    "平均結案時效_hr": {"color": "#A989C5", "display": "line"},
                },
            },
        ),
    ]

    card_ids: list[int] = []
    for name, sql, description, display, viz_settings in cards:
        cid = create_card(
            client, collection_id, name, sql, db_id,
            description=description,
            display=display,
            visualization_settings=viz_settings,
        )
        card_ids.append(cid)
    return card_ids


def create_realtime_cards(
    client: MetabaseClient,
    collection_id: int,
    db_id: int,
) -> list[int]:
    """Create hourly real-time cards from cache_ticket_hourly."""
    cards = [
        (
            "今日問題單即時動態",
            """
SELECT
  CONCAT(DATE_FORMAT(date_sk, '%Y-%m-%d'), ' ', LPAD(hour_of_day, 2, '0'), ':00') AS 時段,
  SUM(total_tickets)    AS 總問題單數,
  SUM(resolved_tickets) AS 已結案數,
  ROUND(100.0 * SUM(resolved_tickets) / NULLIF(SUM(total_tickets), 0), 1) AS 結案率_pct
FROM cache_ticket_hourly
WHERE date_sk = CURDATE()
GROUP BY date_sk, hour_of_day
ORDER BY hour_of_day
            """.strip(),
            "今日各小時問題單量與結案率（即時更新）",
            "bar",
            {
                "graph.dimensions":        ["時段"],
                "graph.metrics":           ["總問題單數", "已結案數"],
                "graph.x_axis.title_text": "時段",
                "graph.y_axis.title_text": "問題單數",
                "graph.show_values":       True,
                "series_settings": {
                    "總問題單數": {"color": "#509EE3", "display": "bar"},
                    "已結案數":   {"color": "#88BF4D", "display": "bar"},
                },
            },
        ),
        (
            "各子站即時問題單分布",
            """
SELECT
  CASE catsub_id
    WHEN 1 THEN '購物(台灣)'  WHEN 2 THEN '購物(香港)'
    WHEN 3 THEN '拍賣(台灣)'  WHEN 4 THEN '超市(台灣)'
    WHEN 5 THEN '旅遊(台灣)'  WHEN 6 THEN '票券(台灣)'
    WHEN 7 THEN '金融(台灣)'  WHEN 8 THEN '企業(台灣)'
    WHEN 9 THEN '廣告(台灣)'  ELSE '其他'
  END                              AS 子站名稱,
  SUM(total_tickets)               AS 今日問題單數,
  SUM(within_sla_tickets)          AS 時效內問題單數,
  ROUND(100.0 * SUM(within_sla_tickets) / NULLIF(SUM(total_tickets), 0), 1) AS SLA達標率_pct
FROM cache_ticket_hourly
WHERE date_sk = CURDATE()
GROUP BY catsub_id
ORDER BY 今日問題單數 DESC
            """.strip(),
            "今日各子站問題單量與SLA達標率（即時更新）",
            "row",
            {
                "graph.dimensions":        ["子站名稱"],
                "graph.metrics":           ["今日問題單數"],
                "graph.x_axis.title_text": "問題單數",
                "graph.show_values":       True,
                "series_settings": {"今日問題單數": {"color": "#509EE3"}},
            },
        ),
    ]

    card_ids: list[int] = []
    for name, sql, description, display, viz_settings in cards:
        cid = create_card(
            client, collection_id, name, sql, db_id,
            description=description,
            display=display,
            visualization_settings=viz_settings,
        )
        card_ids.append(cid)
    return card_ids


# ── Dashboard ─────────────────────────────────────────────────────────────────

def create_dashboard(
    client: MetabaseClient,
    collection_id: int,
    card_ids: list[int],
) -> int:
    # Check if dashboard already exists
    r = client.get(f"/dashboard?collection_id={collection_id}")
    if r.ok:
        items = r.json() if isinstance(r.json(), list) else r.json().get("data", [])
        for dash in items:
            if dash.get("name") == "客服問題單日報":
                dash_id = dash["id"]
                log.info("Dashboard '客服問題單日報' already exists (id=%d) — re-syncing cards.", dash_id)
                existing = client.get(f"/dashboard/{dash_id}")
                existing_cards = existing.json().get("dashcards", []) if existing.ok else []
                if len(existing_cards) != len(card_ids):
                    col_width, row_height = 12, 8
                    dashcards = [
                        {"id": str(-(i+1)), "card_id": cid,
                         "row": (i//2)*row_height, "col": (i%2)*col_width,
                         "size_x": col_width, "size_y": row_height,
                         "parameter_mappings": [], "visualization_settings": {}}
                        for i, cid in enumerate(card_ids)
                    ]
                    client.put(f"/dashboard/{dash_id}", {"dashcards": dashcards})
                    log.info("  Cards re-synced.")
                return dash_id

    log.info("Creating dashboard '客服問題單日報' …")
    r = client.post("/dashboard", {
        "name": "客服問題單日報",
        "description": "客服問題單核心KPI，含結案率、SLA達標率、客訴比例、時效分析",
        "collection_id": collection_id,
        "parameters": [],
    })
    client._raise(r, "POST /dashboard")
    dash_id = r.json()["id"]
    log.info("  Dashboard created with id=%d.", dash_id)

    # Add cards via PUT /dashboard/{id} with dashcards array (Metabase v0.50+).
    # Negative id values signal new dashcards (no existing id yet).
    col_width  = 12   # 24-unit grid → 2 columns
    row_height = 8
    dashcards = [
        {
            "id":                     str(-(i + 1)),
            "card_id":                cid,
            "row":                    (i // 2) * row_height,
            "col":                    (i % 2) * col_width,
            "size_x":                 col_width,
            "size_y":                 row_height,
            "parameter_mappings":     [],
            "visualization_settings": {},
        }
        for i, cid in enumerate(card_ids)
    ]
    r2 = client.put(f"/dashboard/{dash_id}", {"dashcards": dashcards})
    client._raise(r2, f"PUT /dashboard/{dash_id} (dashcards)")
    actual = len(r2.json().get("dashcards", []))
    log.info("  %d cards added to dashboard.", actual)
    return dash_id


# ── Real-time dashboard ───────────────────────────────────────────────────────

def _create_realtime_dashboard(
    client: MetabaseClient,
    collection_id: int,
    card_ids: list[int],
) -> int:
    dash_name = "客服問題單即時看板"
    r = client.get(f"/dashboard?collection_id={collection_id}")
    if r.ok:
        items = r.json() if isinstance(r.json(), list) else r.json().get("data", [])
        for dash in items:
            if dash.get("name") == dash_name:
                dash_id = dash["id"]
                log.info("Dashboard '%s' already exists (id=%d).", dash_name, dash_id)
                return dash_id

    log.info("Creating real-time dashboard '%s' …", dash_name)
    r = client.post("/dashboard", {
        "name": dash_name,
        "description": "每15分鐘自動更新的即時問題單看板（來源：cache_ticket_hourly）",
        "collection_id": collection_id,
        "parameters": [],
    })
    client._raise(r, f"POST /dashboard ({dash_name})")
    dash_id = r.json()["id"]

    col_width, row_height = 24, 8
    dashcards = [
        {
            "id":                     str(-(i + 1)),
            "card_id":                cid,
            "row":                    i * row_height,
            "col":                    0,
            "size_x":                 col_width,
            "size_y":                 row_height,
            "parameter_mappings":     [],
            "visualization_settings": {},
        }
        for i, cid in enumerate(card_ids)
    ]
    r2 = client.put(f"/dashboard/{dash_id}", {"dashcards": dashcards})
    client._raise(r2, f"PUT /dashboard/{dash_id} (realtime dashcards)")
    log.info("  Real-time dashboard created (id=%d) with %d cards.", dash_id, len(card_ids))
    return dash_id


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Automate Metabase initial setup.")
    parser.add_argument(
        "--metabase-url",
        default=MB_URL,
        help=f"Metabase base URL (default: {MB_URL})",
    )
    parser.add_argument("--timeout", type=int, default=300, help="Readiness wait timeout (s)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = MetabaseClient(args.metabase_url)

    # 1. Wait for Metabase
    wait_for_metabase(client, timeout=args.timeout)

    # 2. Onboarding / login
    setup_token = get_setup_token(client)
    if setup_token:
        session_token = run_setup(client, setup_token)
        if session_token:
            client.set_token(session_token)
        else:
            login(client)
    else:
        login(client)

    # 3. Add MySQL database
    db_id = add_mysql_database(client)
    wait_for_sync(client, db_id, timeout=120)

    # 4. Create collection
    collection_id = create_collection(client)

    # 5. Create daily cards
    card_ids = create_all_cards(client, collection_id, db_id)

    # 6. Create daily dashboard
    dash_id = create_dashboard(client, collection_id, card_ids)

    # 7. Create real-time hourly cards
    rt_card_ids = create_realtime_cards(client, collection_id, db_id)

    # 8. Create real-time dashboard
    rt_dash_id = _create_realtime_dashboard(client, collection_id, rt_card_ids)

    # 9. Print summary
    dash_url = f"{args.metabase_url.rstrip('/')}/dashboard/{dash_id}"
    rt_dash_url = f"{args.metabase_url.rstrip('/')}/dashboard/{rt_dash_id}"
    print("\n" + "=" * 60)
    print("  Metabase setup complete!")
    print("=" * 60)
    print(f"  Daily dashboard   : {dash_url}")
    print(f"  Realtime dashboard: {rt_dash_url}")
    print(f"  Login             : {ADMIN_EMAIL} / {ADMIN_PASSWORD}")
    print(f"  Collection        : 問題單分析 (id={collection_id})")
    print(f"  Daily cards       : {len(card_ids)}")
    print(f"  Realtime cards    : {len(rt_card_ids)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
