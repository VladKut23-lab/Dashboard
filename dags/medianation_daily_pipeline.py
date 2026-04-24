"""
DAG: medianation_daily_pipeline
Ежедневная оркестрация загрузки и трансформации данных для ООО «Медианация».

Порядок выполнения:
  truncate_stg
      ├── load_stg_clients
      ├── load_stg_campaigns
      ├── load_stg_ad_stats
      └── load_stg_site_monitoring
              └── load_dds_clients
                      └── load_dds_sites
                              └── load_dds_campaigns
                                      ├── load_dds_ad_facts
                                      └── load_dds_site_facts
                                                  ├── build_dm_finance
                                                  ├── build_dm_platform
                                                  ├── build_dm_client_kpi
                                                  └── build_dm_site_reliability
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ─── Конфигурация ────────────────────────────────────────────────────────────

POSTGRES_CONN_ID = "medianation_pg"          # задать в Airflow Connections
DATA_DIR = Path("/opt/airflow/data/sample")  # путь к файлам внутри контейнера
SQL_DIR  = Path("/opt/airflow/sql")

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,   # поменять на True и указать email в продакшне
    "depends_on_past": False,
}

# ─── Вспомогательные функции ─────────────────────────────────────────────────

def get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def run_sql(sql: str) -> None:
    """Выполнить SQL-запрос через hook."""
    hook = get_hook()
    hook.run(sql)
    log.info("SQL executed successfully.")


def read_sql_file(path: Path) -> str:
    with open(path) as f:
        return f.read()


# ─── Задачи ──────────────────────────────────────────────────────────────────

def task_truncate_stg(**ctx):
    """Очистка STG-слоя перед загрузкой (идемпотентность)."""
    log.info("Truncating STG tables...")
    run_sql("""
        TRUNCATE stg.clients, stg.campaigns, stg.ad_stats, stg.site_monitoring;
    """)
    log.info("STG tables truncated.")


def task_load_stg_clients(**ctx):
    """Загрузка справочника клиентов из clients.csv."""
    path = DATA_DIR / "clients.csv"
    log.info("Loading clients from %s", path)
    df = pd.read_csv(path)

    # Базовые проверки качества
    null_ids = df["client_id"].isna().sum()
    if null_ids > 0:
        raise ValueError(f"clients.csv: {null_ids} строк с пустым client_id")
    dups = df["client_id"].duplicated().sum()
    if dups > 0:
        log.warning("clients.csv: найдено %d дублирующихся client_id — удалены", dups)
        df = df.drop_duplicates(subset=["client_id"])

    hook = get_hook()
    hook.insert_rows(
        table="stg.clients",
        rows=df.values.tolist(),
        target_fields=list(df.columns),
    )
    log.info("Loaded %d client rows into stg.clients", len(df))


def task_load_stg_campaigns(**ctx):
    """Загрузка справочника кампаний из campaigns.csv."""
    path = DATA_DIR / "campaigns.csv"
    log.info("Loading campaigns from %s", path)
    df = pd.read_csv(path, parse_dates=["start_date"])

    null_check = df[["campaign_id", "client_id"]].isna().any(axis=1).sum()
    if null_check > 0:
        raise ValueError(f"campaigns.csv: {null_check} строк с пустыми ключами")

    hook = get_hook()
    hook.insert_rows(
        table="stg.campaigns",
        rows=df.values.tolist(),
        target_fields=list(df.columns),
    )
    log.info("Loaded %d campaign rows into stg.campaigns", len(df))


def task_load_stg_ad_stats(**ctx):
    """Загрузка ежедневной рекламной статистики за дату выполнения DAG."""
    ds = ctx["ds"]  # YYYY-MM-DD
    path = DATA_DIR / f"ad_stats_{ds}.csv"
    if not path.exists():
        log.warning("File %s not found — skipping ad_stats load for %s", path, ds)
        return

    df = pd.read_csv(path, parse_dates=["stat_date"])

    # Проверки: отрицательные значения недопустимы
    for col in ["impressions", "clicks", "spend", "conversions"]:
        neg = (df[col] < 0).sum()
        if neg > 0:
            raise ValueError(f"ad_stats_{ds}.csv: {neg} отрицательных значений в {col}")

    hook = get_hook()
    hook.insert_rows(
        table="stg.ad_stats",
        rows=df.values.tolist(),
        target_fields=list(df.columns),
    )
    log.info("Loaded %d ad_stats rows for %s", len(df), ds)


def task_load_stg_site_monitoring(**ctx):
    """Загрузка данных мониторинга сайтов за дату выполнения DAG."""
    ds = ctx["ds"]
    path = DATA_DIR / f"site_monitoring_{ds}.csv"
    if not path.exists():
        log.warning("File %s not found — skipping site_monitoring for %s", path, ds)
        return

    df = pd.read_csv(path, parse_dates=["check_date"])

    avail_out = ((df["availability_pct"] < 0) | (df["availability_pct"] > 1)).sum()
    if avail_out > 0:
        raise ValueError(f"site_monitoring_{ds}.csv: {avail_out} строк с недопустимым availability_pct")

    hook = get_hook()
    hook.insert_rows(
        table="stg.site_monitoring",
        rows=df.values.tolist(),
        target_fields=list(df.columns),
    )
    log.info("Loaded %d site_monitoring rows for %s", len(df), ds)


def task_load_dds_clients(**ctx):
    log.info("Loading DDS clients...")
    run_sql("""
        INSERT INTO dds.clients (client_id, client_name, industry, manager, updated_at)
        SELECT client_id, client_name, industry, manager, NOW()
        FROM stg.clients
        WHERE client_id IS NOT NULL AND client_name IS NOT NULL
        ON CONFLICT (client_id) DO UPDATE
            SET client_name = EXCLUDED.client_name,
                industry    = EXCLUDED.industry,
                manager     = EXCLUDED.manager,
                updated_at  = EXCLUDED.updated_at;
    """)


def task_load_dds_sites(**ctx):
    log.info("Loading DDS sites...")
    run_sql("""
        INSERT INTO dds.sites (site_id, site_url, client_id, updated_at)
        SELECT client_id, site_url, client_id, NOW()
        FROM stg.clients
        WHERE client_id IS NOT NULL AND site_url IS NOT NULL
        ON CONFLICT (site_id) DO UPDATE
            SET site_url = EXCLUDED.site_url, updated_at = EXCLUDED.updated_at;
    """)


def task_load_dds_campaigns(**ctx):
    log.info("Loading DDS campaigns...")
    run_sql("""
        INSERT INTO dds.campaigns (campaign_id, client_id, platform, campaign_type, start_date, updated_at)
        SELECT campaign_id, client_id, platform, campaign_type, start_date, NOW()
        FROM stg.campaigns
        WHERE campaign_id IS NOT NULL AND client_id IS NOT NULL
        ON CONFLICT (campaign_id) DO UPDATE
            SET client_id = EXCLUDED.client_id, platform = EXCLUDED.platform,
                campaign_type = EXCLUDED.campaign_type, start_date = EXCLUDED.start_date,
                updated_at = EXCLUDED.updated_at;
    """)


def task_load_dds_ad_facts(**ctx):
    log.info("Loading DDS advertising facts...")
    run_sql("""
        INSERT INTO dds.fact_advertising
            (stat_date, campaign_id, impressions, clicks, spend, conversions)
        SELECT s.stat_date, s.campaign_id, s.impressions, s.clicks, s.spend, s.conversions
        FROM stg.ad_stats s
        INNER JOIN dds.campaigns c ON c.campaign_id = s.campaign_id
        WHERE s.stat_date IS NOT NULL AND s.impressions >= 0
          AND s.clicks >= 0 AND s.spend >= 0
        ON CONFLICT (stat_date, campaign_id) DO UPDATE
            SET impressions = EXCLUDED.impressions, clicks = EXCLUDED.clicks,
                spend = EXCLUDED.spend, conversions = EXCLUDED.conversions;
    """)


def task_load_dds_site_facts(**ctx):
    log.info("Loading DDS site health facts...")
    run_sql("""
        INSERT INTO dds.fact_site_health
            (check_date, site_id, load_time_ms, availability_pct, server_errors)
        SELECT m.check_date, m.site_id, m.load_time_ms, m.availability_pct, m.server_errors
        FROM stg.site_monitoring m
        INNER JOIN dds.sites s ON s.site_id = m.site_id
        WHERE m.check_date IS NOT NULL AND m.availability_pct BETWEEN 0 AND 1
        ON CONFLICT (check_date, site_id) DO UPDATE
            SET load_time_ms = EXCLUDED.load_time_ms,
                availability_pct = EXCLUDED.availability_pct,
                server_errors = EXCLUDED.server_errors;
    """)


def _build_dm(name: str, sql: str, **ctx):
    log.info("Building dm.%s ...", name)
    run_sql(sql)
    log.info("dm.%s rebuilt successfully.", name)


def task_build_dm_finance(**ctx):
    _build_dm("agency_finance", """
        TRUNCATE dm.agency_finance;
        INSERT INTO dm.agency_finance
        SELECT DATE_TRUNC('month', fa.stat_date)::DATE,
               cl.manager,
               COUNT(DISTINCT cl.client_id),
               SUM(fa.spend),
               ROUND(SUM(fa.spend) * 0.10, 2)
        FROM dds.fact_advertising fa
        JOIN dds.campaigns cp ON cp.campaign_id = fa.campaign_id
        JOIN dds.clients   cl ON cl.client_id   = cp.client_id
        GROUP BY 1, 2;
    """, **ctx)


def task_build_dm_platform(**ctx):
    _build_dm("platform_stats", """
        TRUNCATE dm.platform_stats;
        INSERT INTO dm.platform_stats
        SELECT fa.stat_date, cp.platform,
               ROUND(SUM(fa.clicks)::NUMERIC / NULLIF(SUM(fa.impressions),0), 4),
               ROUND(SUM(fa.spend) / NULLIF(SUM(fa.clicks),0), 2),
               SUM(fa.clicks)
        FROM dds.fact_advertising fa
        JOIN dds.campaigns cp ON cp.campaign_id = fa.campaign_id
        GROUP BY 1, 2;
    """, **ctx)


def task_build_dm_client_kpi(**ctx):
    _build_dm("client_kpi", """
        TRUNCATE dm.client_kpi;
        INSERT INTO dm.client_kpi
        SELECT DATE_TRUNC('month', fa.stat_date)::DATE,
               cl.client_id, cl.client_name, cl.industry,
               SUM(fa.spend),
               SUM(fa.conversions),
               ROUND(SUM(fa.spend) / NULLIF(SUM(fa.conversions),0), 2)
        FROM dds.fact_advertising fa
        JOIN dds.campaigns cp ON cp.campaign_id = fa.campaign_id
        JOIN dds.clients   cl ON cl.client_id   = cp.client_id
        GROUP BY 1, 2, 3, 4;
    """, **ctx)


def task_build_dm_site_reliability(**ctx):
    _build_dm("site_reliability", """
        TRUNCATE dm.site_reliability;
        INSERT INTO dm.site_reliability
        SELECT DATE_TRUNC('month', sh.check_date)::DATE,
               sh.site_id, cl.client_name,
               ROUND(AVG(sh.load_time_ms),2),
               ROUND(AVG(sh.availability_pct),4),
               SUM(sh.server_errors)
        FROM dds.fact_site_health sh
        JOIN dds.sites   s  ON s.site_id    = sh.site_id
        JOIN dds.clients cl ON cl.client_id = s.client_id
        GROUP BY 1, 2, 3;
    """, **ctx)


# ─── Определение DAG ─────────────────────────────────────────────────────────

with DAG(
    dag_id="medianation_daily_pipeline",
    description="Ежедневная загрузка и трансформация данных для ООО Медианация",
    schedule_interval="0 6 * * *",   # каждый день в 06:00
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["medianation", "etl"],
) as dag:

    # STG
    truncate_stg = PythonOperator(task_id="truncate_stg", python_callable=task_truncate_stg)

    load_stg_clients    = PythonOperator(task_id="load_stg_clients",    python_callable=task_load_stg_clients)
    load_stg_campaigns  = PythonOperator(task_id="load_stg_campaigns",  python_callable=task_load_stg_campaigns)
    load_stg_ad_stats   = PythonOperator(task_id="load_stg_ad_stats",   python_callable=task_load_stg_ad_stats)
    load_stg_monitoring = PythonOperator(task_id="load_stg_site_monitoring", python_callable=task_load_stg_site_monitoring)

    # DDS
    load_dds_clients   = PythonOperator(task_id="load_dds_clients",   python_callable=task_load_dds_clients)
    load_dds_sites     = PythonOperator(task_id="load_dds_sites",     python_callable=task_load_dds_sites)
    load_dds_campaigns = PythonOperator(task_id="load_dds_campaigns", python_callable=task_load_dds_campaigns)
    load_dds_ad_facts  = PythonOperator(task_id="load_dds_ad_facts",  python_callable=task_load_dds_ad_facts)
    load_dds_site_facts= PythonOperator(task_id="load_dds_site_facts",python_callable=task_load_dds_site_facts)

    # DM
    build_dm_finance     = PythonOperator(task_id="build_dm_finance",     python_callable=task_build_dm_finance)
    build_dm_platform    = PythonOperator(task_id="build_dm_platform",    python_callable=task_build_dm_platform)
    build_dm_client_kpi  = PythonOperator(task_id="build_dm_client_kpi",  python_callable=task_build_dm_client_kpi)
    build_dm_reliability = PythonOperator(task_id="build_dm_site_reliability", python_callable=task_build_dm_site_reliability)

    # ─── Зависимости ─────────────────────────────────────────────────────────
    truncate_stg >> [load_stg_clients, load_stg_campaigns, load_stg_ad_stats, load_stg_monitoring]

    [load_stg_clients, load_stg_campaigns, load_stg_ad_stats, load_stg_monitoring] >> load_dds_clients
    load_dds_clients >> load_dds_sites >> load_dds_campaigns
    load_dds_campaigns >> [load_dds_ad_facts, load_dds_site_facts]

    load_dds_ad_facts   >> [build_dm_finance, build_dm_platform, build_dm_client_kpi]
    load_dds_site_facts >> build_dm_reliability
