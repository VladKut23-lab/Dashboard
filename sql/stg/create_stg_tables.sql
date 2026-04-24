-- ============================================================
-- СЛОЙ STG: первичная загрузка сырых данных
-- Идемпотентность: таблицы очищаются перед каждой загрузкой
-- ============================================================

CREATE SCHEMA IF NOT EXISTS stg;

-- Справочник клиентов
CREATE TABLE IF NOT EXISTS stg.clients (
    client_id       INTEGER,
    client_name     TEXT,
    industry        TEXT,
    manager         TEXT,
    site_url        TEXT,
    _loaded_at      TIMESTAMP DEFAULT NOW()
);

-- Справочник кампаний
CREATE TABLE IF NOT EXISTS stg.campaigns (
    campaign_id     INTEGER,
    client_id       INTEGER,
    platform        TEXT,
    campaign_type   TEXT,
    start_date      DATE,
    _loaded_at      TIMESTAMP DEFAULT NOW()
);

-- Ежедневная рекламная статистика
CREATE TABLE IF NOT EXISTS stg.ad_stats (
    stat_date       DATE,
    campaign_id     INTEGER,
    impressions     INTEGER,
    clicks          INTEGER,
    spend           NUMERIC(12,2),
    conversions     INTEGER,
    _loaded_at      TIMESTAMP DEFAULT NOW()
);

-- Мониторинг сайтов
CREATE TABLE IF NOT EXISTS stg.site_monitoring (
    check_date      DATE,
    site_id         INTEGER,
    load_time_ms    INTEGER,
    availability_pct NUMERIC(6,4),
    server_errors   INTEGER,
    _loaded_at      TIMESTAMP DEFAULT NOW()
);
