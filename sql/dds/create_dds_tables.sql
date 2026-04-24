-- ============================================================
-- СЛОЙ DDS: детализированные справочники и факты
-- Обеспечивает устойчивые связи и историчность
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dds;

-- Справочник клиентов (с историей изменений по updated_at)
CREATE TABLE IF NOT EXISTS dds.clients (
    client_id       INTEGER PRIMARY KEY,
    client_name     TEXT        NOT NULL,
    industry        TEXT,
    manager         TEXT,
    updated_at      TIMESTAMP   DEFAULT NOW()
);

-- Справочник сайтов (1 сайт = 1 клиент)
CREATE TABLE IF NOT EXISTS dds.sites (
    site_id         INTEGER PRIMARY KEY,   -- = client_id
    site_url        TEXT,
    client_id       INTEGER REFERENCES dds.clients(client_id),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Справочник кампаний
CREATE TABLE IF NOT EXISTS dds.campaigns (
    campaign_id     INTEGER PRIMARY KEY,
    client_id       INTEGER REFERENCES dds.clients(client_id),
    platform        TEXT,
    campaign_type   TEXT,
    start_date      DATE,
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Факты: ежедневная рекламная активность
CREATE TABLE IF NOT EXISTS dds.fact_advertising (
    id              BIGSERIAL PRIMARY KEY,
    stat_date       DATE        NOT NULL,
    campaign_id     INTEGER     REFERENCES dds.campaigns(campaign_id),
    impressions     INTEGER,
    clicks          INTEGER,
    spend           NUMERIC(12,2),
    conversions     INTEGER,
    UNIQUE (stat_date, campaign_id)
);

-- Факты: результаты проверки сайтов
CREATE TABLE IF NOT EXISTS dds.fact_site_health (
    id              BIGSERIAL PRIMARY KEY,
    check_date      DATE        NOT NULL,
    site_id         INTEGER     REFERENCES dds.sites(site_id),
    load_time_ms    INTEGER,
    availability_pct NUMERIC(6,4),
    server_errors   INTEGER,
    UNIQUE (check_date, site_id)
);

-- Индексы для аналитических запросов
CREATE INDEX IF NOT EXISTS idx_fact_adv_date      ON dds.fact_advertising(stat_date);
CREATE INDEX IF NOT EXISTS idx_fact_adv_campaign  ON dds.fact_advertising(campaign_id);
CREATE INDEX IF NOT EXISTS idx_fact_health_date   ON dds.fact_site_health(check_date);
