-- ============================================================
-- СЛОЙ DM: аналитические витрины
-- Витрины пересчитываются полностью при каждом запуске
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dm;

-- -----------------------------------------------------------
-- dm.agency_finance — KPI менеджеров по бюджету
-- Вопрос 1: Кто из менеджеров управляет наибольшим портфелем?
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS dm.agency_finance (
    report_month    DATE,           -- первое число месяца
    manager         TEXT,
    active_clients  INTEGER,
    turnover        NUMERIC(14,2),  -- суммарный рекламный расход
    commission      NUMERIC(14,2),  -- расчётная комиссия (10%)
    PRIMARY KEY (report_month, manager)
);

-- -----------------------------------------------------------
-- dm.platform_stats — сравнение платформ
-- Вопрос 2: Какая платформа эффективнее по CTR и стоимости клика?
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS dm.platform_stats (
    report_date     DATE,
    platform        TEXT,
    avg_ctr         NUMERIC(6,4),   -- средний CTR (клики/показы)
    avg_cpc         NUMERIC(10,2),  -- средняя стоимость клика
    total_clicks    BIGINT,
    PRIMARY KEY (report_date, platform)
);

-- -----------------------------------------------------------
-- dm.client_kpi — эффективность клиентов и отраслевой срез
-- Вопрос 3: Какие отрасли формируют основной бюджет?
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS dm.client_kpi (
    report_month    DATE,
    client_id       INTEGER,
    client_name     TEXT,
    industry        TEXT,
    total_spend     NUMERIC(14,2),
    total_conversions BIGINT,
    cpl             NUMERIC(10,2),  -- cost per lead
    PRIMARY KEY (report_month, client_id)
);

-- -----------------------------------------------------------
-- dm.site_reliability — надёжность сайтов клиентов
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS dm.site_reliability (
    report_month        DATE,
    site_id             INTEGER,
    client_name         TEXT,
    avg_load_time_ms    NUMERIC(8,2),
    avg_availability    NUMERIC(6,4),
    total_errors        BIGINT,
    PRIMARY KEY (report_month, site_id)
);


-- ============================================================
-- BUILD: пересчёт витрин (запускается после загрузки DDS)
-- ============================================================

-- 1. agency_finance
TRUNCATE dm.agency_finance;
INSERT INTO dm.agency_finance
SELECT
    DATE_TRUNC('month', fa.stat_date)::DATE AS report_month,
    cl.manager,
    COUNT(DISTINCT cl.client_id)             AS active_clients,
    SUM(fa.spend)                            AS turnover,
    ROUND(SUM(fa.spend) * 0.10, 2)          AS commission
FROM dds.fact_advertising fa
JOIN dds.campaigns         cp ON cp.campaign_id = fa.campaign_id
JOIN dds.clients           cl ON cl.client_id   = cp.client_id
GROUP BY 1, 2;

-- 2. platform_stats
TRUNCATE dm.platform_stats;
INSERT INTO dm.platform_stats
SELECT
    fa.stat_date                             AS report_date,
    cp.platform,
    ROUND(
        SUM(fa.clicks)::NUMERIC /
        NULLIF(SUM(fa.impressions), 0), 4)  AS avg_ctr,
    ROUND(
        SUM(fa.spend) /
        NULLIF(SUM(fa.clicks), 0), 2)       AS avg_cpc,
    SUM(fa.clicks)                          AS total_clicks
FROM dds.fact_advertising fa
JOIN dds.campaigns         cp ON cp.campaign_id = fa.campaign_id
GROUP BY 1, 2;

-- 3. client_kpi
TRUNCATE dm.client_kpi;
INSERT INTO dm.client_kpi
SELECT
    DATE_TRUNC('month', fa.stat_date)::DATE AS report_month,
    cl.client_id,
    cl.client_name,
    cl.industry,
    SUM(fa.spend)                           AS total_spend,
    SUM(fa.conversions)                     AS total_conversions,
    ROUND(
        SUM(fa.spend) /
        NULLIF(SUM(fa.conversions), 0), 2)  AS cpl
FROM dds.fact_advertising fa
JOIN dds.campaigns         cp ON cp.campaign_id = fa.campaign_id
JOIN dds.clients           cl ON cl.client_id   = cp.client_id
GROUP BY 1, 2, 3, 4;

-- 4. site_reliability
TRUNCATE dm.site_reliability;
INSERT INTO dm.site_reliability
SELECT
    DATE_TRUNC('month', sh.check_date)::DATE AS report_month,
    sh.site_id,
    cl.client_name,
    ROUND(AVG(sh.load_time_ms), 2)           AS avg_load_time_ms,
    ROUND(AVG(sh.availability_pct), 4)       AS avg_availability,
    SUM(sh.server_errors)                    AS total_errors
FROM dds.fact_site_health sh
JOIN dds.sites             s  ON s.site_id   = sh.site_id
JOIN dds.clients           cl ON cl.client_id = s.client_id
GROUP BY 1, 2, 3;
