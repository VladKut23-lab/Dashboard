-- ============================================================
-- Загрузка STG → DDS: справочники и факты
-- Все операции идемпотентны (INSERT … ON CONFLICT DO UPDATE)
-- ============================================================

-- 1. Клиенты
INSERT INTO dds.clients (client_id, client_name, industry, manager, updated_at)
SELECT
    client_id,
    client_name,
    industry,
    manager,
    NOW()
FROM stg.clients
WHERE client_id IS NOT NULL
  AND client_name IS NOT NULL
ON CONFLICT (client_id) DO UPDATE
    SET client_name = EXCLUDED.client_name,
        industry    = EXCLUDED.industry,
        manager     = EXCLUDED.manager,
        updated_at  = EXCLUDED.updated_at;

-- 2. Сайты (site_id = client_id)
INSERT INTO dds.sites (site_id, site_url, client_id, updated_at)
SELECT
    client_id AS site_id,
    site_url,
    client_id,
    NOW()
FROM stg.clients
WHERE client_id IS NOT NULL
  AND site_url IS NOT NULL
ON CONFLICT (site_id) DO UPDATE
    SET site_url   = EXCLUDED.site_url,
        updated_at = EXCLUDED.updated_at;

-- 3. Кампании
INSERT INTO dds.campaigns (campaign_id, client_id, platform, campaign_type, start_date, updated_at)
SELECT
    campaign_id,
    client_id,
    platform,
    campaign_type,
    start_date,
    NOW()
FROM stg.campaigns
WHERE campaign_id IS NOT NULL
  AND client_id IS NOT NULL
ON CONFLICT (campaign_id) DO UPDATE
    SET client_id     = EXCLUDED.client_id,
        platform      = EXCLUDED.platform,
        campaign_type = EXCLUDED.campaign_type,
        start_date    = EXCLUDED.start_date,
        updated_at    = EXCLUDED.updated_at;

-- 4. Факты рекламной статистики (за текущую дату загрузки)
INSERT INTO dds.fact_advertising (stat_date, campaign_id, impressions, clicks, spend, conversions)
SELECT
    s.stat_date,
    s.campaign_id,
    s.impressions,
    s.clicks,
    s.spend,
    s.conversions
FROM stg.ad_stats s
-- Проверка целостности: только кампании, присутствующие в справочнике
INNER JOIN dds.campaigns c ON c.campaign_id = s.campaign_id
WHERE s.stat_date IS NOT NULL
  AND s.impressions >= 0
  AND s.clicks >= 0
  AND s.spend >= 0
ON CONFLICT (stat_date, campaign_id) DO UPDATE
    SET impressions  = EXCLUDED.impressions,
        clicks       = EXCLUDED.clicks,
        spend        = EXCLUDED.spend,
        conversions  = EXCLUDED.conversions;

-- 5. Факты мониторинга сайтов
INSERT INTO dds.fact_site_health (check_date, site_id, load_time_ms, availability_pct, server_errors)
SELECT
    m.check_date,
    m.site_id,
    m.load_time_ms,
    m.availability_pct,
    m.server_errors
FROM stg.site_monitoring m
INNER JOIN dds.sites s ON s.site_id = m.site_id
WHERE m.check_date IS NOT NULL
  AND m.availability_pct BETWEEN 0 AND 1
ON CONFLICT (check_date, site_id) DO UPDATE
    SET load_time_ms     = EXCLUDED.load_time_ms,
        availability_pct = EXCLUDED.availability_pct,
        server_errors    = EXCLUDED.server_errors;
