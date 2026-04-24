# Медианация — хранилище данных и аналитический дашборд

Прототип аналитического контура для ООО «Медианация» (агентство интернет-маркетинга).  
Стек: **PostgreSQL 16 · Apache Airflow 2.9 · Grafana 10**.

---

## Структура репозитория

```
medianation/
├── dags/
│   └── medianation_daily_pipeline.py   # DAG Airflow
├── sql/
│   ├── stg/create_stg_tables.sql       # DDL слоя STG
│   ├── dds/create_dds_tables.sql       # DDL слоя DDS
│   ├── dds/load_stg_to_dds.sql         # Трансформации STG → DDS
│   └── dm/create_and_build_dm.sql      # DDL и пересчёт витрин DM
├── scripts/
│   ├── init_db.sql                     # Инициализация БД (авто при docker compose up)
│   └── generate_sample_data.py         # Генератор тестовых CSV-файлов
├── data/sample/                        # Тестовые данные (генерируются скриптом)
├── grafana/provisioning/
│   ├── datasources/postgres.yml        # Подключение к PostgreSQL
│   └── dashboards/
│       ├── dashboard.yml               # Провижн конфиг
│       └── medianation.json            # Дашборд (импортируется автоматически)
└── docker-compose.yml
```

---

## Быстрый старт

### Требования
- Docker Desktop 24+ (или Docker Engine + Compose plugin)
- 4 GB RAM свободно

### 1. Клонировать / распаковать репозиторий

```bash
git clone <repo-url> medianation
cd medianation
```

### 2. Сгенерировать тестовые данные

```bash
python scripts/generate_sample_data.py
# Создаёт ~184 CSV-файла в data/sample/ за январь–март 2024
```

### 3. Запустить стек

```bash
docker compose up -d
# Первый запуск: ~3–5 минут (скачивание образов + инициализация БД)
```

### 4. Проверить готовность сервисов

| Сервис | URL | Логин / Пароль |
|---|---|---|
| Airflow Webserver | http://localhost:8080 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| PostgreSQL | localhost:5432 | medianation / medianation |

### 5. Запустить DAG вручную (первый раз)

В Airflow UI:
1. Открыть DAG `medianation_daily_pipeline`
2. Включить тумблер (Enable)
3. Нажать **Trigger DAG w/ config** и выбрать дату, например `2024-03-31`
4. Дождаться зелёных статусов всех задач (~1–2 мин)

Для загрузки всего диапазона январь–март 2024 можно запустить backfill:

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill medianation_daily_pipeline \
  --start-date 2024-01-01 --end-date 2024-03-31
```

### 6. Открыть дашборд

http://localhost:3000 → Dashboards → **Медианация — Аналитический дашборд**

Дашборд загрузится автоматически благодаря provisioning.

---

## Архитектура данных

```
CSV-файлы  ──►  STG (сырые данные)
                  │
                  ▼
               DDS (справочники + факты)
                  │
                  ▼
               DM (витрины)  ──►  Grafana
```

### Слои

| Слой | Таблицы | Назначение |
|---|---|---|
| **stg** | clients, campaigns, ad_stats, site_monitoring | Первичная загрузка, без трансформаций |
| **dds** | clients, sites, campaigns, fact_advertising, fact_site_health | Нормализованное хранение с референсами |
| **dm** | agency_finance, platform_stats, client_kpi, site_reliability | Готовые витрины для BI |

### Аналитические витрины (dm)

| Витрина | Ключевой вопрос |
|---|---|
| `dm.agency_finance` | Кто из менеджеров управляет наибольшим бюджетом? |
| `dm.platform_stats` | Какая платформа эффективнее по CTR и CPC? |
| `dm.client_kpi` | Какие отрасли формируют основной оборот агентства? |
| `dm.site_reliability` | Насколько надёжны сайты клиентов? |

---

## Качество данных и идемпотентность

- STG очищается (`TRUNCATE`) перед каждой загрузкой
- DDS использует `INSERT … ON CONFLICT DO UPDATE` (upsert)
- DM витрины пересчитываются полностью (`TRUNCATE` + `INSERT`)
- В Python-задачах выполняются проверки:
  - отсутствие NULL в ключевых полях
  - удаление дублирующихся ID
  - запрет отрицательных числовых метрик
  - допустимый диапазон `availability_pct` [0, 1]
- При кратковременном сбое Airflow автоматически повторяет задачу (2 retry, интервал 5 мин)

---

## Остановка стека

```bash
docker compose down          # остановить контейнеры (данные сохраняются)
docker compose down -v       # остановить + удалить volumes (полный сброс)
```
