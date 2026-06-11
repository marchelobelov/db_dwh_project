# US Flights DWH — Airflow + Python + SQL

Хранилище данных по внутренним авиаперелётам США (подход Инмона, пакетная загрузка)
и витрины под дашборд в Yandex DataLens.

Поток: **S3 → ods → stg → dds → dm**, оркестрация — один Airflow DAG `flights_us_dwh`.
Все трансформации на чистом SQL (файлы в `sql/`), E/L и обращения к API — на Python.

---

## 1. Решаемая задача

**Анализ качества внутренних авиаперевозок США**: где и у кого чаще задержки и отмены,
как они связаны с маршрутами, авиакомпаниями, регионом и погодой. Под это сделаны 4 чарта
(geomap аэропортов, нагруженность пар аэропортов, качество авиакомпаний с причинами,
задержки/отмены в разрезе температуры/погоды/региона).

**Какие поля берём из источника и почему** (BTS On-Time Performance):

| Источник | Поле в модели | Зачем |
|---|---|---|
| FlightDate | flight_dt / *_dttm_local | партиция + ось времени, основа timestamptz |
| Reporting_Airline | carrier_code | разрез по авиакомпаниям |
| Flight_Number_Reporting_Airline | carrier_flight_num | часть бизнес-ключа рейса |
| Tail_Number | tail_num | атрибут борта |
| Origin / Dest | origin_airport_dk / dest_airport_dk | география, маршруты, geomap |
| OriginCityName/State, DestCityName/State | атрибуты аэропорта | подписи, регион |
| CRSDepTime | flight_dttm_local | плановое время вылета (ключ) |
| DepDelay | dep_delay_min / actual_dep_dttm_local | факт. вылет = план + задержка |
| ArrDelay, ArrDel15 | arr_delay_min, arr_del15 | метрика задержки (≥15 мин — стандарт BTS) |
| Cancelled, CancellationCode | разбиение фактов + причина отмены | таблица отменённых рейсов |
| Diverted | (в completed) | рейс состоялся, отдельной таблицы нет |
| Distance | distance_mi | атрибут рейса |
| CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay | *_delay_min | разбивка задержек по причинам |

Поля диверсий (`Div*`), служебные группы и `Unnamed: 109` в модель не тянем — они не нужны
для задачи (см. требование docx «лишние поля в DDS и выше не добавлять»).

**Доп. источники:**
- **ourairports** `airports.csv` — справочник аэропортов (имя, гео, регион). Таймзоны в нём нет,
  поэтому IANA-таймзону считаем из координат через `timezonefinder` (один раз при загрузке).
- **Open-Meteo Archive API** — почасовая погода (температура, осадки, weather_code) в локальном
  времени аэропорта; джойнится к рейсу по аэропорту вылета и часу.

---

## 2. Архитектура слоёв

Одна БД Postgres, отдельная схема на слой (тег студента, чтобы не пересекаться с другими):
`mblv_ods`, `mblv_stg`, `mblv_dds`, `mblv_dm`. Тег и имена вынесены в `config/pipeline.yaml`.

| Слой | Таблицы | Что делает |
|---|---|---|
| **ods** | `flights_raw`, `airports_raw` | сырое переписывание S3/справочника, только нужные колонки, всё текстом |
| **stg** | `flights`, `airports`, `carriers`, `weather` | дедупликация + типизация; справочники; кэш погоды |
| **dds** | `dim_airport`, `dim_carrier`, `fct_flight_completed`, `fct_flight_cancelled` | нормализованное ядро: 2 факта (успешные / отменённые), timestamptz в локальном времени, обогащение погодой |
| **dm** | `airport_quality`, `route_load`, `carrier_quality`, `carrier_delay_reasons`, `carrier_cancellations`, `weather_delays` | агрегаты под каждый чарт, грань `flight_dt × ключ` |

### Потоки данных (алгоритмы)

1. **`extract_s3_to_ods`** — листинг `flights_us_data/<ds>/*.csv.gz`, чтение (gzip→CSV),
   регистронезависимый отбор нужных колонок (терпит отсутствие части полей на сервере),
   парс `FlightDate`→`flight_dt`, `DELETE` партиции + `COPY` в `ods.flights_raw`.
2. **`ods_to_stg`** (`sql/stg/stg_flights.sql`) — каст текста в типы, сборка наивных локальных
   `sched_dep_local`/`sched_arr_local` из `FlightDate`+`HHMM` (HHMM=2400 корректно даёт +24ч),
   дедуп по натуральному ключу `(flight_dt, carrier, flight_num, origin, dest, crs_dep_time)`
   (берём последнюю по `_loaded_at`).
3. **`load_weather`** — distinct аэропорты вылета за день из `stg.flights`, 1 запрос/аэропорт к
   Open-Meteo (`timezone=auto`), ретраи/бэкофф, `upsert` в `stg.weather`. Нагрузка ограничена
   только аэропортами инкремента.
4. **`stg_to_dds`** — `dim_airport`/`dim_carrier` (upsert), затем факты по `:process_date`
   (delete+insert): локализация `sched_dep_local AT TIME ZONE tz_name` → `flight_dttm_local`,
   `actual_dep_dttm_local = flight_dttm_local + dep_delay`, джойн погоды по часу вылета,
   split `cancelled = 0/1` на две таблицы.
5. **`dds_to_dm`** — пересборка каждой витрины за день (delete+insert) агрегатами из фактов.
6. **`dq_checks`** — sanity: stg>0 ⇒ dds непустой; `cancel_rate ∈ [0,1]`.

`load_reference` (ourairports + carrier seed) идёт параллельно и переустанавливается только
если справочник пуст или передан `refresh_airports=true` (медленно меняющийся).

### Логическая модель DDS

```
dim_airport (airport_dk PK) ─┐                ┌─ dim_carrier (carrier_code PK)
                             │                │
   fct_flight_completed (carrier_flight_num, flight_dttm_local, origin_airport_dk) PK
     origin_airport_dk ─→ dim_airport.airport_dk
     dest_airport_dk   ─→ dim_airport.airport_dk
     carrier_code      ─→ dim_carrier.carrier_code
     + дельты задержек по причинам, погода аэропорта вылета

   fct_flight_cancelled (carrier_flight_num, sched_dttm_local, origin_airport_dk) PK
     те же связи + cancellation_code
```

---

## 3. Структура проекта

```
dags/
  flights_dwh_dag.py          # DAG (PythonOperator-ы + зависимости)
  flights_dwh/
    config/  pipeline.yaml  columns.py  carriers_seed.csv
    common/  config.py  logging_utils.py  sql.py  pg_io.py  s3_io.py  airports.py  weather.py
    sql/
      ddl/  00_schemas 10_ods 20_stg 30_dds 40_dm
      stg/  stg_flights.sql
      dds/  dds_dim_airport  dds_dim_carrier  dds_fct_flight_completed  dds_fct_flight_cancelled
      dm/   dm_airport_quality  dm_route_load  dm_carrier_quality
            dm_carrier_delay_reasons  dm_carrier_cancellations  dm_weather_delays
    tasks.py
    requirements.txt
```

SQL ссылается на схемы как `{ods}/{stg}/{dds}/{dm}` (подставляются из конфига в `common/sql.py`),
параметры — через psycopg2 `%(process_date)s`.

---

## 4. Деплой и запуск

**Предусловия в Airflow (`http://89.169.174.119:8080`):** должны существовать Connections
- `edu_dwh_postgres` (Postgres, целевая БД),
- `s3_avia_ru` (S3/Yandex Object Storage; endpoint в extra либо берётся дефолт из конфига).

1. **Залить код в DAG-бакет** `gsb2024airflow` так, чтобы в папке dags Airflow появились
   `flights_dwh_dag.py` и пакет `flights_dwh/` целиком (включая `sql/`, `config/`, `common/`).
2. Убедиться, что зависимости из `flights_dwh/requirements.txt` установлены в окружении Airflow
   (ключевая — `timezonefinder`).
3. В UI включить DAG `flights_us_dwh`.
4. **Прогнать один день:** Trigger DAG w/ config `{"process_date": "2024-01-01"}`.
5. Проверить, что все задачи зелёные и появились строки:
   `SELECT count(*) FROM mblv_dm.airport_quality;` и т.д.

**Полный перезапуск:** Trigger w/ config `{"full_reload": true}` (очистит слои), затем backfill
нужного диапазона дат. **Инкремент** — по умолчанию: один запуск = одна партиция-день,
идемпотентно (delete+insert / upsert), повторный прогон даты безопасен.

---

## 5. Дашборд в DataLens (≥4 чарта)

Подключение: новое подключение к той же Postgres (`edu_dwh_postgres`), затем датасет на таблицу
`mblv_dm.*`. Везде ставьте фильтр по `flight_dt` (диапазон дат). **Меры** агрегируйте суммой;
**rate** считайте отношением сумм (а не средним готового rate-поля).

| Чарт | Витрина | Измерения | Меры / вычисляемые |
|---|---|---|---|
| **1. Geomap качества аэропортов** | `airport_quality` | `lat,lon` → Geopoint (`GEOPOINT([lat],[lon])`), `airport_dk`, `region` | `SUM(total_flights)`; цвет = `SUM(cancelled_cnt)/SUM(total_flights)` или `SUM(delayed_cnt)/(SUM(total_flights)-SUM(cancelled_cnt))` |
| **2. Нагруженные пары аэропортов** | `route_load` | `origin_dk`, `dest_dk` (+ имена/гео) | `SUM(flights_cnt)`; bar/таблица top-N. Закрепление: добавьте `origin_dk` (или `dest_dk`) в фильтр-селектор дашборда |
| **3. Качество авиакомпаний + причины** | `carrier_quality` (+ `carrier_delay_reasons`, `carrier_cancellations`) | `carrier_name`; для причин — `reason` / `cancellation_reason` | `SUM(cancelled_cnt)`, `SUM(delayed_cnt)`; stacked bar минут задержек по `reason` (`SUM(total_min)`) и отмен по `cancellation_reason` (`SUM(cancelled_cnt)`) |
| **4. Задержки/отмены vs погода/регион** | `weather_delays` | `region`, `temp_bucket`, `weather_code` | `SUM(delayed_cnt)`, `SUM(cancelled_cnt)`, `AVG(avg_arr_delay)`; heatmap температура×регион или bar по `weather_code` |

Вычисляемые поля в датасете (примеры):
- `cancel_rate = SUM([cancelled_cnt]) / SUM([total_flights])`
- `delay_rate  = SUM([delayed_cnt]) / (SUM([total_flights]) - SUM([cancelled_cnt]))`
- `point = GEOPOINT([lat],[lon])`

Закрепление origin/dest (чарт 2) делается селектором дашборда на поле `origin_dk`/`dest_dk`.

---

## 6. Соответствие требованиям

- 2 таблицы DDS — успешные (`fct_flight_completed`) и отменённые (`fct_flight_cancelled`) ✓
- `timestamptz` в локальном времени аэропорта; факт. вылет = план + задержка ✓
- `origin_airport_dk`/`dest_airport_dk` из Origin/Dest + справочник ✓
- ETL `s3 → ods → stg → dds → dm`, инкрементально, идемпотентно ✓
- Первый запуск создаёт схемы (`CREATE IF NOT EXISTS`); полный перезапуск — `full_reload` ✓
- SQL в `sql/`, common-функции и конфиги вынесены ✓
- ≥4 чарта в DataLens ✓
- Секреты только в Airflow Connections, в коде не хардкодятся ✓
