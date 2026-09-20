# NorthCart: пайплайн подготовки batch-признаков в Airflow

## 1. Постановка задачи
ML-модель «покупка в ближайшие 7 дней» опирается на онлайн-признаки и batch-признаки (агрегаты за последние дни и недели). 

**Цель проекта** — воспроизводимый пайплайн batch-признаков:
* Для каждой пары `(customer_id, run_date)` рассчитать признаки за окна 7 и 30 дней.
* **Строго по данным, доступным до даты среза `run_date`** (утечка из будущего исключена).
* Логика расчёта единая для исторических срезов (обучение модели) и инференсного среза: меняется только дата среза `run_date`, передаваемая в DAG параметром.

## 2. What делает DAG
DAG `batch_features` (ручной запуск: `schedule=None`, `catchup=False`, `max_active_runs=1`):

1. `build_and_upload_features`:
   * Читает `run_date` из Airflow Variable `batch_features_run_date`.
   * Загружает сырые таблицы `customers`, `sessions`, `events`, `orders` из PostgreSQL (Connection `ecommerce_db`).
   * Вызывает функции модуля: предобработка → расчёт признаков → валидация.
   * Сохраняет срез локально и загружает его в S3 (Connection `aws_default`) по ключу `run_date=<дата>/batch_features.csv`.
2. `validate_saved_result`: проверяет, что объект в S3 существует и не пустой.

Расчётной логики в DAG нет — он только вызывает функции `dags/calculate_batch_features.py`.

## 3. Структура репозитория

| Путь | Назначение |
| :--- | :--- |
| `dags/calculate_batch_features.py` | Модуль расчёта (единая точка истины): чтение из PostgreSQL, предобработка, расчёт признаков, валидация, сохранение |
| `dags/batch_features.py` | Airflow DAG: оркестрация вызовов модуля |
| `dags/sql/`, `dags/score_funcs.py` | Служебные файлы шаблона, в решении не используются |
| `notebook.ipynb` | Вспомогательный артефакт: разведка данных, отладка логики, сверка результатов прогонов DAG |
| `requirements.txt` | Зависимости проекта |
| `.gitignore` | Исключает `.env`, чекпоинты и кеши из репозитория |
| `app.json` | Служебный файл шаблона (CI) |
| `README.md` | Это описание |

## 4. Зависимости и окружение
* Python 3.9+.
* **Установка:** `pip install -r requirements.txt` (pandas, psycopg2-binary, boto3, pyarrow, python-dotenv).
* Airflow 2.x с Postgres-провайдером доступен в окружении платформы; файлы DAG и модуля лежат в папке `dags/`, которую Airflow сканирует автоматически.
* **Локальный запуск тетрадки:** секреты читаются из файла `.env`, которого нет в репозитории (имена переменных: `DB_NAME`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_ENDPOINT_URL`, `S3_BUCKET`).

## 5. База данных (данные из сниппета «===Активация БД===»)
* **Имя базы данных:** `playground_ds_20260915_ff9d3b0716`
* **user:** `ds_20260915_ff9d3b0716`
* Хост, порт и пароль берутся из сниппета активации и указываются только в Airflow Connection.
* Таблицы: `public.customers`, `public.sessions`, `public.events`, `public.orders`.

## 6. Настройка Airflow (создать заранее)

### Variables (Admin → Variables)

| Имя | Значение |
| :--- | :--- |
| `batch_features_run_date` | дата среза в формате `YYYY-MM-DD`, например `2025-09-01` |

### Connections (Admin → Connections)

| Connection ID | Тип | Содержимое |
| :--- | :--- | :--- |
| `ecommerce_db` | Postgres | Host, Port, Schema (имя БД), Login, Password из сниппета; Extra: `{"sslmode": "require"}` |
| `aws_default` | Amazon Web Services | все параметры в Extra: `{"aws_access_key_id": "<ключ>", "aws_secret_access_key": "<секрет>", "endpoint_url": "https://yandexcloud.net", "bucket": "s3-ds-20260915-ff9d3b0716"}` |

Значения секретов вводятся только в интерфейсе Airflow и не хранятся в репозитории.

## 7. Запуск DAG
1. Установить Variable `batch_features_run_date` в нужную дату среза.
2. DAGs → `batch_features`: включить тумблер (unpause) и нажать **Trigger DAG**.
3. Дождаться успеха обеих задач: `build_and_upload_features` → `validate_saved_result`.
4. Для нового среза — поменять значение Variable и снова Trigger: логика расчёта не меняется, меняется только дата среза.

Даты, использованные в проекте: `2025-09-01` (исторический срез для обучения) и `2025-10-01` (более поздний срез для инференса).

## 8. Результат расчёта и как проверить корректность
* **Куда сохраняется:** S3-бакет `s3-ds-20260915-ff9d3b0716`, ключ `run_date=<дата>/batch_features.csv` (CSV: 17 846 строк — по одной на клиента, 19 колонок). Каждый прогон пишет отдельный ключ и не перезаписывает предыдущие результаты.
* **Встроенные проверки:**
  * Задача DAG `validate_saved_result` — объект существует в S3 и `ContentLength > 0`.
  * Функция модуля `validate_features` — уникальность `(customer_id, run_date)`, число строк = числу клиентов, контракт колонок, отсутствие NaN, счётчики 7d ≤ 30d, `days_since_last_purchase` ∈ {−1} ∪ [0, ∞), конверсии ≥ 0.
  * Анти-утечка: в расчёт попадают только записи с временем строго меньше `run_date` (контроль встроен в `build_batch_features`).
* **Внешняя проверка:** `notebook.ipynb`, ячейка «Сверка результатов двух прогонов DAG»: набор колонок и множество `customer_id` у срезов идентичны, метка `run_date` внутри таблиц совпадает с датой прогона, значения признаков различаются. Выводы прогонов 2025-09-01 and 2025-10-01 сохранены в тетрадке.

## 9. Правила расчёта признаков (кратко)
* Окна агрегации: `[run_date - 7d, run_date)` и `[run_date - 30d, run_date)`; фильтры строгие: `timestamp < run_date`, `start_time < run_date`, `order_time < run_date`.
* Дедупликация по первичным ключам таблиц; `product_id` участвует только в признаке уникальных товаров (строки с заданным значением).
* Денежные агрегаты (`orders_cnt_30d`, `orders_sum_usd_30d`, `orders_avg_usd_30d`) — только из `orders`.
* Длина сессии = `max(timestamp) - min(timestamp)` по событиям сессии; сессия без событий → 0 сек; средняя длина — за окно 30 дней.
* `days_since_last_purchase` — по всей истории до `run_date`; заказов не было → −1.
* Деление на ноль в конверсиях → 0.0; пропуски: счётчики → 0, вещественные → 0.0.

### Состав признаков (контракт колонок)
`customer_id`, `run_date`, `page_view_cnt_7d`, `page_view_cnt_30d`, `add_to_cart_cnt_7d`, `add_to_cart_cnt_30d`, `view_to_cart_conv_7d`, `view_to_cart_conv_30d`, `cart_to_purchase_conv_7d`, `cart_to_purchase_conv_30d`, `unique_products_7d`, `unique_products_30d`, `avg_session_duration_sec_30d`, `sessions_cnt_7d`, `sessions_cnt_30d`, `days_since_last_purchase`, `orders_cnt_30d`, `orders_sum_usd_30d`, `orders_avg_usd_30d`.