# NorthCart: пайплайн подготовки batch-признаков в Airflow



## 1. Постановка задачи



ML-модель «покупка в ближайшие 7 дней» опирается на онлайн-признаки и batch-признаки

(агрегаты за последние дни и недели). Цель проекта — воспроизводимый пайплайн batch-признаков:

для каждой пары `(customer\_id, run\_date)` рассчитать признаки за окна 7 и 30 дней

**строго по данным, доступным до даты среза `run\_date`** (утечка из будущего исключена).



Логика расчёта единая для исторических срезов (обучение модели) и инференсного среза:

меняется только дата среза `run\_date`, передаваемая в DAG параметром.



## 2. Что делает DAG



DAG `batch\_features` (ручной запуск: `schedule=None`, `catchup=False`, `max\_active\_runs=1`):



1. `build\_and\_upload\_features`:

&#x20;  - читает `run\_date` из Airflow Variable `batch\_features\_run\_date`;

&#x20;  - загружает сырые таблицы `customers`, `sessions`, `events`, `orders` из PostgreSQL

&#x20;    (Connection `ecommerce\_db`);

&#x20;  - вызывает функции модуля: предобработка → расчёт признаков → валидация;

&#x20;  - сохраняет срез локально и загружает его в S3 (Connection `aws\_default`)

&#x20;    по ключу `run\_date=<дата>/batch\_features.csv`.

2. `validate\_saved\_result`: проверяет, что объект в S3 существует и не пустой.



Расчётной логики в DAG нет — он только вызывает функции `dags/calculate\_batch\_features.py`.



## 3. Структура репозитория



| Путь | Назначение |

|---|---|

| `dags/calculate\_batch\_features.py` | Модуль расчёта (единая точка истины): чтение из PostgreSQL, предобработка, расчёт признаков, валидация, сохранение |

| `dags/batch\_features.py` | Airflow DAG: оркестрация вызовов модуля |

| `dags/sql/`, `dags/score\_funcs.py` | Служебные файлы шаблона, в решении не используются |

| `notebook.ipynb` | Вспомогательный артефакт: разведка данных, отладка логики, сверка результатов прогонов DAG |

| `requirements.txt` | Зависимости проекта |

| `.gitignore` | Исключает `.env`, чекпоинты и кеши из репозитория |

| `app.json` | Служебный файл шаблона (CI) |

| `README.md` | Это описание |



## 4. Зависимости и окружение



- Python 3.9+.

- Установка: `pip install -r requirements.txt`

&#x20; (pandas, psycopg2-binary, boto3, pyarrow, python-dotenv).

- Airflow 2.x с Postgres-провайдером доступен в окружении платформы; файлы DAG и модуля

&#x20; лежат в папке `dags/`, которую Airflow сканирует автоматически.

- Локальный запуск тетрадки: секреты читаются из файла `.env`, которого нет в репозитории

&#x20; (имена переменных: `DB\_NAME`, `DB\_HOST`, `DB\_PORT`, `DB\_USER`, `DB\_PASSWORD`,

&#x20; `S3\_ACCESS\_KEY`, `S3\_SECRET\_KEY`, `S3\_ENDPOINT\_URL`, `S3\_BUCKET`).



## 5. База данных (данные из сниппета «===Активация БД===»)



- **Имя базы данных:\*\* `playground\_ds\_20260915\_ff9d3b0716`

- **user:** `ds\_20260915\_ff9d3b0716`

- Хост, порт и пароль берутся из сниппета активации и указываются только в Airflow Connection.

- Таблицы: `public.customers`, `public.sessions`, `public.events`, `public.orders`.



## 6. Настройка Airflow (создать заранее)



### Variables (Admin → Variables)

| Имя | Значение |

|---|---|

| `batch\_features\_run\_date` | дата среза в формате `YYYY-MM-DD`, например `2025-09-01` |



### Connections (Admin → Connections)

| Connection ID | Тип | Содержимое |

|---|---|---|

| `ecommerce\_db` | Postgres | Host, Port, Schema (имя БД), Login, Password из сниппета; Extra: `{"sslmode": "require"}` |

| `aws\_default` | Amazon Web Services | все параметры в Extra: `{"aws\_access\_key\_id": "<ключ>", "aws\_secret\_access\_key": "<секрет>", "endpoint\_url": "https://storage.yandex.ru", "bucket": "s3-ds-20260915-ff9d3b0716"}` |



Значения секретов вводятся только в интерфейсе Airflow и не хранятся в репозитории.



## 7. Запуск DAG



1. Установить Variable `batch\_features\_run\_date` в нужную дату среза.

2. DAGs → `batch\_features`: включить тумблер (unpause) и нажать **Trigger DAG**.

3. Дождаться успеха обеих задач: `build\_and\_upload\_features` → `validate\_saved\_result`.

4. Для нового среза — поменять значение Variable и снова Trigger: логика расчёта не меняется,

&#x20;  меняется только дата среза.



Даты, использованные в проекте: `2025-09-01` (исторический срез для обучения)

и `2025-10-01` (более поздний срез для инференса).



## 8. Результат расчёта и как проверить корректность



- **Куда сохраняется:** S3-бакет `s3-ds-20260915-ff9d3b0716`,

&#x20; ключ `run\_date=<дата>/batch\_features.csv` (CSV: 17 846 строк — по одной на клиента, 19 колонок).

&#x20; Каждый прогон пишет отдельный ключ и не перезаписывает предыдущие результаты.

- **Встроенные проверки:**

&#x20; - задача DAG `validate\_saved\_result` — объект существует в S3 и `ContentLength > 0`;

&#x20; - функция модуля `validate\_features` — уникальность `(customer\_id, run\_date)`,

&#x20;   число строк = числу клиентов, контракт колонок, отсутствие NaN, счётчики 7d ≤ 30d,

&#x20;   `days\_since\_last\_purchase` ∈ {−1} ∪ \[0, ∞), конверсии ≥ 0;

&#x20; - анти-утечка: в расчёт попадают только записи с временем строго меньше `run\_date`

&#x20;   (контроль встроен в `build\_batch\_features`).

- **Внешняя проверка:** notebook.ipynb, ячейка «Сверка результатов двух прогонов DAG»:

&#x20; набор колонок и множество `customer\_id` у срезов идентичны, метка `run\_date` внутри таблиц

&#x20; совпадает с датой прогона, значения признаков различаются. Выводы прогонов

&#x20; 2025-09-01 и 2025-10-01 сохранены в тетрадке.



## 9. Правила расчёта признаков (кратко)



- Окна агрегации: `\[run\_date − 7d, run\_date)` и `\[run\_date − 30d, run\_date)`;

&#x20; фильтры строгие: `timestamp < run\_date`, `start\_time < run\_date`, `order\_time < run\_date`.

- Дедупликация по первичным ключам таблиц; `product\_id` участвует только в признаке

&#x20; уникальных товаров (строки с заданным значением).

- Денежные агрегаты (`orders\_cnt\_30d`, `orders\_sum\_usd\_30d`, `orders\_avg\_usd\_30d`) — только из `orders`.

- Длина сессии = `max(timestamp) − min(timestamp)` по событиям сессии; сессия без событий → 0 сек;

&#x20; средняя длина — за окно 30 дней.

- `days\_since\_last\_purchase` — по всей истории до `run\_date`; заказов не было → −1.

- Деление на ноль в конверсиях → 0.0; пропуски: счётчики → 0, вещественные → 0.0.



### Состав признаков (контракт колонок)

`customer\_id`, `run\_date`, `page\_view\_cnt\_7d`, `page\_view\_cnt\_30d`,

`add\_to\_cart\_cnt\_7d`, `add\_to\_cart\_cnt\_30d`, `view\_to\_cart\_conv\_7d`, `view\_to\_cart\_conv\_30d`,

`cart\_to\_purchase\_conv\_7d`, `cart\_to\_purchase\_conv\_30d`, `unique\_products\_7d`, `unique\_products\_30d`,

`avg\_session\_duration\_sec\_30d`, `sessions\_cnt\_7d`, `sessions\_cnt\_30d`,

`days\_since\_last\_purchase`, `orders\_cnt\_30d`, `orders\_sum\_usd\_30d`, `orders\_avg\_usd\_30d`.