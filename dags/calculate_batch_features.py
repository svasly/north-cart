"""Модуль расчёта batch-признаков NorthCart.

Единая точка истины для логики признаков: одни и те же функции используются
в Jupyter-тетради (отладка) и в Airflow DAG (продакшен-запуски).

Правила (собраны в одном месте):
* анти-утечка: строго timestamp < run_date, start_time < run_date, order_time < run_date;
* окна агрегации: [run_date - 7d, run_date) и [run_date - 30d, run_date);
* дедупликация по PK таблиц; product_id участвует только в признаке уникальных товаров;
* денежные агрегаты — только из orders; конверсии: деление на ноль -> 0.0;
* пропуски: счётчики -> 0, вещественные -> 0.0, days_since_last_purchase -> -1;
* база итоговой таблицы — все customer_id из customers (одна строка на пару с run_date).
"""

from __future__ import annotations

import io
from datetime import timedelta

import boto3
import pandas as pd
import psycopg2

TABLES = ("customers", "sessions", "events", "orders")
FUNNEL = ("page_view", "add_to_cart", "checkout", "purchase")
WINDOW_7D = timedelta(days=7)
WINDOW_30D = timedelta(days=30)

# Контракт состава и порядка колонок итоговой таблицы
FEATURE_COLUMNS = [
    "customer_id", "run_date",
    "page_view_cnt_7d", "page_view_cnt_30d",
    "add_to_cart_cnt_7d", "add_to_cart_cnt_30d",
    "view_to_cart_conv_7d", "view_to_cart_conv_30d",
    "cart_to_purchase_conv_7d", "cart_to_purchase_conv_30d",
    "unique_products_7d", "unique_products_30d",
    "avg_session_duration_sec_30d",
    "sessions_cnt_7d", "sessions_cnt_30d",
    "days_since_last_purchase",
    "orders_cnt_30d", "orders_sum_usd_30d", "orders_avg_usd_30d",
]

# ------------------------------------------------------------------ чтение


def load_source_tables(conn_params, tables=TABLES) -> dict:
    """Читает сырые таблицы из PostgreSQL.

    conn_params: dict для psycopg2.connect(**params) ИЛИ строка-URI
    (её отдаёт PostgresHook.get_uri() внутри DAG).
    """
    if isinstance(conn_params, str):
        conn = psycopg2.connect(conn_params)
    else:
        conn = psycopg2.connect(**conn_params)
    try:
        return {t: pd.read_sql(f"SELECT * FROM public.{t}", conn) for t in tables}
    finally:
        conn.close()


# ------------------------------------------------------------- утилиты


def to_utc(series: pd.Series) -> pd.Series:
    """Единый тип datetime64 и единый часовой пояс UTC; мусор -> NaT."""
    parsed = pd.to_datetime(series, errors="coerce")
    if parsed.dt.tz is None:
        parsed = parsed.dt.tz_localize("UTC")
    else:
        parsed = parsed.dt.tz_convert("UTC")
    return parsed


def _in_window(series: pd.Series, run_dt: pd.Timestamp, window: timedelta) -> pd.Series:
    """Маска окна [run_dt - window, run_dt): строго раньше T и не старее окна."""
    return (series >= run_dt - window) & (series < run_dt)


# --------------------------------------------------------- предобработка


def preprocess_customers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["signup_date"] = to_utc(out["signup_date"])
    out = out[out["signup_date"].notna()]
    return out.drop_duplicates(subset=["customer_id"])


def preprocess_sessions(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["start_time"] = to_utc(out["start_time"])
    out = out[out["start_time"].notna()]
    return out.drop_duplicates(subset=["session_id"])


def preprocess_events(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["timestamp"] = to_utc(out["timestamp"])
    out = out[out["timestamp"].notna()]
    return out.drop_duplicates(subset=["event_id"])


def preprocess_orders(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["order_time"] = to_utc(out["order_time"])
    out = out[out["order_time"].notna()]
    out = out.drop_duplicates(subset=["order_id"])
    out["total_usd"] = pd.to_numeric(out["total_usd"], errors="coerce").fillna(0.0)
    return out


# ------------------------------------------------------------ признаки


def add_event_window_features(base: pd.DataFrame, events: pd.DataFrame,
                              run_dt: pd.Timestamp) -> pd.DataFrame:
    """Счётчики событий и уникальные товары по окнам 7/30 дней (events уже обрезаны по T)."""
    for window in (WINDOW_7D, WINDOW_30D):
        w = events[_in_window(events["timestamp"], run_dt, window)]
        cnt = (
            w.pivot_table(index="customer_id", columns="event_type",
                          values="event_id", aggfunc="count")
            .reindex(columns=list(FUNNEL))
            .fillna(0)
            .astype(int)
        )
        uniq = (
            w.loc[w["product_id"].notna()]          # только строки с заданным product_id
            .groupby("customer_id")["product_id"].nunique()
            .rename("unique_products")
        )
        block = cnt.join(uniq, how="left")
        block["unique_products"] = block["unique_products"].fillna(0).astype(int)
        suffix = f"{window.days}d"
        block = block.rename(columns=lambda c: f"{c}_cnt_{suffix}" if c in FUNNEL else f"{c}_{suffix}")
        base = base.merge(block.reset_index(), on="customer_id", how="left")
        cnt_cols = [c for c in block.columns if c.endswith(suffix)]
        base[cnt_cols] = base[cnt_cols].fillna(0).astype(int)
    return base


def add_session_features(base: pd.DataFrame, sessions: pd.DataFrame,
                         events: pd.DataFrame, run_dt: pd.Timestamp) -> pd.DataFrame:
    """Число сессий за 7/30 дней и средняя длина сессии (сек) за 30 дней."""
    dur = (
        events.groupby("session_id")["timestamp"]
        .agg(lambda ts: (ts.max() - ts.min()).total_seconds())
        .rename("duration_sec")
        .reset_index()
    )
    session_duration = (
        sessions[["session_id"]]
        .merge(dur, on="session_id", how="left")
        .fillna({"duration_sec": 0.0})              # сессия без событий -> длина 0
    )
    for window in (WINDOW_7D, WINDOW_30D):
        w = sessions[_in_window(sessions["start_time"], run_dt, window)]
        cnt = (w.groupby("customer_id")["session_id"].count()
               .rename(f"sessions_cnt_{window.days}d"))
        base = base.merge(cnt.reset_index(), on="customer_id", how="left")
        base[f"sessions_cnt_{window.days}d"] = base[f"sessions_cnt_{window.days}d"].fillna(0).astype(int)

    w30 = sessions[_in_window(sessions["start_time"], run_dt, WINDOW_30D)]
    avg_dur = (
        w30.merge(session_duration, on="session_id", how="left")
        .groupby("customer_id")["duration_sec"].mean()
        .rename("avg_session_duration_sec_30d")
    )
    base = base.merge(avg_dur.reset_index(), on="customer_id", how="left")
    base["avg_session_duration_sec_30d"] = base["avg_session_duration_sec_30d"].fillna(0.0).astype(float)
    return base


def add_order_features(base: pd.DataFrame, orders: pd.DataFrame,
                       run_dt: pd.Timestamp) -> pd.DataFrame:
    """Денежные агрегаты за 30 дней (только orders) и давность последней покупки."""
    w30 = orders[_in_window(orders["order_time"], run_dt, WINDOW_30D)]
    agg = (
        w30.groupby("customer_id")
        .agg(orders_cnt_30d=("order_id", "count"),
             orders_sum_usd_30d=("total_usd", "sum"),
             orders_avg_usd_30d=("total_usd", "mean"))
    )
    base = base.merge(agg.reset_index(), on="customer_id", how="left")
    base["orders_cnt_30d"] = base["orders_cnt_30d"].fillna(0).astype(int)
    base[["orders_sum_usd_30d", "orders_avg_usd_30d"]] = (
        base[["orders_sum_usd_30d", "orders_avg_usd_30d"]].fillna(0.0).astype(float))

    last = orders.groupby("customer_id")["order_time"].max()   # вся история до T
    days = (run_dt - last).dt.days.rename("days_since_last_purchase")
    base = base.merge(days.reset_index(), on="customer_id", how="left")
    base["days_since_last_purchase"] = base["days_since_last_purchase"].fillna(-1).astype(int)
    return base


def add_conversion_features(df: pd.DataFrame) -> pd.DataFrame:
    """Конверсии воронки; деление на ноль -> 0.0."""
    def safe_ratio(num, den):
        return (num / den.where(den > 0)).fillna(0.0).astype(float)

    df = df.copy()
    df["view_to_cart_conv_7d"]      = safe_ratio(df["add_to_cart_cnt_7d"],  df["page_view_cnt_7d"])
    df["view_to_cart_conv_30d"]     = safe_ratio(df["add_to_cart_cnt_30d"], df["page_view_cnt_30d"])
    df["cart_to_purchase_conv_7d"]  = safe_ratio(df["purchase_cnt_7d"],     df["add_to_cart_cnt_7d"])
    df["cart_to_purchase_conv_30d"] = safe_ratio(df["purchase_cnt_30d"],    df["add_to_cart_cnt_30d"])
    return df


# ------------------------------------------------------- сборка и проверки


def validate_features(features: pd.DataFrame, n_customers: int) -> None:
    """Базовые проверки качества среза; падает с ValueError при нарушении."""
    checks = {
        "одна строка на (customer_id, run_date)": not features.duplicated(["customer_id", "run_date"]).any(),
        "строк = числу клиентов": len(features) == n_customers,
        "состав и порядок колонок = контракт": list(features.columns) == FEATURE_COLUMNS,
        "нет NaN": not features.isna().any().any(),
        "счётчики 7d <= 30d": bool(
            (features["page_view_cnt_7d"] <= features["page_view_cnt_30d"]).all()
            and (features["add_to_cart_cnt_7d"] <= features["add_to_cart_cnt_30d"]).all()
            and (features["unique_products_7d"] <= features["unique_products_30d"]).all()
            and (features["sessions_cnt_7d"] <= features["sessions_cnt_30d"]).all()),
        "days_since: -1 или >= 0": bool(((features["days_since_last_purchase"] == -1)
                                         | (features["days_since_last_purchase"] >= 0)).all()),
        "конверсии >= 0": bool((features[[c for c in FEATURE_COLUMNS if "conv" in c]] >= 0).all().all()),
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise ValueError(f"Проверки не пройдены: {failed}")


def build_batch_features(customers: pd.DataFrame, sessions: pd.DataFrame,
                         events: pd.DataFrame, orders: pd.DataFrame,
                         run_date: str) -> pd.DataFrame:
    """Полный пайплайн: предобработка -> срез по T -> признаки -> контракт колонок -> проверки."""
    run_dt = pd.Timestamp(run_date, tz="UTC")

    customers_pp = preprocess_customers(customers)
    sessions_pp = preprocess_sessions(sessions)
    events_pp = preprocess_events(events)
    orders_pp = preprocess_orders(orders)

    # у событий нет customer_id — присоединяем через sessions (целостность проверена в EDA)
    events_pp = events_pp.merge(sessions_pp[["session_id", "customer_id"]],
                                on="session_id", how="inner")

    # анти-утечка: единый срез «доступных на момент T» данных
    sessions_pp = sessions_pp[sessions_pp["start_time"] < run_dt]
    events_pp = events_pp[events_pp["timestamp"] < run_dt]
    orders_pp = orders_pp[orders_pp["order_time"] < run_dt]
    for df, col in ((sessions_pp, "start_time"), (events_pp, "timestamp"), (orders_pp, "order_time")):
        if not df.empty and df[col].max() >= run_dt:
            raise ValueError(f"Утечка из будущего: {col} >= run_date")

    base = pd.DataFrame({"customer_id": customers_pp["customer_id"].unique()})
    base = add_event_window_features(base, events_pp, run_dt)
    base = add_session_features(base, sessions_pp, events_pp, run_dt)
    base = add_order_features(base, orders_pp, run_dt)
    base = add_conversion_features(base)

    base["run_date"] = run_dt.date().isoformat()
    base = base[FEATURE_COLUMNS]                    # служебные purchase_cnt_* отбрасываются здесь
    validate_features(base, customers_pp["customer_id"].nunique())
    return base


# ------------------------------------------------------------- сохранение


def save_features(features: pd.DataFrame, run_date: str, s3_params: dict,
                  prefix: str = "batch_features") -> str:
    """Сохраняет срез в S3 в формате parquet по ключу с меткой run_date.

    s3_params: {"endpoint_url": ..., "bucket": ..., "access_key": ..., "secret_key": ...}
    """
    key = f"{prefix}/run_date={run_date}/features.parquet"
    buffer = io.BytesIO()
    features.to_parquet(buffer, index=False)
    client = boto3.client(
        "s3",
        endpoint_url=s3_params["endpoint_url"],
        aws_access_key_id=s3_params["access_key"],
        aws_secret_access_key=s3_params["secret_key"],
    )
    client.put_object(Bucket=s3_params["bucket"], Key=key, Body=buffer.getvalue())
    return key