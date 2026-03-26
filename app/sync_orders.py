from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional

import psycopg
import requests
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger("sync_orders")


@dataclass(frozen=True)
class SyncConfig:
    seller_id: int
    resource: str = "orders"
    overlap_seconds: int = 300
    lookback_days_first_run: int = 30
    high_watermark_lag_seconds: int = 120

    @classmethod
    def from_env(cls) -> "SyncConfig":
        seller_id_raw = os.getenv("ML_SELLER_ID")
        if not seller_id_raw:
            raise ValueError("Missing required env var: ML_SELLER_ID")

        return cls(
            seller_id=int(seller_id_raw),
            resource=os.getenv("SYNC_RESOURCE", "orders"),
            overlap_seconds=int(os.getenv("SYNC_OVERLAP_SECONDS", "300")),
            lookback_days_first_run=int(os.getenv("SYNC_FIRST_RUN_LOOKBACK_DAYS", "30")),
            high_watermark_lag_seconds=int(os.getenv("SYNC_HIGH_WATERMARK_LAG_SECONDS", "120")),
        )


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    connect_timeout: int = 10

    @classmethod
    def from_env(cls) -> "DbConfig":
        host = os.getenv("DB_HOST")
        dbname = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")

        missing = [
            name
            for name, value in {
                "DB_HOST": host,
                "DB_NAME": dbname,
                "DB_USER": user,
                "DB_PASSWORD": password,
            }.items()
            if not value
        ]

        if missing:
            raise ValueError(f"Missing required DB env vars: {', '.join(missing)}")

        return cls(
            host=host,
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=int(os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "10")),
        )


@dataclass(frozen=True)
class MlConfig:
    client_id: str
    client_secret: str
    refresh_token: str
    api_base_url: str = "https://api.mercadolibre.com"
    request_timeout_seconds: int = 30

    @classmethod
    def from_env(cls) -> "MlConfig":
        client_id = os.getenv("ML_CLIENT_ID")
        client_secret = os.getenv("ML_CLIENT_SECRET")
        refresh_token = os.getenv("ML_REFRESH_TOKEN")

        missing = [
            name
            for name, value in {
                "ML_CLIENT_ID": client_id,
                "ML_CLIENT_SECRET": client_secret,
                "ML_REFRESH_TOKEN": refresh_token,
            }.items()
            if not value
        ]

        if missing:
            raise ValueError(f"Missing required ML env vars: {', '.join(missing)}")

        return cls(
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            api_base_url=os.getenv("ML_API_BASE_URL", "https://api.mercadolibre.com"),
            request_timeout_seconds=int(os.getenv("ML_REQUEST_TIMEOUT_SECONDS", "30")),
        )


@dataclass(frozen=True)
class SyncState:
    resource: str
    account_id: int
    cursor_ts: datetime
    overlap_seconds: int
    status: str


@dataclass(frozen=True)
class SyncWindow:
    from_ts: datetime
    to_ts: datetime


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def format_ml_datetime(value: datetime) -> str:
    value_utc = value.astimezone(timezone.utc)
    return value_utc.isoformat(timespec="milliseconds")


def get_db_connection() -> psycopg.Connection:
    db = DbConfig.from_env()
    return psycopg.connect(
        host=db.host,
        port=db.port,
        dbname=db.dbname,
        user=db.user,
        password=db.password,
        connect_timeout=db.connect_timeout,
        row_factory=dict_row,
    )


def compute_sync_window(state: SyncState, cfg: SyncConfig) -> SyncWindow:
    from_ts = state.cursor_ts - timedelta(seconds=state.overlap_seconds)
    to_ts = utcnow() - timedelta(seconds=cfg.high_watermark_lag_seconds)

    if from_ts >= to_ts:
        raise ValueError(
            f"Invalid sync window: from_ts={from_ts.isoformat()} >= to_ts={to_ts.isoformat()}"
        )

    return SyncWindow(from_ts=from_ts, to_ts=to_ts)


def get_sync_state(cfg: SyncConfig) -> SyncState:
    query = """
        SELECT
            resource,
            account_id,
            cursor_ts,
            overlap_seconds,
            status
        FROM oms.sync_state
        WHERE resource = %s
          AND account_id = %s
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (cfg.resource, cfg.seller_id))
            row = cur.fetchone()

    if not row:
        fallback_cursor = utcnow() - timedelta(days=cfg.lookback_days_first_run)
        logger.warning(
            "sync_state row not found for resource=%s seller_id=%s; using fallback cursor_ts=%s",
            cfg.resource,
            cfg.seller_id,
            fallback_cursor.isoformat(),
        )
        return SyncState(
            resource=cfg.resource,
            account_id=cfg.seller_id,
            cursor_ts=fallback_cursor,
            overlap_seconds=cfg.overlap_seconds,
            status="idle",
        )

    return SyncState(
        resource=row["resource"],
        account_id=row["account_id"],
        cursor_ts=row["cursor_ts"],
        overlap_seconds=row["overlap_seconds"],
        status=row["status"],
    )


def mark_sync_running(cfg: SyncConfig, high_watermark: datetime) -> None:
    query = """
        UPDATE oms.sync_state
        SET status = 'running',
            last_attempt_at = NOW(),
            last_high_watermark = %s,
            last_error = NULL,
            updated_at = NOW()
        WHERE resource = %s
          AND account_id = %s
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (high_watermark, cfg.resource, cfg.seller_id))
            if cur.rowcount != 1:
                raise RuntimeError(
                    f"Failed to mark sync_state running for resource={cfg.resource} seller_id={cfg.seller_id}"
                )


def mark_sync_success(cfg: SyncConfig, new_cursor_ts: datetime) -> None:
    query = """
        UPDATE oms.sync_state
        SET status = 'idle',
            cursor_ts = %s,
            last_success_at = NOW(),
            last_error = NULL,
            updated_at = NOW()
        WHERE resource = %s
          AND account_id = %s
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (new_cursor_ts, cfg.resource, cfg.seller_id))
            if cur.rowcount != 1:
                raise RuntimeError(
                    f"Failed to mark sync_state success for resource={cfg.resource} seller_id={cfg.seller_id}"
                )


def mark_sync_error(cfg: SyncConfig, error_message: str) -> None:
    safe_error_message = error_message[:4000]

    query = """
        UPDATE oms.sync_state
        SET status = 'error',
            last_error = %s,
            updated_at = NOW()
        WHERE resource = %s
          AND account_id = %s
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (safe_error_message, cfg.resource, cfg.seller_id))
            if cur.rowcount != 1:
                raise RuntimeError(
                    f"Failed to mark sync_state error for resource={cfg.resource} seller_id={cfg.seller_id}"
                )


def get_ml_access_token() -> str:
    ml = MlConfig.from_env()

    response = requests.post(
        f"{ml.api_base_url}/oauth/token",
        headers={
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "refresh_token",
            "client_id": ml.client_id,
            "client_secret": ml.client_secret,
            "refresh_token": ml.refresh_token,
        },
        timeout=ml.request_timeout_seconds,
    )
    response.raise_for_status()

    payload = response.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise RuntimeError("Mercado Libre token response did not include access_token")

    return access_token


def ml_get(
    access_token: str,
    path: str,
    params: Optional[dict] = None,
    extra_headers: Optional[dict] = None,
) -> dict:
    ml = MlConfig.from_env()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "accept": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)

    response = requests.get(
        f"{ml.api_base_url}{path}",
        headers=headers,
        params=params or {},
        timeout=ml.request_timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def run_db_selftest(cfg: Optional[SyncConfig] = None) -> None:
    cfg = cfg or SyncConfig.from_env()

    logger.info("Starting db self-test for resource=%s seller_id=%s", cfg.resource, cfg.seller_id)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    current_database() AS current_database,
                    current_user AS current_user,
                    NOW() AS db_now
                """
            )
            row = cur.fetchone()

    logger.info(
        "DB self-test connected successfully: current_database=%s current_user=%s db_now=%s",
        row["current_database"],
        row["current_user"],
        row["db_now"],
    )

    state = get_sync_state(cfg)
    window = compute_sync_window(state, cfg)

    logger.info(
        "DB self-test loaded sync_state successfully: resource=%s account_id=%s status=%s cursor_ts=%s",
        state.resource,
        state.account_id,
        state.status,
        state.cursor_ts.isoformat(),
    )

    logger.info(
        "DB self-test computed sync window successfully: from_ts=%s to_ts=%s",
        window.from_ts.isoformat(),
        window.to_ts.isoformat(),
    )


def run_ml_selftest(cfg: Optional[SyncConfig] = None) -> None:
    cfg = cfg or SyncConfig.from_env()

    logger.info("Starting ML self-test for seller_id=%s", cfg.seller_id)

    access_token = get_ml_access_token()
    logger.info("ML self-test obtained access token successfully")

    me = ml_get(access_token, "/users/me")
    logger.info(
        "ML self-test /users/me ok: id=%s nickname=%s site_id=%s",
        me.get("id"),
        me.get("nickname"),
        me.get("site_id"),
    )

    if str(me.get("id")) != str(cfg.seller_id):
        raise RuntimeError(
            f"ML self-test seller mismatch: users/me id={me.get('id')} does not match ML_SELLER_ID={cfg.seller_id}"
        )

    state = get_sync_state(cfg)
    window = compute_sync_window(state, cfg)

    sample = search_orders_incremental(
        cfg,
        window,
        access_token=access_token,
        page_limit=50,
        max_results=1,
    )
    logger.info(
        "ML self-test /orders/search ok: seller_id=%s from_ts=%s to_ts=%s sample_count=%s",
        cfg.seller_id,
        window.from_ts.isoformat(),
        window.to_ts.isoformat(),
        len(sample),
    )

    if not sample:
        logger.info("ML self-test found no orders in window; skipping order detail validation")
        return

    first_order_id = int(sample[0]["id"])
    order_detail = fetch_order_detail(first_order_id, access_token=access_token)

    logger.info(
        "ML self-test /orders/{id} ok: order_id=%s status=%s total_amount=%s order_items=%s",
        order_detail.get("id"),
        order_detail.get("status"),
        order_detail.get("total_amount"),
        len(order_detail.get("order_items", [])),
    )


def search_orders_incremental(
    cfg: SyncConfig,
    window: SyncWindow,
    access_token: Optional[str] = None,
    page_limit: int = 50,
    max_results: Optional[int] = None,
) -> list[dict]:
    token = access_token or get_ml_access_token()

    all_results: list[dict] = []
    offset = 0
    limit = max(1, min(page_limit, 50))

    while True:
        payload = ml_get(
            token,
            "/orders/search",
            params={
                "seller": cfg.seller_id,
                "order.date_last_updated.from": format_ml_datetime(window.from_ts),
                "order.date_last_updated.to": format_ml_datetime(window.to_ts),
                "offset": offset,
                "limit": limit,
                "sort": "date_desc",
            },
        )

        batch = payload.get("results", [])
        if not isinstance(batch, list):
            raise RuntimeError("Unexpected Mercado Libre /orders/search response: results is not a list")

        all_results.extend(batch)

        if max_results is not None and len(all_results) >= max_results:
            return all_results[:max_results]

        paging = payload.get("paging", {}) or {}
        total = int(paging.get("total", len(all_results)))

        if not batch or len(all_results) >= total:
            break

        offset += len(batch)

    return all_results


def fetch_order_detail(order_id: int, access_token: Optional[str] = None) -> dict:
    token = access_token or get_ml_access_token()
    return ml_get(token, f"/orders/{order_id}")


def normalize_phone(value: object) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None

    if not isinstance(value, dict):
        cleaned = str(value).strip()
        return cleaned or None

    parts: list[str] = []

    country_code = value.get("country_code")
    area_code = value.get("area_code")
    number = value.get("number")
    extension = value.get("extension")

    for piece in (country_code, area_code, number):
        if piece is None:
            continue
        cleaned_piece = str(piece).strip()
        if cleaned_piece:
            parts.append(cleaned_piece)

    normalized = " ".join(parts)

    if extension is not None:
        cleaned_extension = str(extension).strip()
        if cleaned_extension:
            normalized = f"{normalized} ext {cleaned_extension}".strip()

    return normalized or None


def parse_ml_datetime(value: object) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    if not isinstance(value, str):
        raise ValueError(f"datetime value must be str or datetime, got {type(value)!r}")

    cleaned = value.strip()
    if not cleaned:
        return None

    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError as exc:
        raise ValueError(f"Invalid Mercado Libre datetime value: {value!r}") from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed


def normalize_int(value: object) -> Optional[int]:
    if value is None:
        return None

    if isinstance(value, bool):
        return int(value)

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value)

    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        return int(cleaned)

    if isinstance(value, dict):
        nested_id = value.get("id")
        if nested_id is None:
            return None
        return normalize_int(nested_id)

    return int(str(value).strip())


def normalize_decimal(value: object) -> Optional[Decimal]:
    if value is None:
        return None

    if isinstance(value, Decimal):
        return value

    if isinstance(value, bool):
        return Decimal(int(value))

    if isinstance(value, int):
        return Decimal(value)

    if isinstance(value, float):
        return Decimal(str(value))

    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            return Decimal(cleaned)
        except InvalidOperation as exc:
            raise ValueError(f"Invalid decimal value: {value!r}") from exc

    try:
        return Decimal(str(value).strip())
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal value: {value!r}") from exc


def normalize_text(value: object) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, (dict, list, tuple, set)):
        return None

    cleaned = str(value).strip()
    return cleaned or None


def first_non_none(*values: object) -> object:
    for value in values:
        if value is not None:
            return value
    return None


def compose_full_name(*values: object) -> Optional[str]:
    parts: list[str] = []

    for value in values:
        cleaned = normalize_text(value)
        if cleaned:
            parts.append(cleaned)

    if not parts:
        return None

    return " ".join(parts)


def extract_attribute_value(attributes: object, attribute_type: str) -> Optional[str]:
    if not isinstance(attributes, list):
        return None

    target_type = attribute_type.strip().upper()

    for attribute in attributes:
        if not isinstance(attribute, dict):
            continue

        current_type = normalize_text(attribute.get("type"))
        if not current_type or current_type.upper() != target_type:
            continue

        return normalize_text(attribute.get("value"))

    return None


def fetch_shipment_detail(
    shipment_id: int,
    access_token: Optional[str] = None,
) -> Optional[dict]:
    token = access_token or get_ml_access_token()
    payload = ml_get(
        token,
        f"/shipments/{int(shipment_id)}",
        extra_headers={"x-format-new": "true"},
    )

    if not isinstance(payload, dict):
        raise RuntimeError(
            f"Unexpected Mercado Libre /shipments/{shipment_id} response: payload is not an object"
        )

    return payload


def fetch_billing_info(
    order_id: int,
    access_token: Optional[str] = None,
) -> Optional[dict]:
    token = access_token or get_ml_access_token()

    try:
        payload = ml_get(token, f"/orders/{int(order_id)}/billing_info")
    except requests.HTTPError as exc:
        response = exc.response
        if response is not None and response.status_code == 404:
            logger.info("No billing info found for order_id=%s", order_id)
            return None
        raise

    if not isinstance(payload, dict):
        raise RuntimeError(
            f"Unexpected Mercado Libre /orders/{order_id}/billing_info response: payload is not an object"
        )

    return payload or None


def upsert_user(user_payload: dict) -> None:
    if not isinstance(user_payload, dict):
        raise ValueError("user_payload must be a dict")

    user_id_raw = user_payload.get("id")
    if user_id_raw is None:
        raise ValueError("user_payload did not include id")

    user_id = int(user_id_raw)
    phone = normalize_phone(user_payload.get("phone"))
    alternative_phone = normalize_phone(user_payload.get("alternative_phone"))

    query = """
        INSERT INTO oms.users (
            user_id,
            nickname,
            first_name,
            last_name,
            email,
            phone,
            alternative_phone,
            site_id,
            raw_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE
        SET nickname = COALESCE(EXCLUDED.nickname, oms.users.nickname),
            first_name = COALESCE(EXCLUDED.first_name, oms.users.first_name),
            last_name = COALESCE(EXCLUDED.last_name, oms.users.last_name),
            email = COALESCE(EXCLUDED.email, oms.users.email),
            phone = COALESCE(EXCLUDED.phone, oms.users.phone),
            alternative_phone = COALESCE(EXCLUDED.alternative_phone, oms.users.alternative_phone),
            site_id = COALESCE(EXCLUDED.site_id, oms.users.site_id),
            raw_json = EXCLUDED.raw_json,
            updated_at = NOW()
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    user_id,
                    user_payload.get("nickname"),
                    user_payload.get("first_name"),
                    user_payload.get("last_name"),
                    user_payload.get("email"),
                    phone,
                    alternative_phone,
                    user_payload.get("site_id"),
                    Jsonb(user_payload),
                ),
            )


def upsert_shipment(shipment_payload: dict) -> None:
    if not isinstance(shipment_payload, dict):
        raise ValueError("shipment_payload must be a dict")

    shipment_id_raw = shipment_payload.get("id")
    if shipment_id_raw is None:
        raise ValueError("shipment_payload did not include id")

    status = shipment_payload.get("status")
    if not status:
        raise ValueError("shipment_payload did not include status")

    logistic = shipment_payload.get("logistic")
    if not isinstance(logistic, dict):
        logistic = {}

    source = shipment_payload.get("source")
    if not isinstance(source, dict):
        source = {}

    shipping_option = shipment_payload.get("shipping_option")
    if not isinstance(shipping_option, dict):
        shipping_option = {}

    lead_time = shipment_payload.get("lead_time")
    if not isinstance(lead_time, dict):
        lead_time = {}

    shipping_method = lead_time.get("shipping_method")
    if not isinstance(shipping_method, dict):
        shipping_method = {}

    site_id = first_non_none(
        shipment_payload.get("site_id"),
        source.get("site_id"),
    )
    mode = first_non_none(
        shipment_payload.get("mode"),
        logistic.get("mode"),
    )
    logistic_type = first_non_none(
        shipment_payload.get("logistic_type"),
        logistic.get("type"),
    )
    tracking_number = shipment_payload.get("tracking_number")
    tracking_method = first_non_none(
        shipment_payload.get("tracking_method"),
        shipping_method.get("type"),
    )
    shipping_option_id = first_non_none(
        normalize_int(shipping_option.get("id")),
        normalize_int(lead_time.get("option_id")),
    )
    shipping_option_name = first_non_none(
        shipping_option.get("name"),
        shipping_method.get("name"),
    )
    date_created = parse_ml_datetime(shipment_payload.get("date_created"))
    last_updated = parse_ml_datetime(shipment_payload.get("last_updated"))

    query = """
        INSERT INTO oms.shipments (
            shipment_id,
            site_id,
            mode,
            logistic_type,
            status,
            substatus,
            tracking_number,
            tracking_method,
            shipping_option_id,
            shipping_option_name,
            date_created,
            last_updated,
            raw_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (shipment_id) DO UPDATE
        SET site_id = COALESCE(EXCLUDED.site_id, oms.shipments.site_id),
            mode = COALESCE(EXCLUDED.mode, oms.shipments.mode),
            logistic_type = COALESCE(EXCLUDED.logistic_type, oms.shipments.logistic_type),
            status = EXCLUDED.status,
            substatus = COALESCE(EXCLUDED.substatus, oms.shipments.substatus),
            tracking_number = COALESCE(EXCLUDED.tracking_number, oms.shipments.tracking_number),
            tracking_method = COALESCE(EXCLUDED.tracking_method, oms.shipments.tracking_method),
            shipping_option_id = COALESCE(EXCLUDED.shipping_option_id, oms.shipments.shipping_option_id),
            shipping_option_name = COALESCE(EXCLUDED.shipping_option_name, oms.shipments.shipping_option_name),
            date_created = COALESCE(EXCLUDED.date_created, oms.shipments.date_created),
            last_updated = COALESCE(EXCLUDED.last_updated, oms.shipments.last_updated),
            raw_json = EXCLUDED.raw_json,
            updated_at = NOW()
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    int(shipment_id_raw),
                    site_id,
                    mode,
                    logistic_type,
                    status,
                    shipment_payload.get("substatus"),
                    tracking_number,
                    tracking_method,
                    shipping_option_id,
                    shipping_option_name,
                    date_created,
                    last_updated,
                    Jsonb(shipment_payload),
                ),
            )


def upsert_order(
    order_payload: dict,
    fallback_last_updated: object = None,
) -> None:
    if not isinstance(order_payload, dict):
        raise ValueError("order_payload must be a dict")

    order_id_raw = order_payload.get("id")
    if order_id_raw is None:
        raise ValueError("order_payload did not include id")

    status = order_payload.get("status")
    if not status:
        raise ValueError("order_payload did not include status")

    date_created = parse_ml_datetime(order_payload.get("date_created"))
    if date_created is None:
        raise ValueError("order_payload did not include date_created")

    primary_last_updated = order_payload.get("date_last_updated")
    secondary_last_updated = order_payload.get("last_updated")
    chosen_last_updated_raw = first_non_none(
        primary_last_updated,
        secondary_last_updated,
        fallback_last_updated,
    )
    last_updated = parse_ml_datetime(chosen_last_updated_raw)
    if last_updated is None:
        raise ValueError(
            "order_payload did not include date_last_updated, last_updated, "
            "or fallback_last_updated"
        )

    if primary_last_updated is None:
        logger.warning(
            "Order %s missing date_last_updated in detail payload; using fallback timestamp source",
            order_id_raw,
        )

    buyer_id = normalize_int(order_payload.get("buyer"))
    seller_id = normalize_int(order_payload.get("seller"))

    shipment_id = normalize_int((order_payload.get("shipping") or {}).get("id"))
    if shipment_id is None:
        shipment_id = normalize_int((order_payload.get("shipment") or {}).get("id"))

    query = """
        INSERT INTO oms.orders (
            order_id,
            site_id,
            pack_id,
            status,
            status_detail,
            date_created,
            last_updated,
            total_amount,
            currency_id,
            buyer_id,
            seller_id,
            shipment_id,
            raw_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO UPDATE
        SET site_id = COALESCE(EXCLUDED.site_id, oms.orders.site_id),
            pack_id = COALESCE(EXCLUDED.pack_id, oms.orders.pack_id),
            status = EXCLUDED.status,
            status_detail = COALESCE(EXCLUDED.status_detail, oms.orders.status_detail),
            date_created = EXCLUDED.date_created,
            last_updated = EXCLUDED.last_updated,
            total_amount = COALESCE(EXCLUDED.total_amount, oms.orders.total_amount),
            currency_id = COALESCE(EXCLUDED.currency_id, oms.orders.currency_id),
            buyer_id = COALESCE(EXCLUDED.buyer_id, oms.orders.buyer_id),
            seller_id = COALESCE(EXCLUDED.seller_id, oms.orders.seller_id),
            shipment_id = COALESCE(EXCLUDED.shipment_id, oms.orders.shipment_id),
            raw_json = EXCLUDED.raw_json,
            updated_at = NOW()
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    int(order_id_raw),
                    order_payload.get("site_id"),
                    normalize_int(order_payload.get("pack_id")),
                    status,
                    order_payload.get("status_detail"),
                    date_created,
                    last_updated,
                    order_payload.get("total_amount"),
                    order_payload.get("currency_id"),
                    buyer_id,
                    seller_id,
                    shipment_id,
                    Jsonb(order_payload),
                ),
            )


def upsert_order_item(order_id: int, item_payload: dict) -> None:
    if not isinstance(item_payload, dict):
        raise ValueError("item_payload must be a dict")

    item = item_payload.get("item")
    if not isinstance(item, dict):
        raise ValueError("item_payload did not include item dict")

    item_id_raw = item.get("id")
    if item_id_raw is None:
        raise ValueError("item_payload.item did not include id")

    item_id = str(item_id_raw).strip()
    if not item_id:
        raise ValueError("item_payload.item id is empty")

    quantity = normalize_int(item_payload.get("quantity"))
    if quantity is None:
        raise ValueError("item_payload did not include quantity")

    unit_price = normalize_decimal(item_payload.get("unit_price"))
    if unit_price is None:
        raise ValueError("item_payload did not include unit_price")

    variation_id = normalize_int(item.get("variation_id"))
    full_unit_price = normalize_decimal(item_payload.get("full_unit_price"))
    sale_fee = normalize_decimal(item_payload.get("sale_fee"))

    query = """
        INSERT INTO oms.order_items (
            order_id,
            item_id,
            variation_id,
            title,
            category_id,
            seller_sku,
            quantity,
            unit_price,
            full_unit_price,
            sale_fee,
            listing_type_id,
            raw_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT uq_order_items_order_variation DO UPDATE
        SET variation_id = EXCLUDED.variation_id,
            title = COALESCE(EXCLUDED.title, oms.order_items.title),
            category_id = COALESCE(EXCLUDED.category_id, oms.order_items.category_id),
            seller_sku = COALESCE(EXCLUDED.seller_sku, oms.order_items.seller_sku),
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            full_unit_price = COALESCE(EXCLUDED.full_unit_price, oms.order_items.full_unit_price),
            sale_fee = COALESCE(EXCLUDED.sale_fee, oms.order_items.sale_fee),
            listing_type_id = COALESCE(EXCLUDED.listing_type_id, oms.order_items.listing_type_id),
            raw_json = EXCLUDED.raw_json,
            updated_at = NOW()
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    int(order_id),
                    item_id,
                    variation_id,
                    item.get("title"),
                    item.get("category_id"),
                    item.get("seller_sku"),
                    quantity,
                    unit_price,
                    full_unit_price,
                    sale_fee,
                    item_payload.get("listing_type_id"),
                    Jsonb(item_payload),
                ),
            )


def upsert_billing_info(order_id: int, billing_payload: dict) -> None:
    if not isinstance(billing_payload, dict):
        raise ValueError("billing_payload must be a dict")

    identification = billing_payload.get("identification")
    if not isinstance(identification, dict):
        identification = {}

    taxpayer_type_obj = billing_payload.get("taxpayer_type")
    if not isinstance(taxpayer_type_obj, dict):
        taxpayer_type_obj = {}

    buyer_info = billing_payload.get("buyer_info")
    if not isinstance(buyer_info, dict):
        buyer_info = {}

    receiver_address = billing_payload.get("receiver_address")
    if not isinstance(receiver_address, dict):
        receiver_address = {}

    if not receiver_address:
        address = billing_payload.get("address")
        if isinstance(address, dict):
            receiver_address = address

    city_obj = receiver_address.get("city")
    if not isinstance(city_obj, dict):
        city_obj = {}

    state_obj = receiver_address.get("state")
    if not isinstance(state_obj, dict):
        state_obj = {}

    country_obj = receiver_address.get("country")
    if not isinstance(country_obj, dict):
        country_obj = {}

    attributes = billing_payload.get("attributes")

    doc_type = first_non_none(
        normalize_text(billing_payload.get("doc_type")),
        normalize_text(identification.get("type")),
        extract_attribute_value(attributes, "DOC_TYPE"),
    )

    doc_number = first_non_none(
        normalize_text(billing_payload.get("doc_number")),
        normalize_text(identification.get("number")),
        extract_attribute_value(attributes, "DOC_NUMBER"),
    )

    taxpayer_type = first_non_none(
        normalize_text(billing_payload.get("taxpayer_type")),
        normalize_text(taxpayer_type_obj.get("description")),
        normalize_text(taxpayer_type_obj.get("id")),
        extract_attribute_value(attributes, "TAXPAYER_TYPE"),
    )

    billing_name = first_non_none(
        normalize_text(billing_payload.get("billing_name")),
        normalize_text(buyer_info.get("billing_name")),
        compose_full_name(billing_payload.get("name"), billing_payload.get("last_name")),
        compose_full_name(buyer_info.get("name"), buyer_info.get("last_name")),
        normalize_text(billing_payload.get("business_name")),
        normalize_text(buyer_info.get("business_name")),
    )

    address_line = first_non_none(
        normalize_text(receiver_address.get("address_line")),
        normalize_text(billing_payload.get("address_line")),
    )

    street_name = first_non_none(
        normalize_text(receiver_address.get("street_name")),
        normalize_text(billing_payload.get("street_name")),
    )

    street_number = first_non_none(
        normalize_text(receiver_address.get("street_number")),
        normalize_text(billing_payload.get("street_number")),
    )

    city = first_non_none(
        normalize_text(receiver_address.get("city_name")),
        normalize_text(city_obj.get("name")),
        normalize_text(receiver_address.get("city")),
        normalize_text(billing_payload.get("city")),
    )

    state = first_non_none(
        normalize_text(state_obj.get("name")),
        normalize_text(state_obj.get("code")),
        normalize_text(receiver_address.get("state_name")),
        normalize_text(receiver_address.get("state")),
        normalize_text(billing_payload.get("state")),
    )

    country = first_non_none(
        normalize_text(country_obj.get("name")),
        normalize_text(country_obj.get("id")),
        normalize_text(receiver_address.get("country")),
        normalize_text(receiver_address.get("country_id")),
        normalize_text(billing_payload.get("country")),
        normalize_text(billing_payload.get("country_id")),
    )

    zip_code = first_non_none(
        normalize_text(receiver_address.get("zip_code")),
        normalize_text(billing_payload.get("zip_code")),
    )

    query = """
        INSERT INTO oms.billing_info (
            order_id,
            doc_type,
            doc_number,
            taxpayer_type,
            billing_name,
            address_line,
            street_name,
            street_number,
            city,
            state,
            country,
            zip_code,
            raw_json,
            fetched_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (order_id) DO UPDATE
        SET doc_type = COALESCE(EXCLUDED.doc_type, oms.billing_info.doc_type),
            doc_number = COALESCE(EXCLUDED.doc_number, oms.billing_info.doc_number),
            taxpayer_type = COALESCE(EXCLUDED.taxpayer_type, oms.billing_info.taxpayer_type),
            billing_name = COALESCE(EXCLUDED.billing_name, oms.billing_info.billing_name),
            address_line = COALESCE(EXCLUDED.address_line, oms.billing_info.address_line),
            street_name = COALESCE(EXCLUDED.street_name, oms.billing_info.street_name),
            street_number = COALESCE(EXCLUDED.street_number, oms.billing_info.street_number),
            city = COALESCE(EXCLUDED.city, oms.billing_info.city),
            state = COALESCE(EXCLUDED.state, oms.billing_info.state),
            country = COALESCE(EXCLUDED.country, oms.billing_info.country),
            zip_code = COALESCE(EXCLUDED.zip_code, oms.billing_info.zip_code),
            raw_json = EXCLUDED.raw_json,
            fetched_at = NOW(),
            updated_at = NOW()
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    int(order_id),
                    doc_type,
                    doc_number,
                    taxpayer_type,
                    billing_name,
                    address_line,
                    street_name,
                    street_number,
                    city,
                    state,
                    country,
                    zip_code,
                    Jsonb(billing_payload),
                ),
            )


def ensure_order_operations_row(order_id: int) -> None:
    query = """
        INSERT INTO oms.order_operations (order_id)
        VALUES (%s)
        ON CONFLICT (order_id) DO NOTHING
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (int(order_id),))


def run_sync(cfg: Optional[SyncConfig] = None) -> None:
    cfg = cfg or SyncConfig.from_env()
    logger.info("Starting sync scaffold for resource=%s seller_id=%s", cfg.resource, cfg.seller_id)

    state = get_sync_state(cfg)
    window = compute_sync_window(state, cfg)

    logger.info(
        "Computed sync window from_ts=%s to_ts=%s overlap_seconds=%s",
        window.from_ts.isoformat(),
        window.to_ts.isoformat(),
        state.overlap_seconds,
    )

    max_orders_raw = os.getenv("SYNC_MAX_ORDERS_PER_RUN", "").strip()
    max_orders_per_run: Optional[int] = None
    if max_orders_raw:
        parsed_max_orders = int(max_orders_raw)
        if parsed_max_orders > 0:
            max_orders_per_run = parsed_max_orders

    page_limit = 50
    if max_orders_per_run is not None:
        page_limit = max(1, min(50, max_orders_per_run))
        logger.info(
            "SYNC_MAX_ORDERS_PER_RUN active: max_orders_per_run=%s page_limit=%s",
            max_orders_per_run,
            page_limit,
        )

    access_token = get_ml_access_token()
    logger.info("Obtained Mercado Libre access token for run_sync")

    mark_sync_running(cfg, window.to_ts)

    orders = search_orders_incremental(
        cfg,
        window,
        access_token=access_token,
        page_limit=page_limit,
        max_results=max_orders_per_run,
    )
    logger.info("Fetched %s incremental order candidates", len(orders))

    success_cursor = window.to_ts

    for index, row in enumerate(orders, start=1):
        order_id = int(row["id"])

        candidate_last_updated_raw = first_non_none(
            row.get("date_last_updated"),
            row.get("last_updated"),
        )
        candidate_last_updated = parse_ml_datetime(candidate_last_updated_raw)

        if max_orders_per_run is not None and candidate_last_updated is not None:
            if candidate_last_updated < success_cursor:
                success_cursor = candidate_last_updated

        order_payload = fetch_order_detail(order_id, access_token=access_token)

        buyer = order_payload.get("buyer")
        if buyer:
            upsert_user(buyer)

        seller = order_payload.get("seller")
        if seller:
            upsert_user(seller)

        shipment_id = (
            order_payload.get("shipping", {}) or {}
        ).get("id") or (
            order_payload.get("shipment", {}) or {}
        ).get("id")

        if shipment_id:
            shipment_payload = fetch_shipment_detail(
                int(shipment_id),
                access_token=access_token,
            )
            if shipment_payload:
                upsert_shipment(shipment_payload)

        upsert_order(
            order_payload,
            fallback_last_updated=candidate_last_updated_raw,
        )

        for item in order_payload.get("order_items", []):
            upsert_order_item(order_id, item)

        billing_payload = fetch_billing_info(
            order_id,
            access_token=access_token,
        )
        if billing_payload:
            upsert_billing_info(order_id, billing_payload)

        ensure_order_operations_row(order_id)

        if index % 10 == 0 or index == len(orders):
            logger.info("Processed %s/%s order candidates", index, len(orders))

    if max_orders_per_run is not None and orders:
        logger.info(
            "Capped run finished: advancing cursor conservatively to oldest processed candidate ts=%s",
            success_cursor.isoformat(),
        )

    mark_sync_success(cfg, success_cursor)
    logger.info("Sync scaffold finished successfully with cursor_ts=%s", success_cursor.isoformat())

def main() -> None:
    cfg: Optional[SyncConfig] = None
    command = os.getenv("SYNC_COMMAND", "run_sync").strip().lower()

    try:
        cfg = SyncConfig.from_env()

        if command == "db_selftest":
            run_db_selftest(cfg)
            return

        if command == "ml_selftest":
            run_ml_selftest(cfg)
            return

        if command == "run_sync":
            run_sync(cfg)
            return

        raise ValueError(f"Unsupported SYNC_COMMAND: {command}")
    except NotImplementedError as exc:
        logger.warning("Scaffold created, but implementation is still pending: %s", exc)
        raise SystemExit(0)
    except Exception as exc:
        if cfg is not None and command == "run_sync":
            try:
                mark_sync_error(cfg, str(exc))
            except Exception as mark_exc:
                logger.warning("Failed to persist sync error state: %s", mark_exc)

        logger.exception("Sync failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
