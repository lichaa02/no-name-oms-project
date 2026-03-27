import os
from typing import Optional

from sync_orders import (
    SyncConfig,
    SyncWindow,
    ensure_order_operations_row,
    fetch_billing_info,
    fetch_order_detail,
    fetch_shipment_detail,
    first_non_none,
    get_ml_access_token,
    logger,
    parse_ml_datetime,
    search_orders_incremental,
    upsert_billing_info,
    upsert_order,
    upsert_order_item,
    upsert_shipment,
    upsert_user,
)


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


def _parse_required_datetime_env(name: str):
    raw_value = _require_env(name)
    parsed_value = parse_ml_datetime(raw_value)
    if parsed_value is None:
        raise ValueError(f"Env var {name} could not be parsed as datetime: {raw_value!r}")
    return parsed_value


def _load_max_orders_per_run() -> Optional[int]:
    raw_value = os.getenv("SYNC_MAX_ORDERS_PER_RUN", "").strip()
    if not raw_value:
        return None

    parsed_value = int(raw_value)
    if parsed_value <= 0:
        return None

    return parsed_value


def run_backfill(cfg: Optional[SyncConfig] = None) -> None:
    cfg = cfg or SyncConfig.from_env()

    from_ts = _parse_required_datetime_env("BACKFILL_FROM_TS")
    to_ts = _parse_required_datetime_env("BACKFILL_TO_TS")

    if from_ts >= to_ts:
        raise ValueError(
            f"Invalid backfill window: BACKFILL_FROM_TS={from_ts.isoformat()} "
            f">= BACKFILL_TO_TS={to_ts.isoformat()}"
        )

    max_orders_per_run = _load_max_orders_per_run()
    page_limit = 50

    if max_orders_per_run is not None:
        page_limit = max(1, min(50, max_orders_per_run))
        logger.info(
            "SYNC_MAX_ORDERS_PER_RUN active in run_backfill: max_orders_per_run=%s page_limit=%s",
            max_orders_per_run,
            page_limit,
        )

    logger.info(
        "Starting run_backfill for seller_id=%s resource=%s from_ts=%s to_ts=%s",
        cfg.seller_id,
        cfg.resource,
        from_ts.isoformat(),
        to_ts.isoformat(),
    )

    access_token = get_ml_access_token()
    logger.info("Obtained Mercado Libre access token for run_backfill")

    window = SyncWindow(from_ts=from_ts, to_ts=to_ts)

    orders = search_orders_incremental(
        cfg,
        window,
        access_token=access_token,
        page_limit=page_limit,
        max_results=max_orders_per_run,
    )

    logger.info(
        "Fetched %s backfill order candidates for fixed window from_ts=%s to_ts=%s",
        len(orders),
        from_ts.isoformat(),
        to_ts.isoformat(),
    )

    for index, row in enumerate(orders, start=1):
        order_id = int(row["id"])

        candidate_last_updated_raw = first_non_none(
            row.get("date_last_updated"),
            row.get("last_updated"),
        )

        order_payload = fetch_order_detail(order_id, access_token=access_token)

        buyer = order_payload.get("buyer")
        if isinstance(buyer, dict) and buyer.get("id") is not None:
            upsert_user(buyer)

        shipment_id = first_non_none(
            (order_payload.get("shipping") or {}).get("id"),
            (order_payload.get("shipment") or {}).get("id"),
        )

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

        order_items = order_payload.get("order_items", [])
        if isinstance(order_items, list):
            for item in order_items:
                if isinstance(item, dict):
                    upsert_order_item(order_id, item)

        billing_payload = fetch_billing_info(
            order_id,
            access_token=access_token,
        )
        if billing_payload:
            upsert_billing_info(order_id, billing_payload)

        ensure_order_operations_row(order_id)

        if index % 10 == 0 or index == len(orders):
            logger.info(
                "Processed %s/%s backfill order candidates",
                index,
                len(orders),
            )

    logger.info(
        "run_backfill finished successfully without updating oms.sync_state: "
        "seller_id=%s from_ts=%s to_ts=%s processed=%s",
        cfg.seller_id,
        from_ts.isoformat(),
        to_ts.isoformat(),
        len(orders),
    )


if __name__ == "__main__":
    run_backfill()