from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from psycopg.types.json import Jsonb

from sync_orders import (
    SyncConfig,
    compute_sync_window,
    first_non_none,
    get_db_connection,
    get_ml_access_token,
    get_sync_state,
    logger,
    mark_sync_error,
    mark_sync_running,
    mark_sync_success,
    ml_get,
    normalize_int,
    normalize_text,
    parse_ml_datetime,
    utcnow,
)


@dataclass(frozen=True)
class ClaimSearchResult:
    summaries: list[dict]
    scanned: int
    skipped_historical: int
    skipped_before_window: int
    skipped_after_window: int
    skipped_invalid_ts: int
    has_more_in_window: bool


def _claims_lookback_days() -> int:
    raw_value = os.getenv("CLAIMS_LOOKBACK_DAYS", "60").strip()
    parsed_value = int(raw_value)
    return max(1, min(parsed_value, 60))


def _claims_page_limit() -> int:
    raw_value = os.getenv("CLAIMS_SEARCH_PAGE_LIMIT", "30").strip()
    parsed_value = int(raw_value)
    return max(1, min(parsed_value, 100))


def _claims_search_statuses() -> list[str]:
    raw_value = os.getenv("CLAIMS_SEARCH_STATUSES", "opened,closed").strip()
    statuses: list[str] = []
    seen: set[str] = set()

    for part in raw_value.split(","):
        normalized = part.strip().lower()
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        statuses.append(normalized)

    if not statuses:
        return ["opened", "closed"]

    return statuses


def _claims_search_sort() -> str:
    raw_value = os.getenv("CLAIMS_SEARCH_SORT", "last_updated:desc").strip()
    if not raw_value:
        return "last_updated:desc"
    return raw_value


def _claims_search_is_desc() -> bool:
    sort_value = _claims_search_sort().strip().lower()
    return sort_value.endswith(":desc")


def _load_max_claims_per_run() -> Optional[int]:
    raw_value = os.getenv("SYNC_MAX_CLAIMS_PER_RUN", "").strip()
    if not raw_value:
        return None

    parsed_value = int(raw_value)
    if parsed_value <= 0:
        return None

    return parsed_value


def _claims_sync_config(base_cfg: Optional[SyncConfig] = None) -> SyncConfig:
    base_cfg = base_cfg or SyncConfig.from_env()
    return SyncConfig(
        seller_id=base_cfg.seller_id,
        resource="claims",
        overlap_seconds=base_cfg.overlap_seconds,
        lookback_days_first_run=_claims_lookback_days(),
        high_watermark_lag_seconds=base_cfg.high_watermark_lag_seconds,
    )


def ensure_claims_sync_state(cfg: SyncConfig) -> None:
    fallback_cursor = utcnow() - timedelta(days=cfg.lookback_days_first_run)
    query = """
        INSERT INTO oms.meli_sync_state (
            resource,
            account_id,
            cursor_ts,
            overlap_seconds,
            status
        )
        VALUES (%s, %s, %s, %s, 'idle')
        ON CONFLICT (resource, account_id) DO NOTHING
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    cfg.resource,
                    cfg.seller_id,
                    fallback_cursor,
                    cfg.overlap_seconds,
                ),
            )


def _extract_claim_batch(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]

    if not isinstance(payload, dict):
        raise RuntimeError(
            "Unexpected Mercado Libre claims search response: payload is not a list or object"
        )

    for key in ("data", "results", "claims"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]

    return []


def _claim_key_from_summary(claim_payload: dict) -> Optional[int]:
    return normalize_int(
        first_non_none(
            claim_payload.get("id"),
            claim_payload.get("claim_id"),
        )
    )


def _claim_effective_ts(claim_payload: dict) -> Optional[datetime]:
    return parse_ml_datetime(
        first_non_none(
            claim_payload.get("last_updated"),
            claim_payload.get("date_last_updated"),
            claim_payload.get("updated_at"),
            claim_payload.get("date_created"),
            claim_payload.get("created_at"),
        )
    )


def search_claim_summaries(
    *,
    window_from_ts: datetime,
    window_to_ts: datetime,
    operational_cutoff: datetime,
    access_token: Optional[str] = None,
    page_limit: Optional[int] = None,
    max_results: Optional[int] = None,
) -> ClaimSearchResult:
    token = access_token or get_ml_access_token()
    limit = page_limit or _claims_page_limit()
    statuses = _claims_search_statuses()
    sort_value = _claims_search_sort()
    sort_desc = _claims_search_is_desc()

    all_results: list[dict] = []
    seen_claim_ids: set[int] = set()
    scanned = 0
    skipped_historical = 0
    skipped_before_window = 0
    skipped_after_window = 0
    skipped_invalid_ts = 0

    for status in statuses:
        offset = 0
        stop_status = False

        while True:
            payload = ml_get(
                token,
                "/post-purchase/v1/claims/search",
                params={
                    "status": status,
                    "offset": offset,
                    "limit": limit,
                    "sort": sort_value,
                },
            )
            batch = _extract_claim_batch(payload)
            paging = payload.get("paging", {}) if isinstance(payload, dict) else {}
            total_raw = paging.get("total") if isinstance(paging, dict) else None
            total = int(total_raw) if total_raw is not None else None

            if not batch:
                break

            for row in batch:
                scanned += 1

                effective_ts = _claim_effective_ts(row)
                if effective_ts is None:
                    skipped_invalid_ts += 1
                    continue

                if effective_ts < operational_cutoff:
                    skipped_historical += 1
                    if sort_desc:
                        stop_status = True
                        break
                    continue

                if effective_ts < window_from_ts:
                    skipped_before_window += 1
                    if sort_desc:
                        stop_status = True
                        break
                    continue

                if effective_ts > window_to_ts:
                    skipped_after_window += 1
                    if not sort_desc:
                        stop_status = True
                        break
                    continue

                claim_id = _claim_key_from_summary(row)
                if claim_id is not None:
                    if claim_id in seen_claim_ids:
                        continue
                    seen_claim_ids.add(claim_id)

                all_results.append(row)

                if max_results is not None and len(all_results) >= max_results:
                    return ClaimSearchResult(
                        summaries=all_results[:max_results],
                        scanned=scanned,
                        skipped_historical=skipped_historical,
                        skipped_before_window=skipped_before_window,
                        skipped_after_window=skipped_after_window,
                        skipped_invalid_ts=skipped_invalid_ts,
                        has_more_in_window=True,
                    )

            if stop_status:
                break

            if total is not None and (offset + len(batch)) >= total:
                break

            if total is None and len(batch) < limit:
                break

            offset += len(batch)

    return ClaimSearchResult(
        summaries=all_results,
        scanned=scanned,
        skipped_historical=skipped_historical,
        skipped_before_window=skipped_before_window,
        skipped_after_window=skipped_after_window,
        skipped_invalid_ts=skipped_invalid_ts,
        has_more_in_window=False,
    )


def _extract_claim_id(claim_payload: dict) -> int:
    claim_id = normalize_int(
        first_non_none(
            claim_payload.get("id"),
            claim_payload.get("claim_id"),
        )
    )
    if claim_id is None:
        raise ValueError("claim_payload did not include id or claim_id")
    return claim_id


def fetch_claim_detail(
    claim_id: int,
    access_token: Optional[str] = None,
) -> dict:
    token = access_token or get_ml_access_token()
    payload = ml_get(token, f"/post-purchase/v1/claims/{int(claim_id)}")
    if not isinstance(payload, dict):
        raise RuntimeError(
            f"Unexpected Mercado Libre /post-purchase/v1/claims/{claim_id} response: payload is not an object"
        )
    return payload


def _resource_parts(claim_payload: dict) -> tuple[str, Optional[int]]:
    resource_value = claim_payload.get("resource")
    resource_name: Optional[str] = None
    resource_id: Optional[int] = None

    if isinstance(resource_value, dict):
        resource_name = normalize_text(
            first_non_none(
                resource_value.get("name"),
                resource_value.get("type"),
                resource_value.get("resource"),
            )
        )
        resource_id = normalize_int(resource_value.get("id"))
    else:
        resource_name = normalize_text(resource_value)

    if resource_id is None:
        resource_id = normalize_int(claim_payload.get("resource_id"))

    if resource_name is None:
        order_id = normalize_int(
            first_non_none(
                claim_payload.get("order_id"),
                claim_payload.get("order"),
            )
        )
        if order_id is not None:
            return "order", order_id
        return "unknown", resource_id

    return resource_name, resource_id


def _extract_order_id(
    claim_payload: dict,
    resource_name: str,
    resource_id: Optional[int],
) -> Optional[int]:
    direct_order_id = normalize_int(
        first_non_none(
            claim_payload.get("order_id"),
            claim_payload.get("order"),
        )
    )
    if direct_order_id is not None:
        return direct_order_id

    normalized_resource = (resource_name or "").strip().lower()
    if normalized_resource in {"order", "orders"}:
        return resource_id

    return None


def _extract_reason_id(claim_payload: dict) -> Optional[str]:
    reason_value = claim_payload.get("reason")
    if isinstance(reason_value, dict):
        return normalize_text(
            first_non_none(
                reason_value.get("id"),
                reason_value.get("name"),
            )
        )

    return normalize_text(first_non_none(claim_payload.get("reason_id"), reason_value))


def _as_optional_bool(value: object) -> Optional[bool]:
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(value)

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "t", "1", "yes", "y"}:
            return True
        if normalized in {"false", "f", "0", "no", "n"}:
            return False

    return None


def order_exists(order_id: Optional[int]) -> bool:
    if order_id is None:
        return True

    query = """
        SELECT 1
        FROM oms.meli_orders
        WHERE order_id = %s
        LIMIT 1
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (order_id,))
            row = cur.fetchone()
            return row is not None


def upsert_claim(claim_payload: dict) -> None:
    if not isinstance(claim_payload, dict):
        raise ValueError("claim_payload must be a dict")

    claim_id = _extract_claim_id(claim_payload)
    resource_name, resource_id = _resource_parts(claim_payload)
    order_id = _extract_order_id(claim_payload, resource_name, resource_id)
    status = normalize_text(claim_payload.get("status")) or "unknown"
    claim_type = (
        normalize_text(
            first_non_none(
                claim_payload.get("claim_type"),
                claim_payload.get("type"),
                claim_payload.get("classification"),
                claim_payload.get("subtype"),
            )
        )
        or "unknown"
    )
    stage = normalize_text(claim_payload.get("stage"))
    parent_claim_id = normalize_int(
        first_non_none(
            claim_payload.get("parent_claim_id"),
            claim_payload.get("parent_id"),
        )
    )
    reason_id = _extract_reason_id(claim_payload)
    fulfilled = _as_optional_bool(claim_payload.get("fulfilled"))
    quantity_type = normalize_text(
        first_non_none(
            claim_payload.get("quantity_type"),
            (claim_payload.get("quantity") or {}).get("type")
            if isinstance(claim_payload.get("quantity"), dict)
            else None,
        )
    )
    site_id = normalize_text(
        first_non_none(
            claim_payload.get("site_id"),
            claim_payload.get("site"),
        )
    )

    date_created = parse_ml_datetime(
        first_non_none(
            claim_payload.get("date_created"),
            claim_payload.get("created_at"),
        )
    )
    last_updated = parse_ml_datetime(
        first_non_none(
            claim_payload.get("last_updated"),
            claim_payload.get("date_last_updated"),
            claim_payload.get("updated_at"),
            claim_payload.get("date_created"),
            claim_payload.get("created_at"),
        )
    )

    if date_created is None and last_updated is not None:
        date_created = last_updated
    if last_updated is None and date_created is not None:
        last_updated = date_created

    if date_created is None or last_updated is None:
        raise ValueError(f"claim_id={claim_id} missing date_created/last_updated")

    if (
        resource_id is None
        and order_id is not None
        and resource_name.strip().lower() in {"order", "orders"}
    ):
        resource_id = order_id

    query = """
        INSERT INTO oms.meli_claims (
            claim_id,
            order_id,
            resource,
            resource_id,
            status,
            claim_type,
            stage,
            parent_claim_id,
            reason_id,
            fulfilled,
            quantity_type,
            site_id,
            date_created,
            last_updated,
            raw_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (claim_id) DO UPDATE
        SET order_id = COALESCE(EXCLUDED.order_id, oms.meli_claims.order_id),
            resource = EXCLUDED.resource,
            resource_id = COALESCE(EXCLUDED.resource_id, oms.meli_claims.resource_id),
            status = EXCLUDED.status,
            claim_type = EXCLUDED.claim_type,
            stage = COALESCE(EXCLUDED.stage, oms.meli_claims.stage),
            parent_claim_id = COALESCE(EXCLUDED.parent_claim_id, oms.meli_claims.parent_claim_id),
            reason_id = COALESCE(EXCLUDED.reason_id, oms.meli_claims.reason_id),
            fulfilled = COALESCE(EXCLUDED.fulfilled, oms.meli_claims.fulfilled),
            quantity_type = COALESCE(EXCLUDED.quantity_type, oms.meli_claims.quantity_type),
            site_id = COALESCE(EXCLUDED.site_id, oms.meli_claims.site_id),
            date_created = COALESCE(EXCLUDED.date_created, oms.meli_claims.date_created),
            last_updated = EXCLUDED.last_updated,
            raw_json = EXCLUDED.raw_json,
            updated_at = NOW()
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    claim_id,
                    order_id,
                    resource_name,
                    resource_id,
                    status,
                    claim_type,
                    stage,
                    parent_claim_id,
                    reason_id,
                    fulfilled,
                    quantity_type,
                    site_id,
                    date_created,
                    last_updated,
                    Jsonb(claim_payload),
                ),
            )


def _min_datetime(
    current: Optional[datetime],
    candidate: Optional[datetime],
) -> Optional[datetime]:
    if candidate is None:
        return current
    if current is None:
        return candidate
    return min(current, candidate)


def _max_datetime(
    current: Optional[datetime],
    candidate: Optional[datetime],
) -> Optional[datetime]:
    if candidate is None:
        return current
    if current is None:
        return candidate
    return max(current, candidate)


def run_claims_sync(cfg: Optional[SyncConfig] = None) -> None:
    base_cfg = cfg or SyncConfig.from_env()
    claims_cfg = _claims_sync_config(base_cfg)

    logger.info(
        "Starting run_claims_sync for seller_id=%s resource=%s",
        claims_cfg.seller_id,
        claims_cfg.resource,
    )

    ensure_claims_sync_state(claims_cfg)
    state = get_sync_state(claims_cfg)
    window = compute_sync_window(state, claims_cfg)
    operational_cutoff = utcnow() - timedelta(days=_claims_lookback_days())
    max_claims_per_run = _load_max_claims_per_run()
    search_statuses = _claims_search_statuses()

    if max_claims_per_run is not None:
        logger.info(
            "SYNC_MAX_CLAIMS_PER_RUN active in run_claims_sync: max_claims_per_run=%s",
            max_claims_per_run,
        )

    logger.info(
        "Claims sync window ready: from_ts=%s to_ts=%s operational_cutoff=%s search_statuses=%s search_sort=%s",
        window.from_ts.isoformat(),
        window.to_ts.isoformat(),
        operational_cutoff.isoformat(),
        ",".join(search_statuses),
        _claims_search_sort(),
    )

    try:
        mark_sync_running(claims_cfg, window.to_ts)

        access_token = get_ml_access_token()
        logger.info("Obtained Mercado Libre access token for run_claims_sync")

        search_result = search_claim_summaries(
            window_from_ts=window.from_ts,
            window_to_ts=window.to_ts,
            operational_cutoff=operational_cutoff,
            access_token=access_token,
            page_limit=_claims_page_limit(),
            max_results=max_claims_per_run,
        )
        claim_summaries = search_result.summaries

        logger.info(
            "Claims search completed: scanned=%s candidates=%s skipped_historical=%s skipped_before_window=%s skipped_after_window=%s skipped_invalid_ts=%s has_more_in_window=%s",
            search_result.scanned,
            len(claim_summaries),
            search_result.skipped_historical,
            search_result.skipped_before_window,
            search_result.skipped_after_window,
            search_result.skipped_invalid_ts,
            search_result.has_more_in_window,
        )

        upserted = 0
        skipped_historical = 0
        skipped_outside_window = 0
        skipped_missing_order = 0
        skipped_invalid = 0
        max_scanned_in_window_ts: Optional[datetime] = None
        earliest_retry_ts: Optional[datetime] = None

        for index, claim_summary in enumerate(claim_summaries, start=1):
            effective_ts: Optional[datetime] = None

            try:
                claim_id = _extract_claim_id(claim_summary)
                claim_detail = fetch_claim_detail(claim_id, access_token=access_token)
                effective_ts = _claim_effective_ts(claim_detail)

                if effective_ts is None:
                    logger.warning(
                        "Skipping claim_id=%s because effective timestamp could not be determined",
                        claim_id,
                    )
                    skipped_invalid += 1
                    continue

                if effective_ts < operational_cutoff:
                    skipped_historical += 1
                    continue

                if effective_ts < window.from_ts or effective_ts > window.to_ts:
                    skipped_outside_window += 1
                    continue

                max_scanned_in_window_ts = _max_datetime(max_scanned_in_window_ts, effective_ts)

                resource_name, resource_id = _resource_parts(claim_detail)
                order_id = _extract_order_id(claim_detail, resource_name, resource_id)
                if not order_exists(order_id):
                    logger.warning(
                        "Skipping claim_id=%s because related order_id=%s does not exist yet in oms.meli_orders",
                        claim_id,
                        order_id,
                    )
                    skipped_missing_order += 1
                    earliest_retry_ts = _min_datetime(earliest_retry_ts, effective_ts)
                    continue

                upsert_claim(claim_detail)
                upserted += 1

                if index % 10 == 0 or index == len(claim_summaries):
                    logger.info(
                        "Claims sync progress: scanned=%s/%s upserted=%s skipped_historical=%s skipped_outside_window=%s skipped_missing_order=%s skipped_invalid=%s",
                        index,
                        len(claim_summaries),
                        upserted,
                        skipped_historical,
                        skipped_outside_window,
                        skipped_missing_order,
                        skipped_invalid,
                    )

            except Exception as claim_exc:
                skipped_invalid += 1
                if effective_ts is not None and window.from_ts <= effective_ts <= window.to_ts:
                    earliest_retry_ts = _min_datetime(earliest_retry_ts, effective_ts)
                logger.warning(
                    "Failed to process claim summary at index=%s: %s",
                    index,
                    claim_exc,
                )

        cursor_target = window.to_ts

        if search_result.has_more_in_window:
            cursor_target = max_scanned_in_window_ts or state.cursor_ts

        if earliest_retry_ts is not None:
            cursor_target = min(cursor_target, max(state.cursor_ts, earliest_retry_ts))

        mark_sync_success(claims_cfg, cursor_target)

        logger.info(
            "run_claims_sync finished successfully: seller_id=%s candidates=%s upserted=%s skipped_historical=%s skipped_outside_window=%s skipped_missing_order=%s skipped_invalid=%s has_more_in_window=%s cursor_target=%s",
            claims_cfg.seller_id,
            len(claim_summaries),
            upserted,
            skipped_historical,
            skipped_outside_window,
            skipped_missing_order,
            skipped_invalid,
            search_result.has_more_in_window,
            cursor_target.isoformat(),
        )

    except Exception as exc:
        try:
            mark_sync_error(claims_cfg, str(exc))
        except Exception as mark_exc:
            logger.warning("Failed to persist claims sync error state: %s", mark_exc)
        raise


if __name__ == "__main__":
    run_claims_sync()
