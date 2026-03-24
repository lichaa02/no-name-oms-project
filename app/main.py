import json
import os
from typing import Any, Dict

import requests


ML_API_BASE = "https://api.mercadolibre.com"
ML_TOKEN_URL = f"{ML_API_BASE}/oauth/token"

SENSITIVE_KEYS = {
    "client_secret",
    "refresh_token",
    "access_token",
    "auth_code",
}


def log_info(message: str, **extra: Any) -> None:
    payload = {"level": "INFO", "message": message}
    if extra:
        payload["extra"] = extra
    print(json.dumps(payload, ensure_ascii=False))


def log_error(message: str, **extra: Any) -> None:
    payload = {"level": "ERROR", "message": message}
    if extra:
        payload["extra"] = extra
    print(json.dumps(payload, ensure_ascii=False))


def sanitize_config_for_logs(config: Dict[str, str]) -> Dict[str, str]:
    safe: Dict[str, str] = {}
    for k, v in config.items():
        value = "" if v is None else str(v).strip()
        if k in SENSITIVE_KEYS:
            safe[k] = "present" if value else "missing"
        else:
            safe[k] = value
    return safe


def load_config_from_env() -> Dict[str, str]:
    config = {
        "client_id": os.environ.get("ML_CLIENT_ID", "").strip(),
        "client_secret": os.environ.get("ML_CLIENT_SECRET", "").strip(),
        "refresh_token": os.environ.get("ML_REFRESH_TOKEN", "").strip(),
        "user_id": os.environ.get("ML_USER_ID", "").strip(),
        "redirect_uri": os.environ.get("ML_REDIRECT_URI", "").strip(),
        # opcional: si en algún momento decidís inyectarlo también
        "access_token": os.environ.get("ML_ACCESS_TOKEN", "").strip(),
    }

    required = ["client_id", "client_secret", "refresh_token", "user_id", "redirect_uri"]
    missing = [key for key in required if not config.get(key)]
    if missing:
        raise RuntimeError(f"Faltan variables de entorno críticas: {missing}")

    return config


def ml_headers(access_token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }


def refresh_access_token(config: Dict[str, str]) -> Dict[str, Any]:
    payload = {
        "grant_type": "refresh_token",
        "client_id": config["client_id"],
        "client_secret": config["client_secret"],
        "refresh_token": config["refresh_token"],
    }

    response = requests.post(ML_TOKEN_URL, data=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def apply_refreshed_tokens(
    token_data: Dict[str, Any],
    current_config: Dict[str, str],
) -> Dict[str, str]:
    new_access_token = str(token_data.get("access_token", "")).strip()
    new_refresh_token = str(
        token_data.get("refresh_token", current_config.get("refresh_token", ""))
    ).strip()

    merged = dict(current_config)
    merged["access_token"] = new_access_token
    merged["refresh_token"] = new_refresh_token
    return merged


def get_ml_user_me(access_token: str) -> Dict[str, Any]:
    response = requests.get(
        f"{ML_API_BASE}/users/me",
        headers=ml_headers(access_token),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def should_try_refresh_from_http_error(error: requests.HTTPError) -> bool:
    response = error.response
    if response is None:
        return False
    return response.status_code in (400, 401)


def main() -> None:
    try:
        config = load_config_from_env()
        log_info("Configuración cargada desde variables de entorno", config=sanitize_config_for_logs(config))

        access_token = config.get("access_token", "").strip()
        refreshed_token = False

        # Como ahora no persistimos access_token, lo normal es que venga vacío
        # y hagamos refresh directamente.
        if not access_token:
            log_info("No hay access token en entorno; se intentará refresh")
            token_data = refresh_access_token(config)
            config = apply_refreshed_tokens(token_data, config)
            access_token = config["access_token"]
            refreshed_token = True

        try:
            ml_user = get_ml_user_me(access_token)
        except requests.HTTPError as e:
            if not refreshed_token and should_try_refresh_from_http_error(e):
                log_info("Access token inválido o vencido; intentando refresh")
                token_data = refresh_access_token(config)
                config = apply_refreshed_tokens(token_data, config)
                access_token = config["access_token"]
                refreshed_token = True
                ml_user = get_ml_user_me(access_token)
            else:
                raise

        log_info(
            "Conexión con Mercado Libre validada",
            refreshed_token=refreshed_token,
            ml_user={
                "id": ml_user.get("id"),
                "nickname": ml_user.get("nickname"),
                "site_id": ml_user.get("site_id"),
                "country_id": ml_user.get("country_id"),
            },
        )

    except requests.HTTPError as e:
        response = e.response
        status_code = response.status_code if response is not None else None
        response_text = response.text[:1000] if response is not None and response.text else ""

        log_error(
            "HTTP error en integración con Mercado Libre",
            status_code=status_code,
            response_text=response_text,
        )
        raise

    except Exception as e:
        log_error(
            "Fallo general en ejecución",
            error_type=type(e).__name__,
            error=str(e),
        )
        raise


if __name__ == "__main__":
    main()
