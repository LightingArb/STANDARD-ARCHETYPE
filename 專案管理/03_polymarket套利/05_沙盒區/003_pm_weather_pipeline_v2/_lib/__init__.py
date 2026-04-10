# _lib — snapshot test helper modules
#
# Shared constants and utilities for D snapshot probe / fetch.

from datetime import datetime, timezone

D_ALIAS_MAP: dict[str, str] = {
    "D1": "gfs_seamless",
    "D2": "jma_seamless",
    "D3": "jma_gsm",
    "D4": "knmi_seamless",
    "D5": "dmi_seamless",
    "D6": "metno_seamless",
    "D7": "best_match",
    "D8": "gfs_global",
    "D9": "ukmo_seamless",
    "D10": "ukmo_global_deterministic_10km",
    "D11": "icon_seamless",
    "D12": "icon_global",
    "D13": "icon_eu",
    "D14": "icon_d2",
    "D15": "gem_seamless",
    "D16": "gem_global",
    "D17": "meteofrance_seamless",
    "D18": "meteofrance_arpege",
    "D19": "cma_grapes_global",
}

D_MODEL_TO_ALIAS: dict[str, str] = {model: alias for alias, model in D_ALIAS_MAP.items()}
D_MODELS_19 = list(D_ALIAS_MAP.values())

# Keep these aliases in the registry, but exclude them from monthly / snapshot
# full-batch defaults until coverage / API issues are resolved.
D_DISABLED_MODEL_CODES: tuple[str, ...] = ("D13", "D14", "D18")
D_DISABLED_MODEL_REASONS: dict[str, str] = {
    "D13": "known_issue: icon_eu is a regional Europe-only model; disabled for global batch runs",
    "D14": "known_issue: icon_d2 is a Germany-region model; disabled for global batch runs",
    "D18": "known_issue: meteofrance_arpege API path is currently unstable / failing",
}
D_DISABLED_MODEL_NAMES = [D_ALIAS_MAP[alias] for alias in D_DISABLED_MODEL_CODES]
D_MODELS_ACTIVE_DEFAULT = [
    model for alias, model in D_ALIAS_MAP.items() if alias not in D_DISABLED_MODEL_CODES
]

D_HOURLY_VARS = ["temperature_2m"] + [
    f"temperature_2m_previous_day{i}" for i in range(1, 8)
]

OM_HISTORICAL_FORECAST_URL = (
    "https://historical-forecast-api.open-meteo.com/v1/forecast"
)

OM_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

C_HOURLY_VARS = ["temperature_2m"]


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_single_d_model(token: str) -> tuple[str, str]:
    token = token.strip()
    upper = token.upper()
    if upper in D_ALIAS_MAP:
        return upper, D_ALIAS_MAP[upper]
    if token in D_MODEL_TO_ALIAS:
        return D_MODEL_TO_ALIAS[token], token
    raise ValueError(
        f"無法辨識的 D 模型: '{token}'\n"
        f"  可用 alias: D1~D19\n"
        f"  可用全名: {', '.join(D_MODELS_19)}"
    )


def resolve_d_models(raw: list[str]) -> list[str]:
    """Resolve D alias (D1~D19) or full model name to list of model names."""
    if not raw:
        return list(D_MODELS_19)

    resolved: list[str] = []
    for token in raw:
        _alias, model = _resolve_single_d_model(token)
        resolved.append(model)
    return resolved


def resolve_active_d_models(
    raw: list[str] | None = None,
) -> tuple[list[str], list[dict[str, str]]]:
    """
    Resolve user-specified D models, then drop known-disabled models from the
    active set while preserving explicit skip metadata for logging / reporting.
    """
    skipped: list[dict[str, str]] = []

    if not raw:
        for alias in D_DISABLED_MODEL_CODES:
            skipped.append({
                "token": alias,
                "alias": alias,
                "model": D_ALIAS_MAP[alias],
                "reason": D_DISABLED_MODEL_REASONS[alias],
            })
        return list(D_MODELS_ACTIVE_DEFAULT), skipped

    selected: list[str] = []
    seen_selected: set[str] = set()
    seen_skipped: set[str] = set()

    for token in raw:
        alias, model = _resolve_single_d_model(token)
        if alias in D_DISABLED_MODEL_CODES:
            if model not in seen_skipped:
                skipped.append({
                    "token": token.strip(),
                    "alias": alias,
                    "model": model,
                    "reason": D_DISABLED_MODEL_REASONS[alias],
                })
                seen_skipped.add(model)
            continue
        if model not in seen_selected:
            selected.append(model)
            seen_selected.add(model)

    return selected, skipped
