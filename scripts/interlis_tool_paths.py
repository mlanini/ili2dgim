from __future__ import annotations

import os
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_INTERLIS_TOOLS: dict[str, str] = {
    "ili2c": str(WORKSPACE_ROOT / "ressources" / "ili2c-5.6.8" / "ili2c.jar"),
    "ili2validator": str(
        WORKSPACE_ROOT / "ressources" / "ilivalidator-1.15.0" / "ilivalidator-1.15.0.jar"
    ),
    "ili2gpkg": str(
        WORKSPACE_ROOT / "ressources" / "ili2gpkg-5.3.1" / "ili2gpkg-5.3.1.jar"
    ),
    "ili2duckdb": "",
    "ili2pg": "",
    "ili2imd": str(
        WORKSPACE_ROOT / "ressources" / "ili2imd-1.7.1" / "ili2imd.jar"
    ),
}

INTERLIS_TOOL_ENV: dict[str, str] = {
    "ili2c": "ILI2C_JAR",
    "ili2validator": "ILIVALIDATOR_JAR",
    "ili2gpkg": "ILI2GPKG_JAR",
    "ili2duckdb": "ILI2DUCKDB_JAR",
    "ili2pg": "ILI2PG_JAR",
    "ili2imd": "ILI2IMD_JAR",
}

INTERLIS_TOOL_ENV_ALIASES: dict[str, tuple[str, ...]] = {
    "ili2validator": ("ILIVALIDATOR_JAR", "ILI2VALIDATOR_JAR"),
}

LOG_LEVEL_ORDER: dict[str, int] = {
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "ERROR": 40,
}


def resolve_interlis_tool_path(tool_name: str) -> Path | None:
    env_candidates = INTERLIS_TOOL_ENV_ALIASES.get(
        tool_name,
        tuple([INTERLIS_TOOL_ENV.get(tool_name, "")])
    )
    for env_name in env_candidates:
        if not env_name:
            continue
        configured = os.environ.get(env_name, "").strip()
        if configured:
            return Path(configured).expanduser()

    default_path = DEFAULT_INTERLIS_TOOLS.get(tool_name, "").strip()
    if default_path:
        return Path(default_path).expanduser()
    return None


def get_configured_log_level() -> str:
    raw_level = os.environ.get("DGIM_LOG_LEVEL", "INFO").strip().upper()
    return raw_level if raw_level in LOG_LEVEL_ORDER else "INFO"


def should_log(level: str) -> bool:
    configured = LOG_LEVEL_ORDER[get_configured_log_level()]
    wanted = LOG_LEVEL_ORDER.get(level.strip().upper(), LOG_LEVEL_ORDER["INFO"])
    return wanted >= configured


def describe_configured_interlis_tools() -> list[str]:
    configured_tools: list[str] = []
    for tool_name in INTERLIS_TOOL_ENV:
        tool_path = resolve_interlis_tool_path(tool_name)
        if tool_path is not None:
            configured_tools.append(f"{tool_name}={tool_path}")
    return configured_tools