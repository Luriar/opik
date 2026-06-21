from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from enum import Enum
from urllib.parse import urlparse

from dart_agent.dart_keys import DartApiKey, parse_dart_api_keys


class ConfigApprovalState(str, Enum):
    OK = "OK"
    MANUAL_APPROVAL_REQUIRED = "MANUAL_APPROVAL_REQUIRED"


@dataclass(frozen=True)
class RangeConfig:
    value: int
    approval_state: ConfigApprovalState = ConfigApprovalState.OK


@dataclass(frozen=True)
class Settings:
    airflow_db_url: str
    service_db_url: str
    dart_api_key_identifier: str
    dart_api_keys: tuple[DartApiKey, ...]
    dart_env_file: str | None
    storage_backend: str
    local_storage_base_path: str
    s3_bucket: str | None
    s3_base_prefix: str
    bronze_prefix: str
    silver_prefix: str
    gold_prefix: str
    aws_region: str
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    dart_backfill_months: RangeConfig
    dart_backfill_start_date: date | None
    dart_backfill_end_date: date | None
    dart_incremental_days: RangeConfig
    dart_daily_limit: int
    dart_daily_safe_limit: int
    dart_daily_emergency_limit: int
    dart_minute_limit: int
    dart_minute_safe_limit: int
    dart_minute_global_limit: int
    dart_minute_global_safe_limit: int
    dart_quota_request_log_enabled: bool
    dart_collect_mode: str
    embedding_provider: str
    embedding_model: str
    embedding_version: str
    embedding_dimension: int


class ConfigError(ValueError):
    pass


def _optional_str(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _optional_prefix(name: str) -> str | None:
    if name not in os.environ:
        return None
    value = os.environ[name].strip().strip("/")
    return value


def _required_str(name: str) -> str:
    value = _optional_str(name)
    if not value:
        raise ConfigError(f"{name} is required")
    return value


def _int_env(name: str, default: int, minimum: int | None = None) -> int:
    raw = _optional_str(name)
    if raw is None:
        value = default
    else:
        try:
            value = int(raw)
        except ValueError as exc:
            raise ConfigError(f"{name} must be an integer") from exc
    if minimum is not None and value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}")
    return value


def _bool_env(name: str, default: bool) -> bool:
    raw = _optional_str(name)
    if raw is None:
        return default
    normalized = raw.lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ConfigError(f"{name} must be a boolean")


def resolve_storage_backend(raw_backend: str | None, s3_bucket: str | None) -> str:
    """Resolve local/s3 storage from explicit setting and available S3 env values."""
    backend = (raw_backend or "auto").strip().lower()
    if backend not in {"auto", "local", "s3"}:
        raise ConfigError("STORAGE_BACKEND must be auto, local or s3")
    if backend == "local":
        return "local"
    if backend == "s3":
        return "s3" if s3_bucket else "local"
    return "s3" if s3_bucket else "local"


def resolve_s3_location(raw_bucket: str | None, raw_base_prefix: str | None) -> tuple[str | None, str]:
    """Resolve bucket and key prefix from S3 env values.

    `S3_BUCKET` normally contains only a bucket name. For operator convenience, this
    also accepts `s3://bucket/prefix` and treats the URI path as the storage root.
    """
    base_prefix = (raw_base_prefix if raw_base_prefix is not None else "").strip("/")
    if not raw_bucket:
        return None, base_prefix
    if raw_bucket.startswith("s3://"):
        parsed = urlparse(raw_bucket)
        if not parsed.netloc:
            raise ConfigError("S3_BUCKET URI must include a bucket name")
        return parsed.netloc, parsed.path.strip("/")
    if "/" in raw_bucket:
        raise ConfigError("S3_BUCKET must be a bucket name or s3://bucket/prefix URI")
    return raw_bucket, base_prefix


def parse_backfill_months(raw: str | int | None) -> RangeConfig:
    if raw is None or str(raw).strip() == "":
        return RangeConfig(value=12)
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigError("DART_BACKFILL_MONTHS must be an integer") from exc
    if value <= 0:
        raise ConfigError("DART_BACKFILL_MONTHS must be >= 1")
    if value > 36:
        return RangeConfig(value=value, approval_state=ConfigApprovalState.MANUAL_APPROVAL_REQUIRED)
    return RangeConfig(value=value)


def parse_incremental_days(raw: str | int | None) -> RangeConfig:
    if raw is None or str(raw).strip() == "":
        return RangeConfig(value=3)
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigError("DART_INCREMENTAL_DAYS must be an integer") from exc
    if value < 0:
        raise ConfigError("DART_INCREMENTAL_DAYS must be >= 0")
    if value > 30:
        return RangeConfig(value=value, approval_state=ConfigApprovalState.MANUAL_APPROVAL_REQUIRED)
    return RangeConfig(value=value)


def parse_optional_date(name: str, raw: str | None) -> date | None:
    if raw is None or raw.strip() == "":
        return None
    normalized = raw.strip().replace("-", "")
    if len(normalized) != 8 or not normalized.isdigit():
        raise ConfigError(f"{name} must be YYYY-MM-DD or YYYYMMDD")
    try:
        return date(int(normalized[0:4]), int(normalized[4:6]), int(normalized[6:8]))
    except ValueError as exc:
        raise ConfigError(f"{name} must be a valid date") from exc


def parse_backfill_date_range(
    start_raw: str | None,
    end_raw: str | None,
) -> tuple[date | None, date | None]:
    start = parse_optional_date("DART_BACKFILL_START_DATE", start_raw)
    end = parse_optional_date("DART_BACKFILL_END_DATE", end_raw)
    if (start is None) != (end is None):
        raise ConfigError(
            "DART_BACKFILL_START_DATE and DART_BACKFILL_END_DATE must be set together"
        )
    if start is not None and end is not None and start > end:
        raise ConfigError("DART_BACKFILL_START_DATE must be <= DART_BACKFILL_END_DATE")
    return start, end


def get_settings() -> Settings:
    backfill_start_date, backfill_end_date = parse_backfill_date_range(
        _optional_str("DART_BACKFILL_START_DATE"),
        _optional_str("DART_BACKFILL_END_DATE"),
    )
    s3_bucket, s3_base_prefix = resolve_s3_location(
        _optional_str("S3_BUCKET") or _optional_str("AWS_S3_BUCKET"),
        _optional_prefix("S3_BASE_PREFIX")
        if "S3_BASE_PREFIX" in os.environ
        else _optional_prefix("AWS_S3_BASE_PREFIX"),
    )
    storage_backend = resolve_storage_backend(_optional_str("STORAGE_BACKEND", "auto"), s3_bucket)
    dart_api_keys = parse_dart_api_keys(_optional_str("DART_API_KEYS"))
    # mount된 .env가 있으면 런타임에 거기서 키를 읽으므로 환경변수 키는 선택사항이다.
    # 환경변수·.env 파일 둘 다 없을 때만 설정 오류로 막는다.
    dart_env_file = _optional_str("DART_ENV_FILE", "/opt/airflow/.env")
    if not dart_api_keys and not (dart_env_file and os.path.exists(dart_env_file)):
        raise ConfigError("DART_API_KEYS or a mounted .env is required")
    dart_minute_limit = _int_env("DART_MINUTE_LIMIT", 1000, minimum=2)
    if dart_minute_limit > 1000:
        raise ConfigError("DART_MINUTE_LIMIT must be <= 1000")
    dart_minute_safe_limit = _int_env("DART_MINUTE_SAFE_LIMIT", 600, minimum=1)
    if dart_minute_safe_limit >= 1000:
        raise ConfigError("DART_MINUTE_SAFE_LIMIT must be < 1000")
    if dart_minute_safe_limit >= dart_minute_limit:
        raise ConfigError("DART_MINUTE_SAFE_LIMIT must be < DART_MINUTE_LIMIT")
    # IP(프로세스 전체) 분당 한도 — OpenDART는 분당 제한을 IP 기준으로 적용한다. 키를 늘려도 같은 IP면
    # 합산되므로, 모든 키 호출을 합쳐 이 한도를 넘지 않게 한다. safe_limit에서 선제 차단한다.
    dart_minute_global_limit = _int_env("DART_MINUTE_GLOBAL_LIMIT", 1000, minimum=2)
    if dart_minute_global_limit > 1000:
        raise ConfigError("DART_MINUTE_GLOBAL_LIMIT must be <= 1000")
    dart_minute_global_safe_limit = _int_env("DART_MINUTE_GLOBAL_SAFE_LIMIT", 900, minimum=1)
    if dart_minute_global_safe_limit >= dart_minute_global_limit:
        raise ConfigError("DART_MINUTE_GLOBAL_SAFE_LIMIT must be < DART_MINUTE_GLOBAL_LIMIT")

    # 공시 상세 수집 방식:
    #   structured = 구조화 API(DS002~DS006) 위주 수집, 원문 document.xml job은 만들지 않음(기본값)
    #   document   = 원문 document.xml ZIP만 수집(기존 방식)
    #   both       = 구조화 API + 원문 둘 다 수집
    dart_collect_mode = (_optional_str("DART_COLLECT_MODE", "structured") or "structured").lower()
    if dart_collect_mode not in {"structured", "document", "both"}:
        raise ConfigError("DART_COLLECT_MODE must be structured, document or both")

    return Settings(
        airflow_db_url=_optional_str(
            "AIRFLOW_DB_URL",
            "postgresql+psycopg2://airflow:airflow@airflow-db:5432/airflow",
        )
        or "",
        service_db_url=_required_str("SERVICE_DB_URL"),
        dart_api_key_identifier=dart_api_keys[0].identifier if dart_api_keys else "",
        dart_api_keys=dart_api_keys,
        # mount된 .env를 런타임에 읽어 무중단 키 추가/회전을 지원한다.
        # .env가 없거나 깨지면 위 환경변수(dart_api_keys)로 fallback.
        dart_env_file=dart_env_file,
        storage_backend=storage_backend,
        local_storage_base_path=_optional_str(
            "LOCAL_STORAGE_BASE_PATH",
            "/opt/airflow/data",
        )
        or "/opt/airflow/data",
        s3_bucket=s3_bucket,
        s3_base_prefix=s3_base_prefix,
        bronze_prefix=_optional_str("BRONZE_PREFIX", "bronze/dart") or "bronze/dart",
        silver_prefix=_optional_str("SILVER_PREFIX", "silver/dart") or "silver/dart",
        gold_prefix=_optional_str("GOLD_PREFIX", "gold/dart") or "gold/dart",
        aws_region=_optional_str("AWS_REGION") or "ap-northeast-2",
        aws_access_key_id=_optional_str("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_optional_str("AWS_SECRET_ACCESS_KEY"),
        dart_backfill_months=parse_backfill_months(_optional_str("DART_BACKFILL_MONTHS")),
        dart_backfill_start_date=backfill_start_date,
        dart_backfill_end_date=backfill_end_date,
        dart_incremental_days=parse_incremental_days(_optional_str("DART_INCREMENTAL_DAYS")),
        dart_daily_limit=_int_env("DART_DAILY_LIMIT", 20000, minimum=1),
        dart_daily_safe_limit=_int_env("DART_DAILY_SAFE_LIMIT", 18000, minimum=1),
        dart_daily_emergency_limit=_int_env("DART_DAILY_EMERGENCY_LIMIT", 19500, minimum=1),
        dart_minute_limit=dart_minute_limit,
        dart_minute_safe_limit=dart_minute_safe_limit,
        dart_minute_global_limit=dart_minute_global_limit,
        dart_minute_global_safe_limit=dart_minute_global_safe_limit,
        dart_quota_request_log_enabled=_bool_env("DART_QUOTA_REQUEST_LOG_ENABLED", True),
        dart_collect_mode=dart_collect_mode,
        # 임베딩은 provider만 바꿔 교체(e5|local|bedrock|noop|none). 운영 기본은 e5(.env/compose에서 주입).
        # provider 미지정 시 fallback은 local(해시) — CI/오프라인 안전용. dim은 모델 차원과 일치해야 한다.
        embedding_provider=(_optional_str("EMBEDDING_PROVIDER", "local") or "local").lower(),
        embedding_model=_optional_str("EMBEDDING_MODEL", "amazon.titan-embed-text-v2") or "amazon.titan-embed-text-v2",
        embedding_version=_optional_str("EMBEDDING_VERSION", "v1") or "v1",
        embedding_dimension=_int_env("EMBEDDING_DIMENSION", 512, minimum=1),
    )
