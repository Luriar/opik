from __future__ import annotations

from io import BytesIO
from dataclasses import dataclass
from enum import Enum
from time import monotonic
from typing import Any
from xml.etree import ElementTree
from zipfile import is_zipfile

import requests
from requests.adapters import HTTPAdapter


class QuotaPolicy(str, Enum):
    DAILY_AND_MINUTE = "DAILY_AND_MINUTE"
    MINUTE_ONLY = "MINUTE_ONLY"


class OpenDartError(RuntimeError):
    def __init__(self, status: str, message: str, api_name: str) -> None:
        super().__init__(f"OpenDART {api_name} failed: {status} {message}")
        self.status = status
        self.message = message
        self.api_name = api_name


class OpenDartNoData(OpenDartError):
    pass


@dataclass(frozen=True)
class OpenDartResponse:
    api_name: str
    status: str
    message: str
    payload: dict[str, Any]
    http_status: int
    elapsed_ms: int


class OpenDartClient:
    base_url = "https://opendart.fss.or.kr/api"

    def __init__(self, api_key: str, session: requests.Session | None = None) -> None:
        self.api_key = api_key
        self.session = session or requests.Session()
        # detail_collector 동시 처리 시 같은 호스트(opendart.fss.or.kr)로 다수 동시 요청이
        # 발생한다. 기본 pool_maxsize(10)면 "Connection pool is full" 경고 후 대기가 생기므로
        # 동시성 상한 이상으로 키운다.
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def list_disclosures(
        self,
        *,
        bgn_de: str,
        end_de: str,
        corp_cls: str,
        page_no: int = 1,
        page_count: int = 100,
        sort: str = "date",
        sort_mth: str = "desc",
        last_reprt_at: str = "N",
    ) -> OpenDartResponse:
        return self._get_json(
            "list",
            "list.json",
            {
                "bgn_de": bgn_de,
                "end_de": end_de,
                "corp_cls": corp_cls,
                "page_no": str(page_no),
                "page_count": str(page_count),
                "sort": sort,
                "sort_mth": sort_mth,
                "last_reprt_at": last_reprt_at,
            },
        )

    def financial_statement_all(
        self,
        *,
        corp_code: str,
        bsns_year: str,
        reprt_code: str,
        fs_div: str,
    ) -> OpenDartResponse:
        return self._get_json(
            "fnlttSinglAcntAll",
            "fnlttSinglAcntAll.json",
            {
                "corp_code": corp_code,
                "bsns_year": bsns_year,
                "reprt_code": reprt_code,
                "fs_div": fs_div,
            },
        )

    def company_overview(self, corp_code: str) -> OpenDartResponse:
        """DS001 /api/company.json 기업개황. COMPANY_OVERVIEW job."""
        return self._get_json(
            "company",
            "company.json",
            {"corp_code": corp_code},
        )

    def report_json(self, api_name: str, endpoint: str, params: dict[str, str]) -> OpenDartResponse:
        """구조화 상세 API(DS002~DS006)를 endpoint/params로 호출하는 범용 메서드."""
        return self._get_json(api_name, endpoint, params)

    def corp_code_zip(self) -> tuple[bytes, int, int]:
        return self._get_bytes("corpCode", "corpCode.xml", {})

    def document_zip(self, rcept_no: str) -> tuple[bytes, int, int]:
        return self._get_bytes("document", "document.xml", {"rcept_no": rcept_no})

    def _get_json(
        self,
        api_name: str,
        path: str,
        params: dict[str, str],
    ) -> OpenDartResponse:
        params = {"crtfc_key": self.api_key, **params}
        started = monotonic()
        try:
            response = self.session.get(f"{self.base_url}/{path}", params=params, timeout=30)
        except requests.RequestException as exc:
            raise OpenDartError("TRANSPORT", exc.__class__.__name__, api_name) from None
        elapsed_ms = int((monotonic() - started) * 1000)
        if response.status_code >= 400:
            raise OpenDartError(f"HTTP_{response.status_code}", "HTTP request failed", api_name)
        try:
            payload = response.json()
        except ValueError as exc:
            raise OpenDartError(
                "INVALID_JSON",
                "OpenDART JSON response could not be parsed",
                api_name,
            ) from None
        status = str(payload.get("status", ""))
        message = str(payload.get("message", ""))
        if status == "013":
            raise OpenDartNoData(status, message, api_name)
        if status and status != "000":
            raise OpenDartError(status, message, api_name)
        return OpenDartResponse(
            api_name=api_name,
            status=status,
            message=message,
            payload=payload,
            http_status=response.status_code,
            elapsed_ms=elapsed_ms,
        )

    def _get_bytes(
        self,
        api_name: str,
        path: str,
        params: dict[str, str],
    ) -> tuple[bytes, int, int]:
        params = {"crtfc_key": self.api_key, **params}
        started = monotonic()
        try:
            response = self.session.get(f"{self.base_url}/{path}", params=params, timeout=60)
        except requests.RequestException as exc:
            raise OpenDartError("TRANSPORT", exc.__class__.__name__, api_name) from None
        elapsed_ms = int((monotonic() - started) * 1000)
        if response.status_code >= 400:
            raise OpenDartError(f"HTTP_{response.status_code}", "HTTP request failed", api_name)
        content = response.content
        if not is_zipfile(BytesIO(content)):
            _raise_binary_api_error(api_name, content)
        return content, response.status_code, elapsed_ms


def _raise_binary_api_error(api_name: str, content: bytes) -> None:
    try:
        root = ElementTree.fromstring(content)
    except ElementTree.ParseError as exc:
        raise OpenDartError("INVALID_BINARY", "OpenDART binary response was not a ZIP file", api_name) from None

    status = root.findtext("status") or "INVALID_BINARY"
    message = root.findtext("message") or "OpenDART binary response was not a ZIP file"
    if status == "013":
        raise OpenDartNoData(status, message, api_name)
    raise OpenDartError(status, message, api_name)


def quota_policy_for(api_name: str) -> QuotaPolicy:
    if api_name == "list":
        return QuotaPolicy.MINUTE_ONLY
    return QuotaPolicy.DAILY_AND_MINUTE
