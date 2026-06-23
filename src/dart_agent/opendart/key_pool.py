from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sequence
from typing import TypeVar

from dart_agent.dart_keys import DartApiKey
from dart_agent.opendart.client import OpenDartClient, OpenDartError, OpenDartResponse
from dart_agent.rate_limiter import DbRateLimiter, RateLimitExceeded


T = TypeVar("T")


class OpenDartClientPool:
    def __init__(self, api_keys: Sequence[DartApiKey], rate_limiter: DbRateLimiter) -> None:
        if not api_keys:
            raise ValueError("api_keys is required")
        self.api_keys = tuple(api_keys)
        self.rate_limiter = rate_limiter
        self.clients = {
            api_key.identifier: OpenDartClient(api_key.value)
            for api_key in self.api_keys
        }
        self._next_index = 0
        # detail_collector가 batch를 ThreadPoolExecutor로 동시 처리하므로 여러 worker가
        # 동시에 _reserve_key를 호출한다. 시작 인덱스를 lock으로 원자적으로 선점해야
        # 여러 worker가 같은 키부터 시도해 한 키로 쏠리는 것을 막고 키를 고르게 분산한다.
        self._index_lock = threading.Lock()

    def _claim_start_index(self) -> int:
        """다음 라운드로빈 시작 인덱스를 원자적으로 선점하고 한 칸 전진시킨다."""
        with self._index_lock:
            index = self._next_index
            self._next_index = (self._next_index + 1) % len(self.api_keys)
            return index

    def _reserve_key(
        self,
        api_name: str,
        excluded_identifiers: set[str] | None = None,
    ) -> DartApiKey:
        last_error: RateLimitExceeded | None = None
        excluded_identifiers = excluded_identifiers or set()
        # 시작 인덱스만 원자적으로 받고, 느린 reserve(DB IO)는 lock 밖에서 수행해
        # worker 간 직렬화를 피한다. 시작점 자체가 라운드로빈으로 분산되므로
        # 한 키가 막히면 offset 루프가 다음 키로 자연스럽게 넘어간다.
        start_index = self._claim_start_index()
        for offset in range(len(self.api_keys)):
            api_key = self.api_keys[(start_index + offset) % len(self.api_keys)]
            if api_key.identifier in excluded_identifiers:
                continue
            try:
                self.rate_limiter.reserve(api_name, api_key_identifier=api_key.identifier)
            except RateLimitExceeded as exc:
                last_error = exc
                continue
            return api_key
        if last_error is not None:
            raise RateLimitExceeded(
                f"all DART API keys are rate limited for {api_name}",
                quota_type=last_error.quota_type,
                quota_key=last_error.quota_key,
            ) from None
        raise RuntimeError("DART API key selection failed")

    def _reserve_specific_key(self, api_name: str, api_key_identifier: str) -> None:
        while True:
            try:
                self.rate_limiter.reserve(api_name, api_key_identifier=api_key_identifier)
                return
            except RateLimitExceeded as exc:
                if exc.quota_type.startswith("minute"):
                    self.rate_limiter.wait_for_capacity(exc)
                    continue
                raise

    # in-thread 재시도 대기(초). 짧은 blip만 즉시 흡수한다. 지속 실패는 raise되어 collector가
    # job-level로 reschedule(지연 후 PENDING)하므로, 여기서 길게 sleep해 worker thread를
    # 점유하지 않는다. (과거 (2,5,10)=17초는 장애 시 batch 전체를 마비시켰다.)
    _TRANSPORT_BACKOFF = (1, 2)  # 최대 2회, 합 3초

    def _call(self, api_name: str, invoke: Callable[[OpenDartClient], T]) -> T:
        last_limit_error: OpenDartError | None = None
        tried_identifiers: set[str] = set()
        while len(tried_identifiers) < len(self.api_keys):
            try:
                api_key = self._reserve_key(api_name, excluded_identifiers=tried_identifiers)
            except RateLimitExceeded as exc:
                if exc.quota_type.startswith("minute"):
                    self.rate_limiter.wait_for_capacity(exc)
                    continue
                raise
            tried_identifiers.add(api_key.identifier)
            for backoff in (*self._TRANSPORT_BACKOFF, None):
                try:
                    return invoke(self.clients[api_key.identifier])
                except OpenDartError as exc:
                    if exc.status == "TRANSPORT":
                        # 응답을 못 받은 전송 실패 — 과금 quota counter만 원복한다.
                        # IP 시도 counter는 rate_limiter.release가 유지한다.
                        self.rate_limiter.release(api_name, api_key_identifier=api_key.identifier)
                        if backoff is None:
                            raise  # 3회 재시도 모두 소진
                        time.sleep(backoff)
                        # 다음 시도 전 quota 재확인 및 차감
                        self._reserve_specific_key(api_name, api_key.identifier)
                        continue
                    if exc.status == "020":
                        last_limit_error = exc
                        self.rate_limiter.mark_external_daily_limit(
                            api_name,
                            api_key_identifier=api_key.identifier,
                        )
                        break
                    raise
        if last_limit_error is not None:
            raise last_limit_error
        raise RuntimeError("DART API call failed before execution")

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
        return self._call(
            "list",
            lambda client: client.list_disclosures(
                bgn_de=bgn_de,
                end_de=end_de,
                corp_cls=corp_cls,
                page_no=page_no,
                page_count=page_count,
                sort=sort,
                sort_mth=sort_mth,
                last_reprt_at=last_reprt_at,
            ),
        )

    def financial_statement_all(
        self,
        *,
        corp_code: str,
        bsns_year: str,
        reprt_code: str,
        fs_div: str,
    ) -> OpenDartResponse:
        return self._call(
            "fnlttSinglAcntAll",
            lambda client: client.financial_statement_all(
                corp_code=corp_code,
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                fs_div=fs_div,
            ),
        )

    def report_json(self, *, api_name: str, endpoint: str, params: dict[str, str]) -> OpenDartResponse:
        return self._call(
            api_name,
            lambda client: client.report_json(api_name, endpoint, params),
        )

    def company_overview(self, corp_code: str) -> OpenDartResponse:
        return self._call(
            "company",
            lambda client: client.company_overview(corp_code),
        )

    def corp_code_zip(self) -> tuple[bytes, int, int]:
        return self._call("corpCode", lambda client: client.corp_code_zip())

    def document_zip(self, rcept_no: str) -> tuple[bytes, int, int]:
        return self._call("document", lambda client: client.document_zip(rcept_no))
