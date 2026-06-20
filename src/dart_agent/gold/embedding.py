"""임베딩 provider — provider만 바꿔 교체. 설정으로 model/dim/version 주입.

설정(예):
    embedding:
      provider: bedrock                  # bedrock | local | noop
      model: amazon.titan-embed-text-v2
      dim: 512
      version: v1

env 매핑: EMBEDDING_PROVIDER / EMBEDDING_MODEL / EMBEDDING_DIMENSION / EMBEDDING_VERSION.

provider 종류:
  - LocalEmbeddingProvider        : 결정적 해시 벡터(외부 의존 0). 로컬·CI 배선 검증용(의미 검색 품질 없음).
  - BedrockTitanEmbeddingProvider : AWS Bedrock Titan. 운영용(승인·자격증명 필요).
  - NoopEmbeddingProvider         : 0 벡터. 단위테스트용(임베딩 호출 자체를 무력화).

규약: provider.dim == 임베딩 Parquet의 embedding 차원(e5-small=384). 불일치 시 적재 실패.
"""
from __future__ import annotations

import hashlib
import math
from typing import Protocol


class EmbeddingProvider(Protocol):
    provider: str
    model: str
    version: str
    dim: int

    def embed(self, texts: list[str]) -> list[list[float]]: ...


class LocalEmbeddingProvider:
    """결정적 해시 임베딩(로컬 검증용). 같은 입력 → 같은 벡터, 외부 호출 없음."""

    provider = "local"

    def __init__(self, dim: int, model: str = "local-hash", version: str = "v1"):
        self.dim = dim
        self.model = model
        self.version = version

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._one(t) for t in texts]

    def _one(self, text: str) -> list[float]:
        vec = [0.0] * self.dim
        for tok in (text or "").split():
            h = int(hashlib.sha1(tok.encode("utf-8")).hexdigest(), 16)  # noqa: S324 - 식별자용
            vec[h % self.dim] += 1.0
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]


class NoopEmbeddingProvider:
    """0 벡터 반환(테스트용). 임베딩 호출을 무력화하되 적재 경로는 그대로 태운다."""

    provider = "noop"

    def __init__(self, dim: int, model: str = "noop", version: str = "v0"):
        self.dim = dim
        self.model = model
        self.version = version

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [[0.0] * self.dim for _ in texts]


class BedrockTitanEmbeddingProvider:
    """AWS Bedrock Titan Embeddings(amazon.titan-embed-text-v2). boto3 필요. 운영용."""

    provider = "bedrock"

    def __init__(self, dim: int, model: str = "amazon.titan-embed-text-v2", version: str = "v1",
                 region: str | None = None):
        self.dim = dim
        # Bedrock modelId는 버전 suffix(:0)를 요구한다. 설정값에 없으면 보정.
        self.model = model if ":" in model else f"{model}:0"
        self.version = version
        self._region = region

    def embed(self, texts: list[str]) -> list[list[float]]:
        import json
        import os

        import boto3  # 선택적 의존 — bedrock 선택 시에만 import.

        client = boto3.client("bedrock-runtime", region_name=self._region or os.getenv("AWS_REGION", "ap-northeast-2"))
        out: list[list[float]] = []
        for t in texts:
            resp = client.invoke_model(
                modelId=self.model,
                body=json.dumps({"inputText": t or " ", "dimensions": self.dim, "normalize": True}),
            )
            out.append(json.loads(resp["body"].read())["embedding"])
        return out


class E5EmbeddingProvider:
    """HuggingFace e5 계열(intfloat/multilingual-e5-small, 384d) — 로컬 CPU.

    e5 규약 필수:
      - 문서(passage)는 "passage: " , 질의는 "query: " 프리픽스를 붙인다(없으면 품질 급락).
      - 코사인 검색 → 임베딩을 L2 정규화한다(normalize_embeddings=True).
    sentence-transformers 의존(requirements.txt). 모델은 최초 1회 로컬 캐시.
    """

    provider = "e5"

    def __init__(self, dim: int = 384, model: str = "intfloat/multilingual-e5-small",
                 version: str = "v1"):
        self.dim = dim
        self.model = model
        self.version = version
        self._st = None

    def _encoder(self):
        if self._st is None:
            from sentence_transformers import SentenceTransformer  # 무거운 의존 → lazy import

            self._st = SentenceTransformer(self.model)
            got = self._st.get_sentence_embedding_dimension()
            if got != self.dim:
                raise ValueError(f"e5 모델 차원({got}) != 설정 dim({self.dim}). EMBEDDING_DIMENSION 확인")
            # 방어선: 청킹이 토큰 예산을 지키지만(chunking.py), 엣지 케이스가 512를 넘어도
            # 예측 가능하게 truncation되도록 모델 최대 시퀀스를 512로 고정한다.
            self._st.max_seq_length = min(self._st.max_seq_length or 512, 512)
        return self._st

    def _encode(self, texts: list[str]) -> list[list[float]]:
        enc = self._encoder()
        vecs = enc.encode(texts, normalize_embeddings=True, convert_to_numpy=True,
                          batch_size=64, show_progress_bar=False)
        return [v.tolist() for v in vecs]

    def embed(self, texts: list[str]) -> list[list[float]]:
        """문서 임베딩 — passage: 프리픽스."""
        return self._encode([f"passage: {t or ' '}" for t in texts])

    def embed_query(self, text: str) -> list[float]:
        """질의 임베딩 — query: 프리픽스."""
        return self._encode([f"query: {text or ' '}"])[0]


def build_embedding_provider(settings) -> EmbeddingProvider | None:
    """settings.embedding_provider에 맞는 provider를 만든다. 'none'이면 None(임베딩 단계 skip)."""
    provider = (settings.embedding_provider or "local").strip().lower()
    dim = settings.embedding_dimension
    model = settings.embedding_model
    version = settings.embedding_version
    if provider in ("none", "off", ""):
        return None
    if provider == "local":
        return LocalEmbeddingProvider(dim, model=model or "local-hash", version=version or "v1")
    if provider == "noop":
        return NoopEmbeddingProvider(dim, model=model or "noop", version=version or "v0")
    if provider in ("e5", "hf", "huggingface", "sentence-transformers"):
        return E5EmbeddingProvider(dim, model=model or "intfloat/multilingual-e5-small",
                                   version=version or "v1")
    if provider in ("bedrock", "titan"):
        return BedrockTitanEmbeddingProvider(dim, model=model or "amazon.titan-embed-text-v2",
                                             version=version or "v1", region=settings.aws_region)
    raise ValueError(f"unknown embedding provider: {provider}")
