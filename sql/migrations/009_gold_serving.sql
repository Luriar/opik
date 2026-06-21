-- Gold serving 인덱스(Service DB). S3 Parquet은 정본·분석용이고, 서비스 질의(최근 공시 조회,
-- corp/stock/report_type 필터)는 RDB 인덱스로 받는다. dag_dart_gold_incremental/builder가 upsert한다.
-- VectorDB(vector_chunk)는 sql/init_vector_db.sql에 별도 존재(이 마이그레이션 대상 아님).

CREATE TABLE IF NOT EXISTS dart_report_index (
    rcept_no           VARCHAR(14) PRIMARY KEY,
    corp_code          VARCHAR(8) NOT NULL,
    stock_code         VARCHAR(6),
    corp_name          TEXT NOT NULL,
    report_type        VARCHAR(40),
    report_nm          TEXT NOT NULL DEFAULT '',
    rcept_dt           DATE NOT NULL,
    doc_id             VARCHAR(80) NOT NULL,
    source_silver_uri  TEXT,
    is_latest          BOOLEAN NOT NULL DEFAULT TRUE,
    document_available BOOLEAN NOT NULL DEFAULT FALSE,
    gold_version       VARCHAR(10) NOT NULL DEFAULT '',
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dart_report_index_corp_date
    ON dart_report_index (corp_code, rcept_dt DESC);
CREATE INDEX IF NOT EXISTS idx_dart_report_index_stock_date
    ON dart_report_index (stock_code, rcept_dt DESC);
CREATE INDEX IF NOT EXISTS idx_dart_report_index_type_date
    ON dart_report_index (report_type, rcept_dt DESC);
CREATE INDEX IF NOT EXISTS idx_dart_report_index_corp_type_date
    ON dart_report_index (corp_code, report_type, rcept_dt DESC);
