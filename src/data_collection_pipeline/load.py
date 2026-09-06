"""Silver Parquet 데이터를 MySQL books 테이블에 Upsert."""

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import SILVER_DIR
from .database import create_mysql_engine

CREATE_TABLE_SQL = text('''
CREATE TABLE IF NOT EXISTS books (
    book_id VARCHAR(64) PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    rating TINYINT UNSIGNED NOT NULL,
    in_stock BOOLEAN NOT NULL,
    page INT UNSIGNED NOT NULL,
    batch_id VARCHAR(32) NOT NULL,
    source_site VARCHAR(100) NOT NULL,
    detail_url VARCHAR(500) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_books_batch_id (batch_id),
    INDEX idx_books_rating (rating)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
''')

UPSERT_SQL = text('''
INSERT INTO books (
    book_id, title, price, rating, in_stock, page,
    batch_id, source_site, detail_url, updated_at
) VALUES (
    :book_id, :title, :price, :rating, :in_stock, :page,
    :batch_id, :source_site, :detail_url, :updated_at
)
ON DUPLICATE KEY UPDATE
    title = VALUES(title),
    price = VALUES(price),
    rating = VALUES(rating),
    in_stock = VALUES(in_stock),
    page = VALUES(page),
    batch_id = VALUES(batch_id),
    source_site = VALUES(source_site),
    detail_url = VALUES(detail_url),
    updated_at = VALUES(updated_at)
''')


def dataframe_to_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """DataFrame을 MySQL 바인딩 레코드로 변환한다."""
    required = {
        'book_id', 'title', 'price', 'rating', 'in_stock',
        'page', 'batch_id', 'source_site', 'detail_url',
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f'Silver 필수 컬럼이 누락되었습니다: {sorted(missing)}')
    now = datetime.now().replace(microsecond=0)
    return [
        {
            'book_id': str(row.book_id), 'title': str(row.title),
            'price': float(row.price), 'rating': int(row.rating),
            'in_stock': bool(row.in_stock), 'page': int(row.page),
            'batch_id': str(row.batch_id), 'source_site': str(row.source_site),
            'detail_url': str(row.detail_url), 'updated_at': now,
        }
        for row in frame.itertuples(index=False)
    ]


def upsert_books(engine: Engine, records: list[dict[str, Any]]) -> int:
    """books 테이블을 만들고 도서 레코드를 Upsert한다."""
    with engine.begin() as connection:
        connection.execute(CREATE_TABLE_SQL)
        result = connection.execute(UPSERT_SQL, records) if records else None
        return int(result.rowcount) if result is not None else 0


def count_books(engine: Engine) -> int:
    """books 테이블 전체 행 수를 반환한다."""
    with engine.connect() as connection:
        return int(connection.execute(text('SELECT COUNT(*) FROM books')).scalar_one())


def run_load(batch_id: str) -> dict[str, int | str]:
    """배치 Silver Parquet을 읽어 MySQL에 적재한다."""
    silver_path = Path(SILVER_DIR) / batch_id
    if not silver_path.exists():
        raise FileNotFoundError(f'Silver Parquet 경로가 없습니다: {silver_path}')
    frame = pd.read_parquet(silver_path)
    records = dataframe_to_records(frame)
    engine = create_mysql_engine()
    try:
        affected = upsert_books(engine, records)
        stored = count_books(engine)
    finally:
        engine.dispose()
    return {
        'batch_id': batch_id,
        'input_count': len(records),
        'affected_row_count': affected,
        'stored_book_count': stored,
    }
