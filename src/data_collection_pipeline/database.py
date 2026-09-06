"""MySQL 연결과 설정."""

import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine


def database_url() -> URL:
    """환경변수에서 SQLAlchemy MySQL URL을 생성한다."""
    return URL.create(
        'mysql+pymysql',
        username=os.getenv('MYSQL_USER', 'books'),
        password=os.getenv('MYSQL_PASSWORD', 'books'),
        host=os.getenv('MYSQL_HOST', 'mysql'),
        port=int(os.getenv('MYSQL_PORT', '3306')),
        database=os.getenv('MYSQL_DATABASE', 'booksdb'),
    )


def create_mysql_engine() -> Engine:
    """연결 유효성 검사 기능을 포함한 MySQL Engine을 생성한다."""
    return create_engine(database_url(), pool_pre_ping=True, future=True)


def test_connection(engine: Engine) -> str:
    """현재 연결된 데이터베이스 이름을 반환한다."""
    with engine.connect() as connection:
        return str(connection.execute(text('SELECT DATABASE()')).scalar_one())
