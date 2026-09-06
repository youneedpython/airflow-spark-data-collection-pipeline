"""Books to Scrape 원본 HTML 수집."""

import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from .config import (
    BASE_URL,
    END_PAGE,
    HEADERS,
    RAW_HTML_DIR,
    REQUEST_INTERVAL,
    REQUEST_TIMEOUT,
    START_PAGE,
    ensure_page_range,
)

SEOUL = ZoneInfo('Asia/Seoul')


def create_batch_id(now: datetime | None = None) -> str:
    """한 번의 파이프라인 실행을 식별하는 배치 ID를 생성한다."""
    current = now or datetime.now(SEOUL)
    return current.strftime('%Y%m%d_%H%M%S')


def page_url(page: int) -> str:
    """수집 페이지 URL을 반환한다."""
    return f'{BASE_URL}page-{page}.html'


def save_raw_html(content: bytes, batch_dir: Path, page: int) -> Path:
    """원본 HTML을 페이지별 파일로 저장한다."""
    batch_dir.mkdir(parents=True, exist_ok=True)
    path = batch_dir / f'books_page_{page:03d}.html'
    path.write_bytes(content)
    return path


def run_crawling(
    batch_id: str | None = None,
    start_page: int = START_PAGE,
    end_page: int = END_PAGE,
) -> dict[str, object]:
    """지정 범위의 HTML을 수집하고 실행 메타데이터를 반환한다."""
    ensure_page_range(start_page, end_page)
    current_batch_id = batch_id or create_batch_id()
    batch_dir = RAW_HTML_DIR / current_batch_id
    saved_files: list[str] = []

    with requests.Session() as session:
        session.headers.update(HEADERS)
        for page in range(start_page, end_page + 1):
            response = session.get(page_url(page), timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            saved_files.append(str(save_raw_html(response.content, batch_dir, page)))
            if page < end_page:
                time.sleep(REQUEST_INTERVAL)

    return {
        'batch_id': current_batch_id,
        'batch_dir': str(batch_dir),
        'file_count': len(saved_files),
        'files': saved_files,
    }
