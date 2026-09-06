"""프로젝트 공통 환경설정."""

import os
from pathlib import Path

PROJECT_DIR = Path(os.getenv('PROJECT_DIR', '/opt/project'))
DATA_DIR = Path(os.getenv('DATA_DIR', str(PROJECT_DIR / 'data')))
RAW_HTML_DIR = DATA_DIR / 'raw' / 'html'
INTERIM_DIR = DATA_DIR / 'interim'
SILVER_DIR = DATA_DIR / 'silver'
GOLD_DIR = DATA_DIR / 'gold'

BASE_URL = 'https://books.toscrape.com/catalogue/'
SOURCE_SITE = 'Books to Scrape'
START_PAGE = int(os.getenv('START_PAGE', '1'))
END_PAGE = int(os.getenv('END_PAGE', '3'))
REQUEST_INTERVAL = float(os.getenv('REQUEST_INTERVAL', '0.5'))
REQUEST_TIMEOUT = (5, 30)
HEADERS = {'User-Agent': 'AirflowSparkEducationPipeline/1.0'}


def ensure_page_range(start_page: int, end_page: int) -> None:
    """페이지 범위가 올바른지 검증한다."""
    if start_page < 1 or end_page < start_page:
        raise ValueError('페이지 범위는 1 이상이며 END_PAGE >= START_PAGE여야 합니다.')
