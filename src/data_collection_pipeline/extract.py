"""Raw HTML에서 도서 정보를 추출하여 Interim CSV로 저장."""

import csv
import re
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .config import BASE_URL, INTERIM_DIR, RAW_HTML_DIR

RATING_MAP = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
RAW_FILE_RE = re.compile(r'^books_page_(\d{3})\.html$')
FIELDNAMES = [
    'title', 'price_text', 'availability_text', 'rating',
    'page', 'batch_id', 'detail_url',
]


def parse_page_number(file_path: Path) -> int:
    """Raw HTML 파일명에서 페이지 번호를 추출한다."""
    match = RAW_FILE_RE.fullmatch(file_path.name)
    if not match:
        raise ValueError(f'올바르지 않은 Raw 파일명입니다: {file_path.name}')
    return int(match.group(1))


def parse_books(html: bytes | str, page: int, batch_id: str) -> list[dict[str, object]]:
    """한 페이지의 HTML에서 도서 행을 추출한다."""
    soup = BeautifulSoup(html, 'html.parser')
    rows: list[dict[str, object]] = []
    for product in soup.select('article.product_pod'):
        link = product.select_one('h3 a')
        price = product.select_one('.price_color')
        availability = product.select_one('.availability')
        rating_tag = product.select_one('p.star-rating')
        if not all((link, price, availability, rating_tag)):
            continue
        rating_word = next(
            (value for value in rating_tag.get('class', []) if value in RATING_MAP),
            None,
        )
        if rating_word is None:
            raise ValueError('알 수 없는 평점 클래스입니다.')
        rows.append({
            'title': link.get('title', '').strip(),
            'price_text': price.get_text(strip=True),
            'availability_text': ' '.join(availability.stripped_strings),
            'rating': RATING_MAP[rating_word],
            'page': page,
            'batch_id': batch_id,
            'detail_url': urljoin(BASE_URL, link.get('href', '')),
        })
    return rows


def run_extract(batch_id: str) -> dict[str, object]:
    """배치의 모든 Raw HTML을 파싱해 페이지별 CSV로 저장한다."""
    raw_dir = RAW_HTML_DIR / batch_id
    output_dir = INTERIM_DIR / batch_id
    output_dir.mkdir(parents=True, exist_ok=True)
    output_files: list[str] = []
    total_rows = 0

    raw_files = sorted(raw_dir.glob('books_page_*.html'))
    if not raw_files:
        raise FileNotFoundError(f'Raw HTML 파일이 없습니다: {raw_dir}')

    for raw_file in raw_files:
        page = parse_page_number(raw_file)
        rows = parse_books(raw_file.read_bytes(), page, batch_id)
        output = output_dir / f'books_page_{page:03d}_parsed.csv'
        with output.open('w', encoding='utf-8-sig', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)
        output_files.append(str(output))
        total_rows += len(rows)

    return {
        'batch_id': batch_id,
        'file_count': len(output_files),
        'row_count': total_rows,
        'files': output_files,
    }
