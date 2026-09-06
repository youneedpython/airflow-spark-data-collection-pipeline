"""전처리와 테스트에서 함께 사용하는 작은 변환 함수."""

import hashlib
import re


def normalize_title(value: object) -> str:
    """제목 양끝과 연속 공백을 정리한다."""
    return re.sub(r'\s+', ' ', str(value or '')).strip()


def parse_price_text(value: object) -> float | None:
    """가격 문자열에서 숫자 값을 추출한다."""
    match = re.search(r'(\d+(?:\.\d+)?)', str(value or ''))
    return float(match.group(1)) if match else None


def parse_availability(value: object) -> bool | None:
    """재고 문구를 논리값으로 변환한다."""
    normalized = str(value or '').strip().casefold()
    if 'out of stock' in normalized:
        return False
    if 'in stock' in normalized:
        return True
    return None


def build_book_id(title: str, source_site: str, detail_url: str = '') -> str:
    """기존 URL 상품 ID를 우선 사용하고 불가능하면 SHA-256을 사용한다."""
    match = re.search(r'_(\d+)/index\.html$', detail_url)
    if match:
        return match.group(1)
    key = f'{normalize_title(title)}|{source_site}'.encode()
    return hashlib.sha256(key).hexdigest()
