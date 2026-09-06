import pytest

from data_collection_pipeline.helpers import (
    build_book_id,
    normalize_title,
    parse_availability,
    parse_price_text,
)


def test_normalize_title():
    assert normalize_title('  Data   Engineering ') == 'Data Engineering'


def test_parse_price_text():
    assert parse_price_text('£51.77') == 51.77


@pytest.mark.parametrize(
    ('value', 'expected'),
    [('In stock', True), ('Out of stock', False), ('unknown', None)],
)
def test_parse_availability(value, expected):
    assert parse_availability(value) is expected


def test_build_book_id_prefers_original_site_id():
    assert build_book_id('A', 'Books to Scrape', 'https://x/a_1000/index.html') == '1000'


def test_build_book_id_fallback_is_stable():
    first = build_book_id('A Book', 'Books to Scrape')
    assert first == build_book_id('A Book', 'Books to Scrape')
    assert len(first) == 64
