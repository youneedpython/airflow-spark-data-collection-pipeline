from pathlib import Path

import pytest

from data_collection_pipeline.extract import parse_books, parse_page_number

HTML = '''
<article class="product_pod">
  <p class="star-rating Three"></p>
  <h3><a href="a-book_123/index.html" title="  A Book  ">A Book</a></h3>
  <p class="price_color">£12.34</p>
  <p class="availability"> In stock </p>
</article>
'''


def test_parse_page_number():
    assert parse_page_number(Path('books_page_007.html')) == 7


def test_parse_page_number_rejects_invalid_name():
    with pytest.raises(ValueError):
        parse_page_number(Path('page7.html'))


def test_parse_books_extracts_required_columns():
    rows = parse_books(HTML, 7, '20260905_153000')
    assert rows[0]['title'] == 'A Book'
    assert rows[0]['rating'] == 3
    assert rows[0]['page'] == 7
    assert rows[0]['batch_id'] == '20260905_153000'
