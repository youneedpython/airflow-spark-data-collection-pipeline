from unittest.mock import MagicMock

import pandas as pd

from data_collection_pipeline.load import dataframe_to_records, upsert_books


def sample_frame() -> pd.DataFrame:
    return pd.DataFrame([{
        'book_id': '1000', 'title': 'A Book', 'price': 12.34,
        'rating': 3, 'in_stock': True, 'page': 1,
        'batch_id': '20260905_153000', 'source_site': 'Books to Scrape',
        'detail_url': 'https://books.toscrape.com/catalogue/a_1000/index.html',
    }])


def test_dataframe_to_records():
    records = dataframe_to_records(sample_frame())
    assert records[0]['book_id'] == '1000'
    assert records[0]['rating'] == 3


def test_upsert_books_executes_create_and_upsert():
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.return_value.rowcount = 1
    assert upsert_books(engine, dataframe_to_records(sample_frame())) == 1
    assert connection.execute.call_count == 2
