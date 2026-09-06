from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from data_collection_pipeline.crawling import create_batch_id, save_raw_html


def test_create_batch_id_uses_expected_format():
    now = datetime(2026, 9, 5, 15, 30, tzinfo=ZoneInfo('Asia/Seoul'))
    assert create_batch_id(now) == '20260905_153000'


def test_save_raw_html(tmp_path: Path):
    result = save_raw_html(b'<html>book</html>', tmp_path / 'batch', 1)
    assert result.name == 'books_page_001.html'
    assert result.read_bytes() == b'<html>book</html>'
