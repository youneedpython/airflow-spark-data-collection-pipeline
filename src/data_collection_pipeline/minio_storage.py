"""로컬 데이터 계층을 MinIO Data Lake에 동기화."""

import os
from pathlib import Path

from minio import Minio

from .config import DATA_DIR


def create_minio_client() -> Minio:
    """환경변수 기반 MinIO Client를 생성한다."""
    return Minio(
        os.getenv('MINIO_ENDPOINT', 'minio:9000'),
        access_key=os.getenv('MINIO_ROOT_USER', 'minioadmin'),
        secret_key=os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin'),
        secure=False,
    )


def layer_directory(layer: str, batch_id: str) -> Path:
    """계층별 실제 로컬 배치 경로를 반환한다."""
    if layer == 'raw':
        return DATA_DIR / 'raw' / 'html' / batch_id
    return DATA_DIR / layer / batch_id


def sync_batch_to_minio(batch_id: str) -> dict[str, int | str]:
    """Raw·Interim·Silver·Gold 배치 파일을 MinIO에 업로드한다."""
    client = create_minio_client()
    bucket = os.getenv('MINIO_BUCKET', 'data-lake')
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    upload_count = 0
    for layer in ('raw', 'interim', 'silver', 'gold'):
        source_dir = layer_directory(layer, batch_id)
        if not source_dir.exists():
            raise FileNotFoundError(f'동기화할 계층이 없습니다: {source_dir}')
        for path in source_dir.rglob('*'):
            if path.is_file():
                object_name = f'{layer}/{batch_id}/{path.relative_to(source_dir).as_posix()}'
                client.fput_object(bucket, object_name, str(path))
                upload_count += 1
    return {'batch_id': batch_id, 'bucket': bucket, 'uploaded_file_count': upload_count}
