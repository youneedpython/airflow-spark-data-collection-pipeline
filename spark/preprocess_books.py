"""
Interim CSV를 정제하여 Silver Parquet으로 저장하는 Spark Job

[입력]
data/interim/{batch_id}/*.csv

[처리]
1. 페이지별 Interim CSV 통합
2. 문자열 컬럼 정리
3. 가격 자료형 변환
4. 재고 여부 변환
5. 평점 및 페이지 번호 자료형 변환
6. 데이터 출처 추가
7. book_id 생성
8. 결측치 제거
9. 중복 데이터 제거
10. Silver Parquet 저장

[출력]
data/silver/{batch_id}/

[book_id 생성 규칙]
1. detail_url에서 기존 상품 번호를 추출
2. 상품 번호가 없으면 title과 source_site를 이용해 SHA-256 생성

Python UDF를 사용하지 않고 Spark 기본 함수만 사용하므로
Spark Driver와 Worker의 Python 마이너 버전이 달라도
이 전처리 작업에서는 Python Worker 버전 불일치가 발생하지 않는다.
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


## ===========================================================
## 1. 상수 설정
## ===========================================================

# 데이터 계보(Data Lineage) 추적을 위한 데이터 원천 출처 명시
SOURCE_SITE = 'Books to Scrape'

# Silver 계층 데이터 표준 스키마: 분석 및 모델링 최적화 순서로 정렬된 프로젝션 컬럼 정의
SILVER_COLUMNS = [
    'book_id',
    'title',
    'price',
    'rating',
    'in_stock',
    'page',
    'batch_id',
    'source_site',
    'detail_url',
]


## ===========================================================
## 2. 실행 인자 처리
## ===========================================================

def parse_arguments() -> argparse.Namespace:
    """
    Spark Job 실행에 필요한 명령행 인자를 읽는다.

    Returns:
        batch_id와 data_dir이 저장된 Namespace
    """

    parser = argparse.ArgumentParser(
        description=(
            'Books to Scrape Interim CSV를 전처리하여 '
            'Silver Parquet으로 저장합니다.'
        ),
    )

    # 파이프라인의 멱등성(Idempotency) 및 배치 단위 격리를 위한 필수 배치 식별자
    parser.add_argument(
        '--batch-id',
        required=True,
        help='파이프라인 실행 배치 ID',
    )

    # 볼륨 마운트된 컨테이너 내부 데이터 루트 경로 기본값 주입
    parser.add_argument(
        '--data-dir',
        default='/opt/project/data',
        help='데이터 계층의 최상위 디렉터리',
    )

    return parser.parse_args()


## ===========================================================
## 3. Spark Session 생성
## ===========================================================

def create_spark_session(batch_id: str) -> SparkSession:
    """
    도서 전처리 작업에 사용할 Spark Session을 생성한다.

    Args:
        batch_id:
            현재 파이프라인 실행의 배치 ID

    Returns:
        생성된 SparkSession
    """

    # Spark UI 식별을 위한 배치별 애플리케이션 네이밍 및 타임존을 서울(KST)로 명시적 고정
    return (
        SparkSession.builder
        .appName(f'books-preprocess-{batch_id}')
        .config('spark.sql.session.timeZone', 'Asia/Seoul')
        .getOrCreate()
    )


## ===========================================================
## 4. Interim CSV 읽기
## ===========================================================

def load_interim_csv(
    spark: SparkSession,
    input_path: str,
) -> DataFrame:
    """
    한 배치의 페이지별 Interim CSV를 하나의 DataFrame으로 읽는다.

    Args:
        spark:
            현재 SparkSession

        input_path:
            Interim CSV 경로 패턴

    Returns:
        페이지별 CSV가 통합된 DataFrame
    """

    # mode=FAILFAST: 읽기 도중 손상된 행(Malformed Record) 발견 시 즉시 예외를 발생시켜 비정상 데이터 적재 차단
    return (
        spark.read
        .option('header', True)
        .option('encoding', 'UTF-8')
        .option('mode', 'FAILFAST')
        .csv(input_path)
    )


## ===========================================================
## 5. 필수 컬럼 검증
## ===========================================================

def validate_required_columns(frame: DataFrame) -> None:
    """
    전처리에 필요한 필수 입력 컬럼의 존재 여부를 확인한다.

    Args:
        frame:
            Interim CSV에서 읽은 DataFrame

    Raises:
        ValueError:
            필수 컬럼이 누락된 경우
    """

    # 원천 데이터 계약(Data Contract) 검증을 위한 최소 필수 필드 집합
    required_columns = {
        'title',
        'price_text',
        'availability_text',
        'rating',
        'page',
        'batch_id',
        'detail_url',
    }

    # 차집합 연산을 통해 누락된 스키마 컬럼 실시간 감지
    missing_columns = required_columns - set(frame.columns)

    if missing_columns:
        raise ValueError(
            'Interim CSV의 필수 컬럼이 누락되었습니다. '
            f'누락 컬럼: {sorted(missing_columns)}'
        )


## ===========================================================
## 6. title 정리
## ===========================================================

def build_clean_title() -> F.Column:
    """
    title 컬럼의 양끝 공백과 연속 공백을 정리하는 표현식을 반환한다.

    Returns:
        정리된 title Column 표현식
    """

    # 탭, 개행, 다중 띄어쓰기(\s+)를 단일 공백으로 치환 후 양 끝 Whitespace 제거 (Catalyst 표현식 사용)
    return F.trim(
        F.regexp_replace(
            F.col('title'),
            r'\s+',
            ' ',
        )
    )


## ===========================================================
## 7. price 변환
## ===========================================================

def build_price() -> F.Column:
    """
    price_text에서 숫자를 추출하고 DECIMAL 자료형으로 변환한다.

    예:
        £51.77 -> 51.77

    Returns:
        DECIMAL(10, 2) 자료형의 price Column 표현식
    """

    # 통화 기호(£, $ 등)를 배제하고 소수점을 포함한 숫자 패턴만 정규식 그룹 1번으로 캡처
    price_number = F.regexp_extract(
        F.col('price_text'),
        r'(\d+(?:\.\d+)?)',
        1,
    )

    # 부동 소수점(Float/Double) 오차 방지를 위해 고정 소수점(DECIMAL(10, 2))으로 정밀 캐스팅
    return price_number.cast('decimal(10,2)')


## ===========================================================
## 8. 재고 여부 변환
## ===========================================================

def build_in_stock() -> F.Column:
    """
    availability_text를 Boolean 자료형으로 변환한다.

    변환 규칙:
        In stock     -> True
        Out of stock -> False
        그 외         -> Null

    Returns:
        Boolean 자료형의 in_stock Column 표현식
    """

    # 대소문자 혼용 방지를 위한 소문자 정규화 및 공백 제거
    availability = F.lower(
        F.trim(F.col('availability_text'))
    )

    # 'out of stock'이 'in stock' 문자열을 부분 포함하므로 'out of stock' 검사를 먼저 평가
    return (
        F.when(
            availability.contains('out of stock'),
            F.lit(False),
        )
        .when(
            availability.contains('in stock'),
            F.lit(True),
        )
        .otherwise(
            F.lit(None).cast('boolean')
        )
    )


## ===========================================================
## 9. book_id 생성
## ===========================================================

def build_book_id() -> F.Column:
    """
    도서의 안정적인 식별자인 book_id를 생성한다.

    생성 규칙:
        1. detail_url 끝부분에서 기존 상품 번호를 추출한다.
        2. 상품 번호가 없으면 title과 source_site를 결합하여
           SHA-256 해시값을 생성한다.

    예:
        detail_url:
        https://books.toscrape.com/catalogue/a-light_1000/index.html

        book_id:
        1000

    Returns:
        String 자료형의 book_id Column 표현식
    """

    ## 기존 프로젝트와 호환되는 URL 상품 번호 추출 (URL 말단의 _{고유번호}/index.html 패턴)
    url_book_id = F.regexp_extract(
        F.col('detail_url'),
        r'_(\d+)/index\.html$',
        1,
    )

    ## URL 상품 번호가 없을 때 사용할 SHA-256 식별자 (내추럴 키 조합 기반 결정론적 해시)
    fallback_book_id = F.sha2(
        F.concat_ws(
            '|',
            F.col('title'),
            F.col('source_site'),
        ),
        256,
    )

    # 1차 식별자가 유효하면 그대로 채택하고, 누락 시 Fallback 해시값으로 대체
    return (
        F.when(
            F.length(url_book_id) > 0,
            url_book_id,
        )
        .otherwise(fallback_book_id)
    )


## ===========================================================
## 10. 도서 데이터 전처리
## ===========================================================

def preprocess_books(frame: DataFrame) -> DataFrame:
    """
    Interim 도서 데이터를 Silver 계층에 적합하게 전처리한다.

    Args:
        frame:
            페이지별 Interim CSV가 통합된 DataFrame

    Returns:
        정제, 자료형 변환, 결측치 제거 및 중복 제거가 완료된 DataFrame
    """

    # 입력 스키마 완결성 사전 검증
    validate_required_columns(frame)

    # 선언적 변환 체이닝: JVM 내부 Tungsten 코드로 컴파일되어 고속 실행
    processed_frame = (
        frame

        ## title 문자열 정리 (공백 정규화)
        .withColumn(
            'title',
            build_clean_title(),
        )

        ## 가격 문자열을 숫자 자료형으로 변환 (DECIMAL)
        .withColumn(
            'price',
            build_price(),
        )

        ## 재고 여부를 Boolean 자료형으로 변환 (True/False)
        .withColumn(
            'in_stock',
            build_in_stock(),
        )

        ## 평점을 정수 자료형으로 변환 (Int)
        .withColumn(
            'rating',
            F.col('rating').cast('int'),
        )

        ## 페이지 번호를 정수 자료형으로 변환 (Int)
        .withColumn(
            'page',
            F.col('page').cast('int'),
        )

        ## batch_id를 문자열 자료형으로 통일
        .withColumn(
            'batch_id',
            F.col('batch_id').cast('string'),
        )

        ## 데이터 출처 추가 (리터럴 상수 컬럼 주입)
        .withColumn(
            'source_site',
            F.lit(SOURCE_SITE),
        )

        ## 도서 식별자 생성 (URL 추출 또는 SHA-256 기반)
        .withColumn(
            'book_id',
            build_book_id(),
        )

        ## Silver 계층에 필요한 컬럼만 선택 (불필요 중간 파생 컬럼 제외)
        .select(*SILVER_COLUMNS)

        ## 필수 컬럼의 결측치 제거 (Bronze -> Silver 정제 단계의 Null 배제)
        .dropna(
            subset=[
                'book_id',
                'title',
                'price',
                'rating',
                'in_stock',
                'page',
                'batch_id',
                'source_site',
                'detail_url',
            ],
        )

        ## book_id 기준 중복 제거 (Shuffle 발생: 파티션 간 동일 키 1건만 유지)
        .dropDuplicates(['book_id'])
    )

    return processed_frame


## ===========================================================
## 11. 데이터 품질 검증
## ===========================================================

def validate_silver_data(frame: DataFrame) -> dict[str, int]:
    """
    Silver DataFrame의 기본 데이터 품질을 검증한다.

    Args:
        frame:
            전처리가 완료된 Silver DataFrame

    Returns:
        데이터 품질 검증 결과

    Raises:
        ValueError:
            데이터가 비어 있거나 유효하지 않은 값이 존재하는 경우
    """

    # 전체 레코드 수 측정 (Action 트리거)
    input_count = frame.count()

    # 제로 레코드 검증: 전처리 파이프라인의 전체 유실 방지
    if input_count == 0:
        raise ValueError(
            '전처리 결과가 비어 있습니다. '
            'Interim CSV의 데이터와 전처리 조건을 확인하세요.'
        )

    # 평점 도메인 규칙 검증: 1점 이상 5점 이하의 정수 범위만 허용
    invalid_rating_count = (
        frame
        .filter(
            F.col('rating').isNull()
            | (F.col('rating') < 1)
            | (F.col('rating') > 5)
        )
        .count()
    )

    # 가격 유효성 검증: 0 이하 또는 Null 값 허용 금지
    invalid_price_count = (
        frame
        .filter(
            F.col('price').isNull()
            | (F.col('price') <= 0)
        )
        .count()
    )

    # 식별자 유일성 검증: dropDuplicates 이후에도 키 무결성이 100% 지켜졌는지 확인
    duplicate_book_id_count = (
        frame
        .groupBy('book_id')
        .count()
        .filter(F.col('count') > 1)
        .count()
    )

    errors: list[str] = []

    # 결함 건수 수집 및 집계 에러 리포트 생성
    if invalid_rating_count:
        errors.append(
            f'유효하지 않은 평점: {invalid_rating_count}건'
        )

    if invalid_price_count:
        errors.append(
            f'유효하지 않은 가격: {invalid_price_count}건'
        )

    if duplicate_book_id_count:
        errors.append(
            f'중복 book_id: {duplicate_book_id_count}건'
        )

    # 데이터 정합성 실패 시 파이프라인 강제 중단 (Bad Data의 다운스트림 전파 방지)
    if errors:
        raise ValueError(
            'Silver 데이터 품질 검증에 실패했습니다. '
            + ', '.join(errors)
        )

    return {
        'row_count': input_count,
        'invalid_rating_count': invalid_rating_count,
        'invalid_price_count': invalid_price_count,
        'duplicate_book_id_count': duplicate_book_id_count,
    }


## ===========================================================
## 12. Silver Parquet 저장
## ===========================================================

def save_silver_parquet(
    frame: DataFrame,
    output_path: str,
) -> None:
    """
    전처리 결과를 Silver Parquet으로 저장한다.

    Args:
        frame:
            전처리가 완료된 DataFrame

        output_path:
            Silver Parquet 출력 경로
    """

    # overwrite 모드: 동일 배치 재실행 시 멱등성을 보장하며 열 지향(Columnar) 포맷으로 스토리지 압축 최적화
    (
        frame.write
        .mode('overwrite')
        .parquet(output_path)
    )


## ===========================================================
## 13. Spark 전처리 작업 실행
## ===========================================================

def run_preprocess(
    spark: SparkSession,
    batch_id: str,
    data_dir: str,
) -> dict[str, int | str]:
    """
    한 배치의 Spark 전처리 전체 과정을 실행한다.

    Args:
        spark:
            현재 SparkSession

        batch_id:
            현재 파이프라인 실행의 배치 ID

        data_dir:
            데이터 계층의 최상위 디렉터리

    Returns:
        전처리 결과 요약
    """

    # 배치 디렉터리 기반의 입력/출력 격리 경로 설정
    input_path = (
        f'{data_dir}/interim/'
        f'{batch_id}/*.csv'
    )

    output_path = (
        f'{data_dir}/silver/'
        f'{batch_id}'
    )

    print('=' * 60)
    print('Spark 도서 전처리 시작')
    print('=' * 60)
    print(f'batch_id   : {batch_id}')
    print(f'input_path : {input_path}')
    print(f'output_path: {output_path}')

    ## 1. Interim CSV 읽기 (지연 평가 - Lazy Evaluation)
    interim_frame = load_interim_csv(
        spark=spark,
        input_path=input_path,
    )

    ## 2. 전처리 (실행 계획 DAG 구성)
    silver_frame = preprocess_books(interim_frame)

    ## 여러 Action(품질 검증 count 및 Parquet 쓰기)에서 중복 연산(Lineage 재평가)을 방지하기 위해 캐싱
    silver_frame.cache()

    try:
        ## 3. 데이터 품질 검증 (Action 트리거 1: 캐시 메모리 로드 및 집계 수행)
        validation_result = validate_silver_data(
            silver_frame
        )

        ## 4. Silver Parquet 저장 (Action 트리거 2: 캐시된 메모리 데이터를 기반으로 고속 파일 쓰기)
        save_silver_parquet(
            frame=silver_frame,
            output_path=output_path,
        )

        result: dict[str, int | str] = {
            'batch_id': batch_id,
            'input_path': input_path,
            'output_path': output_path,
            **validation_result,
        }

        print()
        print('Spark 도서 전처리 완료')
        print(f"Silver 데이터 수: {result['row_count']}")
        print(f'Silver 저장 경로: {output_path}')
        print('=' * 60)

        return result

    finally:
        # Job 완료 또는 예외 발생 시 캐시 메모리를 즉시 반환하여 Executor Out Of Memory(OOM) 방지
        silver_frame.unpersist()


## ===========================================================
## 14. 프로그램 진입점
## ===========================================================

def main() -> None:
    """
    명령행 인자를 읽고 Spark 전처리 작업을 실행한다.
    """

    args = parse_arguments()

    spark = create_spark_session(
        batch_id=args.batch_id
    )

    try:
        # 배치 단위 ETL 파이프라인 구동
        run_preprocess(
            spark=spark,
            batch_id=args.batch_id,
            data_dir=args.data_dir,
        )

    finally:
        # 분산 애플리케이션 정상 종료 및 Master-Worker 리소스 해제
        spark.stop()


if __name__ == '__main__':
    main()