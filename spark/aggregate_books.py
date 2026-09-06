"""
Silver Parquet을 평점별로 집계하여 Gold Parquet으로 저장하는 Spark Job

[입력]
data/silver/{batch_id}/

[처리]
1. Silver Parquet 데이터셋 로드
2. 평점(rating) 기준 그룹화(groupBy)
3. 평점별 핵심 통계 지표 계산 (도서 수, 평균/최저/최고 가격)
4. 평점 기준 오름차순 정렬
5. Gold Parquet 저장

[출력]
data/gold/{batch_id}/

[집계 지표 규칙]
1. book_count : 평점별 전체 도서 수 (count)
2. avg_price   : 평균 가격 산출 후 소수점 둘째 자리 반올림 (round, avg)
3. min_price   : 해당 평점 구간의 최저 판매 가격 (min)
4. max_price   : 해당 평점 구간의 최고 판매 가격 (max)

Spark 기본 내장 집계 함수(F.count, F.round, F.avg, F.min, F.max)만
사용하여 Catalyst Optimizer가 연산을 최적화하며,
Python UDF 없이 실행되므로 드라이버와 워커 간 Python 버전 불일치 문제가 발생하지 않는다.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


## ===========================================================
## 1. 실행 인자 처리
## ===========================================================

def arguments() -> argparse.Namespace:
    """
    Gold 계층 집계 작업에 필요한 명령행 인자를 파싱한다.

    Returns:
        batch_id와 data_dir 정보가 담긴 Namespace 객체
    """
    parser = argparse.ArgumentParser()
    # 배치 파이프라인의 멱등적 데이터 처리를 위한 필수 배치 ID 수신
    parser.add_argument('--batch-id', required=True)
    # 볼륨 마운트된 컨테이너 내부 데이터 루트 경로 기본값 설정
    parser.add_argument('--data-dir', default='/opt/project/data')
    return parser.parse_args()


## ===========================================================
## 2. Spark 분산 집계 및 메인 작업 파이프라인
## ===========================================================

def main() -> None:
    """
    Silver 데이터를 읽어 평점별 통계 지표(수량, 평균가, 최저가, 최고가)를
    집계한 후 Gold 계층의 Parquet 포맷으로 저장한다.
    """
    args = arguments()

    # 배치 단위 모니터링을 위한 SparkSession 생성 및 애플리케이션 식별자 등록
    spark = SparkSession.builder.appName(f'books-aggregate-{args.batch_id}').getOrCreate()

    # 정제 완료된 Silver Parquet 데이터셋 로드 (Columnar 포맷 기반 빠른 프로젝션)
    silver = spark.read.parquet(f'{args.data_dir}/silver/{args.batch_id}')

    # --- Gold 계층 비즈니스 메트릭 집계 (Data Mart 생성) ---
    # 평점(rating) 키 기준으로 파티션 간 데이터 재분배(Shuffle) 후 집계 연산 수행
    gold = (
        silver.groupBy('rating')
        .agg(
            # 평점별 전체 도서 수 집계
            F.count('*').alias('book_count'),
            # 소수점 셋째 자리에서 반올림하여 둘째 자리까지의 평균 가격 도출
            F.round(F.avg('price'), 2).alias('avg_price'),
            # 해당 평점 구간의 최저 판매 가격
            F.min('price').alias('min_price'),
            # 해당 평점 구간의 최고 판매 가격
            F.max('price').alias('max_price'),
        )
        # 대시보드 시각화 및 다운스트림 조회를 위해 평점 오름차순 정렬
        .orderBy('rating')
    )

    # 비즈니스 서빙용 Gold 레이어 출력 경로 설정
    output = f'{args.data_dir}/gold/{args.batch_id}'

    # 동일 배치 재실행 시 멱등성을 보장하기 위해 overwrite 모드로 Parquet 저장
    gold.write.mode('overwrite').parquet(output)

    # 작업 완료 로그 출력 (count() 액션을 트리거하여 최종 집계 레코드 수 확인)
    print(f'Gold rows={gold.count()}, path={output}')

    # Spark 애플리케이션 정상 종료 및 할당된 클러스터 리소스 반환
    spark.stop()


## ===========================================================
## 3. 프로그램 진입점
## ===========================================================

if __name__ == '__main__':
    main()