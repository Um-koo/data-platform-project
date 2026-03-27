import requests
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime

# AirKorea API 인증키
API_KEY = "YOUR_AIRKOREA_API_KEY"

# S3 버킷 및 리전 설정
BUCKET_NAME = "data-platform-raw-umkoo"
REGION_NAME = "ap-northeast-2"

# S3 클라이언트 생성
# 주의: 실제 Git 업로드 전에는 Access Key / Secret Key를 placeholder로 교체해야 함
S3 = boto3.client(
    "s3",
    aws_access_key_id="YOUR_AWS_ACCESS_KEY_ID",
    aws_secret_access_key="YOUR_AWS_SECRET_ACCESS_KEY",
    region_name=REGION_NAME
)

# AirKorea 시도별 실시간 측정정보 API 엔드포인트
URL = "https://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"

# 수집 대상 지역 목록
REGIONS = ["서울", "부산", "인천", "대구", "대전"]

# 최종적으로 사용할 컬럼만 선택
COLUMNS = [
    "stationName",
    "sidoName",
    "dataTime",
    "pm10Value",
    "pm25Value",
    "o3Value",
    "no2Value",
    "coValue",
    "so2Value"
]


def fetch_region_data(region: str) -> pd.DataFrame:
    """
    지정한 지역(region)의 AirKorea 데이터를 호출하여
    필요한 컬럼만 정리한 DataFrame으로 반환한다.
    """

    params = {
        "serviceKey": API_KEY,
        "returnType": "json",
        "sidoName": region,
        "numOfRows": "100",
        "pageNo": "1",
        "ver": "1.0"
    }

    # API 호출
    response = requests.get(URL, params=params, timeout=30)
    response.raise_for_status()

    # JSON 응답 파싱
    data = response.json()
    items = data["response"]["body"]["items"]

    # DataFrame 변환 후 필요한 컬럼만 선택
    df = pd.DataFrame(items)
    df = df[COLUMNS].copy()

    # 수치형 컬럼 변환
    # errors="coerce"를 사용하여 변환 불가 값은 NaN 처리
    numeric_cols = ["pm10Value", "pm25Value", "o3Value", "no2Value", "coValue", "so2Value"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 측정 시각을 datetime 타입으로 변환
    df["dataTime"] = pd.to_datetime(df["dataTime"], errors="coerce")

    return df


def upload_df_to_s3(df: pd.DataFrame, region: str):
    """
    DataFrame을 CSV 형태로 변환한 뒤
    지역별/날짜별 S3 Raw 경로에 업로드한다.
    """

    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y%m%d_%H%M%S")

    # DataFrame을 메모리 상 CSV 버퍼로 변환
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")

    # S3 저장 경로
    # 예: raw/airkorea/date=2026-03-26/region=서울/airkorea_서울_20260326_154931.csv
    key = f"raw/airkorea/date={date_str}/region={region}/airkorea_{region}_{ts_str}.csv"

    # S3 업로드
    S3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=csv_buffer.getvalue().encode("utf-8-sig"),
        ContentType="text/csv"
    )

    print(f"업로드 완료: s3://{BUCKET_NAME}/{key}")


def main():
    """
    지역 목록을 순회하면서
    1) API 호출
    2) DataFrame 생성
    3) S3 업로드
    를 수행한다.
    """

    for region in REGIONS:
        try:
            print(f"[시작] {region}")

            # 지역별 데이터 수집
            df = fetch_region_data(region)
            print(f"{region} 데이터 shape: {df.shape}")

            # 수집 데이터를 S3 Raw 영역에 업로드
            upload_df_to_s3(df, region)

            print(f"[완료] {region}\n")

        except Exception as e:
            print(f"[실패] {region}: {e}\n")


if __name__ == "__main__":
    main()