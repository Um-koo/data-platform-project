import os
import boto3

# 🔑 본인 키 입력
AWS_ACCESS_KEY_ID = "AKIA23A62ZK6YMUVY7PP"
AWS_SECRET_ACCESS_KEY = "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"

# AWS 설정
REGION_NAME = "ap-northeast-2"
BUCKET_NAME = "data-platform-raw-umkoo"

# 파일 경로
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_FILE_PATH = os.path.join(BASE_DIR, "data", "sample.json")

# S3 저장 경로
S3_KEY = "raw/weather/2026-03-23/sample.json"

def upload_file_to_s3():
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION_NAME,
    )

    print("로컬 파일 경로:", LOCAL_FILE_PATH)
    print("S3 대상 경로:", f"s3://{BUCKET_NAME}/{S3_KEY}")

    s3.upload_file(LOCAL_FILE_PATH, BUCKET_NAME, S3_KEY)

    print("업로드 성공")

if __name__ == "__main__":
    try:
        upload_file_to_s3()
    except Exception as e:
        print("오류 발생:", e)