# AirKorea Data Platform Project

## 1. 프로젝트 개요

공공데이터 API(AirKorea)를 활용하여  
데이터 수집 → 저장 → 정제 → 분석까지 이어지는  
End-to-End 데이터 파이프라인을 구축한 프로젝트입니다.

---

## 2. 전체 아키텍처

AirKorea API  
→ S3 (Raw Data 저장)  
→ PostgreSQL (raw / staging / mart)  
→ SQL 분석

---

## 3. 데이터 흐름

1. AirKorea API 호출 (지역별 데이터 수집)
2. 데이터를 CSV 형태로 변환 후 S3 저장
3. S3 데이터를 PostgreSQL raw 테이블에 적재
4. raw 데이터를 staging 테이블로 정제
5. staging 데이터를 mart 테이블로 집계

---

## 4. 테이블 구조

### 4.1 raw (원본 데이터)

- S3에서 가져온 원본 데이터 저장
- 데이터 보존 목적

---

### 4.2 staging (정제 데이터)

- 필요한 컬럼만 추출
- 데이터 타입 정리
- 분석 가능한 형태로 가공

---

### 4.3 mart (분석용 데이터)

- 지역별 미세먼지 평균 집계
- 빠른 조회 및 분석을 위한 구조

---

## 5. 실행 방법

python scripts/run_pipeline.py

---

## 6. 분석 예시

### ✔ 지역별 평균 미세먼지

SELECT
    sido_name,
    AVG(pm10_value) AS avg_pm10
FROM staging.staging_airkorea
GROUP BY sido_name;

---

### ✔ TOP 오염 지역

SELECT *
FROM mart.mart_airkorea_region_summary
ORDER BY avg_pm10 DESC
LIMIT 3;

---

### ✔ 이상값 탐지

SELECT *
FROM staging.staging_airkorea
WHERE pm10_value > 80;

---

## 7. 운영 안정성 (Stabilization)

본 파이프라인은 운영 환경을 고려하여 다음과 같이 설계되었습니다.

- 재실행 가능 구조 (Idempotent Pipeline)
- PostgreSQL ON CONFLICT 기반 중복 데이터 방지
- 실행 로그 기록 (mart.pipeline_log 테이블)
- 다건 데이터 처리 및 반복 실행 검증

---

## 8. 기술 스택

- Python
- PostgreSQL
- AWS S3
- Pandas
- Boto3

---

## 9. 프로젝트 특징

- API → S3 → DB → Mart까지 전체 데이터 흐름 구현
- Raw / Staging / Mart 계층 분리 설계
- 데이터 품질 및 안정성 고려
- 실제 운영 환경을 고려한 구조

---

## 10. 향후 확장 계획

- Airflow 기반 스케줄링 적용
- 데이터 시각화 (BI 도구 연동)
- Kafka 기반 스트리밍 파이프라인 확장
- AWS 기반 클라우드 전환
