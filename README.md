====== 신규
# Data Platform Project

AirKorea 데이터를 기반으로  
데이터 수집 → 저장 → 정제 → 집계 → 분석까지 이어지는  
데이터 플랫폼 구조를 구축한 프로젝트

---

## 1. 프로젝트 개요

공공 API(에어코리아)를 활용하여 데이터를 수집하고,  
S3 기반 Raw 저장소와 PostgreSQL의 raw / staging / mart 계층을 구성하여  
분석 가능한 데이터 플랫폼을 구축하였다.

이 프로젝트는 단순 데이터 수집이 아닌,  
데이터를 저장하고 가공하여 활용 가능한 구조로 만드는 것을 목표로 한다.

---

## 2. 목적

- 공공 데이터 API 기반 데이터 수집 구조 확장  
- AWS S3 기반 Raw 데이터 저장 구조 설계  
- PostgreSQL raw / staging / mart 계층 구성  
- SQL 기반 분석 및 View 제공  
- 데이터 활용성을 고려한 구조 설계  

---

## 3. 시스템 구성

AirKorea API  
→ Python Script  
→ AWS S3 (Raw Storage)  
→ PostgreSQL (raw → staging → mart)  
→ SQL / View 기반 분석  

---

## 4. 기술 스택

- Python  
- PostgreSQL  
- AWS S3  
- Pandas  
- Boto3  

---

## 5. 프로젝트 구조

data-platform-project  
├── scripts/  
├── sql/  
├── data/  
├── .gitignore  
├── README.md  
└── LICENSE  

---

## 6. 데이터 처리 흐름

1. AirKorea API 호출  
2. 지역별 데이터 수집  
3. CSV 형태로 변환  
4. S3에 Raw 데이터 저장  
5. S3 데이터를 PostgreSQL raw 테이블에 적재  
6. raw → staging 데이터 정제  
7. staging → mart 집계  
8. SQL 및 View를 통한 분석 구조 제공  

---

## 7. 실행 방법

### 7.1 전체 파이프라인 실행

아래 스크립트를 실행하면 전체 데이터 파이프라인이 순차적으로 수행된다.

python scripts/run_pipeline.py

---

### 7.2 실행 흐름

파이프라인은 다음과 같은 단계로 구성된다.

1. AirKorea API 호출  
2. 지역별 데이터 수집  
3. CSV 변환 및 S3 저장  
4. S3 → raw 적재  
5. raw → staging 데이터 정제  
6. staging → mart 집계  

---

### 7.3 실행 결과 확인

파이프라인 실행 후 다음 항목을 확인한다.

- 데이터가 정상적으로 적재되었는지  
- 집계 결과가 정상적으로 생성되었는지  
- 실행 로그가 기록되었는지  

---

### 7.4 재실행 검증

동일 파이프라인을 반복 실행하여  
중복 데이터가 발생하지 않고 정상적으로 동작하는지 확인한다.

- 재실행 시 데이터 정합성 유지  
- 집계 결과 일관성 유지  
- 실행 로그 정상 누적  

---

## 8. 데이터 플랫폼 설계

본 프로젝트는 Pipeline 단계를 넘어  
데이터를 활용하기 위한 플랫폼 구조로 확장하였다.

### 8.1 계층 구조 설계

- raw: 원본 데이터 보존  
- staging: 정제 및 컬럼 구조화  
- mart: 분석용 집계 데이터  

### 8.2 데이터 활용 구조

- SQL 기반 분석 제공  
- View를 통한 재사용성 확보  
- 분석 쿼리 표준화  

### 8.3 저장소 분리

- S3: Raw 데이터 저장소  
- PostgreSQL: 분석 및 처리 DB  

---

## 9. 데이터 검증 및 분석

### 9.1 실행 로그 확인

SELECT *
FROM mart.pipeline_log
ORDER BY id DESC;

---

### 9.2 적재 데이터 확인

SELECT *
FROM staging.staging_airkorea
ORDER BY data_time DESC;

---

### 9.3 집계 결과 확인

SELECT *
FROM mart.mart_airkorea_region_summary
ORDER BY avg_pm10 DESC;

---

### 9.4 데이터 품질 검증

-- NULL 체크
SELECT *
FROM staging.staging_airkorea
WHERE pm10_value IS NULL;

-- 이상값 체크
SELECT *
FROM staging.staging_airkorea
WHERE pm10_value > 80;

---

### 9.5 분석 예시

-- 지역별 평균
SELECT
    sido_name,
    AVG(pm10_value)
FROM staging.staging_airkorea
GROUP BY sido_name;

-- TOP 오염 지역
SELECT *
FROM mart.mart_airkorea_region_summary
ORDER BY avg_pm10 DESC
LIMIT 3;
---

## 10. 운영 안정성 (Stabilize)

### 10.1 중복 데이터 방지

- UNIQUE 제약 적용  
- ON CONFLICT DO NOTHING 사용  

→ 재실행 시 중복 데이터 방지  

---

### 10.2 재실행 가능 구조

- 동일 파이프라인 반복 실행 가능  
- 데이터 정합성 유지  

---

### 10.3 실행 로그 관리

- mart.pipeline_log 테이블 활용  
- 실행 상태 기록 (SUCCESS / FAILED)  

---

### 10.4 데이터 품질 검증

- NULL 체크  
- 이상값 탐지 SQL 적용  

---

## 11. 향후 확장

- Airflow 기반 자동 스케줄링  
- 데이터 시각화 (BI 연동)  
- Kafka 기반 Streaming Pipeline  
- AWS 기반 클라우드 확장  

---

## 12. 작성자

Um-koo
====
