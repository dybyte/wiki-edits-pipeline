# 장애 실험 기록

규칙: 돌리기 **전에** 예측을 먼저 적는다. 틀렸으면 왜 틀렸는지 적는다. 실패하고 고친 기록도 지우지 않는다.

## 실행 도구

모든 스크립트는 프로젝트 루트에서 Docker 서비스가 실행 중이고 `.venv` 의존성이 설치된 상태를 전제로 한다.
스크립트는 성공 조건을 자동 검사하지만, 아래 표의 실제 숫자와 해석은 실행자가 직접 기록한다.

| 목적 | 명령 |
|---|---|
| 현재 lag·raw 중복 스냅샷 | `experiments/capture_lag.sh` |
| raw 중복을 넣어 staging·mart 영향 확인 | `experiments/run_duplicate_experiment.sh` |
| insert와 commit 사이 강제 종료 재현 | `experiments/run_crash_before_commit.sh` |
| 날짜 파티션 백필 멱등성 확인 | `experiments/run_backfill_experiment.sh YYYY-MM-DD` |
| 알 수 없는 JSON 필드가 추가된 이벤트 확인 | `experiments/run_schema_change_experiment.sh` |

`run_duplicate_experiment.sh`와 `run_schema_change_experiment.sh`가 넣은 실험 행은 종료 시 자동으로 삭제된다.

| 실험 | 날짜 | 예측 | 처리한 이벤트 수 | 결과 숫자 | 예측이 맞았나 / 원인과 수정 |
|---|---|---|---|---|---|
| 0. 컨슈머 정지 → lag 관찰 (Grafana, 정상 5분 → Ctrl+C로 5분 정지 → 재시작. 방법은 GUIDE.md 10장) | | (1) 정상일 때 lag 그래프 모양: (2) 5분 정지 후 lag: (3) 0 근처로 돌아오는 데 걸리는 시간과 그동안의 flush 조건: | | 정지 직전 lag, 최대 lag, 회복 시간, 따라잡는 동안 초당 처리량 | |
| 1a. 컨슈머 무작위 강제 종료 (적재 중 아무 때나 `kill -9`, 30초 뒤 재시작, 5회) | | 중복 0건, 유실 0건 (대부분 batch를 모으는 중에 죽어서, DW에 들어간 적 없는 batch를 다시 읽어 처음으로 넣는다) | | Kafka 고유 event_id 수 vs raw 고유 event_id 수, raw 중복 수, 다시 읽은 메시지 수 | |
| 1b. insert 직후 commit 전 강제 종료 (환경 변수로 켜는 장치, 3회) | | 매번 raw에 그 batch 크기만큼 중복(최대 약 2,000건), 유실 0건, 마트는 영향 없음 | | raw 중복 수, staging 중복 수, dbt 테스트 결과 | |
| 2. 백필 재실행 (같은 날짜 범위 2회) | | | | 1회차와 2회차 mart 값 비교 | |
| 3. 스키마 변경 (새 필드가 섞인 이벤트) | | | | 파이프라인 중단 여부, 새 필드 반영 여부 | |

실험 1을 둘로 나눈 이유: 컨슈머는 대부분의 시간을 batch를 모으는 데 쓰므로, 무작위로 죽이면 중복이 거의 생기지 않는다.
중복은 insert 후 commit 전에 죽을 때만 생기므로, staging 중복 제거는 1b에서 시험한다.

실험 0은 Grafana 그래프를 사람이 관찰해야 하므로 완전 자동화하지 않는다. 정상·중지·재시작 시점마다
`capture_lag.sh`를 실행하고 Grafana 스크린샷과 함께 위 표에 기록한다.

## 2026-11-18 구현 완료 검증

아래는 스크립트와 자동화 구현을 마친 날의 실제 로컬 실행 결과다. 실험 0은 5분 관찰 대신 짧은 smoke run이다.

| 검증 | 실제 결과 |
|---|---|
| 실시간 수집·lag 회복 | producer 1,041건 전송. consumer가 먼저 217건 commit한 뒤 정지 상태의 총 lag 824건을 확인. 재시작 후 500건+324건을 적재해 세 파티션 모두 lag 0 |
| 임의 raw 중복 | 최근 article edit 619건을 복제하자 raw 중복 619건. staging unique 포함 dbt 테스트 12개 통과, mart 전체 `edit_count`는 619로 불변 |
| insert 후 commit 전 장애 | 실제 batch 500건 insert 직후 종료. 재시작 뒤 raw 중복 500건, staging unique와 mart 대조 테스트 포함 12개 통과 |
| 날짜 백필 재실행 | 2026-10-22 파티션 53행. 재실행 전후 체크섬 `16356754536569041800`으로 동일 |
| 스키마 변경 | 기존 모델이 모르는 중첩 `future_field`를 추가해도 `enwiki:edit:1`로 추출되고 테스트 12개 통과 |
| Airflow 자동화 | Airflow 3.4.0 예약 실행에서 `dbt run` 3개 모델과 `dbt test` 12개가 순서대로 성공, 전체 DAG 21.0초 |
