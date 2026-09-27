# Placement funnel metric benchmark

월 10억 event, 최대 2년(약 240억 상세 event) 보관을 전제로 기존 최초 이벤트 상태와 기간별 unique metric을 비교하는 독립 실험입니다. 서비스 코드를 변경하지 않습니다.

## 작업 기준과 현재 구조

- 기준 커밋: `origin/main`의 `c67e2ed` (A3/A4 구현 및 A7 설계 포함).
- 이전 A5/A6 실험은 `a5-a6-summary-tests` 브랜치와 PR #4에 보존되어 있습니다.
- 쇼핑몰 입력: 정규화된 `shopping_events` → local MV → `first_event_states` → `argMinMerge` 조회. 원본은 ReplicatedMergeTree, 상태는 ReplicatedAggregatingMergeTree입니다. standalone에는 `SimpleAggregateFunction(min, received_at)`도 있습니다.
- 큰 tuple은 mall/store/product/group IDs, 발생·수집 시각, source/message ID를 담습니다. 최초 기준은 `received_at`, 동률은 message ID입니다. A3는 조회 시 원본에서 대표를 선택하고 A4는 저장된 상태를 읽습니다.
- A7 이벤트별 ReplacingMergeTree + summary는 설계 문서이며 구현된 경로가 아닙니다.
- JSON 파싱·규칙 평가·매핑 JOIN은 쇼핑몰 아키텍처의 참고 모델입니다. 실제 실행 예제는 보강 완료 event부터 시작합니다.
- 별도 Spring 앱은 HTTP → service/repository → JDBC → push/click Distributed → local MV → 시간별 aggregate state → 조회 View 순서입니다. Kafka 엔진 실험도 별도로 존재하며 쇼핑몰과 자동 연결되어 있지 않습니다.

관련 코드: [기존 DDL](../standalone/schema/clickhouse.sql), [쇼핑몰 구조](../docs/architecture.md), [A4](../cluster/a4-dedup-count/README.md), [A7](../cluster/a7-event-replacing_summary-count/README.md), [Spring 서비스](../../../apps/push-click-service/README.md).

## 의미와 모델

직접 `LANDING / OPEN / CLICK / CONVERSION` 이름을 생성합니다. 기존 쇼핑몰 이벤트를 의미가 다른 단계에 억지로 치환하지 않습니다. 계층은 `campaign → adgroup → placement → tracking`이며 ID의 소속은 실험 중 불변입니다. SEND는 제외합니다.

주 비교의 정답은 **기간 안에 해당 placement/stage가 한 번이라도 발생한 tracking 수**입니다. 전체 이력에서 최초 발생을 확정하고 그 시각을 기간으로 거르는 결과는 별도 지표입니다. 예를 들어 어제와 오늘 클릭한 tracking은 오늘의 기간별 unique에는 포함되지만 오늘 최초 발생 수에는 포함되지 않습니다.

```text
공통 normalized raw (내구성 있는 MergeTree, 모든 안에 동일)
 ├─ Baseline raw: funnel_event → 기간 필터 → uniqExact / GROUP BY+argMin
 ├─ Baseline state: funnel_event → MV → 큰 argMin tuple + min 상태
 ├─ Case A: stage별 MergeTree → MV → tracking_state(min 발생시각)
 │                                  → 명시적 snapshot 재계산 → 1m/1h exact metric
 └─ Case B: stage별 MergeTree → MV → 1m unique state
                                    → MergeState MV → 1h unique state
```

Case A의 최초 이벤트 snapshot은 기간별 unique와 동일하지 않습니다. merge에 의한 min 변경은 downstream incremental MV를 재실행하지 않으므로 Case A metric은 빈 target에 snapshot으로 만듭니다. 실시간 증분 보정이 있다고 가정하지 않습니다.

Metric은 4개 state 열 대신 `(placement_id, metric_time, stage)`별 state 한 열을 사용합니다. 희소 stage에 빈 state를 저장하지 않고 새 stage도 동일한 집계 경로를 쓰기 위함입니다. 서비스 응답은 `...MergeIf(state, stage = 'CLICK')` 등으로 4열로 변환할 수 있습니다.

## 키와 시간

- Event: 월 파티션, `ORDER BY (placement_id, event_datetime, tracking_id)`.
- Metric: 월 파티션, `ORDER BY (placement_id, metric_time, stage, campaign_id, adgroup_id)`.
- Tracking state: 시간 파티션 없음. `(placement_id, tracking_id, stage)`의 최초 시각은 날짜를 넘어 병합해야 합니다.
- 1m: `toStartOfMinute(event_datetime)`, 1h: `toStartOfHour(metric_time)`를 SQL에 명시합니다.
- 조회 API 계약은 `[from, to)`입니다. 인접 기간 경계 중복을 피합니다.
- 1m/1h 숫자를 합하지 않고 전체 조회 기간의 상태를 다시 merge합니다.
- 정각에 맞지 않는 기간은 완전히 포함된 시간의 1h state + 양쪽 경계의 상세 event state를 UNION ALL한 뒤 merge합니다. metric bucket만 필터하면 경계 바깥 event가 섞입니다.

## 비교와 측정 계획

실제 확인 버전은 `26.9.1.1357`입니다. 초기 측정은 기존 클러스터의 한 Pod에 격리 DB를 만들고 비복제 MergeTree로 수행합니다. 복제·분산 비용이 포함된 프로덕션 검증은 후속 단계입니다.

- 1M → 5M → 10M event, 90일 시간 분포, 2,000 placement, 다수 campaign/adgroup, hot placement와 여러 분·시간·날짜의 반복 tracking.
- Baseline raw / 큰 argMin / Case A 최소시각 / Case B Combined64·Exact·Bitmap을 서로 다른 DB에서 순차 적재합니다.
- Bitmap ID는 synthetic UInt64 고유 ID입니다. 운영 String ID를 hash로 바꾸면 충돌 가능성이 있으므로 exact 보장은 별도 무충돌 매핑이 필요합니다.
- placement 1/10/100/1000/2000 × 10분/1시간/1일/30일/90일.
- 정확값과 approximate 결과를 비교하고 재전송·늦은 도착·시간 경계·분/시 중복을 검사합니다.
- query_log: 서버 elapsed, read/result rows, read bytes, 메모리, CPU(ProfileEvents).
- parts/columns: 압축 disk, active parts, state 열 크기, 원본 대비 행 감소율.
- part_log/merges: 자연 merge 횟수·시간·CPU·메모리, 실행 중 샘플. 강제 OPTIMIZE 비용은 자연 merge와 구분합니다.
- 메모리 1.5GB, threads 2, 쿼리 시간 상한을 적용합니다. 대규모 실행을 무리하게 확대하지 않습니다.

실행법은 아래에, 측정 결과는 [RESULTS.md](./RESULTS.md)와 `results/`에 기록합니다. 상세한 [구조·의미 분석](./ARCHITECTURE.md)도 함께 확인합니다. 일일/월간 운영 요청량을 기존 합성 데이터의 날짜 범위에서 유추하지 않습니다. 월 10억은 이번 사용자가 제시한 운영 가정입니다.

## 재현 명령

루트에서 기본 전체 실행:

```bash
./examples/shopping-journey/funnel-benchmark/benchmark.sh
```

Python 표준 라이브러리와 kubectl만 사용합니다. `LAB_KUBE_CONTEXT`, `LAB_NAMESPACE`, `LAB_CLICKHOUSE_POD`를 바꿀 수 있습니다. 비밀번호·토큰을 명령이나 로그에 넣지 않습니다. 각 실행은 `fb_<run_id>_*` 독립 DB를 생성하며 기존 DB를 덮어쓰지 않습니다.

측정 DB는 결과를 직접 확인할 수 있도록 실행 후 유지합니다. `--run-id`로 이름을 고정하면 대상은 `fb_<run_id>_source_<scale>`와 `fb_<run_id>_<scale>_<case>`로 한정됩니다. 검토가 끝난 뒤 해당 run ID의 DB만 명시적으로 삭제합니다.

```bash
# 빠른 문법·측정기 확인
./examples/shopping-journey/funnel-benchmark/benchmark.sh \
  --scales 16000 --placements 1 10 --batch 4000 --repeats 1

# 실제 시간 경계/재전송/늦은 도착 검사 (임시 DB 자동 정리)
python3 -B examples/shopping-journey/funnel-benchmark/verify.py

# UInt64 ID 분포별 serialized state 크기와 정확도
python3 -B examples/shopping-journey/funnel-benchmark/state_probe.py

# 수집한 실제 query_log에서 표 생성
python3 -B examples/shopping-journey/funnel-benchmark/report.py \
  examples/shopping-journey/funnel-benchmark/results/<run_id>
```

`ddl/` SQL의 `{db}`, `{function}`, `{stage}`는 실행기가 치환하는 명시적 template입니다. 실행기에서 `schema()`가 모든 DDL과 MV를 순서대로 적용합니다. `queries/service.sql`은 최종 API의 4열 응답 예시이고, 각 실행 디렉터리의 `*_queries.sql.gz`은 실제 실행한 placement IN 목록/기간을 보존합니다.

전체 명령은 시간 경계 정합성 검사와 결과 표 생성까지 실행합니다. 이미 검사한 반복 측정에서는 `--skip-correctness`로 생략할 수 있습니다.

기본 측정은 INSERT 100,000행 batch, 동시성 1, query별 워밍업 1회+측정 3회입니다. INSERT rows/sec는 공통 raw를 읽어 전체 MV 경로까지 처리한 서버 시간이며 네트워크 수집 처리량이 아닙니다. 생성 비용은 제외합니다. `insert_wall.csv`에는 kubectl과 클라이언트를 포함한 별도 벽시계 시간이 있습니다.

매 단계에서 자연 background merge를 유지합니다. INSERT 이후와 조회 이후 parts를 따로 저장하므로 그 사이 merge에 따른 저장량 변화를 볼 수 있습니다. query CPU는 해당 쿼리의 UserTime+SystemTime 합계이며 전체 호스트 CPU 사용률이 아닙니다. part_log의 MergeParts CPU와 peak memory, system.merges 1초 샘플로 별도 merge 부담을 관측합니다. 짧은 merge는 샘플에 잡히지 않을 수 있습니다.

State 크기는 `system.parts_columns`의 압축/비압축 bytes와 독립 `RowBinary` 직렬화 probe를 함께 사용합니다. compressed disk, 메모리, serialized state는 서로 다른 값입니다. exact/bitmap은 이 데이터 계약에서 오차 0을 강제하고 Combined64 오차는 실제 exact 기준으로 보고합니다.
