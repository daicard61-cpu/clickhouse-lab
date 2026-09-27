# Funnel benchmark 실측 결과

## 결론과 해석 기준

**기간 내 placement별 unique funnel에는 Case B(Event → 1m state → 1h state)를 우선 검토합니다.** 최초 발생 시각이 업무 기능에 필요한 경우에만 별도 scalar tracking state를 추가하는 구성이 이번 의미 계약에 맞습니다. 큰 argMin tuple을 count만을 위해 유지할 이유는 약합니다.

이는 240억 행 운영 적합성 인증이 아닙니다. 원본 조회 자체도 10M 범위에서는 빠른 구간이 많고, metric은 적재·저장·병합 비용을 추가합니다. 아래 실측에서 조회 이득과 쓰기 비용을 함께 확인해야 합니다.

## 실행 조건

- 기준: `origin/main` `c67e2ed`; ClickHouse `26.9.1.1357`, UTC.
- 한 Pod, 비복제 MergeTree 계열, 다른 클러스터 Pod와 로컬 VM/디스크 공유.
- 1M / 5M / 10M event, 90일, 2,000 placement; placement 1에 tracking 약 20% 집중.
- tracking당 4 stage × 각 4회. 동일 단계의 반복이 분·시간·날짜를 넘습니다. 각 stage의 동일 볼륨은 부하 통제를 위한 합성 조건이며 실제 퍼널 이탈률이 아닙니다.
- UInt64 tracking 고유 ID, 배치 100k, INSERT 동시성 1; query threads 2, query memory 1.5GB, external GROUP BY 기준 300MB.
- IN 1/10/100/1000/2000 × 10분/1시간/1일/30일/90일. 정렬된 ID prefix라 모두 hot placement 1을 포함합니다. 무작위 IN 집합 실험이 아닙니다.
- 조회 워밍업 1회 + 본 측정 3회 중앙값. ClickHouse query cache를 끄지만 OS page cache까지 비우지는 않습니다. p95/p99/SLO 결과가 아닙니다.
- 서버 INSERT 시간에는 공통 normalized raw 읽기와 동기 MV cascade가 포함되고 생성·네트워크 수집은 제외됩니다. wall time은 kubectl 전송을 포함합니다.
- disk는 해당 안의 상세+파생 테이블이며 공통 raw, 복제본, 백업, 비활성 part/merge 임시 공간은 제외합니다.

원시 결과와 자동 생성 표: [전체 조회 행렬](./results/measured_20260927/QUERY_MATRIX.md), [환경](./results/measured_20260927/environment.json), [query log](./results/measured_20260927/query_log.csv), [저장량](./results/measured_20260927/storage.csv), [merge log](./results/measured_20260927/part_log.csv), [정확도](./results/measured_20260927/accuracy.csv).

<!-- MEASURED_TABLES -->

## 적재와 저장량

서버가 공통 raw를 읽어 각 안의 테이블과 동기 MV까지 처리한 결과입니다. Case A의 snapshot 재계산은 이 표 다음에 별도로 표시합니다.

| 규모 | 구조 | 서버 적재 s | rows/s | CPU s | peak MiB | wall s | disk MiB |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1M | raw | 0.433 | 2,309,469 | 0.43 | 79.2 | 0.63 | 17.3 |
| 1M | 큰 argMin state | 3.096 | 322,997 | 3.06 | 254.3 | 3.29 | 37.2 |
| 1M | Case A scalar min | 0.504 | 1,984,127 | 0.77 | 79.3 | 0.65 | 41.7 |
| 1M | Case B Combined64 | 1.116 | 896,057 | 1.52 | 80.7 | 1.32 | 44.6 |
| 1M | Case B Exact | 1.303 | 767,460 | 1.76 | 80.7 | 1.49 | 43.1 |
| 1M | Case B Bitmap | 1.174 | 851,789 | 1.65 | 92.0 | 1.38 | 43.2 |
| 5M | raw | 1.996 | 2,505,010 | 1.92 | 80.0 | 2.25 | 93.8 |
| 5M | 큰 argMin state | 17.409 | 287,208 | 16.78 | 255.0 | 17.62 | 202.5 |
| 5M | Case A scalar min | 4.653 | 1,074,576 | 5.15 | 83.8 | 4.89 | 180.3 |
| 5M | Case B Combined64 | 7.952 | 628,773 | 8.97 | 83.4 | 8.27 | 182.7 |
| 5M | Case B Exact | 9.346 | 534,988 | 10.63 | 82.2 | 9.58 | 179.2 |
| 5M | Case B Bitmap | 8.964 | 557,787 | 9.84 | 92.9 | 9.20 | 179.7 |
| 10M | raw | 4.644 | 2,153,316 | 4.23 | 79.9 | 4.99 | 173.8 |
| 10M | 큰 argMin state | 34.311 | 291,452 | 33.30 | 255.0 | 34.54 | 462.0 |
| 10M | Case A scalar min | 11.914 | 839,349 | 10.13 | 81.0 | 12.26 | 414.5 |
| 10M | Case B Combined64 | 12.861 | 777,545 | 15.01 | 84.0 | 12.90 | 419.1 |
| 10M | Case B Exact | 18.420 | 542,888 | 18.42 | 84.6 | 18.58 | 414.2 |
| 10M | Case B Bitmap | 14.614 | 684,275 | 16.63 | 95.2 | 14.61 | 412.6 |

10M의 큰 argMin 안은 raw 적재보다 서버 시간 기준 7.4배 느렸고, 저장량은 raw+state 462.0MiB였습니다. 대표 tuple state 열만 압축 222.3MiB, 비압축 2.12GiB였습니다. Case B의 약 412.6~419.1MiB에는 동일 event를 네 stage 테이블에 나눈 총 10M 상세 행 약 375MiB와 1m/1h metric이 모두 들어 있습니다. raw 통합 테이블과 stage별 event 테이블은 컬럼이 같아도 part/partition 및 압축 조건이 달라 이 저장량을 엔진 차이만으로 해석할 수 없습니다.

10M에서 1m metric의 active physical row는 Combined64 8.69M, Exact 8.91M, Bitmap 8.75M으로 원본 대비 약 11~13%만 줄었습니다. 90일에 데이터가 희소하게 흩어진 합성 조건 때문입니다. 1h는 각각 4.60M 안팎으로 약 54% 줄었습니다. active row에는 아직 하나로 합쳐지지 않은 같은 key의 여러 part row가 포함되므로 최종 논리 key 수와 같지 않습니다.

Case A snapshot은 1M 0.356초/275MiB, 5M 2.676초/885MiB, 10M 4.167초/1,283MiB를 사용했습니다. 5M부터 external aggregation spill이 발생했고 10M에서는 106MiB를 spill했습니다. 이 비용은 위 Case A INSERT 표에 포함되지 않습니다.

## 조회 결과

아래는 대표 조합의 중앙값입니다. `memory`는 query별 peak이며 `disk`는 해당 구조 전체 저장량입니다. 전체 825개 조합은 자동 생성된 [조회 행렬](./results/measured_20260927/QUERY_MATRIX.md)에 있습니다.

| 구조 | 데이터량 | placement 수 | 기간 | elapsed ms | read_rows | memory MiB | disk MiB |
|---|---:|---:|---|---:|---:|---:|---:|
| raw `uniqExact` | 10M | 1 | 10m | 3 | 24,576 | 0.4 | 173.8 |
| raw `uniqExact` | 10M | 1,000 | 30d | 32 | 2,064,384 | 37.2 | 173.8 |
| raw `uniqExact` | 10M | 2,000 | 90d | 146 | 10,000,000 | 157.4 | 173.8 |
| raw query-time 큰 argMin | 10M | 1 | 10m | 5 | 24,576 | 5.7 | 173.8 |
| raw query-time 큰 argMin | 10M | 1,000 | 30d | 1,300 | 2,064,384 | 314.7 | 173.8 |
| raw query-time 큰 argMin | 10M | 2,000 | 90d | 6,881 | 10,000,000 | 334.4 | 173.8 |
| 저장형 큰 argMin 최초수집 | 10M | 1 | 10m | 2,083 | 1,916,928 | 316.3 | 462.0 |
| 저장형 큰 argMin 최초수집 | 10M | 1,000 | 30d | 6,656 | 5,644,288 | 338.0 | 462.0 |
| 저장형 큰 argMin 최초수집 | 10M | 2,000 | 90d | 9,847 | 9,400,000 | 441.5 | 462.0 |
| Case A scalar 최초발생 | 10M | 1 | 10m | 32 | 598,016 | 99.6 | 414.5 |
| Case A scalar 최초발생 | 10M | 1,000 | 30d | 269 | 1,761,280 | 325.4 | 414.5 |
| Case A snapshot metric | 10M | 2,000 | 90d | 101 | 1,923,199 | 118.6 | 414.5 |
| Combined64 hybrid | 10M | 1 | 10m | 8 | 103,036 | 1.3 | 419.1 |
| Combined64 hybrid | 10M | 1,000 | 30d | 28 | 835,584 | 30.2 | 419.1 |
| Combined64 hybrid | 10M | 2,000 | 90d | 130 | 4,597,836 | 79.7 | 419.1 |
| Exact hybrid | 10M | 1,000 | 30d | 32 | 811,008 | 37.0 | 414.2 |
| Exact hybrid | 10M | 2,000 | 90d | 157 | 4,589,412 | 153.2 | 414.2 |
| Bitmap hybrid | 10M | 1,000 | 30d | 66 | 835,584 | 65.1 | 412.6 |
| Bitmap hybrid | 10M | 2,000 | 90d | 190 | 4,621,064 | 77.5 | 412.6 |

이 규모에서는 raw `uniqExact`가 느려지는 명확한 임계점을 찾지 못했습니다. 최대 조합도 146ms였고 Combined64 hybrid 130ms와 비슷했습니다. Exact/Bitmap metric은 오히려 157/190ms였습니다. Case B가 조회를 압도했다고 결론낼 수 없으며, 대신 24B 상세행을 매번 읽지 않도록 읽기 양을 시간 state로 제한하는 구조적 후보입니다. 운영 밀도의 state cardinality와 동시 부하를 추가해야 crossover를 찾을 수 있습니다.

IN과 기간은 함께 커질수록 읽는 row와 latency를 늘렸습니다. 같은 10M raw에서 1 placement·10분은 3ms/24,576행, 1,000 placement·30일은 32ms/2.06M행, 2,000 placement·90일은 146ms/10M행이었습니다. 이 생성기는 IN prefix가 항상 hot placement 1을 포함하므로 placement 수만의 독립 효과는 아닙니다.

큰 tuple의 비용은 뚜렷했습니다. 10M 전체 기간에서 query-time argMin은 raw unique보다 약 47배 느리고 peak memory도 2배 이상이었습니다. 저장형 argMin은 짧은 10분 조회도 전체 최초상태를 placement 기준으로 병합한 뒤 시간 필터를 적용하므로 2.08초가 걸렸습니다. 이 키/쿼리 조합은 기간 pruning에 불리합니다.

## 정확도와 state 크기

실제 ClickHouse fixture의 재전송, late earlier event, 분·시간·날짜 중복, 빈 구간, 부분 초·밀리초 경계 및 `OPTIMIZE FINAL` 전후를 총 138개 assertion으로 확인했습니다. exact와 bitmap은 모두 정답과 같았습니다. 분별 finalized 숫자의 합은 3이지만 동일 한 시간의 state merge 결과가 2인 반례도 통과했습니다.

본 825개 결과 중 exact/bitmap과 Case A snapshot의 같은 의미 비교는 다른 group 0개였습니다. Combined64의 group별 최대 상대 오차는 1M 0.20%, 5M 0.504%, 10M 0.6432%였습니다. 10M에서 각 쿼리의 group 평균 오차를 다시 평균한 값은 약 0.0467%입니다. 작은 실제 count에서는 상대오차가 튈 수 있으므로 운영 허용오차는 global total이 아니라 응답 group 분포로 판단해야 합니다.

아래는 한 개 state를 RowBinary로 직렬화한 독립 probe입니다. `scattered`는 hash 충돌 가능성을 숨기지 않도록 UInt64 공간에서 일대일인 산술 순열을 사용했습니다.

| cardinality | ID 분포 | Combined64 bytes / 오차 | Exact bytes / 오차 | Bitmap bytes / 오차 |
|---:|---|---:|---:|---:|
| 1,000 | dense | 12,274 / 0% | 43,623 / 0% | 205 / 0% |
| 1,000 | scattered | 12,731 / 0% | 12,467 / 0% | 86,519 / 0% |
| 10,000 | dense | 569,769 / 0.22% | 421,779 / 0% | 205 / 0% |
| 10,000 | scattered | 569,667 / 0.16% | 124,750 / 0% | 864,803 / 0% |
| 100,000 | dense | 427,915 / 0.314% | 3,929,176 / 0% | 230 / 0% |
| 100,000 | scattered | 428,249 / 0.146% | 1,246,968 / 0% | 8,647,015 / 0% |
| 1,000,000 | dense | 251,720 / 0.1147% | 37,833,494 / 0% | 1,076 / 0% |
| 1,000,000 | scattered | 251,767 / 0.1422% | 12,468,842 / 0% | 86,468,890 / 0% |

Combined64의 직렬화 크기가 cardinality에 따라 단조 증가하지 않는 것은 내부 표현 전환이 있기 때문이며 고정 bytes/key로 외삽하면 안 됩니다. Bitmap은 dense UInt64에 매우 강하지만 scattered 1M에서는 약 82.5MiB였습니다. 운영 tracking이 String이거나 임의 hash라면 exact 보장이 사라집니다. 무충돌 dense surrogate ID가 보장될 때만 Bitmap을 exact 후보로 봅니다. 일반 ID에서 exact가 필요하면 `uniqExactState`가 가장 단순하며, 실제 bucket cardinality에 따른 state·merge 메모리를 운영 분포로 다시 재야 합니다.

## background merge와 part

10M의 자연 merge 누계에서 큰 argMin state 테이블은 18회, 19.28초, CPU 18.74초, peak 178MiB였습니다. metric 1m+1h는 Combined64가 276회/18.74초/CPU 14.18초, Exact가 273회/32.84초/CPU 20.96초, Bitmap이 274회/31.80초/CPU 24.31초였습니다. 오류는 0건이었습니다.

이 값은 case별 측정이 끝날 때까지 `system.part_log`에 누적된 자연 merge입니다. 각 안의 관측 시간이 다르고 기존 클러스터와 호스트를 공유했으므로 엄격한 merge throughput 대조가 아닙니다. 짧은 merge는 1초 `system.merges` 표본에 잡히지 않을 수 있습니다. active part는 10M 조회 후 raw 21개, argMin 전체 28개, Combined64 전체 118개였습니다. stage/event 및 두 metric 계층이 part와 background 작업을 늘린다는 사실은 운영 병합 여유·batch 크기·partition 정책에 반영해야 합니다.

## 질문별 답

- **funnel_event 직접 조회는 언제 느려지는가?** 10M까지 raw `uniqExact`의 명확한 한계는 관측되지 않았습니다. 최대 10M scan이 146ms였습니다. query-time 큰 argMin은 5M 전체 4.31초, 10M 전체 6.88초로 이미 부담이 컸습니다.
- **placement IN 증가 효과는?** hot placement를 포함한 prefix IN에서 수와 기간이 함께 커질수록 읽기가 24.6K→10M행으로 늘었습니다. 무작위/비인기 placement 집합은 추가 실험이 필요합니다.
- **기간 증가 효과는?** ORDER BY의 placement 다음 시간 조건이 잘 작동했지만 90일·전체 placement는 결국 전체 상세를 읽었습니다. 저장형 최초-state는 시간 key가 없어 짧은 기간도 먼저 많은 state를 읽었습니다.
- **min/argMin이 얼마나 부담인가?** scalar min 자체보다 tracking×stage group과 큰 tuple state가 핵심입니다. 10M 적재는 raw 대비 7.4배 느렸고 representative 압축 state만 222MiB였습니다.
- **tracking_state가 유리한가?** 최초 시각 자체가 필요할 때만 유리합니다. 기간 내 unique와 의미가 다르고, 정확한 snapshot 갱신은 10M에서 4.17초/1.28GiB와 spill을 요구했습니다. count만을 위해 항상 거칠 이유는 없습니다.
- **metric direct state가 유리한가?** 기간 unique의 의미를 그대로 유지하고 incremental MV로 생성할 수 있어 설계상 우선 후보입니다. 이번 10M에서는 raw보다 항상 빠르지 않았으며 write amplification과 merge가 늘었습니다.
- **1m→1h가 unique를 유지하는가?** 유지했습니다. 138개 경계 assertion과 본 exact/bitmap 비교에서 오차 0이었습니다. `MergeState` 후 최종 `Merge`가 필요하고 finalized minute count를 sum하면 틀립니다.
- **Combined64 오차는?** 본 group 최대 0.6432%, group 평균들의 평균 약 0.0467%였습니다. 허용 가능 여부는 상품 요구사항으로 정해야 합니다.
- **exact 대안은?** 일반 ID는 `uniqExactState`, dense collision-free UInt64 계약은 `groupBitmapState`입니다. 후자는 ID가 흩어지면 state가 크게 증가합니다.
- **월 10억/2년의 현실적 구조는?** Raw → stage Event → 1m/1h aggregate state를 우선 검증하되, 24개월 보존을 이 로컬 결과로 승인하면 안 됩니다. 정확성 요구가 엄격하면 Exact, 제한된 오차가 허용되면 Combined64, dense surrogate 계약이 있으면 Bitmap을 실제 분포에서 다시 비교합니다. 최초시각 state는 해당 기능이 있을 때만 병행합니다.

## 의미가 다른 안을 비교할 때

- `raw_unique`, `raw_window_argmin`, Case B: 요청 기간 안에 한 번이라도 해당 stage가 발생한 tracking 수.
- `argmin_first_received`: 전체 이력 중 **최초 수집** 대표 event의 발생 시각으로 기간 귀속.
- `tracking_first_event`, `tracking_snapshot`: 전체 이력 중 **최초 발생** 시각으로 기간 귀속.

따라서 Case A와 저장형 argMin의 원본 window unique 대비 차이는 정합성 오류로 세지 않습니다. Case A snapshot은 scalar state 결과와 별도로 비교합니다. 이전 날짜에 최초 발생하고 오늘 반복한 tracking은 오늘 unique에는 들어가지만 오늘 최초 발생 수에는 들어가지 않습니다.

`raw_window_argmin`은 옵티마이저가 tuple 계산을 없애지 않도록 대표 tuple을 문자열로 변환해 비어 있지 않음을 확인합니다. 이 추가 계산도 측정 시간에 포함되어 단순한 argMin 함수 비용만을 격리한 수치는 아닙니다. 저장형 argMin 실험은 실제 AggregateFunction state 저장/병합 비용을 별도로 측정합니다. tuple의 synthetic attributes 약 130바이트 역시 운영 데이터 폭을 실측한 값은 아닙니다.

## 운영 규모에 대한 판단

월 10억은 30일 평균 약 386 event/s지만, 로컬 batch INSERT rows/s와 비교해 운영 용량을 확정할 수 없습니다. 피크, 작은 batch, Kafka 소비, 동시 조회, shard skew, 복제, backfill, 장애 복구가 빠져 있습니다.

10M/90일은 운영 예상 3개월 약 30억의 **1/300 밀도**이고, 2년 240억의 약 **1/2400 행 수**입니다. 이 테스트는 90일 시간 필터를 검증했지만 운영의 90일 상태 cardinality나 24개월 디스크·메모리를 재현하지 않았습니다. 50M/100M/1B, 실제 2년 누적, 운영 동시성, 장애/복구 부하는 실행하지 않았습니다.

현실적인 다음 구조는 내구성 있는 Raw → placement/시간으로 정렬한 stage Event → 용도에 맞는 exact 또는 approximate 1m/1h state입니다. 월 파티션은 구현했지만 운영 TTL 정책과 2년 삭제 동작은 이번에 적용하지 않았습니다. 원본 보존 범위를 줄이면 임의 시간 경계의 상세 조회·재계산 가능 범위도 함께 줄어든다는 점을 계약에 반영해야 합니다.

프로덕션 결정 전에는 실제 ID/중복률/tuple 폭, 피크 batch 패턴, 24개월에 준하는 cardinality, 여러 shard와 3중 복제, 조회·적재 동시성, MV 실패 후 재처리, late event/TTL 경계, failover와 backlog 복구를 추가 검증해야 합니다. 용량에는 Raw+Event 중복 저장, 모든 state, 복제, merge 여유 공간, 백업을 함께 포함해야 합니다.
