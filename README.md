# 변경 이력 저장 — 아웃박스 → 이력 서비스 → S3

PMWEB-TK-671. 자세한 설계·운영·부하 테스트 문서는 snsb3_api 저장소의 `docs/history-outbox-s3-plan.md` · `docs/history-service.md` · `docs/history-loadtest.md`.

## 구조

```
snsb3_api ──(변경과 같은 트랜잭션)──▶ history_outbox ──(snsb_history, EC2: 폴링)──▶ S3 (JSONL.gz)
```

- 앱은 변경과 **같은 트랜잭션**에서 이력을 `history_outbox` 에 넣는다 — 변경이 커밋되면 이력도 반드시 남고, 롤백되면 이력도 없다.
- 이 저장소의 이력 서비스 `snsb_history` 가 아웃박스를 읽어 S3 에 올리고 지운다.

## 폴링 정책

**10초마다 확인**해서, 이력 종류를 가리지 않고 들어온 순서(outbox id)대로

| 아웃박스에 쌓인 이력 | 동작 |
|---|---|
| 500건 이상 | 바로 올린다 |
| 500건 미만 | 가장 오래된 이력이 **60초 이상** 기다렸으면 올린다(아니면 다음 확인까지 대기) |

- 한 번에 **최대 500건 또는 합계 16MB** 를 `FOR UPDATE SKIP LOCKED` 로 잠가 처리하고, 묶음을 종류·시간 칸별 파일로 나눠 올린다. 한 줄로 가져오니 한 종류가 많이 쌓여도 다른 종류가 밀려 기다리지 않는다. 이 서비스가 모르는 종류는 가져가지 않고 아웃박스에 남긴다. 여러 인스턴스가 동시에 돌아도(롤링 배포 중 겹침, 이후 확장) 서로 다른 행을 가져가 중복 없이 나눠 처리한다.
- 이력이 S3 에 들어가기까지: 바쁠 때 최대 약 10초, 한가할 때 최대 약 70초. 그동안은 아웃박스에 안전하게 있다.

## S3 형식

```
s3://{bucket}/{prefix/}{test_sub|examinee|staff|test}/dt=YYYY-MM-DD/hour=HH/{첫 outbox id}-{끝 outbox id}.jsonl.gz
```

- 한 줄 = 이력 한 건(JSON). gzip 으로 압축한다.
- `dt`·`hour` 는 이력 시각(한국 시간) — Athena 에서 기간으로 좁혀 읽는다.

## 멱등성

- 이력마다 **`event_id`(UUID)** 를 붙인다. 아웃박스는 `event_id` 에 **UNIQUE** 를 걸어 같은 이력이 두 번 들어가지 않는다.
- S3 는 최소 한 번 전달이라 재시도로 같은 줄이 다시 올라갈 수 있다 — 읽을 때 `event_id` 로 중복을 거른다.

## 전환

1. 기존 DB 이력 테이블의 이력은 S3 로 백필한다(`BACKFILL_ENABLED=true` 로 한 번 실행).
2. 새 이력은 S3 에만 쓴다 — 기존 이력 테이블에는 더 쓰지 않는다(읽는 곳 없음).

## 실패 처리

**보낼 수 없는 행은 보내기 전에 걸러 내고, 업로드 실패는 묶음째 다시 올린다.**

| 상황 | 처리 |
|---|---|
| 형식이 깨진 행(필수 칸 없음) | 잠글 때 미리 검사해 묶음에 넣지 않고 바로 **DLT**(`history_outbox_dead`)로 — 다시 해도 같다 |
| 한 건이 묶음 상한(16MB)보다 큰 행 | 위와 같이 바로 DLT — relay 메모리 보호(실제 최대 rawData 는 수 MB) |
| 업로드 실패(S3·네트워크 장애, 시간 초과 30초) | 묶음 전체를 롤백해 아웃박스에 보존하고 **이력 종류별 지수 백오프(최대 60초)** 뒤 같은 묶음을 다시 올린다 — 장애 중에는 어떤 행도 DLT 로 가지 않고, 한 종류가 막혀도 다른 종류는 계속 올린다 |
| 권한·버킷 없음(403·404) | 위와 같이 다시 시도하되 설정 오류 지표를 따로 올린다 — 사람이 고쳐야 한다 |

DLT 이동은 업로드와 따로 커밋한다 — 업로드가 실패해도 보낼 수 없는 행은 치워진다. 한 건씩 나눠 다시 쓰지 않는다. S3 파일 이름은 시각·outbox id 로만 정해지고 업로드는 파일째 되거나 안 되므로, 검사를 통과한 행 하나 때문에 업로드가 실패하지 않는다. 그래도 계속 실패하면 앞에서 멈추고(순서 유지) 아래 감시의 가장 오래된 이력 나이 알람으로 드러난다.

## 감시 — outbox-monitor (Lambda)

아웃박스 상태는 **relay 밖에서** 잰다. relay 안에서 세면 relay 가 꺼질 때 지표도 같이 사라지고, 여러 대면 같은 값을 대수만큼 낸다.

| 무엇 | 어디서 | 지표 |
|---|---|---|
| 상태(적체, 가장 오래된 이력 나이, dead 수·나이) | `outbox-monitor` Lambda 가 1분마다 DB 를 직접 재서 CloudWatch(`SnsbHistory/Outbox`)로 | `Backlog`, `OldestAgeSeconds`, `DeadCount`, `DeadOldestAgeSeconds` (차원 `Stage`, 종류별은 `HistoryType` 추가) |
| relay 가 응답하나 | 같은 Lambda 가 1분마다 relay 의 `/actuator/health`(DB 연결 포함)를 불러 응답하는 인스턴스 수를 CloudWatch 로 | `RelayHealthyInstances` (주소는 `RELAY_HEALTH_URLS`, 쉼표로 여러 개) |
| 사건(올린 수, dead 로 옮긴 수, S3 실패) | relay 의 `/actuator/prometheus` | `history_relayed_total`, `history_dead_moved_total`, `history_s3_put_failures_total`, `history_s3_config_errors_total` — 여러 대면 `sum(rate(...))` |

- 종류는 DB 값 그대로라, relay 가 아직 모르는 종류가 쌓여도 잡힌다.
- 이력이 들어오는데 relay 가 멈추면 `OldestAgeSeconds` 가 오른다. 저장이 없는 시간(밤)에는 아웃박스가 비어 나이로는 모르므로 `RelayHealthyInstances` 가 1 아래로 떨어지는 것으로 잡는다. 감시 Lambda 자체가 멈춰 값이 안 오면 두 알람 모두 "데이터 없음"을 경보로 본다.
- CloudWatch 알람이 상태를 바꾸면 SNS 가 같은 Lambda 를 불러 Discord 웹훅으로 보낸다(넘을 때 한 번, 회복될 때 한 번).
- 빌드: `outbox-monitor/build.sh` → `dist/index.js`. 설정은 `outbox-monitor/src/index.ts` 맨 위 주석.

## S3 버킷 보안·비용

| 설정 | 하는 일 |
|---|---|
| Block Public Access | 정책·ACL 을 잘못 걸어도 버킷·파일이 공개되지 않게 막는다 |
| TLS 전용 | 암호화되지 않은 연결(HTTP)로 오는 요청을 거부한다 |
| IAM 역할만 쓰기 | 이력 서비스의 IAM 역할에만 `PutObject`(쓰기) 권한 — 접근 키 없이 운영 |
| Object Lock | 정해 둔 기간 동안 파일을 고치거나 지울 수 없게 잠근다(감사 로그 위·변조 방지) |
| 90일 뒤 Glacier | 오래된 이력을 더 싼 저장 등급으로 자동 이동(비용 절약) |

## 결과

이전 방식(커밋 후 이벤트 리스너 `@Async` 저장)에서 생기던 **DB 커넥션 타임아웃으로 인한 저장 요청 실패와 이력 유실이 사라졌다.**

| | 이전(이벤트 리스너) | 이후(아웃박스 + 저장 경로 경량화) |
|---|---|---|
| 이력 유실 | 커넥션 대기 초과 2건, 강제 종료 시 8,725건 중 8,724건 | **0건**(강제 종료·S3 중단·이력 서비스 kill -9 포함) |
| 저장 처리량(10·50·100·200명) | 12 · 14 · 15 · 20건/초 | 68 · 76 · 50 · 54건/초 |
| 저장 실패(200명, 30초) | 47건 | 3건 |

(로컬, 운영 JVM 조건 — 측정 방법과 전체 결과는 snsb3_api `docs/history-loadtest.md`)
