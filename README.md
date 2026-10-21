# snsb_history

snsb3_api 의 변경 이력 아웃박스(`history_outbox`)를 S3 에 올리는 서비스 — PMWEB-TK-671. EC2 에 따로 띄운다.
설계: snsb3_api `docs/history-outbox-s3-plan.md`

```
snsb3_api ──(변경과 같은 트랜잭션)──▶ history_outbox ──(이 서비스: SKIP LOCKED 폴링)──▶ S3 (JSONL.gz) ──▶ Athena
```

## 처리

1. `poll-interval-ms`(10초)마다 확인해서, 종류(`history_type`)마다 outbox 에 `batch-size`(500) 건이 쌓였거나 가장 오래된 건이 `flush-interval-ms`(60초)를 기다렸으면
   `occurred_at, id` 순으로 `batch-size` 건을 `FOR UPDATE SKIP LOCKED` 로 잠근다. 여러 대를 띄워도 서로 다른 행을 가져간다.
   한가할 때 작은 파일이 쏟아지지 않게 모아서 올린다 — 이력이 S3 에 들어가기까지 최대 60초(+확인 주기)가 걸리고, 그동안은 outbox 에 안전하게 있다.
2. 한 줄 = 이력 한 건으로 묶어 gzip 파일로 S3 에 올린다(묶음이 시간 경계를 넘으면 시간 칸마다 파일 하나).
3. 성공하면 같은 트랜잭션에서 outbox 행을 지우고 커밋한다.

- **중복**: 2와 3 사이에 죽으면 다시 올라간다(최소 한 번 전달). 읽을 때 `event_id` 로 거른다.
- **dead 는 그 행 탓이 확실할 때만**: 형식이 깨진 행(필수 칸 없음)은 바로. 묶음 쓰기가 실패하면 한 건씩 나눠(한 건마다 `row-tries` 번) 쓰고, 실패한 행은 **그 뒤에 성공한 행이 있을 때만** `history_outbox_dead` 로(`last_error` 에 원인).
- **장애**: 뒤에 성공이 없는 실패(묶음 끝·전부 실패)는 판단을 미루고 아웃박스에 둔다. `outage-threshold` 건 연달아 실패하면 더 시도하지 않는다. 성공한 행만 커밋하고 `backoff-initial-ms` 부터 두 배씩(`backoff-max-ms` 까지) 간격을 늘려 계속 다시 시도한다 — 장애 중에는 어떤 행도 dead 로 가지 않는다. 403·404(권한·버킷 없음)는 설정 오류로 바로 ERROR·`history.s3.config.errors`.
- **크기는 실패가 아니다**: 묶음을 합계 `max-batch-bytes` 로도 자르고, 넘는 한 건은 혼자 파일 하나로.
- **순서**: 이중 쓰기(DB) → S3 업로드 → 아웃박스 삭제 — DB 쪽에서 실패하면 S3 에 아무것도 올라가지 않는다.
- **종료**: SIGTERM 을 받으면 새 묶음을 잡지 않고, 진행 중인 묶음의 업로드 → 삭제 → 커밋까지 마치고 내린다(`server.shutdown=graceful`, 스케줄러 종료 대기 30초). SIGKILL·장비 장애는 `event_id` 중복 제거로 대응한다.

## S3 경로와 형식

```
s3://{bucket}/{prefix/}{test_sub|examinee|staff}/dt=YYYY-MM-DD/hour=HH/{첫 outbox id}-{끝 outbox id}.jsonl.gz
```

- `dt`·`hour` 는 이력 시각(한국 시간). Athena partition projection 으로 읽는다.
- 줄 안의 시각도 모두 한국 시간(`+09:00`)이다 — DB 연결 시간대를 `Asia/Seoul` 로 고정한다(`connection-init-sql`).
- 백필 파일은 `h{첫 history_id}-h{끝 history_id}.jsonl.gz`.

한 줄:

```json
{"event_id":"…","schema_version":1,"history_type":"test_sub","change_type":"UPDATE","changed_at":"…","changed_by":"…","change_reason":null,
 "device_info":"…","app_version":"…","examinee_id":"hg…","test_id":"ht…","sub_id":123,"payload":{…스냅샷(rawData 포함)…}}
```

수검자 이력은 `test_id`·`sub_id`·`staff_id` 가 null. 직원 이력(STD·CMS, `payload.source`)은 `staff_id` 만 있고,
비밀번호 해시·주민번호·계좌번호는 값 없이 `password_changed`·`rrn_changed`·`bank_account_changed` 로만 남는다.
직원 이력은 예전 테이블이 없어 S3 에만 쓴다(이중 쓰기·백필 없음). 스냅샷 칸 이름은 예전 `test_sub_history`·`examinee_history` 와 같다.

## 기존 이력 테이블에서 전환 (설계 8절)

1. **백필**: `BACKFILL_ENABLED=true` 로 한 번 띄운다. `event_id` 가 없는(아웃박스 이전) 행을 같은 형식으로 올리고 끝나면 내린다.
   event_id 는 `md5('{테이블}:{history_id}')` UUID 라 다시 돌려도 같다.
2. **이중 쓰기**: 기본으로 켜져 있다(`RELAY_DUAL_WRITE_TABLE=true`). S3 와 함께 이력 테이블에도 넣는다(같은 트랜잭션, `event_id` 로 중복 방지). 이 기간 기준은 테이블.
3. 같은 기간의 건수·`event_id` 별 내용을 테이블과 Athena(중복 제거 뒤)로 비교한다.
4. 맞으면 `RELAY_DUAL_WRITE_TABLE=false`.

## dead 다시 보내기

원인을 고친 뒤:

```sql
BEGIN;
INSERT INTO history_outbox (event_id, history_type, occurred_at, payload)
SELECT event_id, history_type, occurred_at, payload FROM history_outbox_dead WHERE id = ANY('{…}');
DELETE FROM history_outbox_dead WHERE id = ANY('{…}');
COMMIT;
```

## 지표 (`/actuator/metrics`)

| 이름 | |
|---|---|
| `history.outbox.backlog{type}` | 아직 안 올라간 outbox 행 수 |
| `history.outbox.oldest.age.seconds{type}` | 가장 오래된 outbox 행이 기다린 시간 |
| `history.outbox.dead` | dead 행 수 |
| `history.s3.put.failures` | S3 업로드 실패 횟수 |
| `history.relayed{type}` | S3 에 올린 이력 수 |

## 실행

```bash
DB_URL=jdbc:postgresql://<host>:5432/<DB> DB_USERNAME=... DB_PASSWORD=... S3_BUCKET=<이력 버킷> ./gradlew bootRun
```

| 환경 변수 | 기본값 | |
|---|---|---|
| `DB_URL` / `DB_USERNAME` / `DB_PASSWORD` | — | snsb3_api 와 같은 DB |
| `S3_BUCKET` | — | 이력 전용 버킷 |
| `S3_PREFIX` | (비움) | 버킷 안 경로 앞부분 |
| `S3_REGION` | ap-northeast-2 | |
| `S3_ENDPOINT` | (비움) | 로컬 MinIO 면 `http://localhost:9000` |
| `RELAY_BATCH_SIZE` | 500 | 한 번에 잠가서 올리는 행 수. 이만큼 쌓이면 바로 올린다 |
| `RELAY_FLUSH_INTERVAL_MS` | 60000 | 가장 오래된 이력이 이만큼 기다리면 덜 차도 올린다 |
| `RELAY_POLL_INTERVAL_MS` | 10000 | 올릴 때가 됐는지 보는 주기 |
| `RELAY_MAX_BATCH_BYTES` | 16777216 | 묶음(파일) 합계 크기 상한 |
| `RELAY_ROW_TRIES` | 2 | 한 건씩 쓸 때 한 건마다 시도 횟수 |
| `RELAY_OUTAGE_THRESHOLD` | 3 | 연달아 이만큼 실패하면 장애로 본다 |
| `RELAY_BACKOFF_INITIAL_MS` / `RELAY_BACKOFF_MAX_MS` | 1000 / 60000 | 장애 때 재시도 간격 |
| `RELAY_DUAL_WRITE_TABLE` | true | 전환 기간 이력 테이블 이중 쓰기. 검증 뒤 false |
| `BACKFILL_ENABLED` / `BACKFILL_BATCH_SIZE` | false / 5000 | 기존 이력 테이블 백필 |
| `PORT` | 8090 | actuator |

자격 증명은 AWS 기본 체인 — EC2 는 인스턴스 역할(버킷 `s3:PutObject`), 로컬은 `AWS_ACCESS_KEY_ID`·`AWS_SECRET_ACCESS_KEY`.

### 로컬 (MinIO)

```bash
brew install minio
MINIO_ROOT_USER=localtest MINIO_ROOT_PASSWORD=localtest-secret minio server /tmp/minio-snsb3 --address :9000 --console-address :9001
curl --aws-sigv4 "aws:amz:us-east-1:s3" --user localtest:localtest-secret -X PUT http://localhost:9000/snsb-history-local
S3_BUCKET=snsb-history-local S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=localtest AWS_SECRET_ACCESS_KEY=localtest-secret \
DB_URL=... ./gradlew bootRun
```

## 아직 안 한 것 (인프라)

버킷 보안(SSE-KMS, Block Public Access, TLS 전용, 인스턴스 역할만 쓰기, 버전 관리·Object Lock, 90일 뒤 Glacier),
Athena 테이블(partition projection)과 `event_id` 중복 제거 뷰, CloudWatch 알림 — 설계 6·7절.

JDK 17, Spring Boot 3.4.4, Kotlin 1.9.25, AWS SDK v2.
