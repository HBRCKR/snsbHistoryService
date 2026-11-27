/**
 * outbox-monitor — 아웃박스 상태를 relay 밖에서 재고, 알람을 Discord 로 보낸다.
 *
 * 한 함수, 두 가지 호출:
 * 1. EventBridge Scheduler(1분마다): DB 에서 history_outbox·history_outbox_dead 상태를 재고, relay 의 /actuator/health 를
 *    불러 응답하는 인스턴스 수를 세서 CloudWatch 사용자 지표로 올린다.
 *    - 아웃박스 상태는 relay 가 꺼지거나 여러 대여도 값이 하나다. 이력이 들어오는데 relay 가 멈추면 "가장 오래된 이력 나이"가 오른다.
 *    - 저장이 없는 시간(밤)에는 아웃박스가 비어 나이로는 relay 가 죽은 걸 모른다 — 그래서 응답 확인을 따로 한다.
 *      health 는 DB 연결까지 보므로 "떠 있지만 DB 에 못 붙는" 상태도 잡는다.
 * 2. SNS(CloudWatch 알람이 상태를 바꿀 때): 알람 내용을 Discord 웹훅으로 보낸다. 넘을 때 한 번, 회복될 때 한 번.
 *
 * 설정(환경 변수):
 * - SECRET_ID: Secrets Manager 비밀(JSON: DATABASE_URL, DISCORD_WEBHOOK_URL). 없으면 같은 이름의 환경 변수를 쓴다(로컬)
 * - RELAY_HEALTH_URLS: relay health 주소, 쉼표로 여러 개(예: http://10.0.1.23:8090/actuator/health). 비우면 응답 확인을 하지 않는다
 * - STAGE: 지표 차원(dev / prod)
 * - METRIC_NAMESPACE: 기본 SnsbHistory/Outbox
 * - DB_SSL: off | require(암호화, 인증서 검증 안 함 — 기본) | verify. DATABASE_URL 에는 sslmode 를 넣지 않는다(이 값을 덮어쓴다)
 * - DRY_RUN=true: CloudWatch·Discord 로 보내지 않고 로그로만 남긴다(로컬 확인용)
 */
import { CloudWatchClient, PutMetricDataCommand, type MetricDatum, StandardUnit } from '@aws-sdk/client-cloudwatch';
import { GetSecretValueCommand, SecretsManagerClient } from '@aws-sdk/client-secrets-manager';
import type { SNSEvent, ScheduledEvent } from 'aws-lambda';
import { Client, type ClientConfig } from 'pg';

enum MetricName {
  BACKLOG = 'Backlog',
  OLDEST_AGE_SECONDS = 'OldestAgeSeconds',
  DEAD_COUNT = 'DeadCount',
  DEAD_OLDEST_AGE_SECONDS = 'DeadOldestAgeSeconds',
  RELAY_HEALTHY_INSTANCES = 'RelayHealthyInstances',
}

enum DimensionName {
  STAGE = 'Stage',
  HISTORY_TYPE = 'HistoryType',
}

enum AlarmState {
  ALARM = 'ALARM',
  OK = 'OK',
  INSUFFICIENT_DATA = 'INSUFFICIENT_DATA',
}

enum DbSsl {
  OFF = 'off',
  REQUIRE = 'require',
  VERIFY = 'verify',
}

enum EnvName {
  SECRET_ID = 'SECRET_ID',
  STAGE = 'STAGE',
  METRIC_NAMESPACE = 'METRIC_NAMESPACE',
  DB_SSL = 'DB_SSL',
  DRY_RUN = 'DRY_RUN',
  DATABASE_URL = 'DATABASE_URL',
  DISCORD_WEBHOOK_URL = 'DISCORD_WEBHOOK_URL',
  RELAY_HEALTH_URLS = 'RELAY_HEALTH_URLS',
}

/** Spring Actuator health 의 상태 값 */
enum HealthStatus {
  UP = 'UP',
}

const DEFAULT_NAMESPACE = 'SnsbHistory/Outbox';
const DEFAULT_STAGE = 'dev';
const SNS_EVENT_SOURCE = 'aws:sns';
const APPLICATION_NAME = 'outbox-monitor';
const STATEMENT_TIMEOUT_MS = 10_000;
const CONNECT_TIMEOUT_MS = 5_000;
/** health 한 번의 시간 상한 — 1분 주기 안에서 여러 대를 불러도 넉넉하게 */
const HEALTH_TIMEOUT_MS = 3_000;
const URL_SEPARATOR = ',';
/** Discord 임베드 색 — 알람 빨강, 회복 초록, 데이터 없음 회색 */
const STATE_COLOR: Record<AlarmState, number> = {
  [AlarmState.ALARM]: 0xe24b4a,
  [AlarmState.OK]: 0x3b6d11,
  [AlarmState.INSUFFICIENT_DATA]: 0x888780,
};

/** 종류별 적체와 가장 오래 기다린 이력 나이(초) */
const BACKLOG_SQL = `
  SELECT history_type,
         count(*)::bigint AS backlog,
         coalesce(extract(epoch FROM clock_timestamp() - min(occurred_at)), 0)::bigint AS oldest_age_seconds
    FROM history_outbox
   GROUP BY history_type`;

/** dead 건수와 가장 오래 방치된 dead 나이(초) */
const DEAD_SQL = `
  SELECT count(*)::bigint AS dead_count,
         coalesce(extract(epoch FROM clock_timestamp() - min(dead_at)), 0)::bigint AS dead_oldest_age_seconds
    FROM history_outbox_dead`;

interface Secrets {
  databaseUrl: string;
  discordWebhookUrl: string;
}

interface BacklogRow {
  history_type: string;
  backlog: string;
  oldest_age_seconds: string;
}

interface HealthBody {
  status?: string;
}

interface DeadRow {
  dead_count: string;
  dead_oldest_age_seconds: string;
}

/** CloudWatch 알람이 SNS 로 보내는 메시지 중 쓰는 칸 */
interface AlarmMessage {
  AlarmName: string;
  AlarmDescription: string | null;
  NewStateValue: AlarmState;
  OldStateValue: AlarmState;
  NewStateReason: string;
  StateChangeTime: string;
  Trigger: {
    MetricName: string;
    Threshold: number;
    Dimensions: { name: string; value: string }[];
  };
}

const env = (name: EnvName, fallback = ''): string => process.env[name] ?? fallback;
const dryRun = (): boolean => env(EnvName.DRY_RUN) === 'true';
const stage = (): string => env(EnvName.STAGE, DEFAULT_STAGE);

let secretsPromise: Promise<Secrets> | undefined;

/** 비밀은 한 번만 읽어 둔다(같은 Lambda 실행 환경이 다시 쓰일 때 재사용) */
function secrets(): Promise<Secrets> {
  secretsPromise ??= loadSecrets();
  return secretsPromise;
}

async function loadSecrets(): Promise<Secrets> {
  const secretId = env(EnvName.SECRET_ID);
  if (secretId === '') {
    return { databaseUrl: env(EnvName.DATABASE_URL), discordWebhookUrl: env(EnvName.DISCORD_WEBHOOK_URL) };
  }
  const result = await new SecretsManagerClient({}).send(new GetSecretValueCommand({ SecretId: secretId }));
  const values = JSON.parse(result.SecretString ?? '{}') as Record<string, string>;
  return {
    databaseUrl: values[EnvName.DATABASE_URL] ?? '',
    discordWebhookUrl: values[EnvName.DISCORD_WEBHOOK_URL] ?? '',
  };
}

function sslConfig(): ClientConfig['ssl'] {
  switch (env(EnvName.DB_SSL, DbSsl.REQUIRE) as DbSsl) {
    case DbSsl.OFF:
      return false;
    case DbSsl.VERIFY:
      return { rejectUnauthorized: true };
    default:
      return { rejectUnauthorized: false };
  }
}

const datum = (name: MetricName, value: number, unit: StandardUnit, historyType?: string): MetricDatum => ({
  MetricName: name,
  Value: value,
  Unit: unit,
  Dimensions: [
    { Name: DimensionName.STAGE, Value: stage() },
    ...(historyType === undefined ? [] : [{ Name: DimensionName.HISTORY_TYPE, Value: historyType }]),
  ],
});

/** 1-1. 아웃박스 상태. 종류는 DB 값 그대로라 relay 가 모르는 종류도 잡힌다 */
async function outboxMetrics(): Promise<MetricDatum[]> {
  const { databaseUrl } = await secrets();
  const client = new Client({
    connectionString: databaseUrl,
    ssl: sslConfig(),
    application_name: APPLICATION_NAME,
    statement_timeout: STATEMENT_TIMEOUT_MS,
    connectionTimeoutMillis: CONNECT_TIMEOUT_MS,
  });
  await client.connect();
  try {
    const backlogRows = (await client.query<BacklogRow>(BACKLOG_SQL)).rows;
    const dead = (await client.query<DeadRow>(DEAD_SQL)).rows[0];
    const perType = backlogRows.flatMap((row) => [
      datum(MetricName.BACKLOG, Number(row.backlog), StandardUnit.Count, row.history_type),
      datum(MetricName.OLDEST_AGE_SECONDS, Number(row.oldest_age_seconds), StandardUnit.Seconds, row.history_type),
    ]);
    // 알람은 종류를 가리지 않은 전체 값에 건다 — 비었을 때도 0 을 올려 "데이터 없음"과 구분한다
    return [
      datum(MetricName.BACKLOG, backlogRows.reduce((sum, row) => sum + Number(row.backlog), 0), StandardUnit.Count),
      datum(MetricName.OLDEST_AGE_SECONDS, Math.max(0, ...backlogRows.map((row) => Number(row.oldest_age_seconds))), StandardUnit.Seconds),
      datum(MetricName.DEAD_COUNT, Number(dead.dead_count), StandardUnit.Count),
      datum(MetricName.DEAD_OLDEST_AGE_SECONDS, Number(dead.dead_oldest_age_seconds), StandardUnit.Seconds),
      ...perType,
    ];
  } finally {
    await client.end();
  }
}

/** health 하나 — 3초 안에 200 과 status UP 이면 살아 있다. 연결 실패·시간 초과·DOWN 은 모두 죽은 것으로 */
async function isHealthy(url: string): Promise<boolean> {
  try {
    const response = await fetch(url, { signal: AbortSignal.timeout(HEALTH_TIMEOUT_MS) });
    if (!response.ok) return false;
    return ((await response.json()) as HealthBody).status === HealthStatus.UP;
  } catch {
    return false;
  }
}

/** 1-2. relay 응답 확인 — 응답하는 인스턴스 수. 주소가 없으면 올리지 않는다(알람은 "데이터 없음"을 경보로 본다) */
async function relayHealthMetrics(): Promise<MetricDatum[]> {
  const urls = env(EnvName.RELAY_HEALTH_URLS)
    .split(URL_SEPARATOR)
    .map((url) => url.trim())
    .filter((url) => url !== '');
  if (urls.length === 0) return [];
  const results = await Promise.all(urls.map(async (url) => ({ url, healthy: await isHealthy(url) })));
  results.filter((r) => !r.healthy).forEach((r) => console.warn(JSON.stringify({ relayUnhealthy: r.url })));
  return [datum(MetricName.RELAY_HEALTHY_INSTANCES, results.filter((r) => r.healthy).length, StandardUnit.Count)];
}

/** 로그에 남길 원인 — 접속 거부처럼 메시지가 빈 오류(AggregateError)도 코드와 안쪽 오류까지 남긴다 */
function describe(reason: unknown): string {
  if (!(reason instanceof Error)) return String(reason);
  const code = (reason as Error & { code?: string }).code;
  const inner = reason instanceof AggregateError ? reason.errors.map(describe).join('; ') : '';
  return [reason.name, code, reason.message, inner].filter((part) => part !== undefined && part !== '').join(' ');
}

/** 1. 두 가지를 따로 재서 되는 것은 올린다 — DB 를 못 재도 relay 응답 값은 올라간다. 실패는 마지막에 던진다 */
async function measure(): Promise<void> {
  const [outbox, health] = await Promise.allSettled([outboxMetrics(), relayHealthMetrics()]);
  const metrics = [outbox, health].flatMap((result) => (result.status === 'fulfilled' ? result.value : []));

  console.log(JSON.stringify({ stage: stage(), metrics: metrics.map((m) => ({ name: m.MetricName, value: m.Value, dimensions: m.Dimensions })) }));
  if (!dryRun() && metrics.length > 0) {
    await new CloudWatchClient({}).send(
      new PutMetricDataCommand({ Namespace: env(EnvName.METRIC_NAMESPACE, DEFAULT_NAMESPACE), MetricData: metrics }),
    );
  }
  const failed = [outbox, health].filter((result): result is PromiseRejectedResult => result.status === 'rejected');
  failed.forEach((f) => console.error(JSON.stringify({ measureFailed: describe(f.reason) })));
  if (failed.length > 0) throw new AggregateError(failed.map((f) => f.reason), '지표 일부를 재지 못했다');
}

function discordPayload(alarm: AlarmMessage): object {
  const dimensions = alarm.Trigger.Dimensions.map((d) => `${d.name}=${d.value}`).join(', ');
  return {
    embeds: [
      {
        title: `[${alarm.NewStateValue}] ${alarm.AlarmName}`,
        description: [alarm.AlarmDescription ?? '', alarm.NewStateReason].filter((line) => line !== '').join('\n\n'),
        color: STATE_COLOR[alarm.NewStateValue] ?? STATE_COLOR[AlarmState.INSUFFICIENT_DATA],
        fields: [
          { name: '지표', value: `${alarm.Trigger.MetricName} (기준 ${alarm.Trigger.Threshold})`, inline: true },
          { name: '차원', value: dimensions === '' ? '-' : dimensions, inline: true },
          { name: '이전 상태', value: alarm.OldStateValue, inline: true },
        ],
        timestamp: alarm.StateChangeTime,
      },
    ],
  };
}

/** 2. CloudWatch 알람(SNS) → Discord. 실패하면 던져서 Lambda 비동기 재시도에 맡긴다 */
async function forwardAlarms(event: SNSEvent): Promise<void> {
  const { discordWebhookUrl } = await secrets();
  for (const record of event.Records) {
    const payload = discordPayload(JSON.parse(record.Sns.Message) as AlarmMessage);
    console.log(JSON.stringify({ discord: payload }));
    if (dryRun()) continue;
    const response = await fetch(discordWebhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!response.ok) throw new Error(`Discord 웹훅 실패: ${response.status} ${await response.text()}`);
  }
}

const isSnsEvent = (event: SNSEvent | ScheduledEvent): event is SNSEvent =>
  'Records' in event && event.Records[0]?.EventSource === SNS_EVENT_SOURCE;

export const handler = async (event: SNSEvent | ScheduledEvent): Promise<void> => {
  if (isSnsEvent(event)) await forwardAlarms(event);
  else await measure();
};
