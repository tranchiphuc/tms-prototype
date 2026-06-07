# Alert Engine Streaming (Flink Job 3) — Hướng dẫn triển khai chi tiết & Đặc tả REST API

**Phiên bản:** v1.2
**Loại tài liệu:** Implementation Guide (diễn giải step-by-step từ HLD) + API Specification
**Tài liệu nguồn:** `03-alert_engine_streaming_job3_hld_vi_v1_4.md` (HLD v1.4 — tài liệu chuẩn về ý định thiết kế; nếu lệch, HLD thắng)
**Phạm vi:** (A) Hướng dẫn dev hiện thực Flink Job 3 theo từng bước, đúng thứ tự pha 1→3. (B) Đặc tả REST API của Spring Boot **Alert Rule Service** (thay phần API của tài liệu poll engine `03-alert_engine_hld_vi_v1_2` — poll engine **đã ngừng sử dụng**, tài liệu gốc **không còn trong bộ project** (tham chiếu lịch sử), nhưng API quản lý rule/preview/state vẫn cần và được đặc tả lại đầy đủ ở đây).

## Changelog

| Phiên bản | Thay đổi |
| --- | --- |
| v1.2 | **[CHANGED]** Đồng bộ với **HLD v1.4**: mục [GAP] về REFIRED cho `no_data` được thay bằng ngữ nghĩa **đã chốt** (mặc định không refire chu kỳ — FIRED một lần mỗi đợt im lặng; tuỳ chọn mở rộng đăng ký lại timer). Cập nhật tham chiếu tài liệu nguồn sang v1.4. |
| v1.1 | **[FIXED]** Map `DELETE /alerts/rules/{id}` đúng về **AL-12** (trước nhầm AL-08) và đồng bộ ngữ nghĩa `?force=true` theo AL-12 (vô hiệu composite phụ thuộc). **[FIXED]** Checkpoint mode đổi sang `EXACTLY_ONCE` (tính nhất quán **state nội bộ** — dedup/sustain; delivery ra sink vẫn at-least-once đúng HLD §8.4). **[CLARIFIED]** REFIRED cho `no_data` được đánh dấu là **khoảng trống của HLD §6.5** kèm 2 phương án — không trình bày như hành vi HLD đặc tả. **[CLARIFIED]** Tham chiếu Job 2 ghi rõ "có từ v2.5, tài liệu hiện hành v2.6"; ghi chú tài liệu poll engine không còn trong bộ project. |
| v1.0 | Phiên bản khởi tạo (diễn giải HLD v1.3 + đặc tả REST API). |

> **Cách dùng tài liệu:** Phần A đọc tuần tự — mỗi Bước có *mục tiêu, việc cần làm, pseudocode/code mẫu, cạm bẫy, tiêu chí hoàn thành*. Phần B độc lập, dành cho dev backend (Spring Boot) và frontend. Mọi mã enum, schema, ngữ nghĩa rule **giữ nguyên** HLD v1.3 — tài liệu này chỉ làm rõ, không phát minh mới.

---

## Mục lục

- [Phần 0 — Đọc nhanh: hệ thống làm gì](#phần-0--đọc-nhanh-hệ-thống-làm-gì)
- [Phần A — Triển khai Flink Job 3 step-by-step](#phần-a--triển-khai-flink-job-3-step-by-step)
  - [Bước 0 — Điều kiện tiên quyết](#bước-0--điều-kiện-tiên-quyết)
  - [Bước 1 — Khung job: source Kafka + watermark](#bước-1--khung-job-source-kafka--watermark)
  - [Bước 2 — Rule broadcast source + chỉ mục + gating](#bước-2--rule-broadcast-source--chỉ-mục--gating)
  - [Bước 3 — P1: FanOutFunction (lọc sớm + fan-out)](#bước-3--p1-fanoutfunction-lọc-sớm--fan-out)
  - [Bước 4 — P2: BasicEvaluator (threshold/pct/delta)](#bước-4--p2-basicevaluator-thresholdpctdelta)
  - [Bước 5 — EmitFunction + 3 sink](#bước-5--emitfunction--3-sink)
  - [Bước 6 — no_data (timer-driven) — Pha 2](#bước-6--no_data-timer-driven--pha-2)
  - [Bước 7 — P3: CompositeEvaluator — Pha 3](#bước-7--p3-compositeevaluator--pha-3)
  - [Bước 8 — Cold start, rule động, dọn orphan](#bước-8--cold-start-rule-động-dọn-orphan)
  - [Bước 9 — Kiểm thử, observability, Definition of Done](#bước-9--kiểm-thử-observability-definition-of-done)
- [Phần B — REST API: Alert Rule Service (Spring Boot)](#phần-b--rest-api-alert-rule-service-spring-boot)

---

## Phần 0 — Đọc nhanh: hệ thống làm gì

Job 3 là **Alert Engine duy nhất** của hệ thống (poll engine đọc ClickHouse đã bỏ). Nó consume metric đã chuẩn hoá từ Kafka (`processed_metrics` cho raw, `derived_metrics` cho derived), đối chiếu với **rule snapshot** broadcast từ MariaDB `tlm_alert_rules`, đánh giá ngay trên luồng, và phát quyết định **FIRED / REFIRED / RESOLVED** ra 3 đích: Kafka `alerts` (cho NOC PRO), ClickHouse `ipms.alert_history` (audit 90 ngày), MariaDB `tlm_alert_state` (mirror cho dashboard).

```
processed_metrics ─┐
                   ├─► P1 FanOut ─► keyBy(rule,dev,fp) ─► P2 BasicEval ─┬─► AlertDecision ─┐
derived_metrics  ──┘   (lọc sớm)                          (state+timer) │                  ├─► Emit ─► Kafka alerts
                            ▲                                ChildBool ─┘                  │         ─► CH alert_history
        tlm_alert_rules ────┴──────── broadcast ──────────────► keyBy(comp,dev,fp)         │         ─► MariaDB tlm_alert_state
        (MariaDB, status=1)                                   ─► P3 CompositeEval ─► AlertDecision ─┘
```

Năm khái niệm phải nắm trước khi code (chi tiết ở các bước tương ứng):

| Khái niệm | Một câu giải thích | Tham chiếu HLD |
| --- | --- | --- |
| `entity_fingerprint` | Chuỗi tất định sinh từ giá trị label theo `entity_keys` của rule (vd `"if_name=Gi0/0/0/0"`); `""` = device-level. Là một phần của key Flink và là khoá khớp giữa Flink state ↔ MariaDB ↔ ClickHouse | §3.3 |
| SKIP | "Thiếu dữ liệu để kết luận" (chưa có prev, prev=0, sai kiểu) — **không phải false**: không tăng/không reset sustain, không vào dedup gate, không resolve nhầm | §7.4 |
| sustain | Điều kiện phải đúng **liên tiếp** ≥ `sustain_samples` lần mới "satisfied"; áp ở cấp rule phát alert (basic độc lập / composite) | §6.3 |
| dedup | Máy trạng thái OK↔FIRING per `(rule, device, entity)`; suppress re-fire trong `dedup_seconds`. Nguồn sự thật là **Flink keyed state** (checkpoint), KHÔNG phải MariaDB | §6.4 |
| broadcast rule snapshot | Toàn bộ rule active phát tới mọi subtask; P1/P2/P3 đọc tham số rule từ đây tại runtime → rule động không cần restart job | §3.2, §9 |

Bảng mã enum dùng xuyên suốt (khớp HLD v1.3 + Phụ lục A):

| Trường | Mã |
| --- | --- |
| `rule_kind` | `0=basic`, `1=composite` |
| `condition_kind` | `0=threshold`, `1=pct_change_prev`, `2=no_data`, `3=abs_delta_prev` |
| `comparator` | `0= >`, `1= >=`, `2= <`, `3= <=`, `4= ==`, `5= !=` |
| `severity` | `0=info`, `1=warning`, `2=error`, `3=critical` (v1.3 — tăng dần theo mức nghiêm trọng) |
| `logical_op` | `0=AND`, `1=OR` |
| `missing_as` | `0=false`, `1=skip` |
| `status` | `1=Active`, `0=Disabled` |
| `event_type` | `"fired"`, `"refired"`, `"resolved"` |

---

# Phần A — Triển khai Flink Job 3 step-by-step

Thứ tự bước bám đúng lộ trình HLD §13: Bước 0–5 = **Pha 1** (basic số), Bước 6 = **Pha 2** (no_data), Bước 7 = **Pha 3** (composite), Bước 8–9 xuyên suốt.

---

## Bước 0 — Điều kiện tiên quyết

**Mục tiêu:** mọi thứ Job 3 phụ thuộc phải sẵn sàng trước khi viết dòng code Flink đầu tiên.

**Việc cần làm:**

1. **Tạo schema** theo **Phụ lục A của HLD v1.3** (DDL tham chiếu — đối chiếu lại tài liệu Alert Engine HLD gốc nếu/khi tồn tại):
   - MariaDB: `tlm_alert_rules` (gồm cột `emit_independent` mới của v1.3), `tlm_alert_state`.
   - ClickHouse: `ipms.alert_history` (MergeTree, partition theo ngày, TTL 90 ngày).
2. **Job 2 ≥ v2.5 phải emit topic Kafka `derived_metrics`** (HLD §2.3; đặc tả tại Job 2 HLD **§6.3** — tính năng có từ v2.5, tài liệu hiện hành là **v2.6** và §6.3 không đổi). Đây là **chặn cứng** cho alert trên derived metric: không còn poll nên Job 3 chỉ thấy derived qua Kafka. Alert trên raw metric không bị ảnh hưởng — có thể triển khai Pha 1 trên raw trước nếu Job 2 v2.5 chưa kịp.
   - Kiểm tra: message trên `derived_metrics` có cùng format `MetricEvent`, với `pathId=0`, `rawPath="derived"`, `metricName=output_metric`.
   - **Tuyệt đối không** ghi derived vào `processed_metrics` (Job 2 sẽ tự consume lại output của mình → vòng lặp).
3. **Hiện thực hàm `entity_fingerprint` chuẩn** dưới dạng util dùng chung (Job 3 + Alert Rule Service đều cần — service cần để hiển thị/đối chiếu state):

```java
public static String fingerprint(Map<String,String> labels, List<String> entityKeys) {
    if (entityKeys == null || entityKeys.isEmpty()) return "";          // device-level
    return entityKeys.stream()
        .sorted()                                                        // thứ tự từ điển — BẮT BUỘC
        .map(k -> k + "=" + labels.getOrDefault(k, ""))
        .collect(Collectors.joining(";"));                               // "cpu_id=0;if_name=Gi0/0/0/0"
}
```

   - Viết **unit test golden vectors** và đóng băng: `([], any) → ""`; `(["if_name"], {if_name=Gi0/0/0/0}) → "if_name=Gi0/0/0/0"`; `(["if_name","cpu_id"], ...) → "cpu_id=0;if_name=Gi0/0/0/0"` (sort!); key thiếu trong labels → `"key="` (chuỗi rỗng, không bỏ key).
   - Mọi nơi sinh fingerprint (P1, mirror MariaDB, audit ClickHouse, API hiển thị) **chỉ gọi hàm này**. Lệch một ký tự là dashboard/audit không khớp state.
4. **Kafka topics:** xác nhận `processed_metrics`, `derived_metrics`, `alerts` tồn tại. Khuyến nghị partition `processed_metrics`/`derived_metrics` theo `device_id` (cân tải, không bắt buộc cho tính đúng).
5. **Hạ tầng Flink:** cluster có RocksDB state backend, đường ghi checkpoint (HDFS/S3/NFS), kết nối được MariaDB + ClickHouse + Kafka.

**Tiêu chí hoàn thành Bước 0:** DDL chạy được; insert được một rule mẫu vào `tlm_alert_rules`; consume thử được message từ `processed_metrics` và parse ra `MetricEvent`; golden test fingerprint xanh.

---

## Bước 1 — Khung job: source Kafka + watermark

**Mục tiêu:** dựng skeleton job đọc được 2 topic, gán event-time watermark, cấu hình checkpoint/state backend đúng NFR.

**1.1 Cấu hình môi trường** (HLD §11):

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStateBackend(new EmbeddedRocksDBStateBackend(true));   // incremental checkpoint
env.enableCheckpointing(15_000, CheckpointingMode.EXACTLY_ONCE);
// EXACTLY_ONCE ở đây là tính nhất quán STATE NỘI BỘ (dedup/sustain không nhân đôi khi recover) — chi phí thấp, nên giữ.
// Delivery RA SINK vẫn là at-least-once + NOC PRO idempotent (HLD §8.4) — hai khái niệm độc lập.
env.getCheckpointConfig().setCheckpointStorage("hdfs:///flink/checkpoints/job3");
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
env.getCheckpointConfig().enableUnalignedCheckpoints();        // bật nếu gặp backpressure kéo dài
```

**1.2 Source 2 topic, 1 KafkaSource** (cùng format `MetricEvent` — HLD §3.1, POJO giống hệt Job 2 §4.2, không định nghĩa lại khác đi):

```java
KafkaSource<MetricEvent> source = KafkaSource.<MetricEvent>builder()
    .setBootstrapServers(brokers)
    .setTopics("processed_metrics", "derived_metrics")        // derived_metrics: cần Job 2 v2.5 (Bước 0.2)
    .setGroupId("flink-job3-alert-evaluator")
    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
    .setValueOnlyDeserializer(new MetricEventJsonDeserializer())
    .build();
```

> Deserializer: message hỏng (JSON sai) → **đếm metric `alert_skip_total{reason="bad_json"}` và bỏ qua**, không fail job.

**1.3 Watermark** (HLD §5.3):

```java
WatermarkStrategy<MetricEvent> wm = WatermarkStrategy
    .<MetricEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))   // 1–2s: cân giữa đảo thứ tự và độ trễ
    .withTimestampAssigner((e, ts) -> e.eventTimeMs)
    .withIdleness(Duration.ofSeconds(30));                          // partition im lặng không giữ watermark toàn cục

DataStream<MetricEvent> metrics = env.fromSource(source, wm, "metric-source");
```

**Cạm bẫy:**
- Quên `withIdleness` → một partition vắng dữ liệu kéo watermark toàn job đứng yên. (no_data không bị ảnh hưởng vì dùng processing-time timer, nhưng các phép so event-time vẫn cần watermark tiến.)
- Tăng `boundedOutOfOrderness` lên hàng chục giây "cho an toàn" → ăn thẳng vào ngân sách SLO 5s. Giữ 1–2s; xử lý late theo quy tắc Bước 4.
- POJO `MetricEvent` phải có TypeInformation rõ (getter/setter chuẩn POJO hoặc đăng ký serializer) — tránh Kryo fallback làm chậm shuffle.

**Tiêu chí hoàn thành:** job chạy, in/đếm được event từ cả 2 topic, checkpoint thành công định kỳ.

---

## Bước 2 — Rule broadcast source + chỉ mục + gating

**Mục tiêu:** mọi operator luôn có **rule snapshot mới nhất** mà không cần restart job; event tới trước khi snapshot đầu tiên load xong không bị đánh giá sai.

**2.1 Chọn nguồn snapshot** (HLD §9 — chọn 1):
- **Cách 2 (khuyến nghị bắt đầu): polling MariaDB** trong một source phụ — đơn giản, độ trễ rule = chu kỳ poll (≤ vài chục giây, khớp NFR "rule refresh ≤ vài chục giây").
- Cách 1: relay `tlm_alert_rules` → Kafka topic rồi consume (gần realtime). Nâng cấp sau nếu cần, không chặn Pha 1.

**2.2 Source poll** (SourceFunction/Source song song = 1):

```
mỗi refreshIntervalSeconds (vd 20s):
    rows = SELECT * FROM tlm_alert_rules WHERE status = 1
    snapshot = buildSnapshot(rows)        // 2.3
    ctx.collect(snapshot)                 // phát NGUYÊN BỘ snapshot (atomic-ish — HLD §9)
```

> Phát **nguyên bộ** snapshot mỗi lần (không phát delta từng rule): đơn giản hoá hợp nhất, tự xử lý cả rule bị disable (biến khỏi snapshot). Với 500–2.000 rule, payload nhỏ, không thành vấn đề.

**2.3 Dựng chỉ mục phụ trong snapshot** (HLD §3.2):

```java
class RuleSnapshot {
    long version;                                        // max(updated_at) — để log/quan sát
    Map<Integer, AlertRule> ruleById;                    // mọi rule active (basic + composite)
    Map<String, List<AlertRule>> byMetric;               // aliasMetric -> basic rules (cho P1 fan-out)
    Map<Integer, List<Integer>> childToComposites;       // basicId -> [compositeId] (cho P2 route ChildBool)
    Map<Integer, AlertRule> compositeById;               // cho P3
}
```

**2.4 Broadcast & gating load lần đầu** (HLD §9):

```java
MapStateDescriptor<Void, RuleSnapshot> RULE_STATE =
    new MapStateDescriptor<>("rules", Types.VOID, TypeInformation.of(RuleSnapshot.class));
BroadcastStream<RuleSnapshot> ruleBroadcast = ruleSource.broadcast(RULE_STATE);
```

- Trong `processBroadcastElement` của P1/P2/P3: thay nguyên bộ snapshot trong broadcast state.
- **Gating:** trước khi snapshot đầu tiên tới, P1 chưa có rule → `byMetric` rỗng → event tự động bị bỏ (no-op). Để không bỏ sót vài giây đầu, áp khuyến nghị (a) của HLD §9: P1 **buffer ngắn** event (list state nhỏ + giới hạn, vd vài nghìn event hoặc 10s) cho tới khi nhận snapshot đầu tiên rồi xả buffer; hoặc đơn giản nhất ở Pha 1: chấp nhận bỏ vài giây đầu (rủi ro thấp vì sample 60s) và nâng cấp buffer ở Pha 2. Ghi quyết định vào code comment.

**Cạm bẫy:**
- `KeyedProcessFunction` **không đọc được broadcast state** — P2/P3 bắt buộc là `KeyedBroadcastProcessFunction` (lý do tồn tại của [FIXED] v1.3, HLD §5.2). Đừng "tối ưu" bỏ `.connect(ruleBroadcastStream)`.
- Broadcast state per-subtask đều nhận bản sao đầy đủ — đừng nhét dữ liệu lớn ngoài rule vào snapshot.
- `updated_at` là version đơn điệu; log `snapshot.version` mỗi lần refresh để debug "rule đã sống chưa" (đối chiếu API `refresh-status` — Phần B.11).

**Tiêu chí hoàn thành:** sửa một rule trong MariaDB (`UPDATE ... SET threshold=...`) → trong ≤ chu kỳ poll, log của job in ra version snapshot mới; disable rule → rule biến khỏi snapshot.

---

## Bước 3 — P1: FanOutFunction (lọc sớm + fan-out)

**Mục tiêu:** từ mỗi `MetricEvent`, phát đúng một `RuleInstanceEvent` cho **mỗi basic rule khớp**; event không khớp rule nào **chết tại đây** (không bao giờ vào shuffle). Đây là hàng rào throughput của cả job: ~833K event/s vào, chỉ phần có rule đi tiếp (HLD §5.2, §11).

**Toán tử:** `BroadcastProcessFunction<MetricEvent, RuleSnapshot, RuleInstanceEvent>` — **không keyed**, đứng trước `keyBy`.

**3.1 Logic `processElement`** (HLD §7.1):

```
processElement(MetricEvent e, ctx, out):
    snap = readBroadcast(RULE_STATE);  if snap == null: (gating Bước 2.4) return/buffer
    rules = snap.byMetric.get(e.metricName)            // null/rỗng → bỏ event, KHÔNG phát gì
    if rules == null: return
    for r in rules:
        if !pathScopeMatch(r.pathAliasId, e.pathId):        continue
        if r.scopeDeviceIds != null && !r.scopeDeviceIds.contains(e.deviceId): continue
        if !entityFilterMatch(r.entityFilter, e.labels):    continue
        fp = fingerprint(e.labels, r.entityKeys)            // util Bước 0.3
        out.collect(new RuleInstanceEvent(r.id, e.deviceId, e.deviceName, fp,
                    subset(e.labels, r.entityKeys),         // entityMap — để ghi field "entity" output
                    e.metricName, e.valueType, e.valueNumber, e.eventTimeMs));
```

**3.2 Ba hàm match thuần (không state):**

| Hàm | Quy tắc |
| --- | --- |
| `pathScopeMatch(pathAliasId, pathId)` | `pathAliasId == null` → match mọi path; ngược lại `pathAliasId == pathId`. Lưu ý derived metric có `pathId = 0` → rule trên derived đặt `path_alias_id = 0` |
| `scopeDeviceIds` | `null` → mọi thiết bị; ngược lại membership. Convert sang `HashSet<Integer>` khi build snapshot, đừng `List.contains` trong hot path |
| `entityFilterMatch(filter, labels)` | `filter == null` → match-all. Ngược lại với **mỗi** key trong filter: `labels[key]` phải ∈ danh sách giá trị (exact / IN-list, giá trị đã chuẩn hoá). Key của filter ⊆ `entityKeys` (API đã validate — Phần B.4). `entity_filter` chỉ chọn instance, **không** đổi fingerprint |

**Cạm bẫy:**
- **Đừng** thêm operator "pre-filter" riêng trước P1 — lọc sớm là **hành vi mặc định** của P1 ([CLARIFIED] v1.2). Chỉ khi profiling cho thấy P1 nghẽn mới chèn filter siêu nhẹ tra tập `metricName` có rule (chained sau source) — và nó không thêm lợi ích về shuffle.
- Một event có thể khớp **nhiều rule với `entityKeys` khác nhau** → nhiều `RuleInstanceEvent` với fingerprint khác nhau. Đây là chủ đích (vì sao không keyBy ngay ở source — HLD §5.2).
- `subset(labels, entityKeys)`: chỉ copy các key trong `entityKeys` — đừng gửi nguyên map labels (giảm size shuffle).

**Tiêu chí hoàn thành:** test với 1 rule `if_in_errors`, bơm 3 event (`if_in_errors` đúng device, `if_in_errors` sai device-scope, `cpu_util`) → đúng 1 `RuleInstanceEvent` phát ra, fingerprint đúng golden vector.

---

## Bước 4 — P2: BasicEvaluator (threshold/pct/delta)

**Mục tiêu:** trái tim của Pha 1. Mỗi key `(rule_id, device_id, entity_fingerprint)` là một máy trạng thái độc lập: prev-sample → `evalRaw` → sustain → dedup → `AlertDecision`.

**Toán tử:**

```java
fanned.keyBy(ev -> Tuple3.of(ev.ruleId, ev.deviceId, ev.entityFingerprint))
      .connect(ruleBroadcast)
      .process(new BasicEvaluator())   // KeyedBroadcastProcessFunction — BẮT BUỘC (đọc broadcast sau keyBy)
```

**4.1 Khai báo state** (HLD §6.1 — tất cả keyed, bền qua checkpoint):

| State | Kiểu | TTL |
| --- | --- | --- |
| `prevSample` | `ValueState<PrevSample{eventTimeMs, value}>` | `StateTtlConfig` ~180s (≥3× sample interval) |
| `consecHits` | `ValueState<Integer>` | như prevSample |
| `dedup` | `ValueState<DedupState{lastState, lastFiredAtMs, lastResolvedAtMs}>` | **KHÔNG TTL ngắn** (xoá nhầm → re-fire); dọn theo orphan Bước 8 |
| `lastSeen` + `noDataTimerTs` | `ValueState<Long>` ×2 | Bước 6 (Pha 2) |

**4.2 Thứ tự xử lý trong `processElement`** (HLD §7.2 — giữ đúng thứ tự):

```
processElement(RuleInstanceEvent ev, broadcastCtx, out):
    r = broadcast.ruleById.get(ev.ruleId)
    if r == null:                                    // rule vừa bị disable → orphan (Bước 8.3)
        if dedup.lastState == FIRING: emit RESOLVED; clearAllState()
        return

    // ----- Pha 2: nhánh no_data xử lý TRƯỚC, rồi return (Bước 6) -----
    if r.conditionKind == 2: { ...Bước 6...; return; }

    // ----- Pha 1: điều kiện số -----
    raw = evalRaw(r, ev, prevSample)                 // TRUE | FALSE | SKIP — bảng 4.3
    updatePrevSample(ev)                             // quy tắc 4.4 — SAU evalRaw

    if isChild(r.id):                                // Pha 3: đẩy raw (CHƯA sustain) lên composite
        for cId in broadcast.childToComposites.get(r.id):
            ctx.output(CHILD_TAG, new ChildBool(cId, r.id, ev.deviceId, ev.entityFingerprint, raw==TRUE, ev.eventTimeMs));
        // raw == SKIP: KHÔNG đẩy ChildBool (composite xử lý "thiếu data" bằng missing_as/staleness)

    if isIndependentFiring(r):                       // mặc định true (emit_independent=1) — kể cả khi là child
        satisfied = sustainGate(raw, consecHits, r.sustainSamples)        // 4.5
        decision  = dedupGate(r, satisfied, dedup, ev.eventTimeMs)        // 4.6
        if decision != NONE: out.collect(buildAlertDecision(decision, r, ev, ...));
```

> **Một rule vừa độc lập vừa là child:** tính `raw` **một lần**; nhánh child dùng `raw` thô, nhánh độc lập mới qua `sustainGate`. `emit_independent=0` → bỏ nhánh độc lập, chỉ làm child ([CLARIFIED] v1.3 §7.2).

**4.3 `evalRaw` per `condition_kind`** (HLD §7.4 — chỉ áp khi `valueType == 1`; `valueType ∈ {2,3}` → **bỏ qua hoàn toàn** cho điều kiện số, không SKIP-resolve, không cập nhật prev... đơn giản là "không phải đối tượng của rule số này"):

| `conditionKind` | raw | SKIP khi |
| --- | --- | --- |
| `0` threshold | `cmp(cur, threshold)` | `valueType != 1` (bỏ qua) |
| `1` pct_change_prev | `p = (cur−prev)/prev*100`; nếu `pctAbs=1` → `\|p\|`; `raw = cmp(p, threshold)` | `prev == null` **hoặc** `prev == 0` |
| `3` abs_delta_prev | `d = cur−prev`; nếu `pctAbs=1` → `\|d\|`; `raw = cmp(d, threshold)` | `prev == null` |

`cmp(a,b)` theo `comparator`: `0:> 1:>= 2:< 3:<= 4:== 5:!=` (với `==`/`!=` trên double: so sánh trực tiếp theo HLD; nếu cần epsilon, phải thống nhất với preview API — mặc định không epsilon).

**4.4 Quy tắc prev-sample & late event** (HLD §5.3 — chống tính sai khi đảo thứ tự):

```
if ev.eventTimeMs >  prevSample.eventTimeMs:  prev = prevSample.value (dùng cho pct/delta), rồi prevSample = (ev.eventTimeMs, cur)
if ev.eventTimeMs <= prevSample.eventTimeMs:  // late / đảo thứ tự
    - pct/delta: SKIP, KHÔNG cập nhật prevSample
    - threshold: vẫn eval bình thường trên giá trị của chính event (không cần prev)
```

**4.5 `sustainGate`** (HLD §6.3 — checkpoint nên bền qua restart, cải thiện so với poll engine giữ RAM):

```
sustainGate(raw, consecState, n):
    if raw == SKIP: return KẾT_QUẢ_TRƯỚC_GIỮ_NGUYÊN   // không +1, không reset, không quyết định mới → dedupGate nhận SKIP
    consec = raw ? consecState + 1 : 0
    consecState = consec
    return consec >= n
```

Hiện thực gọn: cho `sustainGate` trả `TRUE | FALSE | SKIP` (SKIP truyền xuyên suốt xuống dedup).

**4.6 `dedupGate` — máy trạng thái fire/refire/resolve** (HLD §6.4 — học thuộc bảng này, viết test phủ đủ 6 dòng):

```
dedupGate(rule, satisfied, st, now):
    if satisfied == SKIP: return NONE
    if satisfied == TRUE:
        if st.lastState == OK:                       st={FIRING, lastFiredAt=now}; return FIRED
        if (now - st.lastFiredAt) >= rule.dedupSeconds*1000: st.lastFiredAt=now;   return REFIRED
        return NONE                                  // suppress trong cửa sổ dedup
    else: // FALSE
        if st.lastState == FIRING:                   st={OK, lastResolvedAt=now};  return RESOLVED
        return NONE
```

| lastState | satisfied | Trong cửa sổ dedup? | Kết quả |
| --- | --- | --- | --- |
| OK | true | — | **FIRED** |
| FIRING | true | có | suppress (đếm `alert_suppressed_total`) |
| FIRING | true | không | **REFIRED** |
| FIRING | false | — | **RESOLVED** |
| OK | false | — | no-op |
| * | SKIP | — | no-op |

`now` = `ev.eventTimeMs` cho đường event; processing time cho timer no_data (Bước 6).

**4.7 Dựng `AlertDecision`** (HLD §3.4): mang **đủ trường** (rule meta, entity map, observed/threshold, detailJson, `stateChanged`, lastState, lastFiredAt/lastResolvedAt) để EmitFunction **không phải đọc lại state**. `detailJson` = `{"prev_value": ...}` cho pct/delta; `observedValue` = cur (threshold) | pct (pct_change) | delta (abs_delta).

**Kiểm tra chéo bằng ví dụ truy vết HLD §7.5:** chạy unit test tái hiện đúng bảng VD1 (threshold + sustain=2 + dedup=1800: FIRED tại t=120s, suppress t=180s, RESOLVED t=240s) và VD2 (pct: SKIP sample đầu, SKIP khi prev=0). Hai bảng này là acceptance test của Bước 4.

**Tiêu chí hoàn thành:** VD1/VD2 pass; restart job từ checkpoint giữa chừng VD1 → không re-fire, không mất consecHits.

---

## Bước 5 — EmitFunction + 3 sink

**Mục tiêu:** một quyết định → 3 đích, **history-first**, không chặn alert-path, downstream khử trùng được.

**Toán tử:** union `AlertDecision` từ P2 (+P3 sau này) → `ProcessFunction` + 3 sink (HLD §8.5).

```
processElement(AlertDecision d):
    1. ClickHouse ipms.alert_history   — sink batch nhỏ (flush 1–2s) — "nguồn sự thật audit", ghi TRƯỚC
    2. Kafka alerts                    — key = (rule_id, device_id, entity_fingerprint) → thứ tự per-instance
                                         value = AlertEvent JSON (5.1)
    3. if d.stateChanged: side output StateChange → MariaDB UPSERT tlm_alert_state
```

**5.1 `AlertEvent` JSON → Kafka `alerts`** (HLD §8.1 — **không đổi so với hợp đồng cũ**, NOC PRO không phải sửa):

```json
{
  "event_type": "fired",
  "event_time": "2026-06-03T10:15:30.123+07:00",
  "rule_id": 101, "rule_name": "Interface in-errors cao", "rule_kind": 0, "severity": 2,
  "device_id": 42, "device_name": "PE-HN-01",
  "entity": { "if_name": "GigabitEthernet0/0/0/0" },
  "metric_name": "if_in_errors", "condition_kind": 0,
  "observed_value": 250, "threshold": 100,
  "detail": { "prev_value": 30 }
}
```

- Composite: `metric_name=""`, `condition_kind=null`, `detail.children=[{child_rule_id, raw}, ...]`.
- no_data: `observed_value` = tuổi im lặng (giây), `threshold=null`.
- `event_time` = `decisionTimeMs` format ISO-8601 `+07:00`.
- **Idempotency key downstream:** `(rule_id, device_id, entity_fingerprint, event_type, event_time)` — at-least-once cho phép trùng khi retry/replay, NOC PRO khử theo khoá này.

**5.2 MariaDB upsert mirror** (HLD §8.3 — idempotent theo PK):

```sql
INSERT INTO tlm_alert_state (rule_id, device_id, entity_fingerprint, last_state, last_fired_at, last_resolved_at)
VALUES (?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE last_state=VALUES(last_state),
  last_fired_at=COALESCE(VALUES(last_fired_at), last_fired_at),
  last_resolved_at=COALESCE(VALUES(last_resolved_at), last_resolved_at);
```

**Bất biến & xử lý lỗi** (HLD §8.4, §10):
- Hai sink ClickHouse/Kafka **độc lập** — một bên lỗi kéo dài không kéo bên kia chết; không atomic giữa hai (chấp nhận theo history-first + idempotent).
- Lỗi tạm thời → retry/backoff trong sink, **không** ném exception fail job; backpressure tự làm chậm source (đúng và chấp nhận).
- MariaDB mirror fail → không chặn alert-path, retry; dashboard trễ là chấp nhận được.
- Delivery mặc định **at-least-once**. Exactly-once outbound (Kafka transactional theo checkpoint) là tuỳ chọn — tăng độ trễ commit, cân với SLO 5s trước khi bật.

**Tiêu chí hoàn thành:** bơm chuỗi VD1 end-to-end → thấy 2 message trên `alerts` (FIRED, RESOLVED) đúng key/payload, 2 row `alert_history`, `tlm_alert_state` đúng trạng thái cuối (`last_state=0`, có cả `last_fired_at` lẫn `last_resolved_at`).

---

## Bước 6 — no_data (timer-driven) — Pha 2

**Mục tiêu:** fire khi series **từng có** dữ liệu rồi im lặng quá `no_data_seconds`; tự RESOLVED khi dữ liệu quay lại; không báo nhầm series mới onboard.

Stream không có "event vắng mặt" → dùng **processing-time timer** trên key `(rule, dev, fp)` trong chính P2 (HLD §6.5; chọn processing-time vì event-time timer chết khi cả partition im lặng).

**6.1 Nhánh `conditionKind == 2` trong `processElement`** (chạy trước nhánh số, rồi return — Bước 4.2):

```
1. nếu noDataTimerTs != null: deleteProcessingTimeTimer(noDataTimerTs)
2. lastSeen = ctx.currentProcessingTime()
   noDataTimerTs = lastSeen + r.noDataSeconds*1000
   registerProcessingTimeTimer(noDataTimerTs)                    // mỗi sample "đẩy lùi" thời điểm fire
3. nếu isChild(r.id): đẩy ChildBool(raw=false) lên MỌI composite cha     // [FIXED] v1.3 — data đã về ⇒ no_data=FALSE
4. nếu isIndependentFiring(r) && dedup.lastState == FIRING:
       emit RESOLVED (now = processing time); dedup = OK         // series báo lại sau khi đã FIRING
```

**6.2 `onTimer`:**

```
1. phân biệt timer: nếu ts == noDataTimerTs (timer no_data):
       satisfied = TRUE; observedValue = (now - lastSeen)/1000   // tuổi im lặng, giây
       decision = dedupGate(r, TRUE, dedup, now)
       nếu independent && decision != NONE: emit (FIRED/REFIRED)
       nếu isChild: đẩy ChildBool(raw=true) lên composite
       đăng ký AGING timer tại now + agingTtl (đề xuất ≥ 3–5× noDataSeconds), lưu agingTimerTs
2. nếu ts == agingTimerTs (timer aging): series vẫn câm sau thời gian dài
       nếu dedup.lastState == FIRING: emit RESOLVED
       clear toàn bộ state của key ("quên" series — tránh state rò rỉ vô hạn)
```

**Bất biến phải giữ (test bắt buộc):**
- Series **chưa từng có event** → chưa từng có timer → **không bao giờ** fire no_data (chống báo nhầm khi onboard thiết bị mới).
- `no_data` áp cho **mọi** `valueType` (chỉ quan tâm sự hiện diện sample).
- **REFIRED cho no_data (đã chốt — HLD v1.4 §6.5):** mặc định `onTimer` **không** đăng ký lại timer no_data (chỉ aging timer) ⇒ mỗi đợt im lặng sinh **đúng một** FIRED, sau đó resolve-on-return hoặc aging-RESOLVED — `REFIRED` không xảy ra cho no_data. *Tuỳ chọn mở rộng* (chỉ bật có chủ đích, ghi vào cấu hình triển khai): đăng ký lại timer tại `now + noDataSeconds*1000` trong `onTimer` để `dedupGate` sinh REFIRED theo `dedup_seconds`. Test mặc định phải khẳng định KHÔNG có REFIRED cho no_data.
- Restart job: processing-time timer khôi phục từ checkpoint, timer quá hạn **fire ngay** → tệ nhất một lần kiểm tra no_data sớm (chấp nhận — HLD §6.5).

**Acceptance:** tái hiện đúng bảng VD3 HLD §7.5 (sample 0s, 60s → timer fire 240s FIRED observed=180 → sample 300s RESOLVED + timer mới 480s).

---

## Bước 7 — P3: CompositeEvaluator — Pha 3

**Mục tiêu:** gộp boolean tức thời của các child (cùng `entityKeys` — API đảm bảo) bằng AND/OR; sustain + dedup ở **cấp composite**.

**Toán tử:**

```java
childBoolStream                                  // side output CHILD_TAG từ P2
  .keyBy(cb -> Tuple3.of(cb.compositeId, cb.deviceId, cb.entityFingerprint))
  .connect(ruleBroadcast)
  .process(new CompositeEvaluator())             // KeyedBroadcastProcessFunction
```

**7.1 State:** `childBools: MapState<Integer childRuleId, ChildEntry{raw, updatedAtMs}>`, `consecHits`, `dedup` (TTL như Bước 4.1; dedup không TTL ngắn).

**7.2 `processElement(ChildBool cb)`** (HLD §7.3):

```
comp = broadcast.compositeById.get(cb.compositeId)
if comp == null: orphan như Bước 8.3; return
childBools.put(cb.childRuleId, {cb.raw, now})
combined = combine(comp, childBools, now)
if combined == SKIP: return
satisfied = sustainGate(combined, consecHits, comp.sustainSamples)
decision  = dedupGate(comp, satisfied, dedup, cb.eventTimeMs)
if decision != NONE: out.collect(AlertDecision{... metricName="", conditionKind=null,
                                  detailJson={"children":[{child_rule_id, raw}...]} ...})
```

**7.3 `combine` — hợp nhất child với staleness & missing_as:**

```
combine(comp, childBools, now):
    results = []
    for childId in comp.childRuleIds:
        e = childBools.get(childId)
        hasData = (e != null && now - e.updatedAtMs <= stalenessMs)    // staleness = 2–3× sample interval (vd 150s)
        if hasData:                  results.add(e.raw)
        else if comp.missingAs == 1: continue            // skip — bỏ child khỏi phép AND/OR
        else:                        results.add(false)  // missing_as = 0 (mặc định)
    if results rỗng: return SKIP                          // mọi child thiếu data
    return comp.logicalOp == 0 ? all(results) : any(results)
```

**7.4 Child going stale (khuyến nghị, có thể hoãn):** đăng ký processing-time timer chu kỳ `staleness/2` trên key composite để tái đánh giá ngay cả khi không có ChildBool mới — bắt trường hợp AND đang FIRING rồi một child biến mất (phải chuyển false/skip → RESOLVED). Nếu bỏ qua, composite chỉ tái đánh giá khi có child update — ghi rõ trade-off trong code.

**Lưu ý ngữ nghĩa quan trọng:**
- Child đẩy **raw** (boolean tức thời), **không** qua sustain — sustain chỉ ở cấp composite (tránh sustain kép).
- Child no_data tham gia composite đầy đủ hai chiều: timer fire → `ChildBool(true)`; sample quay lại → `ChildBool(false)` ([FIXED] v1.3 — thiếu chiều false là bug kinh điển khiến composite kẹt FIRING).
- Mặc định child vẫn fire alert riêng (`emit_independent=1`); muốn giảm nhiễu, operator đặt `emit_independent=0` qua API.

**Acceptance:** tái hiện đúng bảng VD4 HLD §7.5 (AND: A=true → false vì B chưa có; B=true → FIRED-path; A=false → RESOLVED).

---

## Bước 8 — Cold start, rule động, dọn orphan

**8.1 Khởi động từ checkpoint/savepoint (đường bình thường):** state đầy đủ → dedup đúng, không re-fire. Mọi deploy nâng cấp dùng savepoint.

**8.2 Cold start (lần đầu, không state)** — chọn 1 trong 3 (HLD §6.7), theo thứ tự ưu tiên:
1. **State Processor API:** dựng savepoint ban đầu từ `tlm_alert_state` (sạch nhất, công sức cao nhất).
2. **Lazy seed (khuyến nghị thực dụng):** lần đầu key được chạm mà `dedup` state rỗng → tra `tlm_alert_state` (cache + Async I/O tới MariaDB) khởi tạo `lastState/lastFiredAt`. Một lần tra mỗi key.
3. **Tối giản:** chấp nhận re-fire một lần (NOC PRO idempotent + cửa sổ dedup hấp thụ). Chỉ chọn khi re-fire hiếm chấp nhận được — ghi quyết định vào README triển khai.

**8.3 Rule động & orphan (HLD §9):**
- Disable (`status=0`) → rule biến khỏi snapshot → P2/P3 tra broadcast trả `null`: nếu `dedup.lastState == FIRING` → emit **RESOLVED** + xoá state key đó (cơ chế chính — kích hoạt khi còn event cùng key tới).
- Bổ trợ cho series không còn event: TTL an toàn dài trên dedup, hoặc job quét định kỳ phát RESOLVED cho rule vừa disable.
- Đổi `entity_keys` → fingerprint đổi → state cũ thành orphan (dọn như trên). API phải cảnh báo operator khi sửa `entity_keys` (Phần B.6). Đổi `entity_filter` **không** đổi fingerprint — chỉ thu hẹp/mở rộng tập instance.

---

## Bước 9 — Kiểm thử, observability, Definition of Done

**9.1 Unit test bắt buộc (theo bước):**

| Nhóm | Ca kiểm thử |
| --- | --- |
| fingerprint (B0) | golden vectors, sort key, key thiếu, device-level `""` |
| evalRaw (B4) | mỗi `condition_kind` × mỗi `comparator`; SKIP: prev null, prev 0, valueType≠1; pctAbs |
| prev/late (B4) | event đảo thứ tự không cập nhật prev, pct/delta SKIP, threshold vẫn eval |
| sustainGate (B4) | n=1, n=3, chuỗi T-T-F-T-T-T, SKIP xen giữa không reset |
| dedupGate (B4) | phủ đủ 6 dòng bảng 4.6; biên `now-lastFiredAt == dedupSeconds` (≥ → REFIRED) |
| no_data (B6) | VD3; series mới không fire; resolve-on-return; aging clear; restart timer quá hạn |
| composite (B7) | VD4; missing_as=0 vs 1; staleness; no_data-child hai chiều true/false |

**9.2 Integration/E2E:** Kafka + MariaDB + ClickHouse (testcontainers hoặc môi trường staging); kịch bản VD1–VD4 end-to-end; kill-and-restore từ checkpoint giữa chuỗi VD1; sửa rule runtime (đổi threshold, disable đang FIRING → RESOLVED).

**9.3 Metrics expose Prometheus** (HLD §12 — làm cùng Pha 1, không để sau): `alert_fired_total{rule_id,event_type}`, `alert_eval_latency_ms` (histogram, từ `eventTimeMs` → emit), `alert_suppressed_total{rule_id}`, `alert_skip_total{reason}`, `nodata_timers_active`, `composite_missing_child_total{composite_id}`, cùng `kafka_consumer_lag`, `watermark_lag_ms`, `state_size_bytes`, `checkpoint_duration_ms`. Tuỳ chọn giai đoạn chuyển đổi: `validation_decision_mismatch_total{rule_id,type}` so với nguồn đối chiếu (recompute ClickHouse hoặc poll engine cũ nếu còn chạy tạm) — mục tiêu = 0 trước khi mở emit thật.

**9.4 Rollout an toàn (HLD §13):** mỗi pha mở rộng tập rule dần; có cờ bật/tắt emit thật theo nhóm rule (đọc từ snapshot) để rollback nhanh; Pha 1 chạy chế độ đối chiếu (ghi history nhãn riêng) tới khi mismatch ≈ 0 rồi mới bật emit ra NOC PRO; tự tin → gỡ hẳn poll engine.

**9.5 Definition of Done toàn job:** SLO eval latency p99 ≤ 5s (đo `alert_eval_latency_ms`) ở tải staging; mọi acceptance VD1–VD4 pass; restart không re-fire; rule CRUD qua API có hiệu lực ≤ chu kỳ refresh; dashboard AL-09/AL-10 phản ánh đúng `tlm_alert_state`.

---

# Phần B — REST API: Alert Rule Service (Spring Boot)

> **Bối cảnh:** Phần này đặc tả lại REST API trước đây nằm trong tài liệu Alert Engine HLD poll-based (`03-alert_engine_hld_vi_v1_2` §7 — **tài liệu/poll engine đã ngừng sử dụng**). Vai trò service thu hẹp theo HLD streaming §1.2: **(a)** CRUD/validate rule (AL-01…AL-08), **(b)** preview/dry-run chạy trực tiếp trên ClickHouse (AL-07 — độc lập với engine), **(c)** đọc state/active/history cho dashboard (AL-09…AL-11), **(d)** quan sát sức khoẻ engine (AL-13 — nay là Flink job health thay cho poll interval). Service **không còn vòng lặp đánh giá** và **không bao giờ** ghi `tlm_alert_state` (chỉ Job 3 ghi).
>
> **Đường truyền hiệu lực rule:** API ghi MariaDB → `updated_at` tự tiến (version) → Job 3 broadcast refresh pick up trong **≤ vài chục giây** (chu kỳ poll snapshot — Bước 2). API **không** gọi Flink trực tiếp; response các thao tác ghi nên nêu rõ độ trễ này (trường `effective_note`).

## B.1 Quy ước chung

- **Base path:** `/api/v1/alerts`. JSON UTF-8. Thời gian ISO-8601 kèm offset `+07:00`. Đặt sau auth chung của hệ thống (`401/403` chuẩn).
- **Mã trạng thái:** `200` OK · `201` Created · `204` No Content (DELETE) · `400` sai cú pháp/kiểu · `404` không thấy · `409` xung đột (`If-Match` mismatch; composite tham chiếu child không hợp lệ ở thời điểm ghi) · `422` vi phạm ràng buộc ngữ nghĩa (validation B.4/B.5) · `500` lỗi máy chủ.
- **Concurrency:** `PUT`/`PATCH` hỗ trợ header `If-Match: <updated_at>` (giá trị lấy từ response trước); `updated_at` hiện tại khác → `409` (chống ghi đè đồng thời). Đồng nhất quy ước với API Flink rule (Job 1 §8) và derived rule (Job 2 §7).
- **Versioning:** mọi write đẩy `updated_at` (DATETIME(6)) tiến lên — đây chính là version mà broadcast refresh của Job 3 dùng.
- **Soft-delete:** `DELETE` = `status=0`. Không xoá vật lý (giữ tham chiếu `child_rule_ids`, audit `created_by`/`updated_at`).
- **Lỗi trả về thống nhất:**

```json
{ "error": { "code": "VALIDATION_FAILED", "message": "no_data_seconds là bắt buộc khi condition_kind=2",
             "field_errors": [ { "field": "no_data_seconds", "reason": "required" } ] } }
```

## B.2 Bảng endpoint

| Method & Path | Mục đích | User story |
| --- | --- | --- |
| `GET  /alerts/rules` | Liệt kê rule (lọc + phân trang) | AL-01 |
| `GET  /alerts/rules/{id}` | Chi tiết một rule | AL-01 |
| `POST /alerts/rules` | Tạo rule (basic mọi `condition_kind` + composite) | AL-02…AL-06 |
| `PUT  /alerts/rules/{id}` | Cập nhật toàn bộ (full replace) | AL-02…AL-06 |
| `PATCH /alerts/rules/{id}` | Cập nhật một phần (vd `{"status":0}`, `{"threshold":90}`) | AL-08 |
| `DELETE /alerts/rules/{id}` | Soft-delete (`status=0`) an toàn với cảnh báo phụ thuộc composite | AL-12 |
| `POST /alerts/rules/preview` | Dry-run rule trên dữ liệu thực gần nhất (ClickHouse), không lưu | AL-07 |
| `GET  /alerts/rules/{id}/state` | Firing state per-instance (đọc mirror `tlm_alert_state`) | AL-10 |
| `GET  /alerts/active` | Danh sách alert đang FIRING (dashboard Active Alerts) | AL-09 |
| `GET  /alerts/history` | Truy vấn `ipms.alert_history` (FIRED/REFIRED/RESOLVED) | AL-11 |
| `GET  /alerts/engine/status` | Sức khoẻ engine: Flink job health + độ trễ rule | AL-13 (reframed) |
| `GET  /alerts/refresh-status` | `updated_at` mới nhất vs snapshot version Job 3 đang dùng | hỗ trợ vận hành |

## B.3 List, lọc, phân trang — `GET /alerts/rules`

Query params: `status` (mặc định `1`), `rule_kind`, `condition_kind`, `severity`, `alias_metric` (khớp chính xác), `path_alias_id`, `q` (tìm theo `rule_name`), `page` (mặc định 1), `page_size` (mặc định 50, tối đa 200), `sort` (vd `-updated_at`, `severity`).

```json
{
  "page": 1, "page_size": 50, "total": 124,
  "items": [
    {
      "id": 101, "rule_name": "Interface in-errors cao",
      "rule_kind": 0, "severity": 2, "status": 1,
      "alias_metric": "if_in_errors", "path_alias_id": 5,
      "condition_kind": 0, "comparator": 0, "threshold": 100, "pct_abs": 0,
      "entity_keys": ["if_name"], "entity_filter": null, "scope_device_ids": null,
      "sustain_samples": 2, "dedup_seconds": 1800, "emit_independent": 1,
      "created_by": 7,
      "updated_at": "2026-06-03T08:00:00.000000+07:00"
    }
  ]
}
```

`GET /alerts/rules/{id}` trả object đơn cùng cấu trúc; composite có thêm `logical_op`, `child_rule_ids`, `missing_as` và (tiện UI) `children: [{id, rule_name, condition_kind, status}]` expand sẵn.

## B.4 Tạo basic rule — `POST /alerts/rules` (AL-02…AL-05)

Body chung mọi basic: `rule_name`, `rule_kind: 0`, `severity (0..3)`, `alias_metric`, `path_alias_id` (null = any-path; **derived metric = 0**), `entity_keys` (array; `[]` = device-level), `entity_filter` (tuỳ chọn), `scope_device_ids` (tuỳ chọn), `sustain_samples` (≥1, mặc định 1), `dedup_seconds` (≥0, mặc định 3600), `emit_independent` (mặc định 1), `condition_kind` + trường điều kiện.

Ví dụ threshold (AL-02):

```json
{
  "rule_name": "Interface in-errors cao",
  "rule_kind": 0, "severity": 2,
  "alias_metric": "if_in_errors", "path_alias_id": 5,
  "entity_keys": ["if_name"],
  "entity_filter": { "if_name": ["GigabitEthernet0/0/0/0", "GigabitEthernet0/0/0/1"] },
  "scope_device_ids": [42, 43],
  "condition_kind": 0, "comparator": 0, "threshold": 100,
  "sustain_samples": 2, "dedup_seconds": 1800
}
```

Ví dụ no_data (AL-04): `{"condition_kind": 2, "no_data_seconds": 180, ...}` — **không** gửi `comparator`/`threshold`.

**Ma trận validation (server-side, `422` nếu vi phạm — khớp CHECK Phụ lục A + ràng buộc app-layer):**

| Trường | threshold (0) | pct_change (1) | no_data (2) | abs_delta (3) |
| --- | --- | --- | --- | --- |
| `comparator` ∈ 0..5 | bắt buộc | bắt buộc | **phải vắng** | bắt buộc |
| `threshold` | bắt buộc | bắt buộc (đơn vị %) | **phải vắng** | bắt buộc |
| `pct_abs` ∈ {0,1} | bỏ qua | tuỳ chọn | bỏ qua | tuỳ chọn |
| `no_data_seconds` > 0 | phải vắng | phải vắng | **bắt buộc** | phải vắng |

Validation chung: `severity ∈ 0..3`; `sustain_samples ≥ 1`; `dedup_seconds ≥ 0`; `entity_filter` key ⊆ `entity_keys` (nếu lệch → `422` kèm field_errors); `alias_metric` nên đối chiếu tồn tại qua `GET /flink/metric-aliases`/`GET /derived-rules` (cảnh báo soft trong response nếu không thấy — không chặn, vì metric có thể xuất hiện sau).

Response `201` trả full object + ghi chú hiệu lực:

```json
{ "id": 101, "...": "...", "updated_at": "2026-06-03T08:00:00.000000+07:00",
  "effective_note": "Rule sẽ được Job 3 áp dụng trong vòng <= 30s (chu kỳ refresh broadcast)." }
```

## B.5 Tạo composite rule — `POST /alerts/rules` (AL-06)

```json
{
  "rule_name": "Interface lỗi VÀ discard cao",
  "rule_kind": 1, "severity": 3,
  "entity_keys": ["if_name"],
  "logical_op": 0, "child_rule_ids": [101, 102], "missing_as": 0,
  "sustain_samples": 1, "dedup_seconds": 3600
}
```

Validation (app-layer — không biểu diễn được bằng CHECK, **bắt buộc kiểm ở API** vì engine tin tưởng dữ liệu này):
- `logical_op ∈ {0,1}`, `child_rule_ids` không rỗng, `missing_as ∈ {0,1}`.
- Mọi child: **tồn tại**, là **basic** (`rule_kind=0`), `status=1`, **cùng `entity_keys`** với composite (so sánh tập, không phụ thuộc thứ tự).
- Không tự tham chiếu; **không lồng composite** (child không được là composite).
- Khi child sau này bị disable/sửa `entity_keys`: API phải chặn (`409`) hoặc yêu cầu `?force=true` kèm cảnh báo danh sách composite bị ảnh hưởng (cùng pattern cascade của Flink API §8.7).

## B.6 Cập nhật, bật/tắt (AL-08), xoá (AL-12)

- `PATCH /alerts/rules/{id}` body chỉ chứa field thay đổi. Hai thao tác phổ biến: `{"status": 0}` (tạm dừng), `{"threshold": 90}` (chỉnh ngưỡng). `PUT` = full replace, validate như POST.
- Mọi write: `updated_at` tự tiến → Job 3 pick up ≤ chu kỳ refresh.
- **Tác động phía engine (API nêu trong response `warnings[]` để UI hiển thị):**
  - `status: 0` → rule biến khỏi snapshot; instance đang FIRING sẽ được engine emit `RESOLVED` + dọn state (Bước 8.3).
  - Đổi `entity_keys` → toàn bộ state/fingerprint cũ thành **orphan** (dọn + RESOLVED); cảnh báo rõ "state hiện tại của rule sẽ reset".
  - Đổi `entity_filter` → không đổi fingerprint, chỉ thu hẹp/mở rộng tập instance.
- `DELETE /alerts/rules/{id}` (AL-12) → soft-delete `status=0`, trả `204`; engine emit `RESOLVED` cho instance đang FIRING (Bước 8.3). Nếu rule là **basic đang được composite Active tham chiếu** → chặn `409` kèm danh sách composite phụ thuộc; `?force=true` → thực hiện xoá **và vô hiệu (`status=0`) các composite phụ thuộc** đó (đúng acceptance AL-12), response liệt kê các composite đã bị vô hiệu.

## B.7 Preview / dry-run — `POST /alerts/rules/preview` (AL-07)

Chạy **trực tiếp trên ClickHouse `ipms.tlm_metrics`** (độc lập với Job 3 — không đụng state engine, không ghi gì). Nhận **rule chưa lưu** (body như B.4/B.5) hoặc `{"rule_id": 101}`; tham số tuỳ chọn `sample_window_seconds` (mặc định ~300s gần nhất), `device_id` để khoanh vùng.

Logic phía server (gợi ý): lấy 2 sample mới nhất mỗi series khớp `(alias_metric, path scope, entity_filter, scope_device_ids)`; tính boolean tức thời theo đúng bảng evalRaw (Bước 4.3); phân loại series.

```json
{
  "evaluated_series": 1240,
  "matching_series": 17,
  "skipped_series": 3,
  "skipped_reasons": { "no_prev": 2, "prev_zero": 1 },
  "samples": [
    { "device_id": 42, "device_name": "PE-HN-01",
      "entity": { "if_name": "GigabitEthernet0/0/0/0" },
      "cur_value": 250, "prev_value": 30, "would_fire": true }
  ],
  "note": "Preview phản ánh boolean tức thời tại thời điểm truy vấn; KHÔNG mô phỏng sustain/dedup/no_data theo thời gian."
}
```

> Với composite, preview hợp nhất boolean tức thời của các child theo `logical_op`/`missing_as` trên cùng cửa sổ — vẫn không mô phỏng sustain. Với `condition_kind=2` (no_data), preview trả các series có sample mới nhất cũ hơn `no_data_seconds` (xấp xỉ — engine thật dùng processing-time timer).

## B.8 Firing state per-instance — `GET /alerts/rules/{id}/state` (AL-10)

Đọc `tlm_alert_state` (mirror Job 3 ghi — read-only với service). Query: `last_state` (lọc 0/1), `device_id`, `page`/`page_size`.

```json
{
  "rule_id": 101, "rule_name": "Interface in-errors cao", "dedup_seconds": 1800,
  "items": [
    { "device_id": 42, "device_name": "PE-HN-01",
      "entity_fingerprint": "if_name=GigabitEthernet0/0/0/0",
      "last_state": 1,
      "last_fired_at": "2026-06-03T10:15:30.123+07:00",
      "last_resolved_at": null,
      "dedup_remaining_seconds": 1432 }
  ]
}
```

`dedup_remaining_seconds` = `max(0, dedup_seconds − (now − last_fired_at))`, tính tại API (giúp operator hiểu vì sao alert đang bị suppress). Lưu ý nhỏ trong docs cho UI: mirror có thể trễ vài giây so với Flink state — chấp nhận theo thiết kế.

## B.9 Active Alerts — `GET /alerts/active` (AL-09)

Join `tlm_alert_state (last_state=1)` ⋈ `tlm_alert_rules` (+ tuỳ chọn bổ sung giá trị trigger gần nhất từ `alert_history`). Query: `severity`, `device_id`, `rule_kind`, `page`/`page_size`, `sort` (mặc định `-last_fired_at`).

```json
{
  "total": 12,
  "items": [
    { "rule_id": 101, "rule_name": "Interface in-errors cao", "rule_kind": 0, "severity": 2,
      "device_id": 42, "device_name": "PE-HN-01",
      "entity_fingerprint": "if_name=GigabitEthernet0/0/0/0",
      "fired_at": "2026-06-03T10:15:30.123+07:00",
      "observed_value": 250, "threshold": 100,
      "dedup_status": "SUPPRESSED", "dedup_remaining_seconds": 1432 }
  ]
}
```

`dedup_status`: `FIRING` (vừa fire/ngoài cửa sổ) | `SUPPRESSED` (trong cửa sổ dedup). UI refresh theo nhịp (vd 5–10s) — dữ liệu gần realtime vì Job 3 mirror ngay khi state đổi.

## B.10 Alert history — `GET /alerts/history` (AL-11)

Query ClickHouse `ipms.alert_history`. Params: `from`/`to` (mặc định 24h gần nhất; tối đa 90 ngày theo TTL), `rule_id`, `device_id`, `severity`, `event_type` (`fired|refired|resolved`), `page`/`page_size`.

```json
{
  "total": 342,
  "items": [
    { "event_time": "2026-06-03T10:15:30.123+07:00", "event_type": "fired",
      "rule_id": 101, "rule_name": "Interface in-errors cao", "rule_kind": 0, "severity": 2,
      "device_id": 42, "device_name": "PE-HN-01",
      "entity": { "if_name": "GigabitEthernet0/0/0/0" },
      "metric_name": "if_in_errors", "condition_kind": 0,
      "observed_value": 250, "threshold": 100,
      "detail": { "prev_value": 30 } }
  ]
}
```

> History là at-least-once — có thể trùng row khi engine retry. API khử trùng ở tầng truy vấn theo `(rule_id, device_id, entity, event_type, event_time)` (DISTINCT/LIMIT BY) trước khi trả về.

## B.11 Sức khoẻ engine — `GET /alerts/engine/status` (AL-13 reframed) & `GET /alerts/refresh-status`

AL-13 trước đây hiển thị poll interval; nay phản ánh **sức khoẻ Flink Job 3**. Service tổng hợp từ Flink REST API (`/jobs/{id}`, checkpoint stats) + Prometheus (lag/latency):

```json
{
  "engine": "flink-job3-streaming",
  "job_state": "RUNNING",
  "last_checkpoint_at": "2026-06-05T09:41:12+07:00",
  "checkpoint_duration_ms": 820,
  "kafka_consumer_lag": 1532,
  "watermark_lag_ms": 1900,
  "eval_latency_p99_ms": 2400,
  "uptime_seconds": 864000
}
```

`GET /alerts/refresh-status` — "rule của tôi đã sống chưa":

```json
{
  "max_updated_at": "2026-06-05T09:40:01.482931+07:00",
  "engine_snapshot_version": "2026-06-05T09:40:01.482931+07:00",
  "rules_pending": 0,
  "note": "engine_snapshot_version = version snapshot Job 3 đang dùng (job expose qua metric/endpoint); rules_pending > 0 kéo dài => kiểm tra broadcast refresh."
}
```

## B.12 Bản đồ user story → endpoint → bước engine

| User story | Endpoint | Phần engine liên quan |
| --- | --- | --- |
| AL-01 danh sách rule | `GET /alerts/rules` | — |
| AL-02 threshold | `POST /alerts/rules` (cond=0) | Bước 4 |
| AL-03 pct_change_prev | `POST /alerts/rules` (cond=1) | Bước 4 (prev-sample) |
| AL-04 no_data | `POST /alerts/rules` (cond=2) | Bước 6 |
| AL-05 abs_delta_prev | `POST /alerts/rules` (cond=3) | Bước 4 |
| AL-06 composite | `POST /alerts/rules` (kind=1) | Bước 7 |
| AL-07 preview | `POST /alerts/rules/preview` | độc lập (ClickHouse) |
| AL-08 bật/tắt | `PATCH /alerts/rules/{id}` | Bước 8.3 (orphan/RESOLVED) |
| AL-09 active alerts | `GET /alerts/active` | Bước 5 (mirror) |
| AL-10 firing state | `GET /alerts/rules/{id}/state` | Bước 5 (mirror) |
| AL-11 history | `GET /alerts/history` | Bước 5 (audit) |
| AL-12 xóa rule an toàn | `DELETE /alerts/rules/{id}` (`409`/`?force=true`) | Bước 8.3 (orphan/RESOLVED) |
| AL-13 engine health | `GET /alerts/engine/status` | Bước 9.3 (metrics) |

---

*Hết. Mọi điểm mơ hồ ưu tiên đối chiếu HLD v1.3; điểm thuộc API ưu tiên quy ước chung đã dùng ở Job 1 (§8) / Job 2 (§7) để toàn hệ thống đồng nhất.*
