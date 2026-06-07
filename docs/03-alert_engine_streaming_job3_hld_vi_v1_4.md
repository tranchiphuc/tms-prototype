# Thiết kế Alert Engine theo luồng — Flink Job 3 (Streaming Alert Evaluator) — HLD

**Phiên bản:** v1.4
**Loại tài liệu:** High-Level Design
**Ngôn ngữ triển khai:** Apache Flink (Java/Scala — dev tự quyết)
**Phạm vi:** Đánh giá alert rule **trong luồng** trên Kafka `processed_metrics` (+ derived), thay cho cơ chế ClickHouse-poll, nhằm đạt **Alerting latency SLO ≤ 5s**. Output fire/refire/resolve sang NOC PRO (Kafka `alerts`) + audit (`ipms.alert_history`).

> **Mục đích tài liệu:** Mô tả **ý định thiết kế** và **các ràng buộc/bất biến** đủ để dev hiện thực. Dev tự do chọn cấu trúc class, thư viện, chi tiết serialization — miễn giữ đúng hợp đồng I/O, ngữ nghĩa rule, các bất biến state, và SLO.
>
> **Quan hệ với tài liệu khác:** Tài liệu này **giữ nguyên mô hình rule và ngữ nghĩa fire/refire/resolve** của `03-alert_engine_hld` (v1.2/v1.3). Phần khác biệt duy nhất là **cơ chế đánh giá**: từ poll-theo-lô sang xử-lý-sự-kiện theo luồng. Mọi mã enum (`condition_kind`, `comparator`, `severity`, …) và schema (`tlm_alert_rules`, `tlm_alert_state`, `ipms.alert_history`) **không đổi** — chỉ được tái sử dụng.
>
> **⚠ Phụ thuộc schema (v1.3):** DDL của `tlm_alert_rules`, `tlm_alert_state` (MariaDB) và `ipms.alert_history` (ClickHouse) nằm ở tài liệu **Alert Engine HLD gốc** (`03-alert_engine_hld`), **không** ở bộ MariaDB v4.3 / ClickHouse v4.2 hiện có trong project. Nếu tài liệu gốc chưa sẵn sàng khi dev bắt đầu, dùng **Phụ lục A** (DDL tham chiếu được dựng lại từ các trường mà tài liệu này tham chiếu) làm điểm khởi đầu — và **đối chiếu lại** với tài liệu gốc nếu/khi nó tồn tại để tránh lệch tên cột/kiểu.

---

## Changelog

| Phiên bản | Thay đổi |
| --------- | -------- |
| v1.4 | **[CLARIFIED]** **§6.5 chốt tường minh REFIRED cho `no_data`:** mặc định **không refire chu kỳ** — mỗi đợt im lặng FIRED đúng một lần (timer không đăng ký lại; chỉ aging timer), sau đó resolve-on-return hoặc aging-RESOLVED; nêu *tuỳ chọn mở rộng* đăng ký lại timer trong `onTimer` nếu vận hành yêu cầu nhắc lại theo `dedup_seconds`. Đóng khoảng trống hai-cách-đọc giữa §6.4 (dedupGate có nhánh REFIRED) và §6.5. **[FIXED]** Cập nhật tham chiếu phiên bản Job 2 tại §2.3, §10, §13: "Job 2 v2.5" → "Job 2 ≥ v2.5 (hiện hành v2.6)" — tính năng `derived_metrics` có từ v2.5, §6.3 không đổi ở v2.6. Không đổi kiến trúc, ngữ nghĩa rule, hợp đồng I/O ra NOC PRO, schema hay SLO. |
| v1.3 | **[CHANGED]** **Thứ tự severity** đổi thành `0=info, 1=warning, 2=error, 3=critical` (trước v1.2: `2=critical, 3=error`) để mã tăng dần đúng theo mức nghiêm trọng (`critical` nặng nhất = 3). Cập nhật POJO §3.2, Phụ lục A. *(Giá trị `severity` trong ví dụ §8.1 giữ nguyên là số minh hoạ — không gắn nhãn.)* **[FIXED]** **Truy cập broadcast sau `keyBy`:** P2/P3 trước ghi là `KeyedProcessFunction` nhưng lại đọc `broadcastRuleRef(...)`/`compositeById` — `KeyedProcessFunction` **không** đọc được broadcast state. Đổi thành **`KeyedBroadcastProcessFunction`** (keyed stream `.connect(ruleBroadcastStream)`) ở §5.1, §7.2, §7.3, §3.4 để hợp lệ về mặt API và giữ rule động. **[FIXED]** **no_data là child của composite:** khi sample về (no_data = false) phải đẩy `ChildBool(raw=false)` lên composite, không chỉ đẩy `raw=true` lúc timer fire — bổ sung ở §6.5, §7.2 (trước đây composite không bao giờ thấy no_data-child trở lại false). **[CLARIFIED]** Ngữ nghĩa **basic vừa độc lập vừa là child**: nêu rõ mặc định (mọi basic `status=1` vẫn fire độc lập) và cách tắt fire độc lập nếu chỉ muốn dùng làm child (§7.2). **[ADDED]** **Phụ lục A — DDL tham chiếu** cho `tlm_alert_rules`, `tlm_alert_state`, `ipms.alert_history` (dựng lại từ các trường tham chiếu trong tài liệu) để dev tự thực thi khi chưa có sẵn tài liệu Alert Engine HLD gốc; đánh dấu rõ cần đối chiếu nếu tài liệu gốc tồn tại. **[FIXED]** Cấu trúc danh sách §9 (gating vs nguồn rule bị lồng nhầm). Không đổi ngữ nghĩa rule, hợp đồng I/O ra NOC PRO, hay SLO. |
| v1.2 | **[CLARIFIED/FIXED]** "Lọc sớm" (pre-filter) **không phải tuỳ chọn** mà là **hành vi mặc định** của P1 (`FanOutFunction`): P1 đứng trước `keyBy` và đã loại mọi event không khớp rule (metric/path/device/entity_filter), dùng chính broadcast snapshot nên tự cập nhật theo rule. Bỏ nhãn "pre-filter (tùy chọn)" gây hiểu nhầm ở §11; làm rõ ở §5.1, §5.2, §7.1. Không đổi kiến trúc hay logic — chỉ sửa cách trình bày. |
| v1.1 | **[CHANGED]** Poll engine **ngừng sử dụng**: tài liệu này trở thành **Alert Engine duy nhất** (streaming). Bỏ khung "song song/shadow với poll" làm trung tâm; reframe §1.2 (vai trò Spring Boot chỉ còn quản lý rule + preview/state), §13 (validation không phụ thuộc poll). **[CLARIFIED]** `derived_metrics` là **topic Kafka riêng** (KHÔNG ghi vào `processed_metrics` để tránh Job 2 tự consume vòng lặp) và là **bắt buộc** khi không còn poll (§2.3). **[ADDED]** §3.4 hợp đồng record nội bộ (JSON), §7.5 ví dụ truy vết end-to-end, §8.5 chi tiết `EmitFunction`, gating load rule lần đầu (§9). |
| v1.0 | Phiên bản khởi tạo. Streaming evaluator (Flink Job 3): consume `processed_metrics` (+ `derived_metrics`), broadcast rule snapshot, keyed state cho prev-sample/sustain/dedup, processing-time timer cho no_data, hai pha basic→composite, sink Kafka `alerts` + ClickHouse `alert_history` + mirror `tlm_alert_state`. Định nghĩa hợp đồng I/O, chiến lược keying, watermark/late handling, máy trạng thái, NFR, observability, lộ trình. |

---

## Mục lục

1. [Tổng quan](#1-tổng-quan)
2. [Ngữ cảnh & vị trí kiến trúc](#2-ngữ-cảnh--vị-trí-kiến-trúc)
3. [Hợp đồng đầu vào](#3-hợp-đồng-đầu-vào)
4. [Mô hình rule (tóm tắt — giữ nguyên từ Alert Engine HLD)](#4-mô-hình-rule)
5. [Kiến trúc job & mô hình thực thi](#5-kiến-trúc-job--mô-hình-thực-thi)
6. [Thiết kế state](#6-thiết-kế-state)
7. [Pipeline đánh giá theo pha](#7-pipeline-đánh-giá-theo-pha)
8. [Hợp đồng đầu ra](#8-hợp-đồng-đầu-ra)
9. [Rule động (broadcast refresh)](#9-rule-động-broadcast-refresh)
10. [Xử lý lỗi & trường hợp biên](#10-xử-lý-lỗi--trường-hợp-biên)
11. [Yêu cầu phi chức năng (NFR)](#11-yêu-cầu-phi-chức-năng)
12. [Observability](#12-observability)
13. [Lộ trình triển khai](#13-lộ-trình-triển-khai)
14. [Thuật ngữ](#14-thuật-ngữ)
- [Phụ lục A — DDL tham chiếu (dựng lại, v1.3)](#phụ-lục-a--ddl-tham-chiếu-dựng-lại-v13)

---

## 1. Tổng quan

### 1.1 Mục tiêu

Đánh giá alert rule **ngay khi metric chảy qua stream**, thay vì poll ClickHouse sau khi đã lưu. Loại bỏ độ trễ poll (10–15s) → độ trễ alert-path xuống mili-giây tới ~1s **tính từ lúc metric sẵn sàng trên `processed_metrics`**.

> **Định nghĩa SLO (giữ từ Alert Engine HLD §8):** "Alerting latency" đo **từ lúc sample/đối tượng cần thiết sẵn sàng** (ở đây: tới được `processed_metrics`), **không** tính từ sự kiện vật lý ở router. Theo định nghĩa này, Job 3 đạt **≤ 5s cho mọi loại rule**. Lưu ý: wall-clock *từ sự kiện vật lý → alert* vẫn bị chặn bởi chu kỳ sampling router (~60s) và ngữ nghĩa rule (pct/no_data/sustain cần nhiều sample); để rút ngắn phần đó cần bật **gNMI on-change** ở tầng thu thập — **ngoài phạm vi tài liệu này**.

### 1.2 Vai trò các thành phần (poll engine đã ngừng dùng)

Quyết định kiến trúc: **không còn poll engine.** Tài liệu này là **Alert Engine duy nhất**, hiện thực bằng một Flink job (vị trí "Job 3" trong pipeline). Phân chia trách nhiệm còn lại:

- **Flink Job 3 (tài liệu này):** toàn bộ **đánh giá** rule (basic + composite), sustain, dedup, no_data, phát fire/refire/resolve. Là **nguồn quyết định** alert.
- **Spring Boot "Alert Rule Service" (giữ lại, thu hẹp vai trò):** chỉ còn (a) **REST API quản lý rule** (CRUD/validate — Alert Engine HLD §7, user story AL-01…AL-08), (b) **preview/dry-run** `POST /alerts/rules/preview` chạy trực tiếp trên ClickHouse (độc lập với engine — không đổi), (c) **đọc state** `GET /alerts/rules/{id}/state` và dashboard "Active Alerts" (AL-09/AL-10) đọc từ MariaDB `tlm_alert_state` (bản mirror Job 3 ghi ra — §8.3). Service này **không còn vòng lặp đánh giá**.
- **Hợp đồng ra NOC PRO không đổi** (cùng payload, cùng khoá idempotency — §8.1): NOC PRO không cần biết engine là streaming hay poll.

> **Lưu ý dashboard AL-13** (trước hiển thị "poll interval"): nay phản ánh **sức khoẻ Flink job** (running, checkpoint gần nhất, consumer lag, watermark lag, eval latency — §12) thay cho poll interval.

### 1.3 Nguyên tắc thiết kế

- **Event-driven, không poll:** quyết định fire/resolve ra ngay trên event/timer.
- **State là nguồn sự thật runtime, bền qua Flink checkpoint.** `tlm_alert_state` (MariaDB) trở thành **bản mirror để dashboard/API đọc**, không còn là nguồn quyết định dedup.
- **Giữ nguyên ngữ nghĩa rule** của Alert Engine HLD — không phát minh ngữ nghĩa mới.
- **Tách quản lý rule (Spring Boot) khỏi đánh giá (Flink).**
- **Fail-safe & ít nhiễu:** thiếu dữ liệu → skip (không resolve nhầm); sustain + dedup giữ nguyên vai trò; history-first khi emit.

### 1.4 Phạm vi loại rule (theo pha)

| Pha | Loại rule | Lý do |
| --- | --- | --- |
| 1 | basic `threshold` (0), `abs_delta_prev` (3), `pct_change_prev` (1) | State per-instance đơn giản (prev-sample). Cho giá trị ≤5s sớm nhất |
| 2 | basic `no_data` (2) | Cần timer-driven; tách riêng để kiểm thử kỹ |
| 3 | `composite` (AND/OR) | Phức tạp nhất (alignment child); làm sau khi basic ổn định |

---

## 2. Ngữ cảnh & vị trí kiến trúc

### 2.1 Sơ đồ luồng

```
[Flink Job 1] ──► Kafka: processed_metrics ──┬──► [Flink Job 2: derived] ──► ClickHouse: tlm_metrics
                                              │                            └──► Kafka: derived_metrics  (MỚI — xem 2.3)
                                              │
                                              ├──► [Flink Job 2] (như trên)
                                              │
                                              └──► [FLINK JOB 3: ALERT EVALUATOR] ◄── MariaDB: tlm_alert_rules (broadcast)
                                                        │
                       ┌──────────────────────┬────────┴───────────────┬─────────────────────────┐
                       ▼                       ▼                        ▼                         ▼
              Kafka: alerts            ClickHouse:              MariaDB:                  (metrics/observability)
              → NOC PRO                ipms.alert_history       tlm_alert_state (mirror)
              (fired/refired/resolved) (audit, TTL 90d)         (cho dashboard AL-09/AL-10)
```

### 2.2 Quan hệ thành phần

| Thành phần | Quan hệ |
| --- | --- |
| Kafka `processed_metrics` | **Input chính** — raw metric đã chuẩn hoá (Job 1 produce) |
| Kafka `derived_metrics` | **Input phụ** — derived metric; **cần Job 2 emit thêm** (xem §2.3) |
| MariaDB `tlm_alert_rules` | **Reference (broadcast)** — load `status=1`, refresh runtime |
| MariaDB `tlm_alert_state` | **Output (mirror)** — Job 3 upsert khi state đổi; dashboard đọc. **Cũng dùng để seed cold-start** (§6.7) |
| Kafka `alerts` | **Output** — event fire/refire/resolve sang NOC PRO |
| ClickHouse `ipms.alert_history` | **Output (audit)** — TTL 90 ngày |
| NOC PRO | **Downstream** — idempotent theo `(rule_id, device_id, entity_fingerprint, event_type, event_time)` |

### 2.3 Thay đổi cần ở Job 2 (BẮT BUỘC — vì không còn poll)

Trước đây poll engine đọc derived metric từ ClickHouse. **Không còn poll** ⇒ Job 3 phải nhận derived **qua Kafka**. Vì vậy Job 2 **bắt buộc** emit derived row ra một **topic Kafka riêng `derived_metrics`** (đã đặc tả trong tài liệu Job 2 **§6.3** — tính năng có từ v2.5, tài liệu hiện hành **v2.6**, §6.3 không đổi).

- **Phải là topic riêng**, KHÔNG ghi vào `processed_metrics`: vì `processed_metrics` là **input** của Job 2 → ghi derived vào đó sẽ khiến **Job 2 tự consume lại chính output của mình** (vòng lặp / derived-of-derived). `derived_metrics` tách biệt phá vòng lặp này.
- Định dạng message **giống `MetricEvent`** (§3.1), với `pathId = 0`, `rawPath = "derived"`, `metricName = output_metric`.
- Job 3 consume **cả hai** topic (`processed_metrics` cho raw, `derived_metrics` cho derived) qua cùng một `KafkaSource` (hoặc hai source union).
- **Độ trễ alert trên derived** vẫn bị chặn bởi cửa sổ tính của Job 2 (computed ~60–90s, aggregated ~window×1.25, delta ~60s) — đây là trần dữ liệu, không phải của Job 3.

### 2.4 Ngân sách độ trễ (Job 3)

| Chặng | Độ trễ |
| --- | --- |
| `processed_metrics` → Job 3 source (Kafka consume) | chục ms |
| Fan-out + keyBy shuffle | chục–trăm ms |
| Eval + state access (RocksDB) | < vài chục ms |
| Watermark/lateness chờ (bounded) | = `max_out_of_orderness` (đề xuất 1–2s) |
| Sink Kafka `alerts` | chục ms |
| **Tổng (alert-path, từ lúc metric sẵn sàng)** | **~1–3s ≤ SLO 5s** |

---

## 3. Hợp đồng đầu vào

### 3.1 `MetricEvent` (trên `processed_metrics` / `derived_metrics`)

Giống POJO Job 2 dùng (`02-flink_job2` §4.2) — **không định nghĩa lại khác đi**:

```java
class MetricEvent {
    long   eventTimeMs;     // event_time (ms) — dùng cho watermark & "previous sample"
    long   receiveTimeMs;   // receive_time
    int    deviceId;
    String deviceName;
    int    pathId;          // dùng kiểm path scope của rule (path_alias_id); derived = 0
    String rawPath;         // "derived" cho derived metric
    String rawMetricName;
    String metricName;      // = alias_metric; dùng match alias_metric của rule
    int    valueType;       // 1=number, 2=string, 3=bool
    double valueNumber;     // dùng cho threshold/pct/delta
    String valueString;
    boolean valueBool;
    String unit;
    Map<String,String> labels;   // keys đã chuẩn hoá (sau LabelRename của Job 1)
}
```

> **Điều kiện number:** `threshold`/`pct_change_prev`/`abs_delta_prev` chỉ áp khi `valueType == 1` (đọc `valueNumber`). Event `valueType ∈ {2,3}` được **bỏ qua** cho các điều kiện số (không skip-resolve — coi như "không phải đối tượng của rule số này"). `no_data` áp cho **mọi** `valueType` (chỉ quan tâm sự hiện diện của sample).

### 3.2 Rule snapshot (broadcast)

Load từ MariaDB `tlm_alert_rules WHERE status=1`. POJO đề xuất (khớp schema Alert Engine HLD §4.1, gồm `entity_filter` v1.3):

```java
class AlertRule {
    int     id;
    String  ruleName;
    int     ruleKind;            // 0=basic, 1=composite
    int     severity;            // 0=info,1=warning,2=error,3=critical
    List<String> entityKeys;     // [] = device-level
    Map<String,List<String>> entityFilter;  // null = mọi giá trị; value chuẩn hoá; key ⊆ entityKeys
    List<Integer> scopeDeviceIds;            // null = mọi thiết bị
    int     sustainSamples;      // ≥1
    long    dedupSeconds;        // ≥0

    // basic (ruleKind=0):
    String  aliasMetric;
    Integer pathAliasId;         // null = any-path; else = path_id cụ thể; derived = 0
    Integer conditionKind;       // 0=threshold,1=pct,2=no_data,3=abs_delta
    Integer comparator;          // 0=>,1=>=,2=<,3=<=,4===,5=!=  (null khi no_data)
    Double  threshold;           // null khi no_data
    int     pctAbs;              // 1 = dùng |.|  (cho conditionKind ∈ {1,3})
    Long    noDataSeconds;       // bắt buộc khi conditionKind=2

    // composite (ruleKind=1):
    Integer logicalOp;           // 0=AND,1=OR
    List<Integer> childRuleIds;
    int     missingAs;           // 0=false, 1=skip
}
```

Job 3 dựng thêm **chỉ mục phụ** từ snapshot (tính lại mỗi lần refresh):
- `byMetric: aliasMetric -> List<AlertRule basic>` — để fan-out nhanh.
- `childToComposites: childRuleId -> List<compositeId>` — để route boolean child lên composite.
- `compositeById: id -> AlertRule` — lấy `logicalOp`, `childRuleIds`, `missingAs`, `sustainSamples`, `dedupSeconds`.

### 3.3 `entity_fingerprint` — định nghĩa canonical (BẮT BUỘC thống nhất)

Sinh tất định từ giá trị label trong `entityKeys`:

```
fingerprint(labels, entityKeys):
    if entityKeys rỗng: return ""                       // device-level (khớp "" trong tlm_alert_state)
    sort entityKeys theo thứ tự từ điển
    parts = [ k + "=" + labels.getOrDefault(k, "") for k in entityKeys_sorted ]
    return join(parts, ";")                             // vd: "if_name=Gi0/0/0/0"  |  "cpu_id=0;if_name=Gi0/0/0/0"
```

> **Bất biến:** format này là **chuẩn duy nhất** cho `entity_fingerprint` trong toàn hệ thống. `tlm_alert_state.entity_fingerprint` (MariaDB) và `entity` trong `alert_history` (ClickHouse) phải sinh từ đúng hàm này để dashboard/audit khớp.

### 3.4 Hợp đồng record nội bộ (giữa các toán tử)

Các record dưới đây **chỉ chạy trong Flink** (giữa operator), không bắt buộc serialize ra Kafka — nhưng nên dùng POJO có TypeInformation rõ (tránh Kryo fallback). Trình bày dạng trường để dev định nghĩa class/serializer tương ứng.

```java
// P1 → P2 : một bản ghi cho mỗi (basic rule × instance) khớp event
class RuleInstanceEvent {
    int    ruleId;
    int    deviceId;
    String deviceName;
    String entityFingerprint;          // §3.3
    Map<String,String> entityMap;      // subset labels theo entityKeys (để ghi field "entity" output)
    String metricName;
    int    valueType;
    double valueNumber;
    long   eventTimeMs;
}

// P2 (side output) → P3 : boolean tức thời của một child gửi lên composite
class ChildBool {
    int    compositeId;
    int    childRuleId;
    int    deviceId;
    String entityFingerprint;
    boolean raw;                       // boolean tức thời (KHÔNG qua sustain)
    long   eventTimeMs;
}

// P2/P3 → EmitFunction : một quyết định alert đã qua dedup gate
class AlertDecision {
    String eventType;                  // "fired" | "refired" | "resolved"
    long   decisionTimeMs;             // = eventTimeMs (event) hoặc processing time (no_data timer)
    int    ruleId; String ruleName; int ruleKind; int severity;
    int    deviceId; String deviceName;
    Map<String,String> entity;
    String metricName;                 // "" với composite
    Integer conditionKind;             // null/"" với composite
    double observedValue;              // cur | pct | tuổi no_data(s)
    Double threshold;                  // null với no_data/composite
    String detailJson;                 // {"prev_value":..} hoặc {"children":[..]}
    boolean stateChanged;              // true nếu OK↔FIRING (kích hoạt mirror MariaDB)
    int    lastState;                  // 0=ok,1=firing (sau quyết định) — cho mirror
    Long   lastFiredAtMs; Long lastResolvedAtMs;   // cho mirror
}
```

> `AlertDecision` mang **đủ trường** để vừa dựng `AlertEvent` Kafka (§8.1), vừa ghi `alert_history` (§8.2), vừa upsert `tlm_alert_state` (§8.3) — `EmitFunction` không cần đọc lại state.
>
> **Lưu ý (v1.3):** `RuleInstanceEvent`/`ChildBool` **không** mang tham số rule (comparator, threshold, sustain, logical_op…). P2/P3 đọc các tham số đó từ **broadcast rule snapshot** tại runtime — vì vậy P2/P3 là `KeyedBroadcastProcessFunction` (§5.2, §7.2, §7.3), không phải `KeyedProcessFunction`.

---

## 4. Mô hình rule

Giữ **nguyên văn** từ `03-alert_engine_hld` §3. Tóm tắt để đọc độc lập:

- **basic** (`ruleKind=0`): 1 điều kiện trên 1 metric. `conditionKind`: `0=threshold`, `1=pct_change_prev`, `2=no_data`, `3=abs_delta_prev`.
- **composite** (`ruleKind=1`): gộp nhiều **basic cùng `entityKeys`** trên một device bằng AND/OR. `missingAs`: `0=false`, `1=skip`.
- **`comparator`:** `0=>,1=>=,2=<,3=<=,4===,5=!=`.
- **"previous" = đúng 1 sample liền trước** theo `eventTimeMs` trong cùng series `(deviceId, entity_fingerprint)`.
- **sustain:** điều kiện phải đúng **liên tiếp** ≥ `sustainSamples` lần mới "satisfied". Áp ở **cấp rule phát alert** (basic độc lập: cấp basic; composite: cấp composite — child chỉ cấp boolean tức thời).
- **dedup:** suppress re-fire cùng `(rule, device, entity)` trong `dedupSeconds`.
- **entity_filter** (v1.3): lọc theo **giá trị** label; áp như predicate chọn instance trước khi eval; không đổi fingerprint.

---

## 5. Kiến trúc job & mô hình thực thi

### 5.1 Chuỗi toán tử

```
KafkaSource<MetricEvent>(processed_metrics [, derived_metrics])
  → assignTimestampsAndWatermarks( eventTimeMs, boundedOutOfOrderness = 1–2s )
  → .connect( ruleBroadcastStream ).process( FanOutFunction )            // P1 — §7.1  (BroadcastProcessFunction, KHÔNG keyed)
        │  LỌC SỚM + FAN-OUT: chỉ phát RuleInstanceEvent cho rule khớp;
        │  event không khớp rule nào (metric/path/device/entity_filter) → BỎ tại đây, KHÔNG đi tiếp keyBy
        │  main output: RuleInstanceEvent (cho mỗi basic rule khớp)
        ▼
  → keyBy( rule_id, device_id, entity_fingerprint )                       // shuffle CHỈ mang event đã khớp rule
  → .connect( ruleBroadcastStream ).process( BasicEvaluator )            // P2 — §7.2  (KeyedBroadcastProcessFunction + timer)
        │  main output:  AlertDecision (cho basic ĐỘC LẬP)
        │  side output:  ChildBool   (cho basic là CHILD của composite)
        ▼  (ChildBool)
  → keyBy( composite_id, device_id, entity_fingerprint )
  → .connect( ruleBroadcastStream ).process( CompositeEvaluator )        // P3 — §7.3  (KeyedBroadcastProcessFunction + timer)
        │  main output:  AlertDecision (cho composite)
        ▼
  union(AlertDecision từ P2 và P3)
  → process( EmitFunction )                                              // §8 — history-first
        ├─► KafkaSink<AlertEvent>( alerts )
        ├─► ClickHouseSink( alert_history )
        └─► side output StateChange → MariaDBSink( tlm_alert_state upsert )
```

### 5.2 Chiến lược keying (phần cốt lõi — đọc kỹ)

Một `MetricEvent` có thể khớp **nhiều** rule với **`entityKeys` khác nhau** ⇒ **không thể** keyBy một fingerprint duy nhất ngay ở source. Giải pháp: **fan-out trước, keyBy sau**.

- **P1 (FanOut, không keyed):** với mỗi basic rule khớp `(metricName, path scope, scopeDeviceIds, entityFilter)`, tính `entity_fingerprint` **theo `entityKeys` của chính rule đó** rồi phát một `RuleInstanceEvent` mang khoá logic `(rule_id, device_id, entity_fingerprint)`.
- **P2 (BasicEvaluator):** `keyBy(rule_id, device_id, entity_fingerprint)` rồi **`.connect(ruleBroadcastStream)`** → mỗi **instance** của một basic rule là một keyed context độc lập (prev-sample/sustain/dedup/no_data-timer nằm ở đây). Đây là khoá cho song song hoá mịn.
- **P3 (CompositeEvaluator):** children của một composite **cùng `entityKeys`** (ràng buộc API) ⇒ **cùng `entity_fingerprint`**. Route boolean child về `keyBy(composite_id, device_id, entity_fingerprint)` rồi **`.connect(ruleBroadcastStream)`** để các child gặp nhau trên một instance.

> **Vì sao P2/P3 phải là `KeyedBroadcastProcessFunction` (không phải `KeyedProcessFunction`):** cả hai cần đọc tham số rule động từ broadcast snapshot tại runtime — P2 cần `comparator/threshold/conditionKind/sustainSamples/dedupSeconds` + bảng `childToComposites`; P3 cần `compositeById` (`logicalOp/childRuleIds/missingAs/...`). Flink **chỉ** cho phép đọc broadcast state bên trong một `(Keyed)BroadcastProcessFunction`; một `KeyedProcessFunction` thuần **không** truy cập được broadcast state. Do đó keyed stream phải `.connect(ruleBroadcastStream)` ở cả P2 và P3 (xem §5.1, §7.2, §7.3). *(Phương án thay thế: nhồi đủ tham số basic vào `RuleInstanceEvent` ở P1 để P2 tự đủ; nhưng P2 vẫn cần `childToComposites` và P3 vẫn cần `compositeById` từ broadcast, nên dùng `KeyedBroadcastProcessFunction` đồng nhất là gọn nhất.)*

> **Vì sao không keyBy chỉ `device_id`:** một PE router có hàng nghìn interface → một key gánh toàn bộ → hot key/skew, mất song song. Keyed theo `(rule, device, entity)` phân tán đều.

> **"Lọc sớm" (pre-filter) là MẶC ĐỊNH, không phải tuỳ chọn.** Vì P1 đứng **trước** `keyBy` và chỉ phát `RuleInstanceEvent` cho rule khớp, mọi event không có rule nào tham chiếu (phần lớn trong ~10.000 metric/router) **bị bỏ ngay tại P1**, không bao giờ phải serialize + shuffle. Đây chính là lý do thiết kế không cần một operator "pre-filter" riêng: P1 đã đảm nhiệm. Bộ lọc dùng **chính broadcast rule snapshot** nên **tự cập nhật** khi rule được thêm/sửa/xoá — không bao giờ bỏ sót event của một rule vừa tạo (khác với một bộ lọc cấu hình tĩnh). Điều duy nhất không tránh được là **Kafka source phải đọc toàn bộ topic** (Kafka không lọc theo nội dung phía server).

### 5.3 Watermark & xử lý out-of-order / late

- **Watermark theo `eventTimeMs`**, `boundedOutOfOrderness` nhỏ (đề xuất **1–2s**) để giữ độ trễ thấp. Tăng giá trị này = an toàn hơn với đảo thứ tự nhưng tăng độ trễ.
- **"Previous sample" theo event-time:** trong BasicEvaluator giữ `(lastEventTimeMs, lastValue)`. Khi event mới:
  - `eventTimeMs > lastEventTimeMs` → dùng `lastValue` làm prev (cho pct/delta), rồi cập nhật.
  - `eventTimeMs <= lastEventTimeMs` (late/đảo) → coi là **late**: **không** dùng làm cur cho pct/delta (skip emit lần này), **không** cập nhật prev. (Đơn giản, tránh tính sai. Threshold không cần prev nên vẫn xử lý bình thường giá trị của chính event.)
- **Late quá watermark:** Flink coi là late record. Cho threshold vẫn eval (boolean tức thời của chính event); cho pct/delta → skip (đã nêu). Không dùng `allowedLateness` window vì ta không dùng window cho basic.
- **Idle source:** nếu một partition Kafka im lặng, watermark có thể đứng. Điều này **không** ảnh hưởng `no_data` vì §6.5 dùng **processing-time timer** (không phụ thuộc watermark). Cấu hình `withIdleness(...)` để watermark toàn cục vẫn tiến khi một partition rảnh.

### 5.4 Parallelism, partition

- Source parallelism ≤ số partition của `processed_metrics`. Khuyến nghị partition Kafka theo `device_id` để cân tải và tăng locality (không bắt buộc cho tính đúng).
- Sau `keyBy`, Flink tự phân phối key. State backend: **RocksDB** (state lớn, xem §11).

---

## 6. Thiết kế state

### 6.1 Bảng tổng hợp state

| State | Toán tử / key | Kiểu | Mục đích | Bền (checkpoint)? | TTL |
| --- | --- | --- | --- | --- | --- |
| `prevSample` | P2 `(rule,dev,fp)` | `ValueState<{eventTimeMs, value}>` | prev cho pct/delta | Có | ≥ 3× sample interval (vd 180s) |
| `consecHits` | P2 `(rule,dev,fp)` | `ValueState<Integer>` | sustain (basic độc lập) | Có | như prevSample |
| `dedup` (basic) | P2 `(rule,dev,fp)` | `ValueState<{lastState,lastFiredAt,lastResolvedAt}>` | máy trạng thái fire/resolve | **Có (quan trọng)** | không TTL theo thời gian ngắn; dọn khi orphan (§9) |
| `lastSeen` + `noDataTimer` | P2 `(rule,dev,fp)` | `ValueState<Long>` + processing-time timer | no_data | Có | aging theo §6.5 |
| `childBools` | P3 `(composite,dev,fp)` | `MapState<childRuleId,{raw,updatedAtMs}>` | boolean tức thời từng child | Có | ≥ 3× sample interval |
| `consecHits` (comp) | P3 `(composite,dev,fp)` | `ValueState<Integer>` | sustain cấp composite | Có | như childBools |
| `dedup` (comp) | P3 `(composite,dev,fp)` | `ValueState<{...}>` | máy trạng thái composite | **Có** | như dedup basic |

> Dùng `StateTtlConfig` cho các state có TTL để Flink tự dọn series biến mất. **Riêng `dedup` không đặt TTL ngắn** (nếu xoá nhầm sẽ re-fire); dọn theo cơ chế orphan (§9).

### 6.2 `prevSample` (pct / delta)
Lưu `(lastEventTimeMs, lastValue)`. Quy tắc cập nhật theo §5.3. Khi chưa có prev (`null`) → pct/delta **skip** (giống Alert Engine §5.4).

### 6.3 `consecHits` (sustain)
```
sustainGate(raw, consecState, sustainSamples):
    if raw == SKIP: return PREVIOUS_SATISFIED        // không đổi consec, không quyết định mới
    consec = (raw ? consecState+1 : 0)
    consecState = consec
    return consec >= sustainSamples
```
> Khác poll engine: ở đây `consecHits` **được checkpoint** → bền qua restart (poll engine giữ RAM, mất khi restart). Đây là cải thiện, không phải thay đổi ngữ nghĩa.

### 6.4 `dedup` — máy trạng thái fire/refire/resolve
Giữ **nguyên** ngữ nghĩa Alert Engine §5.7. `cycle_now` thay bằng **thời điểm xử lý event/timer hiện tại** (`ctx.timestamp()` cho event-time, hoặc `ctx.timerService().currentProcessingTime()` cho no_data timer).

```
dedupGate(rule, satisfied, st, now):
    if satisfied == SKIP: return NONE                       // thiếu dữ liệu → không đổi state, không emit
    if satisfied == true:
        if st.lastState == OK:
            st = {FIRING, lastFiredAt=now};      return FIRED
        else if (now - st.lastFiredAt) >= rule.dedupSeconds*1000:
            st.lastFiredAt = now;                return REFIRED
        else:                                    return NONE        // suppress (đang FIRING, trong cửa sổ dedup)
    else: // false
        if st.lastState == FIRING:
            st = {OK, lastResolvedAt=now};       return RESOLVED
        else                                     return NONE
```

| lastState | satisfied | Trong cửa sổ dedup? | Event | State mới |
| --- | --- | --- | --- | --- |
| OK | true | — | **FIRED** | FIRING, lastFiredAt=now |
| FIRING | true | Có | *(suppress)* | không đổi |
| FIRING | true | Không | **REFIRED** | FIRING, lastFiredAt=now |
| FIRING | false | — | **RESOLVED** | OK, lastResolvedAt=now |
| OK | false | — | *(no-op)* | OK |
| bất kỳ | SKIP | — | *(no-op)* | không đổi |

> Mỗi lần state đổi (OK↔FIRING) → phát **StateChange** ra side output để mirror sang MariaDB `tlm_alert_state` (§8.3).

### 6.5 `no_data` (timer-driven)
Vì stream không có "event vắng mặt", dùng **processing-time timer**:
```
onEvent (conditionKind==2):
    deleteTimer(prevTimer) nếu có
    lastSeen = ctx.currentProcessingTime()
    register processing-time timer tại lastSeen + noDataSeconds*1000   // lưu timestamp timer vào ValueState để xoá lần sau
    // mỗi sample mới "đẩy lùi" thời điểm fire
    nếu rule là child → đẩy ChildBool(raw=false) lên composite   // data đã về ⇒ no_data = false (v1.3)
onTimer (fired):
    // không có sample mới reset trong noDataSeconds → series im lặng
    satisfied = true; observedValue = (now - lastSeen)/1000   // tuổi im lặng (giây)
    decision = dedupGate(rule, true, dedupState, now)
    nếu independent → emit; nếu child → đẩy ChildBool(raw=true) lên composite
    register **aging timer** tại now + agingTtl (đề xuất ≥ vài lần noDataSeconds):
        khi aging timer fire mà vẫn không có sample → nếu đang FIRING: emit RESOLVED, xoá state, "quên" series
```
Bất biến (giữ từ Alert Engine §5.5):
- **Series mới hoàn toàn** chưa từng có event → **không** có timer → không fire no_data (tránh báo nhầm onboard).
- `observedValue` ghi history = tuổi im lặng (giây).
- Khi series báo lại (event tới) sau khi đã no_data-FIRING: `onEvent` reset timer; và vì series đã "fresh" lại, cần **RESOLVED**. Hiện thực: trong `onEvent` của rule no_data, nếu `dedupState.lastState == FIRING` → emit RESOLVED + set OK (vì đã có sample mới).

> **REFIRED cho no_data (chốt v1.4):** pseudo `onTimer` ở trên **không** đăng ký lại timer no_data — chỉ đăng ký aging timer. Hệ quả (mặc định): mỗi đợt im lặng sinh **đúng một** FIRED, sau đó hoặc resolve-on-return (sample quay lại) hoặc aging-RESOLVED; `REFIRED` **không xảy ra** cho no_data vì `satisfied=true` chỉ phát sinh từ timer. *Tuỳ chọn mở rộng* (nếu vận hành cần nhắc lại theo `dedup_seconds`, đồng nhất với rule số): trong nhánh timer no_data, đăng ký lại timer tại `now + noDataSeconds*1000` — khi đó `dedupGate` tự sinh REFIRED mỗi khi qua cửa sổ dedup. Nếu bật tuỳ chọn này, ghi rõ trong cấu hình triển khai và đồng bộ với tài liệu impl guide.
>
> **processing-time vs event-time cho no_data:** chọn **processing-time** vì no_data là về vắng mặt theo đồng hồ thực; event-time timer phụ thuộc watermark, sẽ không fire nếu cả partition im lặng. Đánh đổi: khi job restart, processing-time timer được khôi phục từ checkpoint nhưng "đồng hồ" đã trôi → Flink fire ngay các timer quá hạn (chấp nhận được; tệ nhất là một lần kiểm tra no_data sớm).

### 6.6 `childBools` (composite)
`MapState<childRuleId, {raw, updatedAtMs}>`. Cập nhật mỗi khi nhận ChildBool. "Child thiếu data" = chưa có entry **hoặc** `updatedAtMs` quá cũ (đề xuất quá `staleness = 2–3× sample interval`). Áp `missingAs`.

### 6.7 Cold start / seed từ MariaDB
- **Khởi động từ checkpoint/savepoint:** state khôi phục đầy đủ → dedup đúng, không re-fire. (Đường bình thường.)
- **Khởi động lạnh (không có state — lần đầu deploy):** để tránh re-fire alert đang FIRING, **seed** `dedup.lastFiredAt`/`lastState` từ MariaDB `tlm_alert_state`. Hai cách:
  1. **State Processor API** dựng savepoint ban đầu từ `tlm_alert_state` (sạch nhất).
  2. **Lazy seed:** lần đầu một key được chạm mà Flink state rỗng → tra `tlm_alert_state` (qua một cache/Async I/O tới MariaDB) để khởi tạo. Đơn giản hơn, chấp nhận một lần tra.
- **Phương án tối giản:** chấp nhận khả năng re-fire một lần khi cold-start (NOC PRO idempotent + cửa sổ dedup hấp thụ phần lớn). Chỉ chọn nếu re-fire hiếm là chấp nhận được.

---

## 7. Pipeline đánh giá theo pha

### 7.1 P1 — FanOut (`BroadcastProcessFunction`)
```
processElement(MetricEvent e, ctx, out):
    rules = broadcast.byMetric[e.metricName]                  // basic rules trên metric này
    for r in rules where r.status active:
        if not pathScopeMatch(r.pathAliasId, e.pathId): continue   // null=any; else == pathId
        if r.scopeDeviceIds != null and e.deviceId not in r.scopeDeviceIds: continue
        if not entityFilterMatch(r.entityFilter, e.labels): continue   // null=match-all; key⊆entityKeys; exact/IN
        fp = fingerprint(e.labels, r.entityKeys)
        out.collect( RuleInstanceEvent{
            ruleId=r.id, deviceId=e.deviceId, deviceName=e.deviceName,
            entityFingerprint=fp, entityMap=subset(e.labels, r.entityKeys),
            valueType=e.valueType, valueNumber=e.valueNumber,
            eventTimeMs=e.eventTimeMs, metricName=e.metricName
        })
processBroadcastElement(ruleUpdate, ctx): cập nhật broadcast state + chỉ mục phụ (§3.2, §9)
```
> `pathScopeMatch`, `entityFilterMatch` thuần hàm, không state. `entityFilter` value so khớp **exact / IN-list** trên giá trị label đã chuẩn hoá (v1.3).
> **P1 là điểm lọc sớm chính thức:** vòng lặp trên chỉ `collect` khi có rule khớp; nếu `byMetric[e.metricName]` rỗng hoặc không rule nào qua được path/device/entity_filter → **không phát gì** → event bị loại trước `keyBy`. Không cần operator pre-filter riêng (§5.2).

### 7.2 P2 — BasicEvaluator (`KeyedBroadcastProcessFunction`, key = rule_id, device, fp)
```
processElement(RuleInstanceEvent ev, broadcastCtx, out):
    r = broadcastCtx.getBroadcastState(RULE_STATE).get(ev.ruleId)   // đọc tham số rule từ broadcast
    raw = evalRaw(r, ev, prevSampleState)   // §7.5; có thể = SKIP
    updatePrevSample(ev)                    // theo §5.3

    isChild       = childToComposites(broadcast).contains(r.id)

    if conditionKind == 2:                  // no_data: không tính raw ở đây
        resetNoDataTimer(ctx, r)
        if isChild:                         // báo composite: data đã về ⇒ no_data = FALSE
            for cId in childToComposites[r.id]:
                ctx.output(CHILD_TAG, ChildBool{compositeId=cId, deviceId, fp,
                                                childRuleId=r.id, raw=false, eventTimeMs})
        if isIndependentFiring(r) and dedupState.lastState == FIRING:  // series báo lại → resolve
            emit RESOLVED; dedupState=OK
        return

    // mọi basic status=1 fire ĐỘC LẬP theo mặc định (xem ghi chú bên dưới)
    isIndependent = isIndependentFiring(r)

    if isChild:
        for cId in childToComposites[r.id]:
            ctx.output(CHILD_TAG, ChildBool{compositeId=cId, deviceId, fp, childRuleId=r.id, raw, eventTimeMs})
            // LƯU Ý: child đẩy RAW (boolean tức thời), KHÔNG sustain (sustain ở cấp composite)

    if isIndependent:
        satisfied = sustainGate(raw, consecHitsState, r.sustainSamples)
        decision  = dedupGate(r, satisfied, dedupState, ev.eventTimeMs)
        if decision != NONE: out.collect(AlertDecision{...})   // kèm StateChange nếu state đổi

onTimer(ts, ctx, out):                       // no_data fire / aging — §6.5
    ... emit FIRED/RESOLVED theo §6.5
    ... nếu rule là child: đẩy ChildBool(raw=true) lên composite (§6.5)
```

> **Một rule vừa độc lập vừa là child:** tính `raw` **một lần**; nhánh child dùng `raw`, nhánh độc lập dùng `sustainGate(raw)`.
>
> **Ngữ nghĩa fire độc lập (làm rõ v1.3):** **mặc định mọi basic `status=1` đều fire độc lập** — kể cả khi nó đang là child của một composite (nó vừa phát alert riêng, vừa cấp boolean cho composite). Nếu operator muốn một basic **chỉ** làm child (không gây alert riêng để giảm nhiễu), cần một cờ điều khiển `isIndependentFiring(r)` — gợi ý hiện thực bằng một cột boolean trên `tlm_alert_rules` (vd `emit_independent`, mặc định `1`). **Quy ước này phải khớp tài liệu Alert Engine HLD gốc**; nếu gốc chưa định nghĩa, chốt mặc định "luôn fire độc lập" và ghi vào Phụ lục A. `isIndependentFiring` trả `true` theo mặc định.

### 7.3 P3 — CompositeEvaluator (`KeyedBroadcastProcessFunction`, key = composite_id, device, fp)
```
processElement(ChildBool cb, broadcastCtx, out):
    comp = compositeById(broadcast).get(cb.compositeId)   // đọc tham số composite từ broadcast
    childBools.put(cb.childRuleId, {cb.raw, now})
    combined = combine(comp, childBools, now)     // §dưới
    if combined == SKIP: return                   // mọi child thiếu data
    satisfied = sustainGate(combined, consecHitsState, comp.sustainSamples)
    decision  = dedupGate(comp, satisfied, dedupState, cb.eventTimeMs)
    if decision != NONE: out.collect(AlertDecision{...})

combine(comp, childBools, now):
    results = []
    for childId in comp.childRuleIds:
        e = childBools.get(childId)
        hasData = (e != null && now - e.updatedAtMs <= staleness)
        if hasData:        results.add(e.raw)
        else if comp.missingAs == SKIP: continue          // bỏ child khỏi AND/OR
        else:              results.add(false)             // missingAs=false (mặc định)
    if results rỗng: return SKIP
    return (comp.logicalOp == AND) ? all(results) : any(results)
```
> **Child going stale (không báo nữa):** đăng ký một processing-time timer định kỳ (vd mỗi `staleness/2`) trên key composite để **tái đánh giá** ngay cả khi không có ChildBool mới — bắt trường hợp AND đang true rồi một child biến mất (phải chuyển false/skip → có thể RESOLVED). (Tùy chọn ở Pha 3; có thể bỏ nếu chấp nhận chỉ tái đánh giá khi có child update.)

### 7.4 Ngữ nghĩa từng `condition_kind` (boolean tức thời `raw`) — `evalRaw`
Giữ **nguyên** Alert Engine §5.4. `cmp(a,b)` áp `comparator`.

| `conditionKind` | `raw` | SKIP khi |
| --- | --- | --- |
| `0` threshold | `cmp(valueNumber, threshold)` | `valueType != 1` |
| `1` pct_change_prev | `p=(cur−prev)/prev*100`; `pctAbs=1` → `|p|`; `raw=cmp(p,threshold)` | `prev==null` **hoặc** `prev==0` |
| `2` no_data | (xử lý bằng timer §6.5, không qua `evalRaw`) | series chưa từng có event |
| `3` abs_delta_prev | `d=cur−prev`; `pctAbs=1` → `|d|`; `raw=cmp(d,threshold)` | `prev==null` |

> **SKIP** ≠ false: không +1/không reset `consecHits`, không vào dedup gate. (Quan trọng để không resolve nhầm khi thiếu prev.)

### 7.5 Ví dụ truy vết end-to-end

Giúp dev (và AI review) kiểm tra logic. Giả định `sample interval = 60s`.

**VD1 — threshold, `sustainSamples=2`, `dedupSeconds=1800`, rule độc lập.** Rule: `if_in_errors > 100`, `entityKeys=["if_name"]`. Interface `Gi0/0/0/1`, device 42 → key `(rule=101, dev=42, fp="if_name=Gi0/0/0/1")`.

| t | valueNumber | raw (`>100`) | consecHits | satisfied | dedup state | Event |
| --- | --- | --- | --- | --- | --- | --- |
| 0s | 80 | false | 0 | false | OK | — |
| 60s | 150 | true | 1 | false (chưa đủ sustain) | OK | — |
| 120s | 160 | true | 2 | **true** | OK→FIRING (`lastFiredAt=120s`) | **FIRED** |
| 180s | 170 | true | 3 | true | FIRING, trong dedup (180−120<1800) | *(suppress)* |
| 240s | 50 | false | 0 | false | FIRING→OK (`lastResolvedAt=240s`) | **RESOLVED** |

**VD2 — pct_change_prev, `pctAbs=1`, ngưỡng `>50`%.** Cùng key. Cần prev:

| t | cur | prev | p=`|(cur−prev)/prev*100|` | raw | Ghi chú |
| --- | --- | --- | --- | --- | --- |
| 0s | 100 | null | — | **SKIP** | mới 1 sample (consec không đổi) |
| 60s | 160 | 100 | 60% | true | đủ prev |
| 120s | 0 (prev=160) | 160 | 100% | true | — |
| 180s | 50 (prev=0) | 0 | chia 0 | **SKIP** | `prev==0` → skip, không resolve nhầm |

**VD3 — no_data, `noDataSeconds=180`.** Series từng có dữ liệu rồi im lặng:

| Thời điểm (processing time) | Hành động |
| --- | --- |
| sample tới lúc 0s | `lastSeen=0`; đặt timer tại 180s |
| sample tới lúc 60s | xoá timer cũ; `lastSeen=60`; đặt timer tại 240s |
| (không sample sau 60s) | tại **240s** timer fire → no_data **FIRED**, `observedValue=(240−60)=180s` |
| sample tới lúc 300s | `onEvent`: `lastState==FIRING` → **RESOLVED**; reset timer tại 480s |

**VD4 — composite AND**, child A=`if_in_errors>100`, child B=`if_in_discards>50`, `missingAs=0`, key `(composite=200, dev=42, fp="if_name=Gi0/0/0/1")`:

| Sự kiện | childBools | combined (AND) | Ghi chú |
| --- | --- | --- | --- |
| A.raw=true tới | {A:true} | B chưa có → `missingAs=0` ⇒ false | AND chưa thoả |
| B.raw=true tới | {A:true, B:true} | true | → sustain(comp) → dedup → có thể FIRED |
| A.raw=false tới | {A:false, B:true} | false | → nếu đang FIRING ⇒ RESOLVED |

---

## 8. Hợp đồng đầu ra

### 8.1 `AlertEvent` → Kafka `alerts`
Payload **giống Alert Engine §5.7.3** (NOC PRO không phải đổi):
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
- `event_time` = thời điểm event/timer kích hoạt quyết định.
- Composite: `metric_name=""`, `detail.children=[{child_rule_id, raw}, ...]`.
- no_data: `observed_value` = tuổi im lặng (giây).

### 8.2 `alert_history` → ClickHouse
Ghi **đúng schema** `ipms.alert_history` (Alert Engine §4.3): `event_time, event_type, rule_id, rule_name, rule_kind, severity, device_id, device_name, entity (Map), metric_name, condition_kind, observed_value, threshold, detail`.

### 8.3 Mirror `tlm_alert_state` → MariaDB
Khi `dedupGate` đổi state (OK↔FIRING), upsert `(rule_id, device_id, entity_fingerprint, last_state, last_fired_at, last_resolved_at, updated_at)`. Upsert **idempotent** theo PK. Phục vụ dashboard AL-09/AL-10. **Không** phải nguồn quyết định dedup (Flink state mới là nguồn).

### 8.4 Bất biến emit & delivery
- **History-first:** ghi `alert_history` trước, publish Kafka `alerts` sau (như Alert Engine §5.7.3). Nếu publish lỗi → retry/backoff; history vẫn còn.
- **Delivery (khuyến nghị):** **at-least-once** + NOC PRO idempotent (khớp thiết kế hiện tại). Event có thể trùng khi retry/replay.
- **Tùy chọn exactly-once outbound:** KafkaSink với `DeliveryGuarantee.EXACTLY_ONCE` (transactional, gắn checkpoint). Đánh đổi: tăng độ trễ (commit theo checkpoint) — cân nhắc với SLO 5s.
- **alert_history (ClickHouse):** at-least-once (có thể trùng row audit khi retry); khử ở query nếu cần hoặc chấp nhận.

### 8.5 `EmitFunction` (chi tiết hiện thực)

Nhận union `AlertDecision` từ P2 + P3. Mục tiêu: **history-first**, không chặn alert-path, idempotent ở downstream.

```
processElement(AlertDecision d):
    1. Ghi alert_history (ClickHouse):
         - Dùng ClickHouse sink có buffer (batch theo size/flush-interval nhỏ, vd 1–2s) để giữ độ trễ.
         - Đây là "nguồn sự thật audit": ưu tiên ghi thành công.
    2. Publish AlertEvent (Kafka alerts):
         - key Kafka = (rule_id, device_id, entity_fingerprint) → giữ thứ tự per-instance.
         - giá trị = AlertEvent JSON (§8.1).
    3. Nếu d.stateChanged == true:
         - side output StateChange → MariaDB upsert tlm_alert_state (§8.3).
```

Bất biến & lưu ý:
- **Idempotency key downstream:** `(rule_id, device_id, entity_fingerprint, event_type, event_time)` — NOC PRO khử trùng theo khoá này (at-least-once cho phép trùng khi retry/replay từ checkpoint).
- **Thứ tự per-instance:** dùng Kafka key như trên để FIRED→RESOLVED không bị đảo cho cùng một instance. Giữa các instance không cần thứ tự.
- **Không chặn:** sink dùng buffer + async; lỗi tạm thời → retry/backoff, KHÔNG ném exception làm fail cycle (trừ lỗi không phục hồi). Backpressure từ sink sẽ tự làm chậm source (cơ chế Flink) — chấp nhận, vẫn đúng.
- **Tách sink:** `alert_history` (ClickHouse) và `alerts` (Kafka) là hai sink độc lập; nếu một bên lỗi kéo dài, bên kia vẫn hoạt động (không atomic giữa hai — chấp nhận theo nguyên tắc history-first + idempotent).

---

## 9. Rule động (broadcast refresh)

- **Nguồn rule snapshot (chọn 1):**
  1. **Broadcast từ topic** (nếu có publisher relay `tlm_alert_rules` → Kafka, giống mô hình rule khác). Ưu tiên — đẩy gần realtime.
  2. **Polling MariaDB** `SELECT ... WHERE status=1` (hoặc `WHERE updated_at > lastSeen`) định kỳ (≤ vài chục giây) trong một source phụ, rồi `.broadcast()`. Đơn giản, độ trễ rule = chu kỳ poll.
- **Load lần đầu (gating):** trước khi broadcast state có rule, BasicEvaluator/CompositeEvaluator **không có rule để áp** → các event tới sớm hơn lần load đầu sẽ **không sinh alert** (no-op). Để tránh bỏ sót khi vừa khởi động: (a) **chặn** xử lý event cho tới khi broadcast nhận snapshot đầu tiên (buffer ngắn / chờ một "ready" signal trên broadcast), **hoặc** (b) chấp nhận bỏ qua vài giây đầu (rủi ro thấp vì sample 60s). Khuyến nghị (a) bằng cách phát một bản ghi "snapshot loaded" trên broadcast và chỉ eval khi cờ này bật.
- **Atomic-ish:** mỗi lần refresh dựng lại `byMetric`/`childToComposites`/`compositeById`; thay nguyên bộ trong broadcast state.
- **`status=0` (disable/soft-delete):** rule biến khỏi snapshot → ngừng đánh giá ở event kế. Với instance đang FIRING của rule đó → cần **RESOLVED**: một cơ chế dọn orphan (xem dưới).
- **Đổi `entityKeys`:** `entity_fingerprint` đổi → state cũ thành **orphan**. **Đổi `entityFilter`** KHÔNG đổi fingerprint (chỉ thu hẹp/mở rộng tập instance — v1.3).
- **Dọn orphan + RESOLVED:** Flink keyed state không tự biết rule đã biến mất. Hai cách:
  1. Khi BasicEvaluator/CompositeEvaluator xử lý event mà rule tra trong broadcast trả null (rule đã disable) **và** `dedupState.lastState==FIRING` → emit RESOLVED + xoá state. (Chỉ kích hoạt khi còn event cùng key tới.)
  2. State TTL cho dedup đặt một giá trị an toàn dài; hoặc một job/operator quét định kỳ phát RESOLVED cho rule vừa disable (đọc danh sách rule disable gần đây từ broadcast, đối chiếu state). Khuyến nghị (1) là chính, (2) bổ trợ cho series không còn event.

---

## 10. Xử lý lỗi & trường hợp biên

| Tình huống | Hành vi |
| --- | --- |
| `prev==0` (pct) | SKIP (không fire, không chia 0). |
| `prev==null` (mới 1 sample) | SKIP; chờ sample kế. |
| Event đảo thứ tự / late (eventTime ≤ lastEventTime) | pct/delta: SKIP + không cập nhật prev; threshold: vẫn eval giá trị event. |
| `valueType ∈ {2,3}` cho điều kiện số | Bỏ qua (không skip-resolve). |
| Series mới onboard | no_data KHÔNG fire (chưa từng có event → không timer). threshold/pct eval khi đủ sample. |
| Composite child thiếu data | Theo `missingAs` (mặc định false), §7.3. |
| Composite child khác `entityKeys` | Chặn ở API; không xảy ra runtime. |
| Idle Kafka partition | no_data dùng processing-time timer nên vẫn fire; cấu hình watermark `withIdleness`. |
| Job restart (từ checkpoint) | State khôi phục → dedup đúng, không re-fire. processing-time timer quá hạn fire ngay (chấp nhận). |
| Cold start (không state) | Seed từ `tlm_alert_state` (§6.7) hoặc chấp nhận re-fire một lần. |
| Kafka rebalance / scale-out | Flink rescale từ savepoint; key redistribute kèm state. Không reprocess quá khứ ngoài checkpoint. |
| Rule update giữa luồng | Broadcast cập nhật; cycle/event kế dùng rule mới. Đang FIRING + rule disable → RESOLVED (§9). |
| ClickHouse history sink fail | Retry/backoff; **không** chặn publish `alerts` quá lâu — buffer; ưu tiên không mất audit (history-first nhưng có hàng đợi). |
| MariaDB mirror fail | Không chặn alert-path; retry upsert. Dashboard có thể trễ phản ánh state — chấp nhận. |
| `derived_metrics` chưa được Job 2 emit | Alert trên derived metric **không hoạt động** cho tới khi Job 2 (≥ v2.5, hiện hành v2.6) emit topic này. Vì không còn poll, đây là **chặn cứng** cho alert derived → triển khai Job 2 ≥ v2.5 **trước/đồng thời** (§2.3). Alert trên raw vẫn chạy bình thường. |

---

## 11. Yêu cầu phi chức năng

| Yêu cầu | Giá trị |
| --- | --- |
| Alerting latency SLO | **≤ 5s** (đo từ lúc metric sẵn sàng trên `processed_metrics`); thực tế ~1–3s (§2.4) |
| Throughput input | ~50 triệu metrics/phút (~833K/s) toàn fleet. **Kafka source đọc toàn bộ topic** (không lọc server-side được), nhưng **P1 lọc sớm trước `keyBy`** nên chỉ phần event có rule mới đi vào shuffle + state (xem dòng dưới). |
| Lọc sớm (mặc định — trong P1) | **Không phải optimization tuỳ chọn.** P1 (`FanOutFunction`, trước `keyBy`) bỏ mọi event không khớp rule nào → phần lớn ~10.000 metric/router không có alert rule bị loại ngay, không serialize/shuffle. Bộ lọc dùng broadcast rule snapshot nên tự cập nhật theo rule (§5.2, §7.1). *Tuỳ chọn tinh chỉnh (không bắt buộc):* nếu profiling cho thấy chính P1 là nút thắt, có thể chèn một filter cực nhẹ (chỉ tra tập `metricName` có rule) chained ngay sau source để giảm tải dựng `RuleInstanceEvent` — nhưng về mặt loại bỏ shuffle thì không thêm lợi ích so với P1. |
| Rule volume | 500–2,000 active |
| Firing rate (sự cố lớn) | 100–1,000 alert/giờ |
| State backend | **RocksDB** (incremental checkpoint) |
| State size (ước lượng) | ≈ Σ số instance `(rule, device, entity)` đang theo dõi. Vd 1,000 rule × ~vài chục–trăm instance → hàng trăm nghìn–triệu key × vài trăm byte → cỡ GB. Tuning RocksDB + TTL. |
| Checkpoint interval | đề xuất 10–30s (cân giữa độ phục hồi và overhead); aligned hoặc unaligned tuỳ backpressure |
| Delivery | at-least-once + NOC PRO idempotent (mặc định); exactly-once outbound là tùy chọn (§8.4) |
| Availability | Tự recover từ checkpoint/savepoint; outbound `alerts` retry/buffer |
| Rule refresh latency | ≤ vài chục giây (source phụ poll MariaDB) hoặc gần realtime (broadcast qua Kafka topic rule) — §9 |

---

## 12. Observability

Flink metrics (expose Prometheus/JMX) đề xuất:
- `alert_fired_total{rule_id,event_type}` — đếm FIRED/REFIRED/RESOLVED.
- `alert_eval_latency_ms` — từ `eventTimeMs`/ingest tới lúc emit (histogram).
- `alert_suppressed_total{rule_id}` — số lần suppress trong cửa sổ dedup.
- `alert_skip_total{reason}` — prev null / div0 / late / wrong-type.
- `nodata_timers_active` — số timer no_data đang treo.
- `composite_missing_child_total{composite_id}` — child thiếu data.
- `kafka_consumer_lag`, `watermark_lag_ms`, `state_size_bytes`, `checkpoint_duration_ms`, `busy/backpressure`.
- **Validation diff (tùy chọn, chỉ khi có nguồn đối chiếu):** `validation_decision_mismatch_total{rule_id,type}` — lệch so với một nguồn tham chiếu (recompute từ ClickHouse, hoặc poll engine cũ nếu còn chạy tạm trong giai đoạn chuyển đổi). Mục tiêu = 0 trước khi mở rộng. (Khi greenfield, dùng đối chiếu recompute thay cho poll.)

---

## 13. Lộ trình triển khai

Vì **không còn poll engine ở trạng thái cuối**, lộ trình tập trung vào dựng Job 3 an toàn theo độ phức tạp tăng dần. Có thể chạy poll engine **tạm thời** trong lúc chuyển đổi (nếu nó đã được triển khai trước đó) để đối chiếu, rồi gỡ hẳn.

1. **Pha 0 — Chuẩn bị:** chốt hàm `entity_fingerprint` chuẩn (§3.3); triển khai **Job 2 ≥ v2.5 (hiện hành v2.6)** emit `derived_metrics` (điều kiện cho alert derived — §2.3); dựng `KafkaSource` + watermark + broadcast rule source.
2. **Pha 1 — Basic số (threshold/abs_delta/pct):** hiện thực FanOut + BasicEvaluator + EmitFunction. **Validation:** chạy đối chiếu bằng recompute từ ClickHouse hoặc (nếu còn) poll engine cũ; ghi history vào nhãn riêng cho tới khi `validation_decision_mismatch_total ≈ 0`, rồi bật emit thật ra NOC PRO.
3. **Pha 2 — no_data:** thêm processing-time timer + aging (§6.5); seed `tlm_alert_state` cold-start (§6.7).
4. **Pha 3 — Composite:** thêm CompositeEvaluator (P3); kiểm thử alignment + staleness + `missing_as`.
5. **Pha 4 (tùy chọn, hạ tầng) — true sub-5s event→alert:** bật gNMI on-change cho nhóm metric tới hạn (tầng thu thập gNMIc) để phá trần sampling 60s. Không chặn các pha trên.

> **Nguyên tắc an toàn:** mỗi pha mở rộng tập rule dần; luôn có cờ bật/tắt emit thật theo nhóm rule (đọc từ rule/snapshot) để rollback nhanh nếu phát hiện sai lệch. Khi tự tin → **gỡ poll engine** khỏi vận hành.

---

## 14. Thuật ngữ

| Thuật ngữ | Định nghĩa |
| --- | --- |
| `MetricEvent` | Message metric đã chuẩn hoá trên `processed_metrics` (§3.1) |
| `RuleInstanceEvent` | Event sau fan-out, mang khoá `(rule_id, device_id, entity_fingerprint)` |
| `ChildBool` | Boolean tức thời của một child rule, route lên composite |
| `AlertDecision` | Quyết định FIRED/REFIRED/RESOLVED trước khi emit |
| `entity_fingerprint` | Chuỗi tất định từ giá trị `entity_keys` (§3.3) |
| SKIP | Trạng thái "thiếu dữ liệu" — không +1/không reset sustain, không vào dedup gate |
| broadcast state | Rule snapshot phát tới mọi operator để rule động không cần restart |
| keyed state | State theo `(rule/composite, device, entity)` — prev/sustain/dedup/no_data |
| fast path | Nhóm rule độ-trễ-thấp đánh giá bằng Job 3 |
| validation đối chiếu | Chạy Job 3 và so quyết định với nguồn tham chiếu (recompute ClickHouse hoặc poll engine cũ nếu còn) trước khi mở rộng emit thật |

---

## Phụ lục A — DDL tham chiếu (dựng lại, v1.3)

> **⚠ Trạng thái:** Các DDL dưới đây được **dựng lại từ những trường mà tài liệu này tham chiếu** (POJO §3.2, state §6, output §8.2/§8.3). Chúng **chưa** thay thế tài liệu Alert Engine HLD gốc (`03-alert_engine_hld`). Mục tiêu: cho dev một điểm khởi đầu **tự thực thi được** khi tài liệu gốc chưa sẵn sàng. **Bắt buộc đối chiếu** lại tên cột/kiểu/CHECK với tài liệu gốc nếu/khi nó tồn tại; nếu lệch, tài liệu gốc thắng. Quy ước chung tuân theo bộ schema hiện có (MariaDB v4.3 / ClickHouse v4.2): không ENUM, không FK, versioning bằng `updated_at`, `pushed_at` là cờ mirror.

### A.1 MariaDB `tlm_alert_rules`

```sql
CREATE TABLE IF NOT EXISTS tlm_alert_rules (
    id              INT UNSIGNED NOT NULL AUTO_INCREMENT,
    rule_name       VARCHAR(128) NOT NULL,
    rule_kind       TINYINT UNSIGNED NOT NULL
        COMMENT '0=basic, 1=composite.',
    severity        TINYINT UNSIGNED NOT NULL DEFAULT 1
        COMMENT '0=info, 1=warning, 2=error, 3=critical (v1.3).',

    -- Nhóm đối tượng & phạm vi (áp cho cả basic lẫn composite)
    entity_keys     JSON NOT NULL
        COMMENT 'JSON array of string. [] = device-level. Sinh entity_fingerprint (§3.3).',
    entity_filter   JSON NULL
        COMMENT 'v1.3: Map<labelKey, JSON array value>. NULL = mọi giá trị. key ⊆ entity_keys.
                 Predicate chọn instance trước eval; KHÔNG đổi fingerprint.',
    scope_device_ids JSON NULL
        COMMENT 'JSON array of INT (device_id). NULL = mọi thiết bị.',
    sustain_samples INT UNSIGNED NOT NULL DEFAULT 1
        COMMENT '>=1. Số lần evaluate liên tiếp đúng mới satisfied.',
    dedup_seconds   BIGINT UNSIGNED NOT NULL DEFAULT 3600
        COMMENT '>=0. Cửa sổ suppress re-fire cùng (rule, device, entity).',
    emit_independent TINYINT UNSIGNED NOT NULL DEFAULT 1
        COMMENT 'v1.3: 1=basic fire alert độc lập (kể cả khi là child composite). 0=chỉ làm child.
                 Chỉ áp cho rule_kind=0. Chốt mặc định nếu Alert Engine HLD gốc chưa định nghĩa (§7.2).',

    -- ── basic (rule_kind=0) ───────────────────────────────────────────
    alias_metric    VARCHAR(128) NULL
        COMMENT 'Metric đối tượng. Khớp MetricEvent.metricName. NOT NULL khi rule_kind=0.',
    path_alias_id   INT UNSIGNED NULL
        COMMENT 'NULL = any-path; giá trị cụ thể = path_id; derived metric = 0. Logic-FK tlm_path_aliases.id.',
    condition_kind  TINYINT UNSIGNED NULL
        COMMENT '0=threshold, 1=pct_change_prev, 2=no_data, 3=abs_delta_prev. NOT NULL khi rule_kind=0.',
    comparator      TINYINT UNSIGNED NULL
        COMMENT '0=>,1=>=,2=<,3=<=,4===,5=!=. NULL khi condition_kind=2 (no_data).',
    threshold       DOUBLE NULL
        COMMENT 'Ngưỡng so sánh. NULL khi condition_kind=2.',
    pct_abs         TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '1 = dùng trị tuyệt đối |.| cho condition_kind IN (1,3).',
    no_data_seconds BIGINT UNSIGNED NULL
        COMMENT '>0. Bắt buộc khi condition_kind=2; vắng với các kind khác.',

    -- ── composite (rule_kind=1) ───────────────────────────────────────
    logical_op      TINYINT UNSIGNED NULL
        COMMENT '0=AND, 1=OR. NOT NULL khi rule_kind=1.',
    child_rule_ids  JSON NULL
        COMMENT 'JSON array of INT. Các basic child (cùng entity_keys). NOT NULL khi rule_kind=1.',
    missing_as      TINYINT UNSIGNED NULL
        COMMENT '0=false, 1=skip. Cách xử lý child thiếu data. NOT NULL khi rule_kind=1.',

    status          TINYINT UNSIGNED NOT NULL DEFAULT 1
        COMMENT '1=Active, 0=Disabled.',
    pushed_at       DATETIME(6) NULL
        COMMENT 'Cờ mirror (nếu mirror sang ClickHouse). NULL = chưa sync.',
    created_by      INT UNSIGNED NOT NULL,
    created_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                    ON UPDATE CURRENT_TIMESTAMP(6)
        COMMENT 'Version đơn điệu — broadcast refresh dùng để pick thay đổi.',

    PRIMARY KEY (id),
    KEY idx_alert_rules_status      (status),
    KEY idx_alert_rules_kind        (rule_kind),
    KEY idx_alert_rules_alias       (alias_metric),
    KEY idx_alert_rules_updated_at  (updated_at),

    CONSTRAINT chk_ar_kind      CHECK (rule_kind IN (0,1)),
    CONSTRAINT chk_ar_severity  CHECK (severity IN (0,1,2,3)),
    CONSTRAINT chk_ar_status    CHECK (status IN (0,1)),
    CONSTRAINT chk_ar_sustain   CHECK (sustain_samples >= 1),
    -- basic phải có alias_metric + condition_kind; composite phải có logical_op + child_rule_ids + missing_as
    CONSTRAINT chk_ar_basic     CHECK (rule_kind <> 0
        OR (alias_metric IS NOT NULL AND condition_kind IN (0,1,2,3))),
    CONSTRAINT chk_ar_composite CHECK (rule_kind <> 1
        OR (logical_op IN (0,1) AND child_rule_ids IS NOT NULL AND missing_as IN (0,1))),
    -- no_data: no_data_seconds bắt buộc, comparator/threshold phải vắng
    CONSTRAINT chk_ar_nodata    CHECK (condition_kind IS NULL OR condition_kind <> 2
        OR (no_data_seconds IS NOT NULL AND comparator IS NULL AND threshold IS NULL)),
    -- threshold/pct/abs_delta: comparator + threshold bắt buộc
    CONSTRAINT chk_ar_cmp       CHECK (condition_kind IS NULL OR condition_kind NOT IN (0,1,3)
        OR (comparator IN (0,1,2,3,4,5) AND threshold IS NOT NULL))
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci
  COMMENT = 'Alert rules (reconstructed v1.3). basic + composite. Đối chiếu Alert Engine HLD gốc nếu có.';
```

> **Ràng buộc tầng ứng dụng (không biểu diễn được bằng CHECK):** mọi `child_rule_ids` phải trỏ tới basic (`rule_kind=0`) `status=1`, **cùng `entity_keys`** với composite, không tự tham chiếu, không lồng composite (AL-06). Kiểm ở API.

### A.2 MariaDB `tlm_alert_state` (mirror — dashboard đọc)

```sql
CREATE TABLE IF NOT EXISTS tlm_alert_state (
    rule_id            INT UNSIGNED NOT NULL,
    device_id          INT UNSIGNED NOT NULL,
    entity_fingerprint VARCHAR(512) NOT NULL
        COMMENT 'Sinh từ hàm canonical §3.3. "" = device-level.',
    last_state         TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '0=ok, 1=firing.',
    last_fired_at      DATETIME(6) NULL,
    last_resolved_at   DATETIME(6) NULL,
    updated_at         DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                       ON UPDATE CURRENT_TIMESTAMP(6),

    PRIMARY KEY (rule_id, device_id, entity_fingerprint),
    KEY idx_alert_state_state (last_state),
    CONSTRAINT chk_as_state CHECK (last_state IN (0,1))
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci
  COMMENT = 'Mirror runtime state cho dashboard AL-09/AL-10 + seed cold-start. NGUỒN quyết định dedup là Flink state (§1.3), KHÔNG phải bảng này.';
```

### A.3 ClickHouse `ipms.alert_history` (audit, TTL 90 ngày)

```sql
CREATE TABLE IF NOT EXISTS ipms.alert_history
(
    event_time     DateTime64(3, 'Asia/Ho_Chi_Minh')
        COMMENT 'Thời điểm event/timer kích hoạt quyết định.',
    event_type     LowCardinality(String)
        COMMENT '"fired" | "refired" | "resolved".',
    rule_id        UInt32,
    rule_name      String,
    rule_kind      UInt8        COMMENT '0=basic, 1=composite.',
    severity       UInt8        COMMENT '0=info,1=warning,2=error,3=critical.',
    device_id      UInt32,
    device_name    LowCardinality(String),
    entity         Map(LowCardinality(String), String)
        COMMENT 'Subset labels theo entity_keys. Khớp entity_fingerprint §3.3.',
    metric_name    LowCardinality(String)
        COMMENT '"" với composite.',
    condition_kind Nullable(UInt8)
        COMMENT 'NULL với composite.',
    observed_value Float64
        COMMENT 'cur | pct | tuổi no_data (giây).',
    threshold      Nullable(Float64)
        COMMENT 'NULL với no_data/composite.',
    detail         String
        COMMENT 'JSON text: {"prev_value":..} hoặc {"children":[..]}.'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (rule_id, device_id, event_time)
TTL toDateTime(event_time) + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;
```

> **At-least-once:** §8.4 cho phép row audit trùng khi retry. Nếu cần "đúng một dòng/quyết định" khi query, khử trùng theo `(rule_id, device_id, entity, event_type, event_time)` ở tầng truy vấn, hoặc dùng `ReplacingMergeTree` với version. Mặc định giữ `MergeTree` để không ẩn mất sự kiện.
