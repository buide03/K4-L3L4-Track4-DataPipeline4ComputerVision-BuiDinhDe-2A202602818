# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Bùi Đình Đề |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602818` |
| GitHub | `buide03` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `98f481913304`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `697 / 157 / 165` (tổng 1019 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Từ `scripts/lab.sh step 1 --reset`: bronze có `raw_telemetry_a` 3021 dòng, `raw_telemetry_b` 3234 dòng (bằng đúng số dòng CSV), 26 trip, 2 video. Khác biệt giữa A và B:

1. **Tên cột**: A dùng `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer`; B dùng `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering`.
2. **Đơn vị tốc độ**: A là m/s (`1.213`), B là km/h (`55.11`).
3. **Kiểu thời gian**: A là chuỗi ISO-8601 UTC (`2025-10-07T08:00:00.014Z`), B là epoch mili giây (`1759933801509`).
4. **Đồng hồ lệch**: dòng đầu của B là `1759933801509` = 14:30:01.509 UTC, trong khi video B bắt đầu lúc 14:30:00. `bronze_video_files` ghi `telemetry_clock_offset_ms` = 1500 cho drive_b và 0 cho drive_a.
5. **Thang ga/phanh**: A dùng tỷ lệ 0–1 (`gas 0.028`), B dùng phần trăm (`throttle_pct 2.0`).

Mọi cột dữ liệu đều là `VARCHAR`. Ô trống được giữ nguyên, ví dụ có 30 dòng thiếu tốc độ ở mỗi nguồn. Bronze chỉ thêm các cột metadata `_source_file, _batch_id, _row_no, _ingested_at`.

**Vì sao mỗi nguồn một bảng và để dạng chữ:** bronze là bản sao trung thực của nguồn. Nếu gộp hoặc ép kiểu ngay, mọi lỗi diễn giải (nhầm km/h với m/s, quên trừ 1500 ms, ép `''` thành NULL hoặc 0) sẽ bị "đóng băng" vào dữ liệu gốc. Khi đó ta không còn dữ liệu thô để sửa logic và chạy lại. Giữ nguyên dạng chữ cũng có nghĩa là dòng hỏng không làm sập quá trình nạp và không bị mất âm thầm: dòng lỗi vẫn vào được bronze để silver cách ly có lý do. Mỗi nguồn một bảng còn giúp schema của từng nhà cung cấp tự thay đổi độc lập (schema drift) mà không phá bảng của nguồn kia.

Pipeline nạp mỗi drive **hai lần**. Lần hai báo `already present`, vì file được nhận diện bằng sha256 (A `636ba1a9…`, B `bfa62703…`) chứ không phải theo tên. `cli history bronze.raw_telemetry_a` chỉ có **1 commit** (version 0, WRITE, 3021 dòng), nên chạy lại không làm nhân đôi dữ liệu.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Pipeline có hai model staging, `stg_telemetry_a.sql` và `stg_telemetry_b.sql`, đưa hai nguồn về cùng các cột `ts, speed_mps, heading_deg, lat, lon, …, gas, brake, steer`:
- **Tên cột**: các cột của B (`spd_kmh, hdg, latitude, throttle_pct…`) được đổi tên theo A.
- **Đơn vị**: `spd_kmh / 3.6` để ra m/s; `throttle_pct, brake_pct / 100` để về thang 0–1 như A.
- **Kiểu thời gian**: A parse chuỗi ISO; B dùng `to_timestamp(timestamp_ms / 1000)`. Cả hai ra `timestamp with time zone` UTC.
- **Độ lệch đồng hồ**: B trừ `telemetry_clock_offset_ms` = 1500 ms, lấy từ `bronze.video_files` qua phép join theo `drive_id`. Giá trị này không bị viết cứng trong SQL.

**Bằng chứng**: `bat_dau` của B là `2025-10-08 14:30:00.009`, khớp thời điểm bắt đầu video 14:30:00. Trước khi trừ, giá trị thô là 14:30:01.509. Tốc độ tối đa sau chuẩn hoá hợp lý: A 71 km/h, B 100 km/h. Verify module 2 đạt **7/7**, gồm cả kiểm tra "drive B clock offset removed".

| drive_id | duplicate | speed_missing | speed_out_of_range | tổng cách ly | dòng sạch | nhận về |
|---|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 | 2968 | 3021 |
| drive_b | 20 | 30 | 3 | 53 | 3181 | 3234 |

`silver.telemetry` có 6149 dòng, `silver.telemetry_rejects` có 106 dòng. Hệ thức sạch + cách ly = nhận về đúng ở cả hai drive.

**Vì sao vào `rejects` thay vì xoá**: giữ lại thì đếm được. Ta chứng minh được không dòng nào bị mất âm thầm (2968 + 53 = 3021), và theo dõi được tỷ lệ lỗi của từng nguồn theo thời gian. Nhà cung cấp cảm biến cũng có bằng chứng cụ thể để sửa lỗi. Nếu luật làm sạch sai, ví dụ ngưỡng 70 m/s quá chặt, ta có thể xem lại các dòng bị loại và khôi phục chúng mà không phải nạp lại từ nguồn.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Hai test có mức độ khác nhau vì hậu quả khác nhau:
- **`in_range` (0..70 m/s) là `error`**: tốc độ vô lý là dữ liệu **sai**. Nếu lọt qua, nó làm hỏng nhãn và đặc trưng của dataset huấn luyện, nên pipeline phải dừng trước khi tới gold.
- **`max_gap_ms` (> 1000 ms) là `warn`**: một khoảng trống telemetry là hiện tượng **thật** (cảm biến mất tín hiệu). Các dòng còn lại vẫn đúng, nên chỉ cần báo động. Ở bài chạy của mình, mỗi drive có đúng 1 lỗ hổng: khoảng trống lớn nhất là 3073 ms (A) và 3124 ms (B). Khung hình rơi vào lỗ hổng này bị loại về sau bằng điều kiện đồng bộ, nên không cần dừng cả pipeline.

**Thí nghiệm `break-gate`** (`break_gate.json`): nới `max_speed_mps` từ 70 lên 1000. Khi đó luật làm sạch không còn loại dòng tốc độ vô lý, và 6 dòng này (3 của mỗi drive) lọt vào silver. `dbt test` ra `PASS=5 WARN=1 ERROR=1`: test `in_range_silver_telemetry_speed_mps__70__0` **bắt được 6 dòng**, `gate_failed: true`. Sau khi khôi phục luật, kết quả trở lại `PASS=6 WARN=1 ERROR=0`.

**Ý nghĩa của cổng chất lượng**: đây là lớp phòng thủ **độc lập** với luật làm sạch. Khi có người sửa nhầm luật (đổi cấu hình, refactor SQL), dữ liệu xấu vẫn bị chặn trước khi lan xuống gold và các mart. Test kiểm tra kết quả đầu ra chứ không tin vào logic biến đổi.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Theo `idempotency.json`, thí nghiệm chạy lại partition `trip-00003` (60 khung):
- **Delta `overwrite` theo predicate `trip_id`**: trước là **1246 dòng**, dấu vân tay `6c7480dfde7e`. Sau **2 lần chạy lại** vẫn **1246 dòng**, dấu vân tay `6c7480dfde7e` (`unchanged: true`).
- **Kiểu `append` sai**: chạy 2 lần cho cùng trip ra **120 dòng** thay vì 60, tức nhân đôi (2x). Ở quy mô cả bảng, mỗi lần retry hay backfill sẽ làm phồng dataset và làm lệch phân phối khi huấn luyện.

**Vì sao overwrite theo partition an toàn hơn xoá rồi ghi**: Delta ghi các file Parquet mới trước, sau đó commit **một lần duy nhất** vào `_delta_log`. Commit đó vừa đánh dấu file cũ của trip là đã xoá, vừa thêm file mới (thay thế nguyên tử). Nếu job sập giữa chừng, commit chưa xảy ra: người đọc vẫn thấy phiên bản cũ đầy đủ, file mồ côi chỉ là rác chờ dọn. Còn "xoá rồi ghi" gồm hai thao tác riêng: nếu sập sau bước xoá, trip đó **mất dữ liệu** cho tới khi chạy lại, và người đọc đồng thời thấy bảng thiếu. Ngoài ra overwrite chỉ chạm vào partition của trip đó, nên 25 trip còn lại không bị ảnh hưởng. Nhờ vậy Airflow có thể retry đúng một task.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

**Đen và tối khác nhau thế nào:**
- **Khung đen** có độ sáng trung bình (luma) < `dark_luma` = 12. Đó là khung **hỏng** (camera mất tín hiệu, lỗi encode) và không chứa thông tin nào, nên bị **loại** khỏi gold.
- **Khung tối** có 12 ≤ luma < `dark_soft` = 40. Đó là điều kiện **thật** (đường hầm, bóng râm, đêm), nên chỉ được **gắn cờ** `is_dark` và vẫn **giữ lại**.

Ở bài chạy của mình, drive A có 6 khung đen và 4 khung tối, drive B có 4 khung đen và **61 khung tối** được giữ lại. Mình đồng ý với cách xử lý này: nếu bỏ khung tối, mô hình sẽ không bao giờ thấy đường hầm hay ban đêm, và sẽ thất bại đúng ở những tình huống khó nhất. Cờ `is_dark` vẫn cho phép lọc hoặc cân bằng khi cần.

Theo `threshold.json`:

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Mình chọn **ngưỡng 2**, đúng như pipeline đang dùng.
- **Ngưỡng 0** chỉ bắt khung giống hệt từng bit, bỏ sót khung gần trùng do nhiễu nén. Drive A chỉ còn 61 khung trùng, trong khi camera đứng hình 6 giây và xe chạy chậm.
- **Ngưỡng 6** đã loại 363/602 khung (60%) của A và 244/644 khung (38%) của B. Ở mức này, nhiều khung thật sự khác nhau (xe đang di chuyển) cũng bị loại.
- **Ngưỡng 2** bắt được 11/12 khung đứng hình (verify module 3) mà vẫn giữ phần lớn dữ liệu.

**Ngưỡng phụ thuộc vào tốc độ xe**: ở 2 khung/giây, xe càng nhanh thì hai khung liền nhau càng khác nhau. Drive A có tốc độ trung bình 38.1 km/h nên có nhiều khung trùng hơn hẳn drive B (70.6 km/h), ở mọi ngưỡng (149 so với 53 ở ngưỡng 2). Nếu đặt ngưỡng cố định cao, xe chậm (tắc đường, chờ đèn đỏ) sẽ bị loại gần hết khung. Một cách tốt hơn là cho ngưỡng phụ thuộc tốc độ, hoặc lấy mẫu theo quãng đường đi được thay vì theo thời gian.

**Đồng bộ telemetry**: độ trễ trung bình giữa khung hình và telemetry là 66 ms (A) và 73 ms (B), nhờ join ASOF theo event time. Độ trễ tối đa là 2884 ms và 3112 ms, rơi đúng vào lỗ hổng cảm biến ở Bước 2. Mỗi drive có 7 khung vượt `sync_tol_ms` = 150 ms, bị gắn cờ `mat_dong_bo` và bị loại.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Theo `leakage.json`, có 1019 khung đáng dùng thuộc 26 trip:
- **Chia theo khung hình** (ngẫu nhiên 70/15/15): **26/26 trip** bị chia cắt sang nhiều split, và **26 trip có khung ở cả train lẫn test** (`per_frame_split_trips_in_both_train_and_test: 26`).
- **Chia theo trip** (gold của pipeline): **0 trip** bị chia cắt. Dbt test chống rò rỉ và verify "no trip is in two splits" đều PASS.

**Vì sao điểm kiểm thử đẹp giả tạo**: ở 2 khung/giây, hai khung liền nhau của cùng một trip gần như giống hệt: cùng con đường, cùng ánh sáng, cùng xe phía trước. Khi chia theo khung, gần như mỗi khung test đều có một "anh em sinh đôi" trong train. Mô hình chỉ cần **nhớ** cảnh là đạt điểm test cao, chứ không cần **khái quát hoá**. Con số test lúc đó đo khả năng ghi nhớ chứ không đo hiệu năng trên đường mới. Khi triển khai trên một cung đường chưa từng thấy, độ chính xác sẽ tụt mạnh. Chia theo trip (theo cảnh) mới mô phỏng đúng tình huống đó.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Pipeline tính `dataset_version = sha256(danh sách "frame_id|split", sắp theo frame_id)[:12]` trong `pipeline/publish.py`.

**Vì sao là băm nội dung**: cùng nội dung thì luôn ra cùng version, khác nội dung (dù chỉ 1 khung đổi split) thì version đổi. Ngày giờ hay số đếm không có tính chất này. Nếu dùng chúng, chạy lại y hệt cũng sinh "version mới", và hai lần chạy khác nhau trên hai máy có thể trùng số. Băm nội dung giúp idempotent (chạy lại không tạo bản rác) và cho phép giảng viên **kiểm chứng**: chạy lại từ lake trống phải ra đúng chuỗi đó.

**Hai lần chạy liên tiếp**: `step 4` rồi `cli export-dataset` đều ra `98f481913304`, cùng `manifest_sha256 = d423d0e7…`.

**Vì sao khác bạn bè**: model `gold_dataset_frames.sql` xếp trip theo `md5(trip_id || LAB_STUDENT_ID)`, nên MSSV là "muối" quyết định trip nào vào train, val hay test. Mình đã thử:

| MSSV | dataset_version | train / val / test (khung) |
|---|---|---|
| `2A202602818` (của mình) | `98f481913304` | 697 / 157 / 165 |
| `2024000` (thử) | `8fc131884584` | 703 / 182 / 134 |

Khi đổi MSSV, số trip mỗi split vẫn là 18 / 4 / 4 và tổng vẫn 1019 khung. Chỉ **trip nào vào split nào** thay đổi, kéo theo số khung mỗi split và version. Sau khi đặt lại MSSV của mình, version trở về `98f481913304`.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`manifest.json` của dataset `98f481913304` (đọc từ `lake/gold/datasets/98f481913304/manifest.json`):

```json
"lineage": {
  "bronze.raw_telemetry_a": 0, "bronze.raw_telemetry_b": 0, "bronze.trips_raw": 0,
  "bronze.video_files": 1, "silver.frames": 27, "silver.telemetry": 0,
  "silver.frames_flagged": 0, "silver.frame_telemetry": 0,
  "input_video_sha256": {
    "drive_a": "636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa",
    "drive_b": "bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc" } },
"rules": { "drop": ["is_black","is_blurry","is_near_dup","not sync_ok"],
           "split": "per trip, stratified by drive, md5 order" }
```

**Dataset này được tạo từ** hai video gốc có sha256 như trên, telemetry A/B ở bronze version 0, bảng trips version 0 và `video_files` version 1. Từ đó qua `silver.frames` **version 27** (26 partition trip được ghi lần đầu, cộng các lần ghi lại trip-00003 trong thí nghiệm idempotency), rồi tới `silver.telemetry`, `frames_flagged`, `frame_telemetry` version 0. Luật loại khung là đen, mờ, trùng và mất đồng bộ. Luật chia là theo trip, phân tầng theo drive, thứ tự md5. Kết quả gồm 1019 khung: drive_a 436, drive_b 583.

**Để tái tạo sau 3 tháng**:
1. `input_video_sha256` xác minh đúng file video gốc.
2. Số version của từng bảng Delta cho phép **time travel** (`DeltaTable(..., version=27)`) để đọc lại đúng trạng thái silver lúc đó.
3. `rules` cùng MSSV (muối của split) và commit git của mã `dbt/` cho biết logic.
4. `dataset_version` và `manifest_sha256` dùng để kiểm tra bản tái tạo trùng khớp từng bit.

Hạn chế: manifest **không** ghi commit git của mã, cũng không ghi giá trị các biến dbt (`dup_hamming`, `sync_tol_ms`…). Nếu ai đó đổi ngưỡng trong `dbt_project.yml`, lineage hiện tại không cho biết điều đó.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
