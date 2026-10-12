# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Hoàng Văn Nam |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602853 |
| GitHub | https://github.com/namhv521/K4-L3L4-Track4-DataPipeline4ComputerVision-2A202602853 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-07304` |

MSSV có chữ A nên lệnh `int(MSSV)` trong GUIDE không chạy trực tiếp. Theo lựa chọn của tôi, dùng 5 số cuối để chọn trip: `int("02853") % 26 = 19`; phần tử thứ 20 trong danh sách trip đã sắp xếp là `trip-07304`. Giữ nguyên `LAB_STUDENT_ID=2A202602853` cho split.

## Kết quả cuối

- `dataset_version` của bạn: `851b6745b02b` (đối chiếu `report/evidence/run_summary.json`).
- Số khung hình train / val / test của bạn: **689 / 151 / 179**, tổng **1.019**; số trip tương ứng **18 / 4 / 4**.
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

> 1.Timestamp: Nguồn A dùng ts_utc theo định dạng ISO 8601, còn B dùng timestamp_ms tính bằng mili giây Unix
> 2.Tốc độ: A dùng speed_mps (m/s), B dùng spd_kmh (km/h).
> 3.Hướng di chuyển: A dùng heading_deg, B dùng hdg.
> 4.Tọa độ GPS: A dùng lat, lon, còn B dùng latitude, longitude.
> 5.Chân ga, phanh: A dùng gas, brake với giá trị dạng tỷ lệ; B dùng throttle_pct, brake_pct theo phần trăm.
> 6.Góc lái: A dùng steer, B dùng steering.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.


> 1. Pipeline chuẩn hóa hai nguồn về cùng một schema:
>   - Tên cột: Chuyển các tên cột khác nhau (speed_mps/spd_kmh, lat/latitude, lon/longitude) về cùng cấu trúc.
>   - Đơn vị: Chuẩn hóa tốc độ về m/s, trong đó nguồn B được chuyển từ km/h sang m/s bằng cách chia cho 3,6. Các giá trị chân ga, phanh cũng được chuẩn hóa về cùng thang đo.
>   - Thời gian: Chuyển timestamp của cả hai nguồn về kiểu thời gian UTC thống nhất. Nguồn A dùng ISO 8601, nguồn B dùng Unix milliseconds.
>   - Độ lệch đồng hồ: Nguồn B có đồng hồ nhanh hơn 1,5 giây, nên pipeline trừ 1,5 giây để đồng bộ telemetry với video
> 2. Các bằng chứng cho thấy bước 2 đúng: 
> - Silver có 6.149 dòng hợp lệ, Rejects có 106 dòng lỗi.
> - Drive A: 2.968 + 53 = 3.021 dòng ban đầu.
> - Drive B: 3.181 + 53 = 3.234 dòng ban đầu.
> - Tốc độ tối đa sau chuẩn hóa: A = 71 km/h, B = 100 km/h, đều dưới 130 km/h.
> - Timestamp đầu tiên của B là 2025-10-08 14:30:00.009 UTC, lệch thời điểm bắt đầu video dưới 0,5 giây.
> - Kiểm tra tự động đạt 7/7 PASS. dbt test có 6 PASS, 1 WARN (max_gap_ms cảnh báo mất tín hiệu nhưng không làm dừng pipeline).
> 3. Bảng quarantine 
>   drive_id | reject_reason | so_dong
>   -------- | --------------|  ------
>   drive_a  | duplicate          | 20     
>   drive_a  | speed_missing      | 30     
>   drive_a  | speed_out_of_range | 3      
>   drive_b  | duplicate          | 20     
>   drive_b  | speed_missing      | 30     
>   drive_b  | speed_out_of_range | 3 
> 4. dòng lỗi vào rejects thay vì bị xóa vì: 
> Các dòng lỗi được đưa vào silver.telemetry_rejects để bảo toàn dữ liệu, truy vết nguyên nhân và hỗ trợ kiểm tra, xử lý lại khi cần. Nếu xóa trực tiếp sẽ mất bằng chứng về chất lượng dữ liệu nguồn

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

> Test max_gap_ms được đặt ở mức WARN vì khoảng cách thời gian giữa các mẫu telemetry quá lớn có thể do cảm biến mất tín hiệu, nhưng không nhất thiết khiến toàn bộ dữ liệu sai. Vì vậy, pipeline chỉ đưa ra cảnh báo và vẫn tiếp tục hoạt động. Ngược lại, test in_range được đặt ở mức ERROR vì tốc độ phải nằm trong khoảng 0–70 m/s. Nếu vượt giới hạn này, dữ liệu không hợp lệ và có thể ảnh hưởng đến kết quả phân tích, nên cần báo lỗi để chặn pipeline.

> Trong thí nghiệm break-gate, khi nới giới hạn tốc độ từ 70 m/s lên 1.000 m/s, có 6 dòng tốc độ bất thường lọt vào Silver. Test in_range_silver_telemetry_speed_mps__70__0 đã phát hiện cả 6 dòng vi phạm, với kết quả PASS=5, WARN=1, ERROR=1. Sau khi khôi phục luật ban đầu, kết quả trở về PASS=6, WARN=1, ERROR=0.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

> Theo idempotency.json, trước khi chạy lại có 1.246 dòng, sau 2 lần chạy lại vẫn 1.246 dòng, fingerprint giữ nguyên 6c7480dfde7e. Điều này chứng minh pipeline đảm bảo tính idempotency, không tạo dữ liệu trùng lặp.
> Ngược lại, khi dùng append, một trip có 60 dòng nhưng sau 2 lần ghi tăng lên 120 dòng (gấp đôi).
> Delta overwrite theo partition an toàn hơn vì hỗ trợ giao dịch ACID (atomic), đảm bảo ghi đè toàn bộ hoặc không thay đổi gì nếu xảy ra lỗi. Trong khi đó, xóa rồi ghi lại có thể gây mất dữ liệu nếu lỗi xảy ra giữa hai thao tác. Ngoài ra, overwrite chỉ tác động đến trip cần xử lý, không ảnh hưởng các trip khác.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen có độ sáng trung bình < 12, bị loại khỏi Gold; khung tối có độ sáng từ 12 đến dưới 40, được gắn `is_dark` và vẫn có thể giữ nếu qua các bộ lọc khác. Drive A có 6 khung đen, 4 khung tối (giữ 4); B có 4 khung đen, 61 khung tối (giữ 58). Tôi đồng ý vì đường hầm và bóng râm là điều kiện thực tế cần học, còn ảnh đen gần như không cung cấp thông tin.
Theo `threshold.json`, tôi chọn ngưỡng 2 bit: cân bằng loại khung gần trùng và giữ cảnh thay đổi thật. Ngưỡng 0 bỏ sót biến đổi nhỏ; ngưỡng 6 loại tới 363/602 khung A và 244/644 khung B, có nguy cơ mất cảnh hữu ích. Khi xe chạy chậm/dừng, cảnh thay đổi ít nên nhiều khung bị coi là trùng; khi xe chạy nhanh, cảnh thay đổi lớn hơn. Ngưỡng cần đánh giá cùng tốc độ xe, tần suất lấy mẫu 2 fps và ảnh thực tế, không chỉ dựa vào tổng số khung.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Thí nghiệm `leakage.json` dùng 1.019 khung hợp lệ thuộc 26 trip. Chia theo khung làm 26 trip nằm ở nhiều split, và cả 26 trip có khung trong train lẫn test; chia theo trip của pipeline có 0 trip bị chia cắt. Các khung liền nhau giống nhau về đường, ánh sáng và vật thể, nên mô hình có thể nhớ cảnh đã gặp trong train. Điểm test khi đó phản ánh khả năng nhận lại cảnh quen, làm đánh giá tổng quát hóa sang chuyến mới đẹp giả tạo.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` được tính bằng SHA-256 của các cặp `frame_id|split` đã sắp xếp, lấy 12 ký tự đầu. Chạy Bước 4 rồi `export-dataset` lần nữa đều cho `851b6745b02b`, 1.019 khung, train/val/test = 689/151/179. Cách này ổn định khi nội dung chia split không đổi, khác ngày giờ hoặc số đếm luôn tăng sau mỗi lần chạy. MSSV `2A202602853` được trộn vào thứ tự xếp trip nên đổi MSSV có thể đổi trip trong mỗi split và phiên bản. Đây là băm danh sách khung và split, không phải băm toàn bộ byte ảnh; sha256 video và lineage trong manifest bổ sung truy vết nguồn.
Thử ghi đè biến môi trường cho riêng lệnh Bước 4 bằng `2A202602854`: phiên bản đổi thành `b6afc9d01a26`, train/val/test = 792/141/86, vẫn 1.019 khung và 18/4/4 trip (`report/evidence/version_changed_student.txt`). Sau đó khôi phục MSSV gốc và chạy lại pipeline; không đổi MSSV trong báo cáo hay các thí nghiệm chính.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest của dataset `851b6745b02b` ghi `bronze.raw_telemetry_a=0`, `bronze.raw_telemetry_b=0`, `bronze.trips_raw=0`, `bronze.video_files=1`; `silver.frames=25`, `silver.telemetry=0`, `silver.frames_flagged=0`, `silver.frame_telemetry=0` (snapshot trước lần sập trip-07304 lưu ở `report/evidence/manifest_before_crash.json`).
Video A có SHA-256 `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`; video B có `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Gold gồm 436 khung A và 583 khung B, lọc đen/mờ/gần trùng/mất đồng bộ, rồi chia theo trip có phân tầng theo drive.
Để tái tạo sau 3 tháng cần giữ các snapshot Delta nêu trên, video khớp SHA-256, manifest liệt kê frame/split, metadata trip và offset đồng hồ, MSSV, mã nguồn/config và phiên bản môi trường. `dataset_version` nhận diện danh sách frame/split; chỉ số version Delta không đủ nếu lake đã reset hoặc lịch sử đã bị xóa.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Run `manual__2026-10-12T16:51:09+00:00` có đúng 1 task đỏ: `silver_frames[19]`, log ghi `simulated crash while extracting trip-07304; run it again`; 7 task downstream chuyển `upstream_failed`. Tôi tắt lựa chọn Downstream và clear riêng index 19 (Affected Tasks = 1); task này thành công ở try 2, 25 trip còn lại vẫn try 1.
Thực tế trên Airflow 2.10.5, 7 task `upstream_failed` không tự quay về chạy sau khi clear riêng task đỏ như GUIDE mô tả. Tôi clear thêm `dbt_run_silver` cùng downstream (Affected Tasks = 7), không chọn upstream. Đây là 7 task chạy lần đầu sau khi hết chặn; chỉ 1 task trích khung được chạy lại. DAG kết thúc với 37/37 task instance thành công.
Silver có 1.246 dòng và 1.246 `frame_id` duy nhất; riêng trip-07304 có 34 khung, không nhân đôi. Dataset sau hồi phục vẫn `851b6745b02b`, 1.019 khung với split 689/151/179, bằng trước lỗi.
Bằng chứng: `report/evidence/crash_recovery.json`, `recovered_task_instances.json`, `manifest_before_crash.json`, `manifest_after_recovery.json`; ảnh `airflow_crash_log.png`, `airflow_clear_one.png`, `airflow_after_clear_one.png`, `airflow_clear_downstream.png`, `airflow_recovered_graph.png`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ba nút nghẽn dự kiến khi lên 500 xe/ngày (cần benchmark để xác nhận thứ tự):
1. Decode FFmpeg: mỗi trip tải lại toàn bộ video, `silver_frames` chỉ chạy một task tại một thời điểm. Chuyển sang worker phân tán theo trip bằng Airflow executor phù hợp; cache/đọc đoạn video cần dùng và giới hạn concurrency theo CPU, RAM, I/O.
2. ELT DuckDB một file: tranh chấp ghi, tính toán trên một máy và ghi lại bảng lớn. Với batch lớn, dùng Spark/dbt trên engine phân tán, xử lý tăng dần theo partition thay vì full rebuild. Đây là lựa chọn batch processing liên hệ slide 22.
3. Ingestion/storage một máy: tải video, ghi từng ảnh nhỏ, copy ảnh khi export và MinIO đơn node gây nghẽn băng thông/I/O. Dùng object storage phân tán, gom file/shard và manifest tham chiếu để giảm copy; nếu cần telemetry gần thời gian thực, thêm Kafka và Flink với event time/watermark (stream processing, slide 23). Tách các tầng ingest–storage–processing–serving theo vòng đời dữ liệu (slide 12); Postgres chỉ giữ mart tổng hợp cho BI.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Câu hỏi **“Tỷ lệ khung bị loại theo trip — 2A202602853”** đã lưu và thêm vào dashboard Day 9.

```sql
SELECT drive_id, trip_id, frames, frames_kept,
       frames - frames_kept AS frames_dropped,
       ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 2) AS dropped_pct,
       avg_speed_kmh, split
FROM marts.mart_trip_summary
ORDER BY dropped_pct DESC, drive_id, trip_id;
```

Kết quả có 26 trip: `trip-04622` (A) loại 34/60 khung = 56,67%, cao nhất; `trip-03904` loại 11/22 = 50%; `trip-02086` loại 28/60 = 46,67%. Truy vấn giúp ưu tiên xem lại các trip mất nhiều khung và đối chiếu tốc độ/split; tỷ lệ này là hợp các điều kiện lọc Gold, không phải tổng các cờ có thể chồng lấn.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Kết quả đáng chú ý là chia ngẫu nhiên theo khung làm cả 26 trip xuất hiện trong train và test, dù tỷ lệ split nhìn vẫn hợp lý.
Khung tối không tự động bị loại: cần giữ các điều kiện khó như đường hầm để dataset không thiên lệch.
Chưa có đánh giá mô hình downstream để biết ngưỡng dHash 2 bit ảnh hưởng độ chính xác bao nhiêu; tổng số khung không đủ trả lời.
Pipeline chưa kiểm tra GPS nhảy vị trí bất thường hoặc sai tọa độ trong khi tốc độ vẫn hợp lệ; cần thêm kiểm tra tính liên tục theo event time.
