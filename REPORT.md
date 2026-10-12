# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Hoàng Văn Nam |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602853 |
| GitHub | https://github.com/namhv521/K4-L3L4-Track4-DataPipeline4ComputerVision-2A202602853 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
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

(trả lời ở đây)

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

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
