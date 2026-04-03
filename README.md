# Smartlog service data pack

Bộ code này có 2 chức năng:

1. `validate_existing`  
   Rà 3 file Excel hiện có để xem có chênh lệch schema, sản lượng, severity distribution, KPI gap, duplicate ticket ID, hoặc lỗi timestamp hay không.

2. `generate_unified`  
   Sinh **một bộ dữ liệu thống nhất 2023-2025** từ cùng một logic và cùng một seed, để viết đồng bộ cho:
   - 2.3.1 Response and resolution performance
   - 2.3.2 SLA compliance and issue handling performance
   - 2.3.3 Customer feedback and satisfaction

## Cấu trúc repo

- `scripts/validate_existing_inputs.py`
- `scripts/generate_unified_service_data.py`
- `.github/workflows/service-data.yml`
- `requirements.txt`

## Cách chạy trên GitHub

### Bước 1: Tạo repo mới
Tạo một repo mới trên GitHub.

### Bước 2: Upload code
Upload toàn bộ file trong bộ này lên repo.

### Bước 3: Nếu muốn rà 3 file hiện có
Tạo thư mục `data/` ở root repo, rồi upload đúng 3 file này vào đó:

- `sla_actual_performance_2023_2025_combined.xlsx`
- `sla_issue_handling_2024_filled.xlsx`
- `sla_issue_handling_2025_filled.xlsx`

### Bước 4: Bật workflow
Vào tab **Actions** → chọn workflow **service-data** → bấm **Run workflow**.

### Bước 5: Chọn chế độ
- `validate_existing`: rà xem 3 file đang có bị chênh nhau không
- `generate_unified`: sinh bộ data mới thống nhất

### Bước 6: Tải file kết quả
Sau khi workflow chạy xong:
- vào run đó
- xuống phần **Artifacts**
- tải file `service-data-output`

## Output khi chạy `validate_existing`

Trong thư mục `output/` sẽ có:
- `validation_report.csv`
- `validation_report.xlsx`

## Output khi chạy `generate_unified`

Trong thư mục `output/` sẽ có:
- `smartlog_service_analysis_2023_2025.xlsx`
- thư mục `csv/`

## Sheet chính của file Excel sinh ra

- `Ticket_Raw`
- `SLA_Targets`
- `Data_Dictionary`
- `KPI_Overall`
- `KPI_By_Month`
- `KPI_By_Severity`
- `KPI_By_Customer`
- `Feedback_Direct`
- `Feedback_Proxy`

## Lưu ý

- Bộ data này là **synthetic / mock data** để dùng cho phân tích học thuật.
- Các trường satisfaction gồm cả:
  - **direct feedback**: `closure_feedback_submitted`, `csat_score`, `customer_comment_theme`, `customer_comment_text`
  - **indirect/proxy**: `customer_sentiment_proxy`, `complaint_signal`, `reopened_count`, `escalated`, `overall_sla_status`
- Dùng một script thống nhất sẽ tránh việc generate rời từng file rồi bị lệch logic giữa 2.3.1, 2.3.2 và 2.3.3.
