# TTCS-05-2026

Project skeleton cho Data Platform E-commerce.

## Kiến trúc hệ thống

### Tổng quan kiến trúc

![Kiến trúc tổng quan hệ thống](docs/architecture_overview.svg)

### Luồng dữ liệu ETL Pipeline

![Luồng dữ liệu ETL](docs/etl_pipeline_flow.svg)

### Mô hình dữ liệu (Star Schema)

![Star Schema - ClickHouse Data Model](docs/data_model.svg)

## Key Files

- `README.md`: Tài liệu tổng quan dự án.
- `infra/docker-compose.yml`: Khởi tạo local stack (MinIO, Spark, ClickHouse, Streamlit).
- `pipelines/extract.py`: Stage extract cho các nguồn SQL/API/Excel.
- `pipelines/transform.py`: Stage transform/chuẩn hóa dữ liệu.
- `pipelines/load.py`: Stage load dữ liệu vào serving layer/warehouse.
- `pipelines/run_pipeline.py`: Entry-point chạy pipeline end-to-end.
- `warehouse/run_ddl.sql`: Entry-point chạy toàn bộ DDL ClickHouse.
- `apps/streamlit/app.py`: Entry-point dashboard Streamlit.
- `docs/ProjectOverview.md`: Đặc tả nghiệp vụ, schema và mapping.

## Folder Map (đầy đủ)

```text
TTCS-05-2026/
├── apps/ -- ứng dụng tiêu thụ dữ liệu
│   └── streamlit/ -- dashboard Customer 360
│       ├── components/ -- UI components tái sử dụng
│       ├── pages/ -- các trang dashboard
│       └── services/ -- query layer và ClickHouse client
├── configs/ -- cấu hình tập trung pipeline/storage/warehouse
├── data/ -- lưu trữ Data Lake local theo zone
│   ├── raw/ -- dữ liệu thô sau ingest
│   ├── clean/ -- dữ liệu đã làm sạch/chuẩn hóa
│   ├── serving/ -- dữ liệu sẵn sàng để phục vụ phân tích
│   └── contracts/ -- schema contract và ràng buộc dữ liệu
├── docs/ -- tài liệu nghiệp vụ và kiến trúc
├── generate_fake_data/ -- script sinh dữ liệu giả
│   └── clickstream/ -- sinh log hành vi JSON
├── infra/ -- hạ tầng local (Docker/containers)
│   ├── clickhouse/ -- cấu hình và init ClickHouse
│   │   └── init/ -- tài nguyên khởi tạo DB
│   ├── minio/ -- cấu hình object storage
│   │   └── init/ -- tài nguyên khởi tạo bucket/policy
│   └── spark/ -- cấu hình Spark processing
│       └── conf/ -- file config Spark
├── pipelines/ -- ETL orchestration cấp dự án
│   ├── sources/ -- adapter nguồn SQL/API/Excel
│   └── transforms/ -- transform theo domain nghiệp vụ
├── sample_dataset/ -- dữ liệu Olist mẫu để dev/test
├── scripts/ -- script chạy local stack/pipeline/dashboard
├── tests/ -- test chất lượng dữ liệu và pipeline
│   ├── smoke/ -- kiểm tra luồng chạy tối thiểu
│   ├── contracts/ -- kiểm tra schema contracts
│   └── data_quality/ -- kiểm tra rule chất lượng dữ liệu
└── warehouse/ -- schema và view cho ClickHouse
    ├── ddl/ -- định nghĩa bảng dim/fact
    └── views/ -- view phục vụ phân tích (Customer 360)
```

## Ghi chú

- Luồng chính: `extract.py` -> `transform.py` -> `load.py`.
- Đặc tả nghiệp vụ và mapping dữ liệu tham chiếu tại `docs/ProjectOverview.md`.

## Chạy nhanh pipeline 

0. Dừng tất cả containers + xóa volumes
   ```bash
   docker compose -f infra/docker-compose.yml down -v
   ```

1. Cài Python package cho pipeline:

   ```bash
   pip install -r requirements.txt
   ```

2. Sinh dữ liệu giả cho các nguồn (PostgreSQL init.sql, FastAPI seed, Excel):

   ```bash
   python -m generate_fake_data.run_all
   ```

3. Khởi động local stack:

   ```bash
   docker compose -f infra/docker-compose.yml up -d
   ```

4. Chạy pipeline end-to-end:

   ```bash
   PYTHONPATH=. python -m pipelines.run_pipeline
   ```

5. (Tuỳ chọn) Kiểm tra smoke test:

   ```bash
   PYTHONPATH=. pytest -q tests/smoke/test_pipeline_layout.py
   ```

> File môi trường đã sẵn tại `infra/.env` (dựa trên `infra/.env.example`).

## Truy cập dịch vụ

| Dịch vụ | URL / Host | Credentials |
|---|---|---|
| **Dashboard (Streamlit)** | http://localhost:8501 | — |
| **MinIO Console** (Data Lake) | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| **MinIO S3 API** | http://localhost:9000 | (dùng chung credentials trên) |
| **ClickHouse HTTP** (Data Warehouse) | http://localhost:8123 | `default` / `ttcs` |
| **PostgreSQL** (Source DB) | `localhost:5432` | `source_user` / `source_pass` — DB: `ecommerce` |
| **FastAPI** (Source API) | http://localhost:8000/docs | — |

### Kết nối ClickHouse (DBeaver / CLI)

```bash
# CLI
docker exec -it ttcs-clickhouse clickhouse-client --database=ttcs --password=ttcs

# Connection string
jdbc:ch://localhost:8123/ttcs?user=default&password=ttcs
```

### Kết nối PostgreSQL

```bash
psql -h localhost -p 5432 -U source_user -d ecommerce
# Password: source_pass
```

### Xem Data Lake (MinIO)

Mở http://localhost:9001 → đăng nhập → duyệt 3 bucket: `ttcs-raw`, `ttcs-clean`, `ttcs-serving`.
