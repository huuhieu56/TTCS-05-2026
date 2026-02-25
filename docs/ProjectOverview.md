# TÊN DỰ ÁN (PROJECT TITLE)
Xây dựng Nền tảng Dữ liệu (Data Platform) phân tích Customer 360 và Tối ưu Vận hành cho E-commerce.

## Mô tả ngắn gọn (Short Description)
Dự án xây dựng một luồng dữ liệu (Data Pipeline) tự động thu thập và đồng nhất các nguồn dữ liệu phân mảnh của doanh nghiệp thương mại điện tử: Dữ liệu giao dịch (SQL), Nhật ký hành vi người dùng (API/JSON), và Dữ liệu chăm sóc khách hàng (Excel). Hệ thống sử dụng kiến trúc Medallion (Bronze-Silver-Gold) để lưu trữ tập trung tại Data Lake, làm sạch/chuẩn hóa bằng công cụ xử lý phân tán, và đẩy vào Data Warehouse chuyên dụng để truy vấn tốc độ cao, phục vụ hệ thống Dashboard báo cáo trực quan.

# I. Nguồn dữ liệu

---

### PHẦN 1: CẤU TRÚC CÁC NGUỒN DỮ LIỆU ĐẦU VÀO (DATA SOURCES)

Đây là dữ liệu nguyên bản trước khi được đẩy vào Data Lake (MinIO).

#### 1. Nguồn SQL (Transactional DB) - Cơ sở dữ liệu bán hàng

*Định dạng xuất file: `.csv` hoặc `.sql` dump.*

**Bảng `users` (Thông tin khách hàng):**

* `user_id` (VARCHAR / **NOT NULL**): Khóa chính — Ánh xạ từ `customer_unique_id` của Olist. Đây là mã định danh thực sự của một con người, không phải `customer_id` (mã tạm sinh theo đơn). Hệ thống tự sinh định danh (VD: `861eff4711a542e4b93843c6dd7febb0`).
* `full_name` (VARCHAR / **NOT NULL**): Họ và tên đầy đủ của khách hàng. **[MOCK]** — Olist không có tên thật. Script Python dùng thư viện `Faker` (locale `pt_BR`) sinh tên giả theo `customer_unique_id` (deterministic seed) để mỗi lần chạy đều ra cùng tên. Không gây mâu thuẫn vì tên chỉ là thông tin nhận dạng, không liên quan metric kinh doanh.
* `email` (VARCHAR / **NULL**): Địa chỉ email. **[MOCK]** — Sinh từ `full_name` dạng `ten.ho@gmail.com`. Cho phép NULL nếu khách đăng ký tài khoản bằng Số điện thoại.
* `phone_number` (VARCHAR / **NULL**): Số điện thoại. **[MOCK]** — Sinh ngẫu nhiên theo format Brazil `+55 XX XXXXX-XXXX`. Cho phép NULL nếu khách đăng ký bằng Email/Google/Facebook.
* `customer_city` (VARCHAR / **NOT NULL**): Thành phố khách hàng. **[REMAP]** — Ánh xạ trực tiếp từ cột `customer_city` trong Olist.
* `customer_state` (VARCHAR / **NOT NULL**): Mã bang/tỉnh (VD: `SP`, `RJ`). **[REMAP]** — Ánh xạ từ cột `customer_state`.
* `loyalty_tier` (VARCHAR / **NULL**): Hạng thành viên (`Bronze`, `Silver`, `Gold`, `Platinum`). **[DERIVED]** — Spark tính toán dựa trên tổng chi tiêu thực tế (`SUM(payment_value)` per customer) rồi phân hạng theo percentile:
  * Top 5% chi tiêu → `Platinum`
  * Top 5–15% → `Gold`
  * Top 15–35% → `Silver`
  * Còn lại → `Bronze`
  * Khách mới chưa hoàn thành đơn nào → `NULL`

  > Logic này đảm bảo **nhất quán dữ liệu**: khách chi tiêu nhiều sẽ luôn có hạng cao, không bao giờ xảy ra mâu thuẫn.

* `created_at` (TIMESTAMP / **NOT NULL**): Thời gian tạo tài khoản. **[DERIVED]** — Lấy `MIN(order_purchase_timestamp)` từ bảng orders cho mỗi `customer_unique_id`. Xấp xỉ thời điểm khách lần đầu tương tác với hệ thống.

**Bảng `products` (Danh mục sản phẩm):**

* `product_id` (VARCHAR / **NOT NULL**): Khóa chính (VD: `1e9e8ef04dbcff4541ed26657ea517e5`). Ánh xạ trực tiếp từ `product_id` trong Olist.
* `product_name` (VARCHAR / **NOT NULL**): Tên sản phẩm hiển thị trên hệ thống. **[MOCK]** — Olist chỉ có `product_name_lenght` (độ dài tên, kiểu INT) chứ không có tên thực. Script Python sinh tên sản phẩm giả dựa trên `category` + số thứ tự (VD: category `health_beauty` → `Health Beauty Product #1042`). Không gây mâu thuẫn vì tên chỉ là nhãn hiển thị.
* `category` (VARCHAR / **NULL**): Ngành hàng bằng **tiếng Anh** (VD: `health_beauty`, `computers_accessories`, `furniture_decor`). **[REMAP]** — Lấy `product_category_name` (tiếng Bồ Đào Nha) từ Olist, rồi JOIN với file `product_category_name_translation.csv` để dịch sang tiếng Anh. Cho phép NULL nếu sản phẩm chưa phân loại (~610 sản phẩm trong dataset).
* `cost_price` (DECIMAL / **NOT NULL**): Giá vốn nhập kho. **[DERIVED]** — Tính `AVG(price) * 0.7` từ bảng `order_items` (GROUP BY `product_id`), giả định biên lợi nhuận 30%. Đảm bảo sản phẩm giá bán cao → cost cao, nhất quán về mặt kinh tế.

**Bảng `orders` (Thông tin tổng quan đơn hàng):**

* `order_id` (VARCHAR / **NOT NULL**): Khóa chính mã đơn hàng (VD: `e481f51cbdc54678b7cc49136f2d6af7`). Ánh xạ trực tiếp.
* `user_id` (VARCHAR / **NULL**): Khóa ngoại nối với bảng `users`. **[DERIVED]** — Cần bước trung gian: JOIN `orders.customer_id` → `customers.customer_id` → lấy `customer_unique_id`. Cho phép NULL trong trường hợp E-commerce hỗ trợ tính năng "Guest Checkout".
* `total_amount` (DECIMAL / **NOT NULL**): Tổng tiền khách phải trả cho đơn hàng này. **[DERIVED]** — Tính `SUM(payment_value)` per `order_id` từ file `olist_order_payments_dataset.csv`. Chọn bảng payments thay vì items vì đây là số tiền khách **thực sự trả** (đã bao gồm giảm giá/voucher).
* `order_status` (VARCHAR / **NOT NULL**): Trạng thái (`Pending`, `Processing`, `Completed`, `Cancelled`). **[REMAP]** — Ánh xạ từ giá trị gốc Olist theo bảng sau:

  | Giá trị Olist gốc | → Giá trị chuẩn hóa |
  |---|---|
  | `created` | `Pending` |
  | `approved`, `processing`, `invoiced`, `shipped` | `Processing` |
  | `delivered` | `Completed` |
  | `canceled`, `unavailable` | `Cancelled` |

* `payment_method` (VARCHAR / **NULL**): Phương thức thanh toán chính. **[DERIVED]** — Lấy `payment_type` có `payment_sequential = 1` (phương thức chính) từ `olist_order_payments_dataset.csv`. Ánh xạ giá trị:

  | Giá trị Olist gốc | → Giá trị chuẩn hóa |
  |---|---|
  | `credit_card` | `Credit Card` |
  | `boleto` | `Bank Slip` |
  | `voucher` | `Voucher` |
  | `debit_card` | `Debit Card` |

  Cho phép NULL ở bước khách vừa "Tạo đơn" nhưng chưa tới bước "Chọn phương thức thanh toán".

* `created_at` (TIMESTAMP / **NOT NULL**): Thời điểm đặt hàng. **[REMAP]** — Ánh xạ từ `order_purchase_timestamp` trong Olist.

**Bảng `order_items` (Chi tiết từng món trong đơn):**

* `item_id` (VARCHAR / **NOT NULL**): Khóa chính của dòng chi tiết. **[DERIVED]** — Sinh từ `{order_id}_{order_item_id}` để tạo unique key (Olist chỉ có `order_item_id` dạng sequential integer trong phạm vi mỗi order).
* `order_id` (VARCHAR / **NOT NULL**): Khóa ngoại nối với bảng `orders`. Ánh xạ trực tiếp.
* `product_id` (VARCHAR / **NOT NULL**): Khóa ngoại nối với bảng `products`. Ánh xạ trực tiếp.
* `quantity` (INT / **NOT NULL**): Số lượng mua. Trong Olist, mỗi dòng đại diện cho 1 đơn vị sản phẩm, nên cột này = `1` cho tất cả records.
* `unit_price` (DECIMAL / **NOT NULL**): Đơn giá chốt tại thời điểm mua hàng. **[REMAP]** — Ánh xạ từ cột `price` trong Olist.

> **Lưu ý:** Olist còn có `seller_id` và `freight_value` trong bảng `order_items`. Hai cột này không nằm trong scope schema chính nhưng có sẵn trong dữ liệu gốc nếu cần mở rộng phân tích vận hành.

#### 2. Nguồn API (Clickstream Log) - Hành vi người dùng

*Định dạng: `.json` lines.*

Mỗi object JSON đại diện cho một thao tác của người dùng trên web/app. JSON không có cấu trúc quá chặt chẽ, vì vậy cần phân định rõ field nào có thể bị mất.

* `event_id` (String / **NOT NULL**): Mã sự kiện sinh ngẫu nhiên từ Frontend (VD: `evt_883291`).
* `timestamp` (String / **NOT NULL**): Thời gian chính xác đến mili-giây, định dạng ISO 8601.
* `user_id` (String / **NULL**): ID khách hàng. Sẽ mang giá trị `null` nếu khách chưa đăng nhập (lướt web ẩn danh).
* `session_id` (String / **NOT NULL**): Mã phiên (cookie/token) để gom nhóm hành vi trong một lần truy cập.
* `event_type` (String / **NOT NULL**): Loại thao tác bắt buộc ghi nhận (`view_item`, `add_to_cart`, `cart_abandonment`).
* `product_id` (String / **NULL**): Mã sản phẩm tương tác. Sẽ là `null` nếu khách đang ở trang chủ hoặc chuyên mục mà chưa bấm vào món đồ cụ thể.
* `device_os` (String / **NULL**): Hệ điều hành (`Android`, `iOS`). Có thể bị `null` nếu trình duyệt ẩn danh chặn lấy thông tin thiết bị (Anti-tracking).
* `time_spent_seconds` (Integer / **NULL**): Thời gian nán lại màn hình. Có thể `null` nếu khách vừa vào đã thoát ngay (Bounce) không kịp sinh đủ giây.

#### 3. Nguồn Excel (Customer Service) - Dữ liệu Vận hành

*Định dạng: `.xlsx`.*

Dữ liệu con người nhập, rất "bẩn".

* `Ticket_ID` (String / **NOT NULL**): Mã phiếu. **[REMAP]** — Ánh xạ từ `review_id` trong Olist.
* `Order_ID` (String / **NULL**): Mã đơn hàng liên quan. Ánh xạ trực tiếp từ `order_id` trong reviews. Có thể NULL nếu review bị lỗi dữ liệu.
* `Customer_Email` (String / **NULL**): Email khiếu nại. **[MOCK]** — Olist reviews không chứa email. Script Python sinh email giả từ bảng `users` (JOIN qua `order_id` → `customer_id` → lấy email đã mock của customer). Nhân viên lười gõ có thể bỏ trống, hoặc cố ý tạo lỗi sai định dạng (`thiếu @`, `gmal.com`) — thêm ~5% dòng bị lỗi format để Spark luyện xử lý dữ liệu bẩn.
* `Issue_Type` (String / **NULL**): Phân loại vấn đề. **[DERIVED]** — Suy ra từ `review_score`:

  | Review Score | → Issue_Type |
  |---|---|
  | 1–2 | `Product Issue` |
  | 3 | `General Inquiry` |
  | 4–5 | `Positive Feedback` |

  Nhân viên không rõ xếp loại nào thì bỏ trống.

* `Status` (String / **NOT NULL**): Trạng thái vé (`Open`, `Resolved`). **[DERIVED]** — Nếu `review_answer_timestamp` có giá trị → `Resolved`, nếu NULL → `Open`.
* `Customer_Rating` (Integer / **NULL**): Điểm hài lòng (1-5). **[REMAP]** — Ánh xạ từ `review_score`. Sẽ trống nếu khách không đánh giá phản hồi.
* `Reported_Date` (Date/String / **NOT NULL**): Ngày tiếp nhận. **[REMAP]** — Ánh xạ từ `review_creation_date` trong Olist.

---

### PHẦN 2: THIẾT KẾ DATA WAREHOUSE (CLICKHOUSE SCHEMA)

Trong ClickHouse, mặc định các cột **KHÔNG ĐƯỢC PHÉP CHỨA NULL** để tối ưu hóa khả năng nén. Nếu cột nào có thể nhận giá trị `NULL`, bắt buộc phải bọc bằng hàm `Nullable()`.

#### 1. Các bảng Dimension (Dữ liệu nền)

**Bảng `dim_users`**

```sql
CREATE TABLE dim_users (
    user_id String,
    full_name String,
    email Nullable(String),
    phone_number Nullable(String),
    customer_city LowCardinality(String),
    customer_state LowCardinality(String),
    loyalty_tier Nullable(LowCardinality(String)),
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY user_id;
```

**Bảng `dim_products`**

```sql
CREATE TABLE dim_products (
    product_id String,
    product_name String,
    category Nullable(LowCardinality(String)),
    cost_price Decimal(10,2)
) ENGINE = MergeTree()
ORDER BY product_id;
```

#### 2. Các bảng Fact (Dữ liệu sự kiện sinh ra liên tục)

**Bảng `fact_orders` (Cốt lõi kinh doanh)**

```sql
CREATE TABLE fact_orders (
    order_id String,
    user_id Nullable(String), -- Khách mua không cần tài khoản
    total_amount Decimal(18,2),
    order_status LowCardinality(String),
    payment_method Nullable(LowCardinality(String)),
    created_at DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (order_status, created_at);
```

**Bảng `fact_order_items` (Chi tiết giỏ hàng)**

```sql
CREATE TABLE fact_order_items (
    item_id String,
    order_id String,
    product_id String,
    quantity UInt16,
    unit_price Decimal(12,2)
) ENGINE = MergeTree()
ORDER BY (order_id, product_id);
```

**Bảng `fact_events_log` (Log hành vi)**

```sql
CREATE TABLE fact_events_log (
    event_id String,
    timestamp DateTime64(3), -- Precision mili-giây cho clickstream log
    user_id Nullable(String),
    session_id String,
    event_type LowCardinality(String),
    product_id Nullable(String),
    device_os Nullable(LowCardinality(String)),
    time_spent_seconds Nullable(UInt16)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (event_type, timestamp);
```

**Bảng `fact_cs_tickets` (Vận hành & Chăm sóc khách hàng)**

```sql
CREATE TABLE fact_cs_tickets (
    ticket_id String,
    order_id Nullable(String),
    customer_email Nullable(String),
    issue_category Nullable(LowCardinality(String)), -- Đã được Spark chuẩn hóa từ review_score
    status LowCardinality(String),
    rating Nullable(UInt8), -- Điểm 1-5, có thể khách không vote
    reported_at DateTime
) ENGINE = MergeTree()
ORDER BY (status, reported_at);
```

#### 3. Bảng Customer 360 (Wide Table / Materialized View)

Đây là bảng đắt giá nhất phục vụ Dashboard. Quá trình ETL của Spark sẽ dồn các giá trị lại, đảm bảo các số đo (Metric) không bị `NULL` mà được thay thế bằng `0` nếu không có dữ liệu (ví dụ không khiếu nại thì là `0` thay vì `NULL`).

**Bảng `customer_360_view`**

```sql
CREATE TABLE customer_360_view (
    user_id String,
    full_name String,
    customer_city LowCardinality(String),
    customer_state LowCardinality(String),
    loyalty_tier Nullable(LowCardinality(String)),
    total_lifetime_value Decimal(18,2), -- Nếu chưa mua = 0.00
    total_orders_completed UInt32,      -- Số đơn hàng thành công, chưa có = 0
    total_abandoned_carts UInt32,       -- Mặc định 0
    total_cs_complaints UInt32,         -- Mặc định 0
    last_active_date Nullable(DateTime),-- Có thể NULL nếu vừa đăng ký chưa tương tác
    churn_risk_score Nullable(Float32)  -- Tỉ lệ rủi ro rời bỏ
) ENGINE = MergeTree()
ORDER BY user_id;
```

> **Lưu ý về `total_abandoned_carts`:** Metric này đến từ event `cart_abandonment` trong dữ liệu clickstream (Nguồn API). Do clickstream là dữ liệu **giả lập** (Hybrid Mocking — xem Phần III mục 3), giá trị này mang tính minh họa cho pipeline ETL, không phải dữ liệu thực 100%.

# II. TechStack

Ingestion: Python kéo dữ liệu từ Fake API, MySQL, Excel ném vào MinIO (Data Lake - Raw Zone).

Processing: Apache Spark đọc dữ liệu từ MinIO, làm sạch, gom ID lại, và lưu lại thành file Parquet (Clean Zone).

Serving / OLAP: Spark đẩy dữ liệu sạch này vào ClickHouse.

Dashboard: Ứng dụng Web (viết bằng Node.js, Spring Boot hoặc Python Streamlit) kết nối thẳng vào ClickHouse bằng SQL để vẽ biểu đồ phân tích.

# III. Thiết kế dữ liệu

Dưới đây là chiến lược chia nhỏ và ánh xạ (mapping) bộ dữ liệu Olist vào thiết kế của dự án. Mỗi cột được đánh dấu rõ nguồn gốc:
- **[REMAP]**: Ánh xạ trực tiếp từ cột có sẵn trong Olist
- **[DERIVED]**: Tính toán / suy ra từ dữ liệu Olist thực, đảm bảo nhất quán logic
- **[MOCK]**: Dữ liệu giả lập — chỉ dùng cho các trường **mô tả/nhận dạng** (tên, email, SĐT, tên sản phẩm) không ảnh hưởng metric kinh doanh, và cho Clickstream Log

---

## 1. Phân bổ vào Nguồn SQL (Dữ liệu Giao dịch cốt lõi)

Lấy từ 4 file CSV cốt lõi + 2 file bổ trợ của Olist, nạp vào cơ sở dữ liệu MySQL nội bộ để giả lập hệ thống bán hàng.

### Bảng `users` ← File `olist_customers_dataset.csv` + `olist_orders_dataset.csv` + `olist_order_payments_dataset.csv`

**Lưu ý cực quan trọng của Olist:** Olist có 2 loại ID. `customer_id` (mỗi đơn hàng sinh 1 mã mới) và `customer_unique_id` (mã định danh thực sự của 1 con người). Phải dùng `customer_unique_id` làm `user_id` chính thức.

| Cột thiết kế | Nguồn | Cột Olist / Logic |
|---|---|---|
| `user_id` | [REMAP] | `customer_unique_id` |
| `full_name` | [MOCK] | `Faker(pt_BR)` sinh tên theo seed = hash(`customer_unique_id`) |
| `email` | [MOCK] | Sinh từ `full_name` dạng `ten.ho@gmail.com`, ~10% NULL |
| `phone_number` | [MOCK] | Format `+55 XX XXXXX-XXXX`, ~15% NULL |
| `customer_city` | [REMAP] | `customer_city` |
| `customer_state` | [REMAP] | `customer_state` |
| `loyalty_tier` | [DERIVED] | Spark tính `SUM(payment_value)` per customer → phân hạng theo percentile chi tiêu (Top 5% = Platinum, 5-15% = Gold, 15-35% = Silver, còn lại = Bronze, chưa mua = NULL) |
| `created_at` | [DERIVED] | `MIN(order_purchase_timestamp)` per `customer_unique_id` |

> **Vì sao không fake?** Nếu random `loyalty_tier`, sẽ xảy ra mâu thuẫn: khách chi $5000 nhưng hạng Bronze, khách chi $10 nhưng hạng Platinum. Derive từ dữ liệu thực đảm bảo logic kinh doanh đúng.

### Bảng `products` ← File `olist_products_dataset.csv` + `product_category_name_translation.csv` + `olist_order_items_dataset.csv`

| Cột thiết kế | Nguồn | Cột Olist / Logic |
|---|---|---|
| `product_id` | [REMAP] | `product_id` |
| `product_name` | [MOCK] | Sinh từ `category` + số thứ tự (VD: `Health Beauty Product #1042`) |
| `category` | [REMAP] | `product_category_name` JOIN `product_category_name_translation.csv` → lấy `product_category_name_english` |
| `cost_price` | [DERIVED] | `AVG(price) * 0.7` per `product_id` từ `olist_order_items_dataset.csv` (biên lợi nhuận giả định 30%) |

> **Vì sao `cost_price` derive thay vì random?** Nếu random, sản phẩm bán giá $10 có thể bị gán cost $500, lợi nhuận âm phi logic. Derive từ giá bán thực đảm bảo cost < price luôn đúng.

### Bảng `orders` ← File `olist_orders_dataset.csv` + `olist_customers_dataset.csv` + `olist_order_payments_dataset.csv`

| Cột thiết kế | Nguồn | Cột Olist / Logic |
|---|---|---|
| `order_id` | [REMAP] | `order_id` |
| `user_id` | [DERIVED] | JOIN `orders.customer_id` → `customers.customer_id` → lấy `customer_unique_id` |
| `total_amount` | [DERIVED] | `SUM(payment_value)` per `order_id` từ `olist_order_payments_dataset.csv` |
| `order_status` | [REMAP] | Mapping: `created`→`Pending`, `approved/processing/invoiced/shipped`→`Processing`, `delivered`→`Completed`, `canceled/unavailable`→`Cancelled` |
| `payment_method` | [DERIVED] | `payment_type` WHERE `payment_sequential = 1` per `order_id`. Mapping: `credit_card`→`Credit Card`, `boleto`→`Bank Slip`, `voucher`→`Voucher`, `debit_card`→`Debit Card` |
| `created_at` | [REMAP] | `order_purchase_timestamp` |

> **Lưu ý `payment_method`:** Một đơn Olist có thể dùng nhiều phương thức thanh toán (`payment_sequential` > 1). Schema chỉ lưu phương thức chính (sequential = 1). Nếu cần phân tích đa phương thức, xem xét tạo bảng `fact_order_payments` riêng.

### Bảng `order_items` ← File `olist_order_items_dataset.csv`

| Cột thiết kế | Nguồn | Cột Olist / Logic |
|---|---|---|
| `item_id` | [DERIVED] | `{order_id}_{order_item_id}` |
| `order_id` | [REMAP] | `order_id` |
| `product_id` | [REMAP] | `product_id` |
| `quantity` | Hardcode | `1` — Mỗi dòng Olist = 1 đơn vị sản phẩm |
| `unit_price` | [REMAP] | `price` |

> **Lưu ý:** Olist còn có `seller_id` và `freight_value` trong file này. Không nằm trong scope schema chính nhưng có sẵn nếu cần mở rộng phân tích.

---

## 2. Phân bổ vào Nguồn Excel (Dữ liệu CSKH / Vận hành)

### Nguồn: File `olist_order_reviews_dataset.csv` → Lưu lại dưới dạng `CS_Tickets.xlsx`

Cách làm: Mở file `olist_order_reviews_dataset.csv` bằng Excel, áp dụng transformation theo bảng dưới, rồi lưu lại dưới dạng `CS_Tickets.xlsx`.

| Cột thiết kế | Nguồn | Cột Olist / Logic |
|---|---|---|
| `Ticket_ID` | [REMAP] | `review_id` |
| `Order_ID` | [REMAP] | `order_id` |
| `Customer_Email` | [MOCK] | Lấy email từ bảng `users` (JOIN qua order_id), ~5% dòng cố ý sai format để Spark xử lý dữ liệu bẩn |
| `Issue_Type` | [DERIVED] | `review_score` 1-2 → `Product Issue`, 3 → `General Inquiry`, 4-5 → `Positive Feedback` |
| `Status` | [DERIVED] | Nếu `review_answer_timestamp` NOT NULL → `Resolved`, ngược lại → `Open` |
| `Customer_Rating` | [REMAP] | `review_score` (1-5) |
| `Reported_Date` | [REMAP] | `review_creation_date` (có thể sai format giữa DD-MM và MM-DD — bài toán ETL cho Spark xử lý) |

**Sự hoàn hảo cho bài toán ETL của Spark:**

* File có cột `review_comment_message` chứa lời phàn nàn/khen ngợi bằng tiếng Bồ Đào Nha. Có tới ~50% số dòng bị NULL (khách rate sao nhưng không viết gì). Đây là bài toán tuyệt vời để Spark luyện tập lọc và xử lý NULL (ví dụ: Spark sẽ điền chữ "No Comment" vào các ô NULL này).

* Cột `review_id` đóng vai trò `Ticket_ID`.

* Có sẵn `order_id` để Spark thực hiện lệnh JOIN với bảng `orders` → từ đó truy ngược được `customer_unique_id` (user_id) qua chuỗi: `Ticket.order_id` → `orders.customer_id` → `customers.customer_unique_id`.

---

## 3. Phân bổ vào Nguồn API (Dữ liệu Log Hành vi) - [MOCK] Giả Lập Bổ Sung

**Vấn đề:** Dataset của Olist là dữ liệu hậu cần (sau khi đã mua hàng), nó không có dữ liệu log hành vi trên website (như `view_item`, `add_to_cart`, `cart_abandonment`).

**Cách giải quyết (Hybrid Mocking):** Tự sinh (mock) dữ liệu log JSON này bằng Python. Tuy nhiên, để dữ liệu có ý nghĩa, ta không sinh ngẫu nhiên 100%, mà sẽ lấy các `customer_unique_id` và `product_id` **có thật** từ Olist để ném vào log giả lập.

**Kịch bản sinh Log Hành Vi:**

1. Viết script Python đọc danh sách `user_id` (`customer_unique_id`) và `product_id` từ Olist.
2. Sinh ra các cụm sự kiện hợp lý. Ví dụ: Khách `861eff...` (ID thật từ Olist) có hành vi `view_item` sản phẩm `1e9e8e...` (ID thật), sau đó `add_to_cart`, nhưng không có checkout (sinh ra `cart_abandonment`).
3. Xuất ra các file `.json`.

> **Lưu ý:** Đây là nguồn dữ liệu giả lập **duy nhất** trong dự án. Hai nguồn còn lại (SQL + Excel) đều sử dụng dữ liệu thực từ Olist, chỉ áp dụng DERIVE và REMAP.

---

## 4. Các file Olist không sử dụng trong scope dự án

| File | Lý do không sử dụng |
|---|---|
| `olist_sellers_dataset.csv` | Phân tích seller nằm ngoài scope Customer 360 |
| `olist_geolocation_dataset.csv` | Dữ liệu tọa độ địa lý, không cần cho dashboard hiện tại |

Các file này có thể được đưa vào sử dụng nếu mở rộng scope dự án (VD: phân tích logistics, thời gian giao hàng theo vùng).
