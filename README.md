# Real-Time Fraud Transaction Detection

Hệ thống phát hiện gian lận giao dịch thời gian thực sử dụng Apache Kafka và Java.

## Tính năng

- **Kafka Producer**:  
  Đọc dữ liệu giao dịch từ file CSV và gửi các bản ghi lên topic Kafka.

- **Kafka Consumer**:  
  Nhận dữ liệu từ Kafka topic, xử lý và phát hiện gian lận trong thời gian thực.

- **Quản lý dữ liệu CSV**:
  - Đọc và xử lý dữ liệu giao dịch.
  - Hỗ trợ lấy mẫu ngẫu nhiên (sampling) từ dữ liệu khách hàng và giao dịch.

- **Mở rộng dễ dàng**:
  - Có thể tích hợp với các hệ thống xử lý luồng như Apache Flink, Apache Spark Streaming để phân tích nâng cao và mở rộng quy mô.

##  Workflow

<img width="899" height="543" alt="image" src="https://github.com/user-attachments/assets/ff594ed5-b08d-44a6-852f-12b2f662d9c3" />

