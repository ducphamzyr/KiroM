<p align="center">
  <img src="docs/logo.svg" alt="KiroM" width="80">
</p>

<h1 align="center">KiroM</h1>

<p align="center">
  <strong>Chuyển đổi tài khoản Kiro thành API tương thích OpenAI / Anthropic</strong>
</p>

<p align="center">
  <a href="README.md">English</a> · Tiếng Việt · <a href="README_CN.md">中文</a>
</p>

---

## Nguồn gốc

Project này được phát triển dựa trên [Kiro-Go gốc](https://github.com/Quorinex/Kiro-Go) của Quorinex.

## Nâng cấp so với bản gốc

- **Đa ngôn ngữ hoàn chỉnh** (EN / VI / 中文) — 362 key dịch thuật
- **Telegram bot thông báo** — health report + event alerts, kết nối qua link, 3 mức thông báo
- **Sửa lỗi multi-profile** — per-profile routing (weight/overage riêng), tắt route thực sự có hiệu lực
- **Error mapping** — bọc lỗi sạch cho client, không lộ thông tin nội bộ
- **Toast notifications** — thay thế `alert()` bằng giao diện trên trang
- **Console tab** — live log, test endpoint, system info
- **Lịch sử nhập liệu** — ghi nhớ 3 giá trị gần nhất khi add account
- **Rebrand: KiroM**

## Khởi động nhanh

```bash
# Docker
docker-compose up -d

# Hoặc build từ source
go build -o kirom .
./kirom
```

Mở `http://localhost:8080/admin` → đăng nhập (mặc định: `changeme`) → thêm tài khoản → gọi API.

## Hướng dẫn chi tiết

Xem [README.md](README.md) (tiếng Anh) để biết đầy đủ tính năng, cấu hình, screenshots.

## License

[MIT](LICENSE)
