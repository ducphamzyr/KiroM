<p align="center">
  <img src="docs/logo.svg" alt="KiroM" width="80">
</p>

<h1 align="center">KiroM</h1>

<p align="center">
  <strong>将 Kiro 账号转换为 OpenAI / Anthropic 兼容 API</strong>
</p>

<p align="center">
  <a href="README.md">English</a> · <a href="README_VI.md">Tiếng Việt</a> · 中文
</p>

---

## 项目来源

本项目基于 Quorinex 的 [原版 Kiro-Go](https://github.com/Quorinex/Kiro-Go) 开发。

## 相比原版的改进

- **完整三语支持**（英 / 越 / 中）— 362 个翻译 key
- **Telegram 健康通知** — 定时报告 + 事件提醒，链接连接，3 级通知
- **多 Profile 修复** — 每 Profile 独立路由（权重/超额单独设置），禁用真正生效
- **错误映射层** — 标准化错误返回客户端，不泄露内部信息
- **Toast 通知** — 替代浏览器 `alert()`
- **Console 标签** — 实时日志、端点测试、系统信息
- **输入历史** — 添加账号时记住最近 3 个值
- **品牌重塑：KiroM**

## 快速开始

```bash
# Docker
docker-compose up -d

# 或从源码构建
go build -o kirom .
./kirom
```

打开 `http://localhost:8080/admin` → 登录（默认密码：`changeme`）→ 添加账号 → 调用 API。

## 详细文档

请参阅 [README.md](README.md)（英文）了解完整功能、配置说明和截图。

## License

[MIT](LICENSE)
