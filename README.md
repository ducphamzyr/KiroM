<p align="center">
  <img src="docs/logo.svg" alt="KiroM" width="80">
</p>

<h1 align="center">KiroM</h1>

<p align="center">
  <strong>Convert Kiro accounts into OpenAI / Anthropic compatible API</strong>
</p>

<p align="center">
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat-square&logo=go" alt="Go"></a>
  <a href="https://www.docker.com/"><img src="https://img.shields.io/badge/Docker-Ready-2496ED?style=flat-square&logo=docker" alt="Docker"></a>
  <img src="https://img.shields.io/badge/i18n-EN%20%7C%20VI%20%7C%20中文-blue?style=flat-square" alt="i18n">
  <img src="https://img.shields.io/badge/License-MIT-green?style=flat-square" alt="License">
</p>

<p align="center">
  <a href="README.md">English</a> · <a href="README_VI.md">Tiếng Việt</a> · <a href="README_CN.md">中文</a>
</p>

---

## 📸 Screenshots

| Dashboard | Settings | Console |
|:-:|:-:|:-:|
| ![dashboard](docs/screenshot-dashboard.png) | ![settings](docs/screenshot-settings.png) | ![console](docs/screenshot-console.png) |

> **Note:** Add your screenshots to the `docs/` folder with the filenames above.

---

## ✨ Features

- 🔌 **Dual API** — Anthropic `/v1/messages` + OpenAI `/v1/chat/completions`
- 👥 **Multi-account pool** — Weighted round-robin, per-profile routing, auto-cooldown
- 🔄 **Auto token refresh** — Background OIDC/social token renewal
- 📡 **SSE Streaming** — Real-time streaming for both API formats
- 🧠 **Thinking mode** — Extended reasoning with configurable output format
- 🛡️ **Error mapping** — Clean, standard errors to clients; never leaks internals
- 📱 **Telegram bot** — Periodic health reports + event alerts (3 notification levels)
- 🌐 **Trilingual** — Full EN / VI / 中文 support (362 translation keys)
- 🎨 **Modern admin panel** — Toast notifications, 2-column settings, live console logs
- 🔐 **Multiple auth** — AWS Builder ID, IAM Identity Center, SSO Token, Credentials JSON, Web Cookie
- 📊 **Usage tracking** — Per-profile stats, quota monitoring, overage control
- 🌍 **Outbound proxy** — SOCKS5 / HTTP (global or per-account)
- 📝 **Input history** — Remembers last 3 values for add-account forms
- 🧹 **Prompt filter** — Strip Claude Code prompts, env noise, custom regex rules

---

## 🚀 Quick Start

### Docker Compose (Recommended)

```bash
git clone <your-repo-url>
cd KiroM
mkdir -p data
docker-compose up -d
```

### Build from Source

```bash
go build -o kirom .
./kirom
```

### Pre-built Binary (Windows)

Download `kiro-go.exe` from Releases, place `data/config.json` next to it, and run.

---

## 📖 Usage

1. Open `http://localhost:8080/admin`
2. Login (default password: `changeme`)
3. Add accounts via any supported method
4. Call the API:

```bash
# Claude API
curl http://localhost:8080/v1/messages \
  -H "Content-Type: application/json" \
  -H "anthropic-version: 2023-06-01" \
  -d '{"model":"claude-sonnet-4.5","max_tokens":1024,"messages":[{"role":"user","content":"Hello!"}]}'

# OpenAI API
curl http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"Hello!"}]}'
```

---

## 🧠 Thinking Mode

Append `-thinking` suffix to model name (e.g. `claude-sonnet-4.5-thinking`). Configure output format in Settings.

---

## 📱 Telegram Notifications

1. Create bot via [@BotFather](https://t.me/BotFather) → get token
2. In admin → Settings → Telegram → paste token → "Generate Connect Link"
3. Click the link → Start bot → auto-connected
4. Choose notification level: **Critical** / **Normal** / **Frequent**
5. Set health report interval (presets: 5m / 15m / 30m / 1h / 6h)

---

## 🔧 Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `CONFIG_PATH` | Config file path | `data/config.json` |
| `ADMIN_PASSWORD` | Admin password (overrides config) | — |
| `LOG_LEVEL` | Log verbosity: debug/info/warn/error | `info` |

---

## 📁 Project Structure

```
├── main.go              # Entry point
├── config/              # Config management (JSON persistence)
├── auth/                # OAuth: Builder ID, IAM SSO, OIDC refresh
├── pool/                # Account pool with weighted round-robin
├── proxy/               # Core: handler, translator, Kiro API, error mapper, Telegram
├── logger/              # Leveled logger with in-memory buffer
├── web/index.html       # Single-file admin panel (Vue-less, vanilla JS)
├── data/config.json     # Runtime config (auto-created)
├── Dockerfile           # Multi-stage Docker build
└── docs/                # Screenshots for README
```

---

## 🙏 Credits

This project is developed based on [Kiro-Go](https://github.com/Quorinex/Kiro-Go) by Quorinex. Thanks to the original author for building the foundation.

### Enhancements in this fork

- Complete trilingual support (EN / VI / 中文)
- Telegram health notification bot with connect-link flow
- Per-profile routing (weight/overage per profile, not just per account)
- Error mapping layer (never leaks raw upstream errors)
- Toast notification system (replaces browser `alert()`)
- Console tab with live logs, endpoint testing, system info
- Input history for add-account forms
- Rebranded as KiroM

---

## 📄 License

[MIT](LICENSE)
