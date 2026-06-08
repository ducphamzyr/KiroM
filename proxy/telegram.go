package proxy

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"kiro-go/config"
	"kiro-go/logger"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ==================== Telegram Notifier ====================

// telegramNotifier xử lý thông báo qua Telegram.
// Bao gồm: health report định kỳ, event-based alerts, polling webhook /start.
type telegramNotifier struct {
	handler    *Handler
	stopCh     chan struct{}
	pollStopCh chan struct{}
	mu         sync.Mutex
	running    bool
	polling    bool
}

func newTelegramNotifier(h *Handler) *telegramNotifier {
	return &telegramNotifier{
		handler:    h,
		stopCh:     make(chan struct{}),
		pollStopCh: make(chan struct{}),
	}
}

// Restart re-reads config and (re)starts or stops the notifier.
func (tn *telegramNotifier) Restart() {
	tn.mu.Lock()
	defer tn.mu.Unlock()

	if tn.running {
		close(tn.stopCh)
		tn.running = false
	}

	cfg := config.GetTelegramConfig()
	if !cfg.Enabled || cfg.BotToken == "" || cfg.UserID == "" {
		return
	}

	tn.stopCh = make(chan struct{})
	tn.running = true
	interval := time.Duration(cfg.Interval) * time.Minute

	go tn.healthLoop(tn.stopCh, cfg.BotToken, cfg.UserID, interval, cfg.NotifyLang)
}

// StartPolling starts polling Telegram for /start commands to auto-connect user.
func (tn *telegramNotifier) StartPolling(botToken, connectToken string) {
	tn.mu.Lock()
	defer tn.mu.Unlock()
	if tn.polling {
		close(tn.pollStopCh)
	}
	tn.pollStopCh = make(chan struct{})
	tn.polling = true
	go tn.pollLoop(tn.pollStopCh, botToken, connectToken)
}

// StopPolling stops the /start polling loop.
func (tn *telegramNotifier) StopPolling() {
	tn.mu.Lock()
	defer tn.mu.Unlock()
	if tn.polling {
		close(tn.pollStopCh)
		tn.polling = false
	}
}

func (tn *telegramNotifier) pollLoop(stop chan struct{}, botToken, connectToken string) {
	offset := 0
	for {
		select {
		case <-stop:
			return
		default:
		}

		updates, newOffset := pollTelegramUpdates(botToken, offset)
		if newOffset > offset {
			offset = newOffset
		}

		for _, u := range updates {
			text := strings.TrimSpace(u.Message.Text)
			// Accept /start <token> or plain /start (if token matches config)
			if strings.HasPrefix(text, "/start") {
				parts := strings.Fields(text)
				token := ""
				if len(parts) > 1 {
					token = parts[1]
				}
				if token == connectToken || connectToken == "" {
					userID := fmt.Sprintf("%d", u.Message.From.ID)
					if err := config.SetTelegramUserID(userID); err != nil {
						logger.Warnf("[Telegram] Failed to save user ID: %v", err)
						continue
					}
					lang := config.GetTelegramConfig().NotifyLang
					msg := tgText(lang, "connect_success")
					sendTelegramMessage(botToken, userID, msg)
					logger.Infof("[Telegram] User connected: %s (@%s)", userID, u.Message.From.Username)
					// Stop polling after successful connect
					tn.mu.Lock()
					tn.polling = false
					tn.mu.Unlock()
					// Restart notifier with new user ID
					go tn.Restart()
					return
				}
			}
		}

		time.Sleep(2 * time.Second)
	}
}

// ==================== Health Report Loop ====================

func (tn *telegramNotifier) healthLoop(stop chan struct{}, botToken, userID string, interval time.Duration, lang string) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Gửi báo cáo ngay khi khởi động
	tn.sendHealthReport(botToken, userID, lang)

	for {
		select {
		case <-ticker.C:
			tn.sendHealthReport(botToken, userID, lang)
		case <-stop:
			return
		}
	}
}

func (tn *telegramNotifier) sendHealthReport(botToken, userID, lang string) {
	h := tn.handler
	totalReq := atomic.LoadInt64(&h.totalRequests)
	successReq := atomic.LoadInt64(&h.successRequests)
	failedReq := atomic.LoadInt64(&h.failedRequests)
	totalTokens := atomic.LoadInt64(&h.totalTokens)
	totalCredits := h.getCredits()
	uptime := time.Now().Unix() - h.startTime
	accounts := h.pool.Count()
	available := h.pool.AvailableCount()

	uptimeStr := formatUptime(uptime)
	successRate := 100.0
	if totalReq > 0 {
		successRate = float64(successReq) / float64(totalReq) * 100
	}

	// Trạng thái tổng quan + emoji theo tình hình thực tế.
	var statusEmoji, statusText string
	if available == 0 && accounts > 0 {
		statusEmoji, statusText = "🔴", tgText(lang, "status_no_accounts")
	} else if accounts == 0 {
		statusEmoji, statusText = "🔴", tgText(lang, "status_no_accounts")
	} else if totalReq > 10 && successRate < 80 {
		statusEmoji, statusText = "🟡", tgText(lang, "status_high_errors")
	} else {
		statusEmoji, statusText = "🟢", tgText(lang, "status_ok")
	}

	// Thanh tài khoản trực quan (▰ khả dụng / ▱ không).
	bar := buildAccountBar(available, accounts)

	now := time.Now().Format("2006-01-02 15:04:05")

	text := fmt.Sprintf(
		"%s *%s*\n"+
			"`%s`\n\n"+
			"%s\n\n"+
			"┌ 🖥 *Server*\n"+
			"├ %s: `%s`\n"+
			"├ 🔖 Version: `v%s`\n"+
			"└ 🕐 %s\n\n"+
			"┌ 👥 *%s*\n"+
			"├ %s `%d / %d`\n"+
			"└ %s\n\n"+
			"┌ 📊 *%s*\n"+
			"├ 📨 %s: `%d`\n"+
			"├ ✅ %s: `%d`\n"+
			"├ ❌ %s: `%d`\n"+
			"├ 📈 %s: `%.1f%%`\n"+
			"├ 🔤 Tokens: `%s`\n"+
			"└ 💎 Credits: `%.2f`",
		statusEmoji, tgText(lang, "health_title"),
		statusText,
		statusBadgeLine(lang, statusEmoji),
		tgText(lang, "uptime"), uptimeStr,
		config.Version,
		now,
		tgText(lang, "accounts_section"),
		bar, available, accounts,
		accountHealthLine(lang, available, accounts),
		tgText(lang, "traffic_section"),
		tgText(lang, "requests"), totalReq,
		tgText(lang, "success"), successReq,
		tgText(lang, "failed"), failedReq,
		tgText(lang, "success_rate"), successRate,
		formatNum64(totalTokens),
		totalCredits,
	)

	if err := sendTelegramMessage(botToken, userID, text); err != nil {
		logger.Warnf("[Telegram] Failed to send health report: %v", err)
	}
}

// buildAccountBar tạo thanh biểu thị tỷ lệ account khả dụng.
func buildAccountBar(available, total int) string {
	if total <= 0 {
		return "▱▱▱▱▱▱▱▱▱▱"
	}
	const slots = 10
	filled := int(float64(available) / float64(total) * slots)
	if filled < 0 {
		filled = 0
	}
	if filled > slots {
		filled = slots
	}
	return strings.Repeat("▰", filled) + strings.Repeat("▱", slots-filled)
}

func statusBadgeLine(lang, emoji string) string {
	switch emoji {
	case "🔴":
		return "⚠️ " + tgText(lang, "status_no_accounts")
	case "🟡":
		return "⚠️ " + tgText(lang, "status_high_errors")
	default:
		return "✅ " + tgText(lang, "status_ok")
	}
}

func accountHealthLine(lang string, available, total int) string {
	pct := 0
	if total > 0 {
		pct = int(float64(available) / float64(total) * 100)
	}
	return fmt.Sprintf("%s: `%d%%`", tgText(lang, "availability"), pct)
}

// ==================== Event-based Notifications ====================

// NotifyEvent sends an event notification based on the configured notify level.
// level: "critical", "normal", "frequent"
func (tn *telegramNotifier) NotifyEvent(minLevel, emoji, messageKey string, args ...interface{}) {
	cfg := config.GetTelegramConfig()
	if !cfg.Enabled || cfg.BotToken == "" || cfg.UserID == "" {
		return
	}

	// Level filtering
	if !shouldNotify(cfg.NotifyLevel, minLevel) {
		return
	}

	msg := tgText(cfg.NotifyLang, messageKey)
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	text := emoji + " " + msg

	go func() {
		if err := sendTelegramMessage(cfg.BotToken, cfg.UserID, text); err != nil {
			logger.Warnf("[Telegram] Failed to send event notification: %v", err)
		}
	}()
}

func shouldNotify(configLevel, eventLevel string) bool {
	levels := map[string]int{"critical": 3, "normal": 2, "frequent": 1}
	configRank := levels[configLevel]
	eventRank := levels[eventLevel]
	if configRank == 0 {
		configRank = 2 // default normal
	}
	if eventRank == 0 {
		eventRank = 2
	}
	// Lower rank = more verbose. Event is sent if eventRank >= configRank.
	return eventRank >= configRank
}

// ==================== Telegram API Helpers ====================

func sendTelegramMessage(botToken, chatID, text string) error {
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", botToken)
	params := url.Values{}
	params.Set("chat_id", chatID)
	params.Set("text", text)
	params.Set("parse_mode", "Markdown")

	resp, err := http.Post(apiURL, "application/x-www-form-urlencoded", strings.NewReader(params.Encode()))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

type telegramUpdate struct {
	UpdateID int `json:"update_id"`
	Message  struct {
		Text string `json:"text"`
		From struct {
			ID       int64  `json:"id"`
			Username string `json:"username"`
		} `json:"from"`
	} `json:"message"`
}

func pollTelegramUpdates(botToken string, offset int) ([]telegramUpdate, int) {
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/getUpdates?offset=%d&timeout=5&allowed_updates=[\"message\"]", botToken, offset)
	resp, err := http.Get(apiURL)
	if err != nil {
		return nil, offset
	}
	defer resp.Body.Close()

	var result struct {
		OK     bool             `json:"ok"`
		Result []telegramUpdate `json:"result"`
	}
	json.NewDecoder(resp.Body).Decode(&result)

	newOffset := offset
	for _, u := range result.Result {
		if u.UpdateID >= newOffset {
			newOffset = u.UpdateID + 1
		}
	}
	return result.Result, newOffset
}

// GenerateConnectToken tạo token ngẫu nhiên cho link connect.
func GenerateConnectToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// SendTestMessage gửi tin nhắn test.
func SendTestMessage(botToken, userID string) error {
	return sendTelegramMessage(botToken, userID, "✅ KiroM — Telegram bot connected successfully!")
}

// ==================== i18n for Telegram messages ====================

var tgMessages = map[string]map[string]string{
	"vi": {
		"connect_success":   "✅ Đã kết nối thành công! Bạn sẽ nhận thông báo từ KiroM.",
		"health_title":      "Báo cáo sức khỏe KiroM",
		"status_ok":         "Server đang hoạt động bình thường",
		"status_no_accounts": "Không có tài khoản khả dụng",
		"status_high_errors": "Tỷ lệ lỗi cao bất thường",
		"uptime":            "Thời gian chạy",
		"accounts":          "Tài khoản (khả dụng / tổng)",
		"accounts_section":  "Tài khoản",
		"traffic_section":   "Lưu lượng",
		"availability":      "Tỷ lệ khả dụng",
		"requests":          "Yêu cầu",
		"success":           "Thành công",
		"failed":            "Thất bại",
		"success_rate":      "Tỷ lệ thành công",
		"evt_token_refresh_ok":    "Token đã tự động refresh thành công cho %s",
		"evt_token_refresh_fail":  "⚠️ Token refresh thất bại cho %s: %s",
		"evt_account_banned":      "Tài khoản %s đã bị ban: %s",
		"evt_account_suspended":   "Tài khoản %s bị tạm ngưng",
		"evt_quota_exhausted":     "Tài khoản %s đã hết hạn mức sử dụng",
		"evt_no_accounts":         "Không còn tài khoản khả dụng! Server không thể xử lý yêu cầu.",
		"evt_account_recovered":   "Tài khoản %s đã được khôi phục (hết ban)",
		"evt_server_start":        "Server đã khởi động thành công (v%s)",
		"evt_high_error_rate":     "Tỷ lệ lỗi đã vượt 50%% trong 5 phút gần nhất",
	},
	"en": {
		"connect_success":   "✅ Connected successfully! You will receive notifications from KiroM.",
		"health_title":      "KiroM Health Report",
		"status_ok":         "Server is running normally",
		"status_no_accounts": "No available accounts",
		"status_high_errors": "Abnormally high error rate",
		"uptime":            "Uptime",
		"accounts":          "Accounts (available / total)",
		"accounts_section":  "Accounts",
		"traffic_section":   "Traffic",
		"availability":      "Availability",
		"requests":          "Requests",
		"success":           "Success",
		"failed":            "Failed",
		"success_rate":      "Success rate",
		"evt_token_refresh_ok":    "Token auto-refreshed successfully for %s",
		"evt_token_refresh_fail":  "⚠️ Token refresh failed for %s: %s",
		"evt_account_banned":      "Account %s has been banned: %s",
		"evt_account_suspended":   "Account %s has been suspended",
		"evt_quota_exhausted":     "Account %s quota exhausted",
		"evt_no_accounts":         "No accounts available! Server cannot process requests.",
		"evt_account_recovered":   "Account %s recovered (unbanned)",
		"evt_server_start":        "Server started successfully (v%s)",
		"evt_high_error_rate":     "Error rate exceeded 50%% in the last 5 minutes",
	},
	"zh": {
		"connect_success":   "✅ 连接成功！您将收到来自 KiroM 的通知。",
		"health_title":      "KiroM 健康报告",
		"status_ok":         "服务器运行正常",
		"status_no_accounts": "没有可用账号",
		"status_high_errors": "错误率异常偏高",
		"uptime":            "运行时间",
		"accounts":          "账号（可用 / 总计）",
		"accounts_section":  "账号",
		"traffic_section":   "流量",
		"availability":      "可用率",
		"requests":          "请求",
		"success":           "成功",
		"failed":            "失败",
		"success_rate":      "成功率",
		"evt_token_refresh_ok":    "Token 已为 %s 自动刷新成功",
		"evt_token_refresh_fail":  "⚠️ Token 刷新失败 %s：%s",
		"evt_account_banned":      "账号 %s 已被封禁：%s",
		"evt_account_suspended":   "账号 %s 已被暂停",
		"evt_quota_exhausted":     "账号 %s 额度已耗尽",
		"evt_no_accounts":         "没有可用账号！服务器无法处理请求。",
		"evt_account_recovered":   "账号 %s 已恢复（解除封禁）",
		"evt_server_start":        "服务器已成功启动（v%s）",
		"evt_high_error_rate":     "最近5分钟内错误率超过50%%",
	},
}

func tgText(lang, key string) string {
	if msgs, ok := tgMessages[lang]; ok {
		if msg, ok := msgs[key]; ok {
			return msg
		}
	}
	// Fallback to English
	if msgs, ok := tgMessages["en"]; ok {
		if msg, ok := msgs[key]; ok {
			return msg
		}
	}
	return key
}

func formatUptime(seconds int64) string {
	days := seconds / 86400
	hours := (seconds % 86400) / 3600
	minutes := (seconds % 3600) / 60
	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm", days, hours, minutes)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	return fmt.Sprintf("%dm", minutes)
}

func formatNum64(n int64) string {
	if n >= 1000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	}
	if n >= 1000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%d", n)
}
