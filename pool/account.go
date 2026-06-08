// Package pool Quản lý pool tài khoản
// Triển khai round-robin load balancing, error cooldown, Token refresh
package pool

import (
	"kiro-go/config"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const overageFrequencyScale = 10
const tokenRefreshSkewSeconds int64 = 120

// AccountPool Pool tài khoản
type AccountPool struct {
	mu            sync.RWMutex
	accounts      []config.Account
	totalAccounts int
	currentIndex  uint64
	cooldowns     map[string]time.Time       // 账号冷却时间
	errorCounts   map[string]int             // 连续错误计数
	modelLists    map[string]map[string]bool // accountID → set of modelIDs (from ListAvailableModels)
}

type ModelCacheRouteStats struct {
	RouteID    string `json:"routeId"`
	ModelCount int    `json:"modelCount"`
}

type ModelCacheStats struct {
	RouteCount int                    `json:"routeCount"`
	ModelCount int                    `json:"modelCount"`
	Routes     []ModelCacheRouteStats `json:"routes"`
}

var (
	pool     *AccountPool
	poolOnce sync.Once
)

// GetPool Lấy singleton pool tài khoản toàn cục
func GetPool() *AccountPool {
	poolOnce.Do(func() {
		pool = &AccountPool{
			cooldowns:   make(map[string]time.Time),
			errorCounts: make(map[string]int),
			modelLists:  make(map[string]map[string]bool),
		}
		pool.Reload()
	})
	return pool
}

// Reload Tải lại tài khoản từ config
// Xây dựng danh sách có trọng số: weight<=1 xuất hiện 1 lần, weight>=2 xuất hiện weight lần
func (p *AccountPool) Reload() {
	p.mu.Lock()
	defer p.mu.Unlock()
	enabled := config.GetEnabledAccounts()
	var weighted []config.Account
	for _, a := range enabled {
		for _, route := range a.RuntimeRoutes() {
			w := effectiveWeight(route.Weight) * overageFrequencyScale
			if isOverUsageLimit(route) {
				if !route.AllowOverage {
					continue
				}
				w = effectiveOverageWeight(route.OverageWeight)
			}
			for j := 0; j < w; j++ {
				weighted = append(weighted, route)
			}
		}
	}
	p.accounts = weighted
	p.totalAccounts = len(enabled)
}

func routeID(acc config.Account) string {
	return acc.RouteID()
}

func configID(acc config.Account) string {
	return acc.ConfigID()
}

// GetNext Lấy tài khoản khả dụng tiếp theo (weighted round-robin)
func (p *AccountPool) GetNext() *config.Account {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.accounts) == 0 {
		return nil
	}

	allowOverUsage := config.GetAllowOverUsage()
	now := time.Now()
	n := len(p.accounts)
	seen := make(map[string]bool)

	// Weighted round-robin tìm tài khoản khả dụng
	for i := 0; i < n; i++ {
		idx := atomic.AddUint64(&p.currentIndex, 1) % uint64(n)
		acc := &p.accounts[idx]
		key := routeID(*acc)

		if seen[key] {
			continue
		}

		// Bỏ qua tài khoản đang cooldown
		if cooldown, ok := p.cooldowns[key]; ok && now.Before(cooldown) {
			seen[key] = true
			continue
		}

		// Bỏ qua Token sắp hết hạn
		if acc.ExpiresAt > 0 && time.Now().Unix() > acc.ExpiresAt-tokenRefreshSkewSeconds {
			seen[key] = true
			continue
		}

		// Bỏ qua tài khoản hết quota (AllowOverage cấp account hoặc AllowOverUsage toàn cục có thể cho qua)
		if isOverUsageLimit(*acc) && !acc.AllowOverage && !allowOverUsage {
			seen[key] = true
			continue
		}

		return acc
	}

	// Không có tài khoản khả dụng, trả về tài khoản có cooldown ngắn nhất (loại trừ hết quota, trừ khi cho phép vượt hạn mức)
	for i := range p.accounts {
		acc := &p.accounts[i]
		// Tài khoản hết quota không dùng làm fallback (trừ khi cấp account hoặc toàn cục cho phép vượt hạn mức)
		if isOverUsageLimit(*acc) && !acc.AllowOverage && !allowOverUsage {
			continue
		}
		if cooldown, ok := p.cooldowns[routeID(*acc)]; ok && now.Before(cooldown) {
			continue
		}
		return acc
	}
	return nil
}

// SetModelList Cache tập hợp model mà tài khoản hỗ trợ (handler gọi sau khi refresh)
func (p *AccountPool) SetModelList(accountID string, modelIDs []string) {
	set := make(map[string]bool, len(modelIDs))
	for _, id := range modelIDs {
		set[strings.ToLower(strings.TrimSpace(id))] = true
	}
	p.mu.Lock()
	p.modelLists[accountID] = set
	p.mu.Unlock()
}

// GetModelList Trả về danh sách model ID đã cache của tài khoản (dùng cho admin API).
// Nếu chưa có cache thì trả về slice rỗng.
func (p *AccountPool) GetModelList(accountID string) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	set, ok := p.modelLists[accountID]
	if !ok || len(set) == 0 {
		merged := make(map[string]bool)
		prefix := accountID + "|"
		for key, routeSet := range p.modelLists {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			for modelID := range routeSet {
				merged[modelID] = true
			}
		}
		if len(merged) > 0 {
			set = merged
		}
	}
	if len(set) == 0 {
		return []string{}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	return ids
}

func (p *AccountPool) ModelCacheStats() ModelCacheStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	routes := make([]ModelCacheRouteStats, 0, len(p.modelLists))
	uniqueModels := make(map[string]bool)
	for routeID, set := range p.modelLists {
		routes = append(routes, ModelCacheRouteStats{
			RouteID:    routeID,
			ModelCount: len(set),
		})
		for modelID := range set {
			uniqueModels[modelID] = true
		}
	}
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].RouteID < routes[j].RouteID
	})
	return ModelCacheStats{
		RouteCount: len(routes),
		ModelCount: len(uniqueModels),
		Routes:     routes,
	}
}

// accountHasModel Kiểm tra tài khoản có hỗ trợ model chỉ định không.
// Nếu tài khoản chưa có danh sách model (cold start), coi như hỗ trợ tất cả.
func (p *AccountPool) accountHasModel(accountID, model string) bool {
	list, ok := p.modelLists[accountID]
	if !ok || len(list) == 0 {
		return true // 冷启动：列表未就绪，乐观放行
	}
	return list[strings.ToLower(strings.TrimSpace(model))]
}

// GetNextForModel Lấy tài khoản khả dụng tiếp theo hỗ trợ model chỉ định.
// model phải là tên model thực tế đã bỏ hậu tố thinking.
// Nếu không có tài khoản nào có dữ liệu model list, hành vi giống GetNext (optimistic routing).
func (p *AccountPool) GetNextForModel(model string) *config.Account {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.accounts) == 0 {
		return nil
	}

	allowOverUsage := config.GetAllowOverUsage()
	now := time.Now()
	n := len(p.accounts)
	seen := make(map[string]bool)

	for i := 0; i < n; i++ {
		idx := atomic.AddUint64(&p.currentIndex, 1) % uint64(n)
		acc := &p.accounts[idx]
		key := routeID(*acc)

		if seen[key] {
			continue
		}
		if !p.accountHasModel(key, model) {
			seen[key] = true
			continue
		}
		if cooldown, ok := p.cooldowns[key]; ok && now.Before(cooldown) {
			seen[key] = true
			continue
		}
		if acc.ExpiresAt > 0 && time.Now().Unix() > acc.ExpiresAt-tokenRefreshSkewSeconds {
			seen[key] = true
			continue
		}
		if isOverUsageLimit(*acc) && !acc.AllowOverage && !allowOverUsage {
			seen[key] = true
			continue
		}
		return acc
	}

	// fallback: tìm tài khoản có cooldown ngắn nhất và hỗ trợ model
	for i := range p.accounts {
		acc := &p.accounts[i]
		if !p.accountHasModel(routeID(*acc), model) {
			continue
		}
		if isOverUsageLimit(*acc) && !acc.AllowOverage && !allowOverUsage {
			continue
		}
		if cooldown, ok := p.cooldowns[routeID(*acc)]; ok && now.Before(cooldown) {
			continue
		}
		return acc
	}
	return nil
}

// GetByID Lấy tài khoản theo ID
func (p *AccountPool) GetByID(id string) *config.Account {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for i := range p.accounts {
		if p.accounts[i].ID == id || routeID(p.accounts[i]) == id {
			return &p.accounts[i]
		}
	}
	return nil
}

// RecordSuccess Ghi nhận request thành công, xóa cooldown
func (p *AccountPool) RecordSuccess(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.cooldowns, id)
	p.errorCounts[id] = 0
}

// RecordError Ghi nhận request lỗi, đặt cooldown
func (p *AccountPool) RecordError(id string, isQuotaError bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.errorCounts[id]++

	if isQuotaError {
		// Lỗi quota, cooldown 1 giờ
		p.cooldowns[id] = time.Now().Add(time.Hour)
	} else if p.errorCounts[id] >= 3 {
		// 3 lỗi liên tiếp, cooldown 1 phút
		p.cooldowns[id] = time.Now().Add(time.Minute)
	}
}

// UpdateToken Cập nhật Token tài khoản
func (p *AccountPool) UpdateToken(id, accessToken, refreshToken string, expiresAt int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.accounts {
		if p.accounts[i].ID == id || configID(p.accounts[i]) == id {
			p.accounts[i].AccessToken = accessToken
			if refreshToken != "" {
				p.accounts[i].RefreshToken = refreshToken
			}
			p.accounts[i].ExpiresAt = expiresAt
		}
	}
}

// Count Trả về tổng số tài khoản
func (p *AccountPool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.totalAccounts > 0 {
		return p.totalAccounts
	}

	seen := make(map[string]bool)
	for _, acc := range p.accounts {
		seen[configID(acc)] = true
	}
	return len(seen)
}

// AvailableCount Trả về số tài khoản khả dụng
func (p *AccountPool) AvailableCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	count := 0
	seen := make(map[string]bool)
	for _, acc := range p.accounts {
		key := routeID(acc)
		if seen[key] {
			continue
		}
		seen[key] = true
		if cooldown, ok := p.cooldowns[key]; ok && now.Before(cooldown) {
			continue
		}
		count++
	}
	return count
}

// UpdateStats Cập nhật thống kê tài khoản
func (p *AccountPool) UpdateStats(id string, tokens int, credits float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var updated bool
	var requestCount, errorCount, totalTokens int
	var totalCredits float64
	var lastUsed int64
	var persistID string
	var persistProfileArn string
	for i := range p.accounts {
		if routeID(p.accounts[i]) == id || p.accounts[i].ID == id {
			if !updated {
				p.accounts[i].RequestCount++
				p.accounts[i].TotalTokens += tokens
				p.accounts[i].TotalCredits += credits
				p.accounts[i].LastUsed = time.Now().Unix()

				requestCount = p.accounts[i].RequestCount
				errorCount = p.accounts[i].ErrorCount
				totalTokens = p.accounts[i].TotalTokens
				totalCredits = p.accounts[i].TotalCredits
				lastUsed = p.accounts[i].LastUsed
				persistID = configID(p.accounts[i])
				persistProfileArn = p.accounts[i].ProfileArn
				updated = true
				continue
			}
		}
	}
	if updated && persistID != "" {
		go config.UpdateAccountProfileStats(persistID, persistProfileArn, requestCount, errorCount, totalTokens, totalCredits, lastUsed)
	}
}

// GetAllAccounts Lấy bản sao tất cả tài khoản
func (p *AccountPool) GetAllAccounts() []config.Account {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]config.Account, len(p.accounts))
	copy(result, p.accounts)
	return result
}

func isOverUsageLimit(acc config.Account) bool {
	return acc.UsageLimit > 0 && acc.UsageCurrent >= acc.UsageLimit
}

func effectiveWeight(weight int) int {
	if weight < 1 {
		return 1
	}
	return weight
}

func effectiveOverageWeight(weight int) int {
	if weight < 1 {
		return 1
	}
	if weight > overageFrequencyScale {
		return overageFrequencyScale
	}
	return weight
}
