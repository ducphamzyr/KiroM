// Package config provides configuration management for KiroM.
//
// This package handles persistent storage and retrieval of:
//   - Account credentials and authentication tokens
//   - Server settings (port, host, API keys)
//   - Usage statistics and metrics
//   - Thinking mode configuration for AI responses
//
// All configuration is stored in a JSON file with thread-safe access
// via read-write mutex protection.
package config

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
)

// GenerateMachineId generates a UUID v4 format machine identifier.
// This ID is used to uniquely identify the proxy instance in Kiro API requests,
// helping with request tracking and rate limiting on the server side.
func GenerateMachineId() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	bytes[6] = (bytes[6] & 0x0f) | 0x40 // 版本 4
	bytes[8] = (bytes[8] & 0x3f) | 0x80 // 变体
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		bytes[0:4], bytes[4:6], bytes[6:8], bytes[8:10], bytes[10:16])
}

// Account represents a Kiro API account with authentication credentials and usage statistics.
type Account struct {
	// Basic identification
	ID       string `json:"id"`                 // Unique account identifier (UUID)
	Email    string `json:"email,omitempty"`    // User email address
	UserId   string `json:"userId,omitempty"`   // Kiro user ID
	Nickname string `json:"nickname,omitempty"` // Display name for admin panel

	// Authentication credentials
	AccessToken      string                  `json:"accessToken"`                // OAuth access token for API calls
	RefreshToken     string                  `json:"refreshToken"`               // OAuth refresh token for token renewal
	ClientID         string                  `json:"clientId,omitempty"`         // OIDC client ID (for IdC auth)
	ClientSecret     string                  `json:"clientSecret,omitempty"`     // OIDC client secret (for IdC auth)
	AuthMethod       string                  `json:"authMethod"`                 // Authentication method: "idc" (AWS IdC) or "social" (GitHub/Google)
	Provider         string                  `json:"provider,omitempty"`         // Identity provider name (e.g., "BuilderId", "GitHub")
	Region           string                  `json:"region"`                     // AWS region for OIDC endpoints
	StartUrl         string                  `json:"startUrl,omitempty"`         // AWS SSO start URL
	ExpiresAt        int64                   `json:"expiresAt,omitempty"`        // Token expiration timestamp (Unix seconds)
	MachineId        string                  `json:"machineId,omitempty"`        // UUID machine identifier for request tracking
	ProfileArn       string                  `json:"profileArn,omitempty"`       // CodeWhisperer/Kiro profile ARN for generation requests
	ProfileArns      []string                `json:"profileArns"`                // Enabled profile ARNs for per-profile routing (nil=unconfigured, []=all disabled)
	KnownProfileArns []string                `json:"knownProfileArns,omitempty"` // Discovered profile ARNs shown in admin
	ProfileStates    map[string]ProfileState `json:"profileStates,omitempty"`    // Per-profile usage, quota, and runtime stats
	RuntimeID        string                  `json:"-"`                          // Runtime route ID, not persisted
	ParentID         string                  `json:"-"`                          // Persisted account ID for runtime routes

	// Per-account outbound proxy (falls back to global ProxyURL if empty)
	ProxyURL string `json:"proxyURL,omitempty"`

	// Priority weight for load balancing (higher = more requests)
	Weight int `json:"weight,omitempty"` // 0 or 1 = normal, 2+ = higher priority

	// Overage behavior after the main usage limit is reached.
	AllowOverage  bool `json:"allowOverage,omitempty"`  // Whether to keep using the account after UsageLimit is reached
	OverageWeight int  `json:"overageWeight,omitempty"` // 1-10, lower values reduce overage request frequency

	// Account status
	Enabled   bool   `json:"enabled"`             // Whether account is active in the pool
	BanStatus string `json:"banStatus,omitempty"` // Ban status: "ACTIVE", "BANNED", "SUSPENDED"
	BanReason string `json:"banReason,omitempty"` // Reason for ban/suspension
	BanTime   int64  `json:"banTime,omitempty"`   // Timestamp when ban was detected

	// Subscription information
	SubscriptionType  string `json:"subscriptionType,omitempty"`  // Tier: FREE, PRO, PRO_PLUS, or POWER
	SubscriptionTitle string `json:"subscriptionTitle,omitempty"` // Human-readable subscription name
	DaysRemaining     int    `json:"daysRemaining,omitempty"`     // Days until subscription expires

	// Usage tracking
	UsageCurrent  float64 `json:"usageCurrent,omitempty"`  // Current period usage (credits)
	UsageLimit    float64 `json:"usageLimit,omitempty"`    // Maximum allowed usage per period
	UsagePercent  float64 `json:"usagePercent,omitempty"`  // Usage percentage (0.0-1.0)
	NextResetDate string  `json:"nextResetDate,omitempty"` // Date when usage resets (YYYY-MM-DD)
	LastRefresh   int64   `json:"lastRefresh,omitempty"`   // Last info refresh timestamp

	// Trial usage tracking
	TrialUsageCurrent float64 `json:"trialUsageCurrent,omitempty"` // Trial quota current usage
	TrialUsageLimit   float64 `json:"trialUsageLimit,omitempty"`   // Trial quota total limit
	TrialUsagePercent float64 `json:"trialUsagePercent,omitempty"` // Trial quota usage percentage (0.0-1.0)
	TrialStatus       string  `json:"trialStatus,omitempty"`       // Trial status: ACTIVE, EXPIRED, NONE
	TrialExpiresAt    int64   `json:"trialExpiresAt,omitempty"`    // Trial expiration timestamp (Unix seconds)

	// Runtime statistics (updated during operation)
	RequestCount int     `json:"requestCount,omitempty"` // Total requests processed
	ErrorCount   int     `json:"errorCount,omitempty"`   // Total errors encountered
	LastUsed     int64   `json:"lastUsed,omitempty"`     // Last request timestamp
	TotalTokens  int     `json:"totalTokens,omitempty"`  // Cumulative tokens processed
	TotalCredits float64 `json:"totalCredits,omitempty"` // Cumulative credits consumed
}

// ProfileState stores operational data that belongs to one Kiro profile route.
// The parent account remains only the shared OAuth/login container.
type ProfileState struct {
	SubscriptionType  string  `json:"subscriptionType,omitempty"`
	SubscriptionTitle string  `json:"subscriptionTitle,omitempty"`
	DaysRemaining     int     `json:"daysRemaining,omitempty"`
	UsageCurrent      float64 `json:"usageCurrent,omitempty"`
	UsageLimit        float64 `json:"usageLimit,omitempty"`
	UsagePercent      float64 `json:"usagePercent,omitempty"`
	NextResetDate     string  `json:"nextResetDate,omitempty"`
	LastRefresh       int64   `json:"lastRefresh,omitempty"`

	TrialUsageCurrent float64 `json:"trialUsageCurrent,omitempty"`
	TrialUsageLimit   float64 `json:"trialUsageLimit,omitempty"`
	TrialUsagePercent float64 `json:"trialUsagePercent,omitempty"`
	TrialStatus       string  `json:"trialStatus,omitempty"`
	TrialExpiresAt    int64   `json:"trialExpiresAt,omitempty"`

	RequestCount int     `json:"requestCount,omitempty"`
	ErrorCount   int     `json:"errorCount,omitempty"`
	LastUsed     int64   `json:"lastUsed,omitempty"`
	TotalTokens  int     `json:"totalTokens,omitempty"`
	TotalCredits float64 `json:"totalCredits,omitempty"`

	// Per-profile routing overrides. nil = inherit account-level value.
	Weight        *int  `json:"weight,omitempty"`        // Load-balancing weight for this route
	AllowOverage  *bool `json:"allowOverage,omitempty"`  // Continue serving after quota exhausted
	OverageWeight *int  `json:"overageWeight,omitempty"` // Overage call frequency weight (1-10)
}

func (a Account) ConfigID() string {
	if a.ParentID != "" {
		return a.ParentID
	}
	return a.ID
}

func (a Account) RouteID() string {
	if a.RuntimeID != "" {
		return a.RuntimeID
	}
	if a.ProfileArn != "" {
		return a.ID + "|" + a.ProfileArn
	}
	return a.ID
}

func (a Account) RuntimeRoutes() []Account {
	// Distinguish "unconfigured" (ProfileArns == nil) from "all routes explicitly
	// disabled" (ProfileArns is non-nil but empty after normalization). When the
	// admin turns off every profile route we must honor that and route nothing,
	// instead of silently falling back to a.ProfileArn.
	configured := a.ProfileArns != nil
	profiles := normalizeProfileArns(a.ProfileArns)
	if len(profiles) == 0 {
		if configured {
			// Explicitly disabled all profile routes: emit no routes.
			return nil
		}
		if a.ProfileArn != "" {
			profiles = []string{a.ProfileArn}
		}
	}
	if len(profiles) == 0 {
		route := a
		route.ParentID = a.ID
		route.RuntimeID = a.ID
		return []Account{route}
	}

	routes := make([]Account, 0, len(profiles))
	for _, profileArn := range profiles {
		route := a.WithProfileState(profileArn)
		route.ParentID = a.ID
		route.ProfileArn = profileArn
		route.ProfileArns = []string{profileArn}
		route.RuntimeID = a.ID + "|" + profileArn
		routes = append(routes, route)
	}
	return routes
}

func (a Account) GetProfileState(profileArn string) ProfileState {
	profileArn = strings.TrimSpace(profileArn)
	if profileArn != "" && a.ProfileStates != nil {
		if state, ok := a.ProfileStates[profileArn]; ok {
			return state
		}
	}
	return ProfileState{
		SubscriptionType:  a.SubscriptionType,
		SubscriptionTitle: a.SubscriptionTitle,
		DaysRemaining:     a.DaysRemaining,
		UsageCurrent:      a.UsageCurrent,
		UsageLimit:        a.UsageLimit,
		UsagePercent:      a.UsagePercent,
		NextResetDate:     a.NextResetDate,
		LastRefresh:       a.LastRefresh,

		TrialUsageCurrent: a.TrialUsageCurrent,
		TrialUsageLimit:   a.TrialUsageLimit,
		TrialUsagePercent: a.TrialUsagePercent,
		TrialStatus:       a.TrialStatus,
		TrialExpiresAt:    a.TrialExpiresAt,

		RequestCount: a.RequestCount,
		ErrorCount:   a.ErrorCount,
		LastUsed:     a.LastUsed,
		TotalTokens:  a.TotalTokens,
		TotalCredits: a.TotalCredits,
	}
}

func (a Account) WithProfileState(profileArn string) Account {
	route := a
	state := a.GetProfileState(profileArn)
	route.ProfileArn = strings.TrimSpace(profileArn)
	route.SubscriptionType = state.SubscriptionType
	route.SubscriptionTitle = state.SubscriptionTitle
	route.DaysRemaining = state.DaysRemaining
	route.UsageCurrent = state.UsageCurrent
	route.UsageLimit = state.UsageLimit
	route.UsagePercent = state.UsagePercent
	route.NextResetDate = state.NextResetDate
	route.LastRefresh = state.LastRefresh
	route.TrialUsageCurrent = state.TrialUsageCurrent
	route.TrialUsageLimit = state.TrialUsageLimit
	route.TrialUsagePercent = state.TrialUsagePercent
	route.TrialStatus = state.TrialStatus
	route.TrialExpiresAt = state.TrialExpiresAt
	route.RequestCount = state.RequestCount
	route.ErrorCount = state.ErrorCount
	route.LastUsed = state.LastUsed
	route.TotalTokens = state.TotalTokens
	route.TotalCredits = state.TotalCredits
	// Apply per-profile routing overrides (fall back to account-level when nil).
	if state.Weight != nil {
		route.Weight = *state.Weight
	}
	if state.AllowOverage != nil {
		route.AllowOverage = *state.AllowOverage
	}
	if state.OverageWeight != nil {
		route.OverageWeight = *state.OverageWeight
	}
	return route
}

func normalizeProfileArns(profileArns []string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0, len(profileArns))
	for _, profileArn := range profileArns {
		profileArn = strings.TrimSpace(profileArn)
		if profileArn == "" || seen[profileArn] {
			continue
		}
		seen[profileArn] = true
		result = append(result, profileArn)
	}
	return result
}

// PromptFilterRule defines a single custom prompt sanitization rule.
// Type can be: "regex" (regexp find/replace within prompt) or
// "lines-containing" (remove lines containing the match substring).
type PromptFilterRule struct {
	ID      string `json:"id"`                // Unique rule identifier
	Name    string `json:"name"`              // Human-readable rule name
	Type    string `json:"type"`              // "regex" or "lines-containing"
	Match   string `json:"match"`             // Pattern to match (regex pattern or substring)
	Replace string `json:"replace,omitempty"` // Replacement string (only for regex; empty = delete match)
	Enabled bool   `json:"enabled"`           // Whether this rule is active
}

// Config represents the global application configuration.
type Config struct {
	// Server settings
	Password      string    `json:"password"`         // Admin panel password
	Port          int       `json:"port"`             // HTTP server port (default: 8080)
	Host          string    `json:"host"`             // HTTP server bind address (default: 0.0.0.0)
	ApiKey        string    `json:"apiKey,omitempty"` // API key for client authentication
	RequireApiKey bool      `json:"requireApiKey"`    // Whether to enforce API key validation
	KiroVersion   string    `json:"kiroVersion,omitempty"`
	SystemVersion string    `json:"systemVersion,omitempty"`
	NodeVersion   string    `json:"nodeVersion,omitempty"`
	Accounts      []Account `json:"accounts"` // Registered Kiro accounts

	// Thinking mode configuration for extended reasoning output
	ThinkingSuffix       string `json:"thinkingSuffix,omitempty"`       // Model suffix to trigger thinking mode (default: "-thinking")
	OpenAIThinkingFormat string `json:"openaiThinkingFormat,omitempty"` // OpenAI output format: "reasoning_content", "thinking", or "think"
	ClaudeThinkingFormat string `json:"claudeThinkingFormat,omitempty"` // Claude output format: "reasoning_content", "thinking", or "think"

	// Endpoint configuration: "auto", "kiro", "codewhisperer", or "amazonq"
	PreferredEndpoint string `json:"preferredEndpoint,omitempty"`

	// EndpointFallback controls whether to try other endpoints when the preferred one fails.
	// Defaults to true. Set to false to only use the preferred endpoint.
	EndpointFallback *bool `json:"endpointFallback,omitempty"`

	// AllowOverUsage allows accounts to continue serving requests even when their
	// usage quota has been exhausted. When enabled, the pool will not skip accounts
	// solely because usageCurrent >= usageLimit.
	AllowOverUsage bool `json:"allowOverUsage,omitempty"`

	// Proxy configuration: optional outbound proxy for Kiro API requests
	// Format: "socks5://host:port", "socks5://user:pass@host:port",
	//         "http://host:port",  "http://user:pass@host:port"
	// Leave empty to connect directly.
	ProxyURL string `json:"proxyURL,omitempty"`

	// SanitizeClaudeCodePrompt is kept for backward-compatible JSON loading only.
	// Migrated to FilterClaudeCode on first load. Do not use directly.
	SanitizeClaudeCodePrompt bool `json:"sanitizeClaudeCodePrompt,omitempty"`

	// FilterClaudeCode detects the Claude Code CLI built-in system prompt and replaces it
	// with a compact backend-only prompt, reducing token usage significantly.
	FilterClaudeCode bool `json:"filterClaudeCode,omitempty"`

	// FilterEnvNoise strips environment metadata lines from system prompts:
	// git status, recent commits, environment sections, fast_mode_info tags, etc.
	FilterEnvNoise bool `json:"filterEnvNoise,omitempty"`

	// FilterStripBoundaries removes --- SYSTEM PROMPT --- / --- END SYSTEM PROMPT --- markers.
	FilterStripBoundaries bool `json:"filterStripBoundaries,omitempty"`

	// PromptFilterRules is a list of user-defined prompt sanitization rules (regex or line-filter).
	PromptFilterRules []PromptFilterRule `json:"promptFilterRules,omitempty"`

	// LogLevel controls verbosity of application logs.
	// Accepted values: "debug", "info", "warn", "error". Defaults to "info".
	// Can be overridden by the LOG_LEVEL environment variable.
	LogLevel string `json:"logLevel,omitempty"`

	// Telegram notification bot settings
	TelegramEnabled      bool   `json:"telegramEnabled,omitempty"`      // Master enable switch
	TelegramBotToken     string `json:"telegramBotToken,omitempty"`     // Bot API token from @BotFather
	TelegramUserID       string `json:"telegramUserID,omitempty"`       // Connected user's Telegram ID (auto-set on /start)
	TelegramConnectToken string `json:"telegramConnectToken,omitempty"` // One-time token for link-based connect
	TelegramInterval     int    `json:"telegramInterval,omitempty"`     // Health report interval in minutes (default: 60)
	TelegramNotifyLevel  string `json:"telegramNotifyLevel,omitempty"`  // critical / normal / frequent
	TelegramNotifyLang   string `json:"telegramNotifyLang,omitempty"`   // vi / en / zh
	TelegramChatID       string `json:"telegramChatID,omitempty"`       // DEPRECATED: kept for migration, use TelegramUserID

	// Global statistics (persisted across restarts)
	TotalRequests   int     `json:"totalRequests,omitempty"`   // Total API requests received
	SuccessRequests int     `json:"successRequests,omitempty"` // Successful requests count
	FailedRequests  int     `json:"failedRequests,omitempty"`  // Failed requests count
	TotalTokens     int     `json:"totalTokens,omitempty"`     // Total tokens processed
	TotalCredits    float64 `json:"totalCredits,omitempty"`    // Total credits consumed
}

// AccountInfo contains account metadata retrieved from Kiro API.
// Used for updating subscription and usage information.
type AccountInfo struct {
	Email             string
	UserId            string
	SubscriptionType  string
	SubscriptionTitle string
	DaysRemaining     int
	UsageCurrent      float64
	UsageLimit        float64
	UsagePercent      float64
	NextResetDate     string
	LastRefresh       int64
	TrialUsageCurrent float64
	TrialUsageLimit   float64
	TrialUsagePercent float64
	TrialStatus       string
	TrialExpiresAt    int64
}

// Version current version
const Version = "1.0.8"

var (
	cfg     *Config
	cfgLock sync.RWMutex
	cfgPath string
)

// Init initializes the configuration system with the specified file path.
// If the file doesn't exist, a default configuration is created.
func Init(path string) error {
	cfgPath = path
	return Load()
}

func Load() error {
	cfgLock.Lock()
	defer cfgLock.Unlock()

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Create default configuration.
			// Binds to 0.0.0.0 by default for Docker/container compatibility.
			cfg = &Config{
				Password:      "changeme",
				Port:          8080,
				Host:          "0.0.0.0",
				RequireApiKey: false,
				Accounts:      []Account{},
			}
			return Save()
		}
		return err
	}

	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return err
	}
	cfg = &c
	return nil
}

// Save persists the current configuration to the JSON file.
// Uses indented formatting for human readability.
func Save() error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(cfgPath, data, 0600)
}

// SetPassword updates the admin password.
// Primarily used for environment variable override in containerized deployments.
func SetPassword(password string) {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	cfg.Password = password
}

func Get() *Config {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	return cfg
}

func GetPassword() string {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	return cfg.Password
}

func GetPort() int {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	if cfg.Port == 0 {
		return 8080
	}
	return cfg.Port
}

func GetHost() string {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	if cfg.Host == "" {
		return "127.0.0.1"
	}
	return cfg.Host
}

func GetAccounts() []Account {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	accounts := make([]Account, len(cfg.Accounts))
	copy(accounts, cfg.Accounts)
	return accounts
}

func GetEnabledAccounts() []Account {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	var accounts []Account
	for _, a := range cfg.Accounts {
		if a.Enabled {
			accounts = append(accounts, a)
		}
	}
	return accounts
}

func AddAccount(account Account) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	cfg.Accounts = append(cfg.Accounts, account)
	return Save()
}

func UpdateAccount(id string, account Account) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	for i, a := range cfg.Accounts {
		if a.ID == id {
			cfg.Accounts[i] = account
			return Save()
		}
	}
	return nil
}

// DisableAccountOverage turns off AllowOverage for a specific account.
func DisableAccountOverage(id string) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	for i, a := range cfg.Accounts {
		if a.ID == id {
			cfg.Accounts[i].AllowOverage = false
			return Save()
		}
	}
	return nil
}

func UpdateAccountProfileArn(id, profileArn string) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	for i, a := range cfg.Accounts {
		if a.ID == id {
			cfg.Accounts[i].ProfileArn = strings.TrimSpace(profileArn)
			return Save()
		}
	}
	return nil
}

func UpdateAccountProfileArns(id string, profileArns []string) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	normalized := normalizeProfileArns(profileArns)
	for i, a := range cfg.Accounts {
		if a.ID == id {
			cfg.Accounts[i].ProfileArns = normalized
			cfg.Accounts[i].KnownProfileArns = normalizeProfileArns(append(cfg.Accounts[i].KnownProfileArns, normalized...))
			if len(normalized) > 0 {
				selected := strings.TrimSpace(cfg.Accounts[i].ProfileArn)
				found := false
				for _, profileArn := range normalized {
					if selected == profileArn {
						found = true
						break
					}
				}
				if !found {
					cfg.Accounts[i].ProfileArn = normalized[0]
				}
			}
			return Save()
		}
	}
	return nil
}

func UpdateAccountKnownProfileArns(id string, profileArns []string) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	normalized := normalizeProfileArns(profileArns)
	for i, a := range cfg.Accounts {
		if a.ID == id {
			cfg.Accounts[i].KnownProfileArns = normalized
			return Save()
		}
	}
	return nil
}

func DeleteAccount(id string) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	for i, a := range cfg.Accounts {
		if a.ID == id {
			cfg.Accounts = append(cfg.Accounts[:i], cfg.Accounts[i+1:]...)
			return Save()
		}
	}
	return nil
}

func UpdateAccountToken(id, accessToken, refreshToken string, expiresAt int64) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	for i, a := range cfg.Accounts {
		if a.ID == id {
			cfg.Accounts[i].AccessToken = accessToken
			if refreshToken != "" {
				cfg.Accounts[i].RefreshToken = refreshToken
			}
			cfg.Accounts[i].ExpiresAt = expiresAt
			return Save()
		}
	}
	return nil
}

func GetApiKey() string {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	return cfg.ApiKey
}

func IsApiKeyRequired() bool {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	return cfg.RequireApiKey
}

func UpdateSettings(apiKey string, requireApiKey bool, password string) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	cfg.ApiKey = apiKey
	cfg.RequireApiKey = requireApiKey
	if password != "" {
		cfg.Password = password
	}
	return Save()
}

func UpdateStats(totalReq, successReq, failedReq, totalTokens int, totalCredits float64) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	cfg.TotalRequests = totalReq
	cfg.SuccessRequests = successReq
	cfg.FailedRequests = failedReq
	cfg.TotalTokens = totalTokens
	cfg.TotalCredits = totalCredits
	return Save()
}

func GetStats() (int, int, int, int, float64) {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	return cfg.TotalRequests, cfg.SuccessRequests, cfg.FailedRequests, cfg.TotalTokens, cfg.TotalCredits
}

func UpdateAccountStats(id string, requestCount, errorCount, totalTokens int, totalCredits float64, lastUsed int64) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	for i, a := range cfg.Accounts {
		if a.ID == id {
			cfg.Accounts[i].RequestCount = requestCount
			cfg.Accounts[i].ErrorCount = errorCount
			cfg.Accounts[i].TotalTokens = totalTokens
			cfg.Accounts[i].TotalCredits = totalCredits
			cfg.Accounts[i].LastUsed = lastUsed
			return Save()
		}
	}
	return nil
}

func UpdateAccountProfileStats(id, profileArn string, requestCount, errorCount, totalTokens int, totalCredits float64, lastUsed int64) error {
	profileArn = strings.TrimSpace(profileArn)
	if profileArn == "" {
		return UpdateAccountStats(id, requestCount, errorCount, totalTokens, totalCredits, lastUsed)
	}

	cfgLock.Lock()
	defer cfgLock.Unlock()
	for i, a := range cfg.Accounts {
		if a.ID == id {
			if cfg.Accounts[i].ProfileStates == nil {
				cfg.Accounts[i].ProfileStates = make(map[string]ProfileState)
			}
			state := a.GetProfileState(profileArn)
			state.RequestCount = requestCount
			state.ErrorCount = errorCount
			state.TotalTokens = totalTokens
			state.TotalCredits = totalCredits
			state.LastUsed = lastUsed
			cfg.Accounts[i].ProfileStates[profileArn] = state
			return Save()
		}
	}
	return nil
}

// UpdateAccountInfo updates an account's subscription and usage information.
// Called after refreshing account data from Kiro API.
func UpdateAccountInfo(id string, info AccountInfo) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	for i, a := range cfg.Accounts {
		if a.ID == id {
			if info.Email != "" {
				cfg.Accounts[i].Email = info.Email
			}
			if info.UserId != "" {
				cfg.Accounts[i].UserId = info.UserId
			}
			cfg.Accounts[i].SubscriptionType = info.SubscriptionType
			cfg.Accounts[i].SubscriptionTitle = info.SubscriptionTitle
			cfg.Accounts[i].DaysRemaining = info.DaysRemaining
			cfg.Accounts[i].UsageCurrent = info.UsageCurrent
			cfg.Accounts[i].UsageLimit = info.UsageLimit
			cfg.Accounts[i].UsagePercent = info.UsagePercent
			cfg.Accounts[i].NextResetDate = info.NextResetDate
			cfg.Accounts[i].LastRefresh = info.LastRefresh
			cfg.Accounts[i].TrialUsageCurrent = info.TrialUsageCurrent
			cfg.Accounts[i].TrialUsageLimit = info.TrialUsageLimit
			cfg.Accounts[i].TrialUsagePercent = info.TrialUsagePercent
			cfg.Accounts[i].TrialStatus = info.TrialStatus
			cfg.Accounts[i].TrialExpiresAt = info.TrialExpiresAt
			return Save()
		}
	}
	return nil
}

func UpdateAccountProfileInfo(id, profileArn string, info AccountInfo) error {
	profileArn = strings.TrimSpace(profileArn)
	if profileArn == "" {
		return UpdateAccountInfo(id, info)
	}

	cfgLock.Lock()
	defer cfgLock.Unlock()
	for i, a := range cfg.Accounts {
		if a.ID == id {
			if info.Email != "" {
				cfg.Accounts[i].Email = info.Email
			}
			if info.UserId != "" {
				cfg.Accounts[i].UserId = info.UserId
			}
			if cfg.Accounts[i].ProfileStates == nil {
				cfg.Accounts[i].ProfileStates = make(map[string]ProfileState)
			}
			state := a.GetProfileState(profileArn)
			applyAccountInfoToProfileState(&state, info)
			cfg.Accounts[i].ProfileStates[profileArn] = state
			return Save()
		}
	}
	return nil
}

func applyAccountInfoToProfileState(state *ProfileState, info AccountInfo) {
	hasSubscription := info.SubscriptionType != "" || info.SubscriptionTitle != "" ||
		info.UsageCurrent != 0 || info.UsageLimit != 0 || info.UsagePercent != 0 ||
		info.NextResetDate != "" || info.DaysRemaining != 0
	if hasSubscription {
		state.SubscriptionType = info.SubscriptionType
		state.SubscriptionTitle = info.SubscriptionTitle
		state.DaysRemaining = info.DaysRemaining
		state.UsageCurrent = info.UsageCurrent
		state.UsageLimit = info.UsageLimit
		state.UsagePercent = info.UsagePercent
		state.NextResetDate = info.NextResetDate
	}
	if info.LastRefresh != 0 {
		state.LastRefresh = info.LastRefresh
	}

	hasTrial := info.TrialStatus != "" || info.TrialUsageCurrent != 0 ||
		info.TrialUsageLimit != 0 || info.TrialUsagePercent != 0 || info.TrialExpiresAt != 0
	if hasTrial {
		state.TrialUsageCurrent = info.TrialUsageCurrent
		state.TrialUsageLimit = info.TrialUsageLimit
		state.TrialUsagePercent = info.TrialUsagePercent
		state.TrialStatus = info.TrialStatus
		state.TrialExpiresAt = info.TrialExpiresAt
	}
}

// GetFilterClaudeCode returns whether Claude Code system prompt detection is enabled.
// Also checks the legacy SanitizeClaudeCodePrompt flag for backward compatibility.
func GetFilterClaudeCode() bool {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	if cfg == nil {
		return false
	}
	return cfg.FilterClaudeCode || cfg.SanitizeClaudeCodePrompt
}

// GetFilterEnvNoise returns whether environment noise line stripping is enabled.
func GetFilterEnvNoise() bool {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	if cfg == nil {
		return false
	}
	return cfg.FilterEnvNoise
}

// GetFilterStripBoundaries returns whether boundary marker stripping is enabled.
func GetFilterStripBoundaries() bool {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	if cfg == nil {
		return false
	}
	return cfg.FilterStripBoundaries
}

// PromptFilterConfig holds all prompt filter settings for API responses.
type PromptFilterConfig struct {
	FilterClaudeCode      bool               `json:"filterClaudeCode"`
	FilterEnvNoise        bool               `json:"filterEnvNoise"`
	FilterStripBoundaries bool               `json:"filterStripBoundaries"`
	Rules                 []PromptFilterRule `json:"rules"`
}

// GetPromptFilterConfig returns all prompt filter settings.
func GetPromptFilterConfig() PromptFilterConfig {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	if cfg == nil {
		return PromptFilterConfig{Rules: []PromptFilterRule{}}
	}
	rules := make([]PromptFilterRule, len(cfg.PromptFilterRules))
	copy(rules, cfg.PromptFilterRules)
	return PromptFilterConfig{
		FilterClaudeCode:      cfg.FilterClaudeCode || cfg.SanitizeClaudeCodePrompt,
		FilterEnvNoise:        cfg.FilterEnvNoise,
		FilterStripBoundaries: cfg.FilterStripBoundaries,
		Rules:                 rules,
	}
}

// UpdatePromptFilterConfig saves all prompt filter settings atomically.
func UpdatePromptFilterConfig(filterClaudeCode, filterEnvNoise, filterStripBoundaries bool, rules []PromptFilterRule) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	cfg.FilterClaudeCode = filterClaudeCode
	cfg.FilterEnvNoise = filterEnvNoise
	cfg.FilterStripBoundaries = filterStripBoundaries
	// Clear legacy flag to avoid double-applying after first save
	cfg.SanitizeClaudeCodePrompt = false
	if rules != nil {
		cfg.PromptFilterRules = rules
	}
	return Save()
}

// GetPromptFilterRules returns the current prompt filter rules.
func GetPromptFilterRules() []PromptFilterRule {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	if cfg == nil {
		return nil
	}
	rules := make([]PromptFilterRule, len(cfg.PromptFilterRules))
	copy(rules, cfg.PromptFilterRules)
	return rules
}

// ThinkingConfig holds settings for AI thinking/reasoning mode.
// When enabled, models output their reasoning process alongside the response.
type ThinkingConfig struct {
	Suffix       string `json:"suffix"`       // Model name suffix that triggers thinking mode
	OpenAIFormat string `json:"openaiFormat"` // Output format for OpenAI-compatible responses
	ClaudeFormat string `json:"claudeFormat"` // Output format for Claude-compatible responses
}

// GetThinkingConfig Lấy cấu hình thinking
func GetThinkingConfig() ThinkingConfig {
	cfgLock.RLock()
	defer cfgLock.RUnlock()

	suffix := cfg.ThinkingSuffix
	if suffix == "" {
		suffix = "-thinking"
	}
	openaiFormat := cfg.OpenAIThinkingFormat
	if openaiFormat == "" {
		openaiFormat = "reasoning_content"
	}
	claudeFormat := cfg.ClaudeThinkingFormat
	if claudeFormat == "" {
		claudeFormat = "thinking"
	}

	return ThinkingConfig{
		Suffix:       suffix,
		OpenAIFormat: openaiFormat,
		ClaudeFormat: claudeFormat,
	}
}

// UpdateThinkingConfig Cập nhật cấu hình thinking
func UpdateThinkingConfig(suffix, openaiFormat, claudeFormat string) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	cfg.ThinkingSuffix = suffix
	cfg.OpenAIThinkingFormat = openaiFormat
	cfg.ClaudeThinkingFormat = claudeFormat
	return Save()
}

// GetPreferredEndpoint Lấy cấu hình endpoint ưu tiên
func GetPreferredEndpoint() string {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	if cfg.PreferredEndpoint == "" {
		return "auto"
	}
	return cfg.PreferredEndpoint
}

// UpdatePreferredEndpoint Cập nhật cấu hình endpoint ưu tiên
func UpdatePreferredEndpoint(endpoint string) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	cfg.PreferredEndpoint = endpoint
	return Save()
}

// GetEndpointFallback returns whether endpoint fallback is enabled. Defaults to true.
func GetEndpointFallback() bool {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	if cfg.EndpointFallback == nil {
		return true
	}
	return *cfg.EndpointFallback
}

// UpdateEndpointFallback sets the endpoint fallback switch and persists the change.
func UpdateEndpointFallback(enabled bool) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	cfg.EndpointFallback = &enabled
	return Save()
}

// GetProxyURL Lấy địa chỉ proxy outbound
func GetProxyURL() string {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	return cfg.ProxyURL
}

// UpdateProxySettings Cập nhật cấu hình proxy outbound
func UpdateProxySettings(proxyURL string) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	cfg.ProxyURL = proxyURL
	return Save()
}

// GetAllowOverUsage returns whether over-usage is allowed when account quota is exhausted.
func GetAllowOverUsage() bool {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	if cfg == nil {
		return false
	}
	return cfg.AllowOverUsage
}

// UpdateAllowOverUsage sets the over-usage setting and persists the change.
func UpdateAllowOverUsage(allow bool) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	cfg.AllowOverUsage = allow
	return Save()
}

// GetLogLevel returns the configured log level (debug/info/warn/error). Defaults to "info".
func GetLogLevel() string {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	if cfg == nil || cfg.LogLevel == "" {
		return "info"
	}
	return cfg.LogLevel
}

// UpdateLogLevel updates the log level setting and persists the change.
func UpdateLogLevel(level string) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	cfg.LogLevel = level
	return Save()
}

type KiroClientConfig struct {
	KiroVersion   string
	SystemVersion string
	NodeVersion   string
}

func GetKiroClientConfig() KiroClientConfig {
	cfgLock.RLock()
	defer cfgLock.RUnlock()

	kiroVersion := "0.11.107"
	if cfg != nil && cfg.KiroVersion != "" {
		kiroVersion = cfg.KiroVersion
	}

	systemVersion := ""
	if cfg != nil {
		systemVersion = cfg.SystemVersion
	}
	if systemVersion == "" {
		systemVersion = defaultSystemVersion()
	}

	nodeVersion := "22.22.0"
	if cfg != nil && cfg.NodeVersion != "" {
		nodeVersion = cfg.NodeVersion
	}

	return KiroClientConfig{
		KiroVersion:   kiroVersion,
		SystemVersion: systemVersion,
		NodeVersion:   nodeVersion,
	}
}

func defaultSystemVersion() string {
	switch runtime.GOOS {
	case "windows":
		return "win32#10.0.22631"
	case "darwin":
		return "darwin#24.6.0"
	default:
		return "linux#6.6.87"
	}
}

// TelegramConfig holds Telegram notification settings.
type TelegramConfig struct {
	Enabled      bool   `json:"enabled"`
	BotToken     string `json:"botToken"`
	UserID       string `json:"userID"`
	ConnectToken string `json:"connectToken"`
	Interval     int    `json:"interval"`     // minutes
	NotifyLevel  string `json:"notifyLevel"`  // critical / normal / frequent
	NotifyLang   string `json:"notifyLang"`   // vi / en / zh
}

// GetTelegramConfig returns the current Telegram notification settings.
func GetTelegramConfig() TelegramConfig {
	cfgLock.RLock()
	defer cfgLock.RUnlock()
	interval := cfg.TelegramInterval
	if interval <= 0 {
		interval = 60
	}
	notifyLevel := cfg.TelegramNotifyLevel
	if notifyLevel == "" {
		notifyLevel = "normal"
	}
	notifyLang := cfg.TelegramNotifyLang
	if notifyLang == "" {
		notifyLang = "vi"
	}
	// Migrate old ChatID to UserID
	userID := cfg.TelegramUserID
	if userID == "" && cfg.TelegramChatID != "" {
		userID = cfg.TelegramChatID
	}
	return TelegramConfig{
		Enabled:      cfg.TelegramEnabled,
		BotToken:     cfg.TelegramBotToken,
		UserID:       userID,
		ConnectToken: cfg.TelegramConnectToken,
		Interval:     interval,
		NotifyLevel:  notifyLevel,
		NotifyLang:   notifyLang,
	}
}

// UpdateTelegramConfig saves Telegram notification settings.
func UpdateTelegramConfig(enabled bool, botToken, userID, connectToken, notifyLevel, notifyLang string, interval int) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	cfg.TelegramEnabled = enabled
	cfg.TelegramBotToken = botToken
	cfg.TelegramUserID = userID
	cfg.TelegramConnectToken = connectToken
	if interval <= 0 {
		interval = 60
	}
	cfg.TelegramInterval = interval
	if notifyLevel == "" {
		notifyLevel = "normal"
	}
	cfg.TelegramNotifyLevel = notifyLevel
	if notifyLang == "" {
		notifyLang = "vi"
	}
	cfg.TelegramNotifyLang = notifyLang
	// Clear deprecated field
	cfg.TelegramChatID = ""
	return Save()
}

// SetTelegramUserID sets the connected user ID (called when user /starts the bot).
func SetTelegramUserID(userID string) error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	cfg.TelegramUserID = userID
	cfg.TelegramConnectToken = "" // Invalidate connect token after use
	cfg.TelegramChatID = ""
	return Save()
}

// ClearTelegramUser unbinds the connected Telegram user.
func ClearTelegramUser() error {
	cfgLock.Lock()
	defer cfgLock.Unlock()
	cfg.TelegramUserID = ""
	cfg.TelegramChatID = ""
	return Save()
}

// UpdateProfileRoutingSettings sets per-profile weight/overage overrides.
// When profileArn is empty, it falls back to updating the account-level fields.
func UpdateProfileRoutingSettings(id, profileArn string, weight int, allowOverage bool, overageWeight int) error {
	profileArn = strings.TrimSpace(profileArn)
	cfgLock.Lock()
	defer cfgLock.Unlock()
	for i, a := range cfg.Accounts {
		if a.ID != id {
			continue
		}
		if profileArn == "" {
			// Account-level update (single-profile or unconfigured account).
			cfg.Accounts[i].Weight = weight
			cfg.Accounts[i].AllowOverage = allowOverage
			cfg.Accounts[i].OverageWeight = overageWeight
			return Save()
		}
		if cfg.Accounts[i].ProfileStates == nil {
			cfg.Accounts[i].ProfileStates = make(map[string]ProfileState)
		}
		state := a.GetProfileState(profileArn)
		w := weight
		ow := overageWeight
		ao := allowOverage
		state.Weight = &w
		state.OverageWeight = &ow
		state.AllowOverage = &ao
		cfg.Accounts[i].ProfileStates[profileArn] = state
		return Save()
	}
	return nil
}

