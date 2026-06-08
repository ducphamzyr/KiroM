package proxy

import (
	"regexp"
	"strconv"
	"strings"
)

// MappedError là lỗi đã được chuẩn hóa từ lỗi thô của Kiro/AWS sang
// định dạng tương thích client (Anthropic / OpenAI).
type MappedError struct {
	HTTPStatus    int    // HTTP status trả về cho client
	ClaudeType    string // error.type cho Anthropic API
	OpenAIType    string // error.type cho OpenAI API
	Message       string // Thông báo thân thiện, đã làm sạch
	UpstreamCode  int    // Status code gốc từ Kiro (0 nếu không xác định)
	Retryable     bool   // Client có nên thử lại không
}

// errorRule định nghĩa một quy tắc nhận diện lỗi theo pattern.
type errorRule struct {
	// match: danh sách substring (lowercase). Rule khớp nếu BẤT KỲ phần tử nào
	// xuất hiện trong message lỗi gốc (OR).
	match []string
	// mapped: lỗi chuẩn hóa tương ứng
	mapped MappedError
}

// statusCodeRe trích status code dạng "HTTP 403" hoặc "402" từ message lỗi.
var statusCodeRe = regexp.MustCompile(`\bHTTP (\d{3})\b`)

// errorRules: bảng map lỗi. Quy tắc đầu tiên khớp sẽ được dùng.
// Thứ tự quan trọng — đặt rule cụ thể trước rule chung chung.
var errorRules = []errorRule{
	{
		match: []string{"no available accounts"},
		mapped: MappedError{
			HTTPStatus: 503, ClaudeType: "overloaded_error", OpenAIType: "server_error",
			Message: "No accounts are currently available to handle the request. Please try again shortly.", Retryable: true,
		},
	},
	{
		match: []string{"token refresh failed"},
		mapped: MappedError{
			HTTPStatus: 503, ClaudeType: "api_error", OpenAIType: "server_error",
			Message: "Upstream authentication is being refreshed. Please retry in a moment.", Retryable: true,
		},
	},
	{
		match: []string{"overage"},
		mapped: MappedError{
			HTTPStatus: 402, ClaudeType: "billing_error", OpenAIType: "insufficient_quota",
			Message: "The account usage quota has been exhausted (overage limit reached).", Retryable: false,
		},
	},
	{
		match: []string{"quota exhausted"},
		mapped: MappedError{
			HTTPStatus: 429, ClaudeType: "rate_limit_error", OpenAIType: "rate_limit_exceeded",
			Message: "Rate limit reached for the upstream provider. Please slow down and retry.", Retryable: true,
		},
	},
	{
		match: []string{"user is not authorized"},
		mapped: MappedError{
			HTTPStatus: 403, ClaudeType: "permission_error", OpenAIType: "permission_error",
			Message: "The upstream account is not authorized for this operation.", Retryable: false,
		},
	},
	{
		match: []string{"improperly formed request"},
		mapped: MappedError{
			HTTPStatus: 400, ClaudeType: "invalid_request_error", OpenAIType: "invalid_request_error",
			Message: "The request was rejected by the upstream provider as malformed.", Retryable: false,
		},
	},
	{
		match: []string{"temporarily_suspended", "account suspended"},
		mapped: MappedError{
			HTTPStatus: 403, ClaudeType: "permission_error", OpenAIType: "permission_error",
			Message: "The upstream account is temporarily suspended.", Retryable: false,
		},
	},
	{
		match: []string{"all endpoints failed"},
		mapped: MappedError{
			HTTPStatus: 502, ClaudeType: "api_error", OpenAIType: "server_error",
			Message: "All upstream endpoints failed to respond. Please try again later.", Retryable: true,
		},
	},
	{
		match: []string{"streaming not supported"},
		mapped: MappedError{
			HTTPStatus: 500, ClaudeType: "api_error", OpenAIType: "server_error",
			Message: "Streaming is not supported by this connection.", Retryable: false,
		},
	},
	{
		match: []string{"context deadline exceeded", "timeout", "i/o timeout"},
		mapped: MappedError{
			HTTPStatus: 504, ClaudeType: "api_error", OpenAIType: "server_error",
			Message: "The upstream provider timed out. Please try again.", Retryable: true,
		},
	},
	{
		match: []string{"connection refused", "no such host", "dial tcp", "eof"},
		mapped: MappedError{
			HTTPStatus: 502, ClaudeType: "api_error", OpenAIType: "server_error",
			Message: "Could not reach the upstream provider. Please try again later.", Retryable: true,
		},
	},
}

// statusCodeMap map status code gốc của upstream sang lỗi chuẩn hóa
// khi không có rule nào theo nội dung khớp.
var statusCodeMap = map[int]MappedError{
	400: {HTTPStatus: 400, ClaudeType: "invalid_request_error", OpenAIType: "invalid_request_error", Message: "The upstream provider rejected the request.", Retryable: false},
	401: {HTTPStatus: 401, ClaudeType: "authentication_error", OpenAIType: "authentication_error", Message: "Upstream authentication failed.", Retryable: false},
	403: {HTTPStatus: 403, ClaudeType: "permission_error", OpenAIType: "permission_error", Message: "Access to the upstream provider was denied.", Retryable: false},
	404: {HTTPStatus: 404, ClaudeType: "not_found_error", OpenAIType: "invalid_request_error", Message: "The requested upstream resource was not found.", Retryable: false},
	429: {HTTPStatus: 429, ClaudeType: "rate_limit_error", OpenAIType: "rate_limit_exceeded", Message: "Rate limit reached for the upstream provider.", Retryable: true},
	500: {HTTPStatus: 502, ClaudeType: "api_error", OpenAIType: "server_error", Message: "The upstream provider encountered an internal error.", Retryable: true},
	502: {HTTPStatus: 502, ClaudeType: "api_error", OpenAIType: "server_error", Message: "The upstream provider is temporarily unavailable.", Retryable: true},
	503: {HTTPStatus: 503, ClaudeType: "overloaded_error", OpenAIType: "server_error", Message: "The upstream provider is overloaded. Please try again shortly.", Retryable: true},
	504: {HTTPStatus: 504, ClaudeType: "api_error", OpenAIType: "server_error", Message: "The upstream provider timed out.", Retryable: true},
}

// defaultMappedError dùng khi không nhận diện được lỗi.
var defaultMappedError = MappedError{
	HTTPStatus: 500, ClaudeType: "api_error", OpenAIType: "server_error",
	Message: "An unexpected error occurred while processing the request.", Retryable: false,
}

// MapKiroError chuyển lỗi thô từ Kiro/AWS sang lỗi chuẩn hóa cho client.
// Nó KHÔNG bao giờ rò rỉ chi tiết nội bộ (tên endpoint, JSON thô của AWS).
func MapKiroError(err error) MappedError {
	if err == nil {
		return defaultMappedError
	}
	raw := err.Error()
	lower := strings.ToLower(raw)

	// 1. Ưu tiên rule theo nội dung (cụ thể hơn status code).
	// Mỗi rule khớp nếu BẤT KỲ pattern nào trong match xuất hiện (OR).
	for _, rule := range errorRules {
		for _, m := range rule.match {
			if strings.Contains(lower, m) {
				result := rule.mapped
				result.UpstreamCode = extractStatusCode(raw)
				return result
			}
		}
	}

	// 2. Map theo status code upstream nếu trích được.
	if code := extractStatusCode(raw); code > 0 {
		if mapped, ok := statusCodeMap[code]; ok {
			mapped.UpstreamCode = code
			return mapped
		}
		// Status code lạ nhưng có: phân loại theo nhóm.
		switch {
		case code >= 400 && code < 500:
			return MappedError{HTTPStatus: code, ClaudeType: "invalid_request_error", OpenAIType: "invalid_request_error", Message: "The upstream provider rejected the request.", UpstreamCode: code}
		case code >= 500:
			return MappedError{HTTPStatus: 502, ClaudeType: "api_error", OpenAIType: "server_error", Message: "The upstream provider encountered an error.", UpstreamCode: code, Retryable: true}
		}
	}

	// 3. Không nhận diện được: trả lỗi mặc định an toàn.
	return defaultMappedError
}

// extractStatusCode trích HTTP status code đầu tiên từ message lỗi.
func extractStatusCode(raw string) int {
	if m := statusCodeRe.FindStringSubmatch(raw); len(m) == 2 {
		if code, err := strconv.Atoi(m[1]); err == nil {
			return code
		}
	}
	// Thử các code phổ biến đứng riêng (vd "quota exhausted (429)").
	for _, code := range []string{"402", "429", "401", "403"} {
		if strings.Contains(raw, code) {
			if c, err := strconv.Atoi(code); err == nil {
				return c
			}
		}
	}
	return 0
}
