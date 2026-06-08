package proxy

import (
	"fmt"
	"testing"
)

func TestMapKiroError(t *testing.T) {
	cases := []struct {
		name           string
		err            error
		wantStatus     int
		wantClaudeType string
		wantOpenAIType string
		wantRetryable  bool
	}{
		{
			name:           "403 forbidden from CodeWhisperer",
			err:            fmt.Errorf("HTTP 403 from CodeWhisperer: {\"message\":\"denied\"}"),
			wantStatus:     403,
			wantClaudeType: "permission_error",
			wantOpenAIType: "permission_error",
		},
		{
			name:           "429 quota exhausted",
			err:            fmt.Errorf("quota exhausted on Kiro IDE"),
			wantStatus:     429,
			wantClaudeType: "rate_limit_error",
			wantOpenAIType: "rate_limit_exceeded",
			wantRetryable:  true,
		},
		{
			name:           "402 overage",
			err:            fmt.Errorf("HTTP 402 from CodeWhisperer: OVERAGE limit"),
			wantStatus:     402,
			wantClaudeType: "billing_error",
			wantOpenAIType: "insufficient_quota",
		},
		{
			name:           "no available accounts",
			err:            fmt.Errorf("no available accounts"),
			wantStatus:     503,
			wantClaudeType: "overloaded_error",
			wantOpenAIType: "server_error",
			wantRetryable:  true,
		},
		{
			name:           "token refresh failed",
			err:            fmt.Errorf("token refresh failed: oidc error"),
			wantStatus:     503,
			wantClaudeType: "api_error",
			wantOpenAIType: "server_error",
			wantRetryable:  true,
		},
		{
			name:           "all endpoints failed",
			err:            fmt.Errorf("all endpoints failed"),
			wantStatus:     502,
			wantClaudeType: "api_error",
			wantOpenAIType: "server_error",
			wantRetryable:  true,
		},
		{
			name:           "401 unauthorized via status map",
			err:            fmt.Errorf("HTTP 401 from Kiro IDE: token invalid"),
			wantStatus:     401,
			wantClaudeType: "authentication_error",
			wantOpenAIType: "authentication_error",
		},
		{
			name:           "500 upstream becomes 502",
			err:            fmt.Errorf("HTTP 500 from AmazonQ: internal"),
			wantStatus:     502,
			wantClaudeType: "api_error",
			wantOpenAIType: "server_error",
			wantRetryable:  true,
		},
		{
			name:           "timeout",
			err:            fmt.Errorf("context deadline exceeded"),
			wantStatus:     504,
			wantClaudeType: "api_error",
			wantOpenAIType: "server_error",
			wantRetryable:  true,
		},
		{
			name:           "unknown error defaults safely",
			err:            fmt.Errorf("some weird internal panic xyz"),
			wantStatus:     500,
			wantClaudeType: "api_error",
			wantOpenAIType: "server_error",
		},
		{
			name:           "nil error",
			err:            nil,
			wantStatus:     500,
			wantClaudeType: "api_error",
			wantOpenAIType: "server_error",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MapKiroError(tc.err)
			if got.HTTPStatus != tc.wantStatus {
				t.Errorf("HTTPStatus = %d, want %d", got.HTTPStatus, tc.wantStatus)
			}
			if got.ClaudeType != tc.wantClaudeType {
				t.Errorf("ClaudeType = %q, want %q", got.ClaudeType, tc.wantClaudeType)
			}
			if got.OpenAIType != tc.wantOpenAIType {
				t.Errorf("OpenAIType = %q, want %q", got.OpenAIType, tc.wantOpenAIType)
			}
			if got.Retryable != tc.wantRetryable {
				t.Errorf("Retryable = %v, want %v", got.Retryable, tc.wantRetryable)
			}
		})
	}
}

func TestMapKiroErrorDoesNotLeakRawDetails(t *testing.T) {
	raw := fmt.Errorf("HTTP 403 from CodeWhisperer: {\"reason\":\"internal-secret-detail\",\"arn\":\"arn:aws:secret\"}")
	mapped := MapKiroError(raw)
	if mapped.Message == raw.Error() {
		t.Fatal("mapped message must not equal the raw upstream error")
	}
	// The mapped message must not contain internal AWS details.
	for _, leak := range []string{"CodeWhisperer", "arn:aws", "internal-secret-detail"} {
		if contains(mapped.Message, leak) {
			t.Errorf("mapped message leaked internal detail %q: %s", leak, mapped.Message)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
