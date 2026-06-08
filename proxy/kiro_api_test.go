package proxy

import (
	"io"
	"kiro-go/config"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveProfileArnReturnsCachedValueWithoutRequest(t *testing.T) {
	kiroRestHttpStore.Store(&http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("unexpected HTTP request for cached profile ARN")
			return nil, nil
		}),
	})
	t.Cleanup(func() { InitKiroHttpClient("") })

	account := &config.Account{ProfileArn: " arn:aws:codewhisperer:profile/test "}
	got, err := ResolveProfileArn(account)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "arn:aws:codewhisperer:profile/test" {
		t.Fatalf("expected trimmed cached ARN, got %q", got)
	}
}

func TestResolveProfileArnFetchesAndCachesProfile(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	if err := config.Init(configPath); err != nil {
		t.Fatalf("init config: %v", err)
	}
	account := config.Account{
		ID:           "acct-1",
		Email:        "user@example.com",
		AccessToken:  "access-token",
		Region:       "us-east-1",
		UsageCurrent: 7,
	}
	if err := config.AddAccount(account); err != nil {
		t.Fatalf("add account: %v", err)
	}

	kiroRestHttpStore.Store(&http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method != http.MethodPost {
				t.Fatalf("expected POST, got %s", req.Method)
			}
			if req.URL.Path != "/ListAvailableProfiles" {
				t.Fatalf("expected ListAvailableProfiles path, got %s", req.URL.Path)
			}
			if got := req.Header.Get("Content-Type"); got != "application/x-amz-json-1.0" {
				t.Fatalf("expected JSON content type, got %q", got)
			}
			if got := req.Header.Get("x-amz-target"); got != "AmazonCodeWhispererService.ListAvailableProfiles" {
				t.Fatalf("expected ListAvailableProfiles target, got %q", got)
			}
			body, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatalf("read request body: %v", err)
			}
			if string(body) != "{}" {
				t.Fatalf("expected empty JSON body, got %q", string(body))
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"profiles":[{"arn":" arn:aws:codewhisperer:profile/fetched "}]} `)),
				Header:     make(http.Header),
			}, nil
		}),
	})
	t.Cleanup(func() { InitKiroHttpClient("") })

	requestAccount := account
	requestAccount.UsageCurrent = 0
	got, err := ResolveProfileArn(&requestAccount)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "arn:aws:codewhisperer:profile/fetched" {
		t.Fatalf("expected fetched ARN, got %q", got)
	}
	if requestAccount.ProfileArn != got {
		t.Fatalf("expected account to be updated with fetched ARN, got %q", requestAccount.ProfileArn)
	}

	accounts := config.GetAccounts()
	if len(accounts) != 1 {
		t.Fatalf("expected one persisted account, got %d", len(accounts))
	}
	if accounts[0].ProfileArn != got {
		t.Fatalf("expected persisted account profile ARN %q, got %q", got, accounts[0].ProfileArn)
	}
	if accounts[0].UsageCurrent != 7 {
		t.Fatalf("expected profile cache update to preserve usage fields, got usageCurrent=%v", accounts[0].UsageCurrent)
	}
}

func TestResolveProfileArnRequiresSelectionWhenMultipleProfiles(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	if err := config.Init(configPath); err != nil {
		t.Fatalf("init config: %v", err)
	}
	account := config.Account{
		ID:          "acct-1",
		Email:       "user@example.com",
		AccessToken: "access-token",
		Region:      "us-east-1",
	}
	if err := config.AddAccount(account); err != nil {
		t.Fatalf("add account: %v", err)
	}

	kiroRestHttpStore.Store(&http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(`{"profiles":[` +
					`{"arn":"arn:aws:codewhisperer:profile/first","name":"First"},` +
					`{"arn":"arn:aws:codewhisperer:profile/second","name":"Second"}` +
					`]}`)),
				Header: make(http.Header),
			}, nil
		}),
	})
	t.Cleanup(func() { InitKiroHttpClient("") })

	requestAccount := account
	got, err := ResolveProfileArn(&requestAccount)
	if err == nil {
		t.Fatalf("expected multiple profiles error, got nil")
	}
	if got != "" {
		t.Fatalf("expected empty profile ARN on error, got %q", got)
	}
	if !strings.Contains(err.Error(), "multiple profiles") {
		t.Fatalf("expected multiple profiles error, got %v", err)
	}
	if requestAccount.ProfileArn != "" {
		t.Fatalf("expected request account profile ARN to remain empty, got %q", requestAccount.ProfileArn)
	}

	accounts := config.GetAccounts()
	if len(accounts) != 1 {
		t.Fatalf("expected one persisted account, got %d", len(accounts))
	}
	if accounts[0].ProfileArn != "" {
		t.Fatalf("expected persisted account profile ARN to remain empty, got %q", accounts[0].ProfileArn)
	}
}

func TestListAvailableProfilesReturnsAllProfiles(t *testing.T) {
	kiroRestHttpStore.Store(&http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			body := `{"profiles":[]}`
			switch req.URL.Host {
			case "q.us-east-1.amazonaws.com":
				body = `{"profiles":[{"arn":" arn:aws:codewhisperer:us-east-1:610548660232:profile/USEASTPROFILE ","name":" US East "}]}`
			case "q.eu-central-1.amazonaws.com":
				body = `{"profiles":[{"arn":"arn:aws:codewhisperer:eu-central-1:610548660232:profile/EUCENTRALONE","name":"EU Central"}]}`
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
			}, nil
		}),
	})
	t.Cleanup(func() { InitKiroHttpClient("") })

	profiles, err := ListAvailableProfiles(&config.Account{AccessToken: "access-token", Region: "us-east-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(profiles) != 2 {
		t.Fatalf("expected 2 profiles, got %d", len(profiles))
	}
	if profiles[0].Arn != "arn:aws:codewhisperer:us-east-1:610548660232:profile/USEASTPROFILE" || profiles[0].Name != "US East" {
		t.Fatalf("unexpected first profile: %#v", profiles[0])
	}
	if profiles[1].Arn != "arn:aws:codewhisperer:eu-central-1:610548660232:profile/EUCENTRALONE" || profiles[1].Name != "EU Central" {
		t.Fatalf("unexpected second profile: %#v", profiles[1])
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
