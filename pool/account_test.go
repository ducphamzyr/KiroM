package pool

import (
	"kiro-go/config"
	"testing"
	"time"
)

func TestOverageAccountsAreSkippedByDefault(t *testing.T) {
	p := &AccountPool{}
	normal := config.Account{ID: "normal"}
	overLimit := config.Account{ID: "over", UsageCurrent: 10, UsageLimit: 10}

	p.accounts = []config.Account{normal, overLimit}

	for i := 0; i < 5; i++ {
		acc := p.GetNext()
		if acc == nil {
			t.Fatalf("expected an account")
		}
		if acc.ID == "over" {
			t.Fatalf("expected over-limit account to be skipped by default")
		}
	}
}

func TestOverageAccountsCanBeSelectedWhenAllowed(t *testing.T) {
	p := &AccountPool{}
	overLimit := config.Account{
		ID:            "over",
		UsageCurrent:  10,
		UsageLimit:    10,
		AllowOverage:  true,
		OverageWeight: 1,
	}

	p.accounts = []config.Account{overLimit}

	acc := p.GetNext()
	if acc == nil {
		t.Fatalf("expected allowed overage account")
	}
	if acc.ID != "over" {
		t.Fatalf("expected overage account, got %q", acc.ID)
	}
}

func TestOverageWeightIsLowerThanNormalWeight(t *testing.T) {
	normalWeight := effectiveWeight(1) * overageFrequencyScale
	overageWeight := effectiveOverageWeight(1)

	if overageWeight >= normalWeight {
		t.Fatalf("expected overage weight %d to be lower than normal weight %d", overageWeight, normalWeight)
	}
}

func TestGetNextKeepsFiveMinuteTokenAvailable(t *testing.T) {
	p := &AccountPool{}
	account := config.Account{
		ID:          "acct-1",
		AccessToken: "access-token",
		ExpiresAt:   time.Now().Unix() + 300,
	}

	p.accounts = []config.Account{account}

	got := p.GetNext()
	if got == nil {
		t.Fatalf("expected five-minute token to be available")
	}
	if got.ID != account.ID {
		t.Fatalf("expected account %q, got %q", account.ID, got.ID)
	}
}

func TestRuntimeRoutesUnconfiguredYieldsSingleRoute(t *testing.T) {
	// ProfileArns nil = never configured: fall back to a single account route.
	account := config.Account{ID: "acct-1"}
	routes := account.RuntimeRoutes()
	if len(routes) != 1 {
		t.Fatalf("expected 1 fallback route, got %d", len(routes))
	}
	if routes[0].RouteID() != "acct-1" {
		t.Fatalf("expected route id acct-1, got %q", routes[0].RouteID())
	}
}

func TestRuntimeRoutesUnconfiguredUsesProfileArnFallback(t *testing.T) {
	// ProfileArns nil but a single ProfileArn set: route through that profile.
	account := config.Account{ID: "acct-1", ProfileArn: "arn:profile/one"}
	routes := account.RuntimeRoutes()
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}
	if routes[0].RouteID() != "acct-1|arn:profile/one" {
		t.Fatalf("unexpected route id %q", routes[0].RouteID())
	}
}

func TestRuntimeRoutesAllDisabledYieldsNoRoutes(t *testing.T) {
	// ProfileArns explicitly empty (non-nil) = all routes disabled in admin.
	// Even with a stale ProfileArn, nothing must be routed.
	account := config.Account{
		ID:          "acct-1",
		ProfileArn:  "arn:profile/one",
		ProfileArns: []string{},
	}
	routes := account.RuntimeRoutes()
	if len(routes) != 0 {
		t.Fatalf("expected 0 routes when all profiles disabled, got %d", len(routes))
	}
}

func TestRuntimeRoutesAllDisabledAfterNormalizationYieldsNoRoutes(t *testing.T) {
	// Non-nil slice that normalizes to empty (blanks only) = all disabled.
	account := config.Account{
		ID:          "acct-1",
		ProfileArn:  "arn:profile/one",
		ProfileArns: []string{"", "   "},
	}
	routes := account.RuntimeRoutes()
	if len(routes) != 0 {
		t.Fatalf("expected 0 routes, got %d", len(routes))
	}
}

func TestMultiProfileRoutesUseSeparateRouteKeys(t *testing.T) {
	account := config.Account{
		ID:          "acct-1",
		ProfileArns: []string{"arn:profile/one", "arn:profile/two"},
	}
	routes := account.RuntimeRoutes()
	if len(routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}

	p := &AccountPool{
		accounts:    routes,
		cooldowns:   make(map[string]time.Time),
		errorCounts: make(map[string]int),
	}
	first := p.GetNext()
	second := p.GetNext()
	if first == nil || second == nil {
		t.Fatalf("expected two selectable routes")
	}
	if first.RouteID() == second.RouteID() {
		t.Fatalf("expected separate route IDs, got %q", first.RouteID())
	}

	p.RecordError(first.RouteID(), true)
	for i := 0; i < 3; i++ {
		got := p.GetNext()
		if got == nil {
			t.Fatalf("expected second profile route to remain available")
		}
		if got.RouteID() == first.RouteID() {
			t.Fatalf("expected cooled-down route %q to be skipped", first.RouteID())
		}
	}
}

func TestCooldownRouteIsNotSelectedWhenNoAlternative(t *testing.T) {
	account := config.Account{ID: "acct-1", ProfileArn: "arn:profile/one"}
	p := &AccountPool{
		accounts:    account.RuntimeRoutes(),
		cooldowns:   make(map[string]time.Time),
		errorCounts: make(map[string]int),
	}

	p.RecordError(account.RuntimeRoutes()[0].RouteID(), true)
	if got := p.GetNext(); got != nil {
		t.Fatalf("expected cooled-down route to be unavailable, got %q", got.RouteID())
	}
}

func TestNearExpiredTokenRouteCanBeSelectedForRefresh(t *testing.T) {
	account := config.Account{
		ID:         "acct-1",
		ProfileArn: "arn:profile/one",
		ExpiresAt:  time.Now().Unix() + 30,
	}
	p := &AccountPool{
		accounts:    account.RuntimeRoutes(),
		cooldowns:   make(map[string]time.Time),
		errorCounts: make(map[string]int),
	}

	if got := p.GetNext(); got == nil {
		t.Fatalf("expected near-expired route to be selected so token refresh can run")
	}
}
