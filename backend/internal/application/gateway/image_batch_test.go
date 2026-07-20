package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	accountapp "github.com/chenyme/grok2api/backend/internal/application/account"
	clientkeyapp "github.com/chenyme/grok2api/backend/internal/application/clientkey"
	"github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/domain/clientkey"
	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
	"github.com/chenyme/grok2api/backend/internal/infra/persistence/relational"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
	"github.com/chenyme/grok2api/backend/internal/infra/runtime/memory"
)

func TestLiteImageBatchUsesMultipleAccountsAndRetriesFailedSlot(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "lite-image-batch.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accountRepo := relational.NewAccountRepository(database)
	modelRepo := relational.NewModelRepository(database)
	auditRepo := relational.NewAuditRepository(database)
	keyRepo := relational.NewClientKeyRepository(database)
	credentials := make([]account.Credential, 0, 3)
	for index := range 3 {
		name := "lite-batch-" + string(rune('a'+index))
		credential, _, createErr := accountRepo.UpsertByIdentity(ctx, account.Credential{
			Provider: account.ProviderWeb, AuthType: account.AuthTypeSSO, WebTier: account.WebTierBasic,
			Name: name, SourceKey: name, EncryptedAccessToken: "encrypted-" + name,
			Enabled: true, AuthStatus: account.AuthStatusActive, Priority: 300 - index*100, MaxConcurrent: 1,
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		credentials = append(credentials, credential)
	}
	if err := modelRepo.UpsertRoutes(ctx, []modeldomain.Route{{
		PublicID: "grok-imagine-image", Provider: account.ProviderWeb, UpstreamModel: "grok-imagine-image",
		Capability: modeldomain.CapabilityImage, Enabled: true,
	}}); err != nil {
		t.Fatal(err)
	}
	for _, credential := range credentials {
		if err := modelRepo.ReplaceAccountCapabilities(ctx, credential.ID, []string{"grok-imagine-image"}, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	key, err := keyRepo.Create(ctx, clientkey.Key{
		Name: "lite-batch-key", Prefix: "lite-batch", SecretHash: strings.Repeat("a", 64), EncryptedSecret: "encrypted-key",
		Enabled: true, RPMLimit: 60, MaxConcurrent: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &liteBatchAdapter{failOnceAccountID: credentials[0].ID, started: make(chan struct{}, 8), release: make(chan struct{})}
	registry := provider.NewRegistry(adapter)
	sticky := memory.NewStickyStore()
	accountService := accountapp.NewService(accountRepo, auditRepo, memory.NewDeviceSessionStore(), sticky, registry, testCipher(t), nil)
	selector := NewSelector(accountRepo, memory.NewConcurrencyLimiter(), sticky, registry, time.Hour, 0, 0)
	service := NewService(modelRepo, auditRepo, accountService, clientkeyapp.NewService(keyRepo, nil, nil, 60, 10, nil), registry, selector, relational.NewResponseRepository(database), 0)

	done := make(chan struct{})
	var result *Result
	var generateErr error
	go func() {
		defer close(done)
		result, generateErr = service.GenerateImage(ctx, ImageGenerationInput{
			RequestID: "req-lite-batch", ClientKey: key, PublicModel: "grok-imagine-image",
			Prompt: "draw", Count: 3, ResponseFormat: "url",
		})
	}()
	for range 3 {
		select {
		case <-adapter.started:
		case <-time.After(2 * time.Second):
			t.Fatal("Lite 图片子任务未并发启动")
		}
	}
	close(adapter.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Lite 图片批量请求未结束")
	}
	if generateErr != nil {
		t.Fatal(generateErr)
	}
	defer result.Body.Close()
	var body struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.NewDecoder(result.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data) != 3 {
		t.Fatalf("data count = %d, want 3", len(body.Data))
	}
	result.Finalize(Usage{}, "", "")
	attempts, maxConcurrent := adapter.snapshot()
	if maxConcurrent < 3 {
		t.Fatalf("max concurrent calls = %d, want at least 3", maxConcurrent)
	}
	if attempts[credentials[0].ID] != 1 {
		t.Fatalf("failed account attempts = %d, want 1", attempts[credentials[0].ID])
	}
	totalAttempts := 0
	for _, count := range attempts {
		totalAttempts += count
	}
	if totalAttempts != 4 {
		t.Fatalf("total attempts = %d, want 4; attempts=%#v", totalAttempts, attempts)
	}
	logs, total, err := auditRepo.List(ctx, 0, 10)
	if err != nil || total != 1 || len(logs) != 1 || logs[0].MediaOutputImages != 3 {
		t.Fatalf("audit logs=%#v total=%d err=%v", logs, total, err)
	}

	beforeAllFailed, _ := adapter.snapshot()
	adapter.setFailAll(true)
	failedResult, err := service.GenerateImage(ctx, ImageGenerationInput{
		RequestID: "req-lite-all-failed", ClientKey: key, PublicModel: "grok-imagine-image",
		Prompt: "draw", Count: 1, ResponseFormat: "url",
	})
	if err != nil {
		t.Fatal(err)
	}
	failedBody, err := io.ReadAll(failedResult.Body)
	if err != nil {
		t.Fatal(err)
	}
	failedResult.Finalize(Usage{}, "", "upstream_unavailable")
	_ = failedResult.Body.Close()
	if failedResult.StatusCode != http.StatusBadGateway || !strings.Contains(string(failedBody), "image_generation_incomplete") {
		t.Fatalf("failed response status=%d body=%s", failedResult.StatusCode, failedBody)
	}
	afterAllFailed, _ := adapter.snapshot()
	for _, credential := range credentials {
		if afterAllFailed[credential.ID]-beforeAllFailed[credential.ID] != 1 {
			t.Fatalf("account %d attempts delta = %d, want 1; before=%#v after=%#v", credential.ID, afterAllFailed[credential.ID]-beforeAllFailed[credential.ID], beforeAllFailed, afterAllFailed)
		}
	}
	logs, total, err = auditRepo.List(ctx, 0, 10)
	if err != nil || total != 2 || logs[0].RequestID != "req-lite-all-failed" || logs[0].AttemptCount != 3 {
		t.Fatalf("failed audit logs=%#v total=%d err=%v", logs, total, err)
	}
	failureAudit, err := auditRepo.Get(ctx, logs[0].ID)
	if err != nil || len(failureAudit.Attempts) != 3 {
		t.Fatalf("failure audit=%#v err=%v", failureAudit, err)
	}
	for _, attempt := range failureAudit.Attempts {
		if attempt.UpstreamStatusCode == nil || *attempt.UpstreamStatusCode != http.StatusBadGateway {
			t.Fatalf("failure attempt=%#v", attempt)
		}
	}
}

type liteBatchAdapter struct {
	mu                sync.Mutex
	failOnceAccountID uint64
	failed            bool
	failAll           bool
	attempts          map[uint64]int
	inFlight          int
	maxInFlight       int
	started           chan struct{}
	release           chan struct{}
}

func (a *liteBatchAdapter) Provider() account.Provider { return account.ProviderWeb }

func (a *liteBatchAdapter) Definition() provider.Definition {
	return provider.Definition{
		Provider: account.ProviderWeb, ModelNamespace: account.ProviderWeb.ModelNamespace(),
		Credential: provider.CredentialSurface{AuthType: account.AuthTypeSSO},
		Media:      provider.MediaSurface{ImageGeneration: true},
	}
}

func (a *liteBatchAdapter) QuotaMode(string) string { return "fast" }

func (a *liteBatchAdapter) TierOrder(string) []account.WebTier {
	return []account.WebTier{account.WebTierBasic}
}

func (a *liteBatchAdapter) PricingModel(string) string { return "grok-imagine-image" }

func (a *liteBatchAdapter) GenerateImage(ctx context.Context, request provider.ImageGenerationRequest) (*provider.Response, error) {
	if request.Count != 1 {
		return nil, &unexpectedLiteBatchCount{count: request.Count}
	}
	a.mu.Lock()
	if a.attempts == nil {
		a.attempts = make(map[uint64]int)
	}
	a.attempts[request.Credential.ID]++
	a.inFlight++
	a.maxInFlight = max(a.maxInFlight, a.inFlight)
	shouldFail := a.failAll || request.Credential.ID == a.failOnceAccountID && !a.failed
	if shouldFail {
		a.failed = true
	}
	a.mu.Unlock()
	a.started <- struct{}{}
	select {
	case <-ctx.Done():
		a.finish()
		return nil, ctx.Err()
	case <-a.release:
	}
	a.finish()
	if shouldFail {
		return &provider.Response{
			StatusCode: http.StatusBadGateway, Status: "502 Bad Gateway", Header: http.Header{"Content-Type": {"application/json"}},
			Body: io.NopCloser(strings.NewReader(`{"error":{"code":"image_generation_incomplete"}}`)),
		}, nil
	}
	return &provider.Response{
		StatusCode: http.StatusOK, Status: "200 OK", Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(`{"created":1,"data":[{"url":"https://example.com/image.png"}]}`)), QuotaUnits: 1,
	}, nil
}

func (a *liteBatchAdapter) setFailAll(value bool) {
	a.mu.Lock()
	a.failAll = value
	a.mu.Unlock()
}

func (a *liteBatchAdapter) finish() {
	a.mu.Lock()
	a.inFlight--
	a.mu.Unlock()
}

func (a *liteBatchAdapter) snapshot() (map[uint64]int, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	attempts := make(map[uint64]int, len(a.attempts))
	for id, count := range a.attempts {
		attempts[id] = count
	}
	return attempts, a.maxInFlight
}

type unexpectedLiteBatchCount struct{ count int }

func (e *unexpectedLiteBatchCount) Error() string { return "unexpected Lite batch count" }
