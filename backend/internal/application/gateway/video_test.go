package gateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	accountapp "github.com/chenyme/grok2api/backend/internal/application/account"
	"github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/domain/audit"
	"github.com/chenyme/grok2api/backend/internal/domain/media"
	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
	"github.com/chenyme/grok2api/backend/internal/infra/persistence/relational"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
	"github.com/chenyme/grok2api/backend/internal/infra/runtime/memory"
	"github.com/chenyme/grok2api/backend/internal/repository"
)

func TestVideoInputJSONLimitMatchesPersistenceConstraint(t *testing.T) {
	withinLimit := encodeVideoInput([]string{strings.Repeat("a", maxVideoInputJSONBytes-96)}, true, "1280x720")
	if len(withinLimit) > maxVideoInputJSONBytes {
		t.Fatalf("within-limit fixture encoded to %d bytes", len(withinLimit))
	}
	overLimit := encodeVideoInput([]string{strings.Repeat("a", maxVideoInputJSONBytes)}, true, "1280x720")
	if len(overLimit) <= maxVideoInputJSONBytes {
		t.Fatalf("over-limit fixture encoded to %d bytes", len(overLimit))
	}
}

func TestVideoInputMetadataStoresLargeUploadAsCompactAssetReference(t *testing.T) {
	raw := append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{0x7f}, 900<<10)...)
	assets := newVideoAssetStoreStub()
	service := &Service{mediaAssets: assets, logger: slog.Default()}
	inputJSON, assetIDs, err := service.prepareVideoInput(context.Background(), []VideoReference{{Data: raw}}, true, "1280x720")
	if err != nil {
		t.Fatal(err)
	}
	if len(assetIDs) != 1 || len(inputJSON) >= 1024 || strings.Contains(inputJSON, "data:image") || strings.Contains(inputJSON, base64.StdEncoding.EncodeToString(raw[:64])) {
		t.Fatalf("assetIDs=%#v input bytes=%d input=%s", assetIDs, len(inputJSON), inputJSON)
	}
	decoded := decodeVideoInput(inputJSON)
	if len(decoded.References) != 1 || decoded.References[0].AssetID != assetIDs[0] || len(decoded.ImageURLs) != 0 {
		t.Fatalf("decoded=%#v", decoded)
	}
}

func TestPrepareVideoInputRollsBackSavedAssetsWhenMetadataIsTooLarge(t *testing.T) {
	assets := newVideoAssetStoreStub()
	service := &Service{mediaAssets: assets, logger: slog.Default()}
	_, assetIDs, err := service.prepareVideoInput(context.Background(), []VideoReference{
		{Data: []byte("image")},
		{URL: "https://example.com/" + strings.Repeat("x", maxVideoInputJSONBytes)},
	}, true, "1280x720")
	if !errors.Is(err, ErrVideoInputTooLarge) || len(assetIDs) != 0 {
		t.Fatalf("assetIDs=%#v err=%v", assetIDs, err)
	}
	if len(assets.deleted) != 1 || len(assets.images) != 0 {
		t.Fatalf("deleted=%#v remaining=%#v", assets.deleted, assets.images)
	}
}

func TestStageVideoReferencesRollsBackPartialSave(t *testing.T) {
	assets := newVideoAssetStoreStub()
	assets.saveErrorAt = 2
	service := &Service{mediaAssets: assets, logger: slog.Default()}
	_, _, err := service.stageVideoReferences(context.Background(), []VideoReference{{Data: []byte("first")}, {Data: []byte("second")}})
	if err == nil || len(assets.deleted) != 1 || len(assets.images) != 0 {
		t.Fatalf("error=%v deleted=%#v remaining=%#v", err, assets.deleted, assets.images)
	}
}

func TestResolveVideoReferencesPreservesLegacyAndAssetOrder(t *testing.T) {
	assets := newVideoAssetStoreStub()
	assets.images["img_local"] = storedVideoInputImage{asset: media.Asset{ID: "img_local", Kind: "image", MIMEType: "image/png", SizeBytes: 3}, data: []byte("png")}
	service := &Service{mediaAssets: assets}
	references, err := service.resolveVideoReferences(context.Background(), videoInputMetadata{References: []videoInputReference{
		{URL: "https://example.com/first.png"}, {AssetID: "img_local"}, {URL: "https://example.com/third.png"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(references) != 3 || references[0] != "https://example.com/first.png" || references[2] != "https://example.com/third.png" || references[1] != "data:image/png;base64,"+base64.StdEncoding.EncodeToString([]byte("png")) {
		t.Fatalf("references=%#v", references)
	}
	legacy := decodeVideoInput(`{"image_urls":["https://example.com/legacy.png"]}`)
	legacyReferences, err := (&Service{}).resolveVideoReferences(context.Background(), legacy)
	if err != nil || len(legacyReferences) != 1 || legacyReferences[0] != "https://example.com/legacy.png" {
		t.Fatalf("legacy=%#v err=%v", legacyReferences, err)
	}
}

func TestResolveVideoReferencesReturnsMissingAssetError(t *testing.T) {
	service := &Service{mediaAssets: newVideoAssetStoreStub()}
	if _, err := service.resolveVideoReferences(context.Background(), videoInputMetadata{References: []videoInputReference{{AssetID: "img_missing"}}}); err == nil {
		t.Fatal("missing input asset was accepted")
	}
}

func TestVideoJobResponseMetadataDoesNotInferProtocolFromID(t *testing.T) {
	nativeJob := media.Job{ID: "video_oai_collision", InputJSON: encodeVideoInput([]string{"https://example.com/reference.png"}, false, "")}
	if compatible, _ := VideoJobResponseMetadata(nativeJob); compatible {
		t.Fatal("native job was classified by ID prefix")
	}
	if references := decodeVideoInput(nativeJob.InputJSON).References; len(references) != 1 || references[0].URL != "https://example.com/reference.png" {
		t.Fatalf("reference images=%#v", references)
	}
	compatibleJob := media.Job{ID: "video_random", InputJSON: encodeVideoInput(nil, true, "1280x720")}
	compatible, size := VideoJobResponseMetadata(compatibleJob)
	if !compatible || size != "1280x720" {
		t.Fatalf("compatible=%t size=%q", compatible, size)
	}
	legacyCompatible := media.Job{ID: "video_oai_legacy", InputJSON: `{"image_urls":[]}`}
	if compatible, _ := VideoJobResponseMetadata(legacyCompatible); !compatible {
		t.Fatal("legacy compatible job was not recognized")
	}
}

func TestVideoOAuthUnauthorizedMarksAccountFailure(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "video-oauth-401.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accounts := relational.NewAccountRepository(database)
	credential, _, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderBuild, AuthType: account.AuthTypeOAuth, Name: "video-oauth-401",
		SourceKey: "video-oauth-401", EncryptedAccessToken: "encrypted", AuthStatus: account.AuthStatusActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{selector: NewSelector(accounts, memory.NewConcurrencyLimiter(), memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)}
	startedAt := time.Now().UTC()
	service.handleVideoCredentialRejected(ctx, credential)
	stored, err := accounts.Get(ctx, credential.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.FailureCount != 1 || stored.CooldownUntil == nil || !stored.CooldownUntil.After(startedAt) || !strings.Contains(stored.LastError, "401") {
		t.Fatalf("stored credential after video 401 = %#v", stored)
	}
}

func TestExecuteVideoAttemptsRetriesPremiumAccountThenFailsOverOnce(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "video-account-retry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accounts := relational.NewAccountRepository(database)
	models := relational.NewModelRepository(database)
	credentials := make([]account.Credential, 0, 2)
	for index, tier := range []account.WebTier{account.WebTierSuper, account.WebTierHeavy} {
		name := fmt.Sprintf("video-%s", tier)
		credential, _, createErr := accounts.UpsertByIdentity(ctx, account.Credential{
			Provider: account.ProviderWeb, AuthType: account.AuthTypeSSO, WebTier: tier,
			Name: name, SourceKey: name, EncryptedAccessToken: "encrypted", ExpiresAt: time.Now().Add(time.Hour),
			Enabled: true, AuthStatus: account.AuthStatusActive, Priority: 200 - index, MaxConcurrent: 1,
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		credentials = append(credentials, credential)
	}
	const upstreamModel = "grok-imagine-video"
	if err := models.UpsertDiscovered(ctx, account.ProviderWeb, []string{upstreamModel}); err != nil {
		t.Fatal(err)
	}
	for _, credential := range credentials {
		if err := models.ReplaceAccountCapabilities(ctx, credential.ID, []string{upstreamModel}, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	adapter := &videoRetryAdapter{failAccountID: credentials[0].ID}
	registry := provider.NewRegistry(adapter)
	sticky := memory.NewStickyStore()
	accountService := accountapp.NewService(accounts, relational.NewAuditRepository(database), memory.NewDeviceSessionStore(), sticky, registry, testCipher(t), nil)
	selector := NewSelector(accounts, memory.NewConcurrencyLimiter(), sticky, registry, time.Hour, time.Second, time.Minute)
	service := NewService(models, relational.NewAuditRepository(database), accountService, nil, registry, selector, nil, 0)
	service.UpdateVideoAccountAttempts(2)
	jobs := &videoExecutionRepository{}
	service.mediaJobs = jobs
	job := media.Job{ID: "video_retry", AccountID: credentials[0].ID, AccountName: credentials[0].Name, Progress: 1}
	execution := service.executeVideoAttempts(ctx, &job, modeldomain.Route{
		ID: 1, PublicID: upstreamModel, Provider: account.ProviderWeb, UpstreamModel: upstreamModel, Capability: modeldomain.CapabilityVideo,
	}, adapter, nil)
	if execution.err != nil {
		t.Fatal(execution.err)
	}
	defer execution.lease.Release()
	if len(adapter.accountIDs) != 3 || adapter.accountIDs[0] != credentials[0].ID || adapter.accountIDs[1] != credentials[0].ID || adapter.accountIDs[2] != credentials[1].ID {
		t.Fatalf("video attempts = %#v", adapter.accountIDs)
	}
	if execution.credential.ID != credentials[1].ID || job.AccountID != credentials[1].ID || jobs.job.AccountID != credentials[1].ID {
		t.Fatalf("final credential=%d job=%d stored=%d", execution.credential.ID, job.AccountID, jobs.job.AccountID)
	}
	if len(execution.attempts) != 2 || execution.attempts[0].UpstreamStatusCode == nil || *execution.attempts[0].UpstreamStatusCode != http.StatusForbidden {
		t.Fatalf("failure attempts = %#v", execution.attempts)
	}
	if got := videoAttemptsForCredential(account.Credential{WebTier: account.WebTierBasic}, 9); got != 1 {
		t.Fatalf("basic account attempts = %d", got)
	}
}

func TestRecoverVideoJobsRetriesUsageWithoutRegeneratingVideo(t *testing.T) {
	completedAt := time.Now().UTC()
	repository := &videoUsageRepository{job: media.Job{
		ID: "video_usage_recovery", RequestID: "request-usage-recovery",
		ClientKeyID: 1, ClientKeyName: "client", AccountID: 2, AccountName: "account",
		Provider: "grok_web", Model: "grok-imagine-video", ModelRouteID: 3, UpstreamModel: "video",
		Seconds: 8, Quality: "720p", Status: media.StatusCompleted, InputJSON: `{}`, CreatedAt: completedAt.Add(-time.Minute), CompletedAt: &completedAt,
	}}
	recorder := &durableVideoAuditRecorder{failures: 1}
	service := &Service{mediaJobs: repository, audits: recorder}
	if err := service.RecoverVideoJobs(context.Background()); err == nil {
		t.Fatal("first durable audit failure was ignored")
	}
	if repository.job.UsageRecordedAt != nil {
		t.Fatal("usage was marked before durable audit commit")
	}
	if err := service.RecoverVideoJobs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repository.job.UsageRecordedAt == nil || recorder.calls != 2 {
		t.Fatalf("recordedAt = %v, audit calls = %d", repository.job.UsageRecordedAt, recorder.calls)
	}
	if recorder.last.EventID != "video_usage_video_usage_recovery" || recorder.last.EstimatedCostInUSDTicks <= 0 {
		t.Fatalf("audit = %#v", recorder.last)
	}
}

func TestRecoverVideoJobsRecordsFailedAuditWithEgress(t *testing.T) {
	completedAt := time.Now().UTC()
	nodeID := uint64(42)
	repository := &videoUsageRepository{job: media.Job{
		ID: "video_failed_recovery", RequestID: "request-failed-recovery",
		ClientKeyID: 1, ClientKeyName: "client", AccountID: 2, AccountName: "account",
		Provider: "grok_web", Model: "grok-imagine-video", ModelRouteID: 3, UpstreamModel: "video",
		Seconds: 8, Quality: "720p", Status: media.StatusFailed, ErrorCode: "generation_failed", ErrorMessage: "upstream disconnected",
		EgressNodeID: &nodeID, EgressNodeName: "warp", EgressScope: "grok_web", EgressMode: "proxy",
		InputJSON: `{}`, CreatedAt: completedAt.Add(-time.Minute), CompletedAt: &completedAt,
	}}
	recorder := &durableVideoAuditRecorder{}
	service := &Service{mediaJobs: repository, audits: recorder}
	if err := service.RecoverVideoJobs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repository.job.UsageRecordedAt == nil || recorder.calls != 1 {
		t.Fatalf("recordedAt = %v, audit calls = %d", repository.job.UsageRecordedAt, recorder.calls)
	}
	if recorder.last.StatusCode != 502 || recorder.last.ErrorCode != "generation_failed" || recorder.last.EgressNodeID == nil || *recorder.last.EgressNodeID != nodeID || recorder.last.EgressNodeName != "warp" || recorder.last.EgressMode != audit.EgressModeProxy {
		t.Fatalf("audit = %#v", recorder.last)
	}
	if recorder.last.EstimatedCostInUSDTicks != 0 || recorder.last.MediaOutputSeconds != 0 {
		t.Fatalf("failed job was billed: %#v", recorder.last)
	}
}

func TestRecoverVideoJobsRecordsDetachedAccountSnapshot(t *testing.T) {
	completedAt := time.Now().UTC()
	repository := &videoUsageRepository{job: media.Job{
		ID: "video_detached_account", RequestID: "request-detached-account",
		ClientKeyID: 1, ClientKeyName: "client", AccountName: "deleted account",
		Provider: "grok_web", Model: "grok-imagine-video", ModelRouteID: 3, UpstreamModel: "video",
		Seconds: 8, Quality: "720p", Status: media.StatusFailed, ErrorCode: "generation_failed",
		InputJSON: `{}`, CreatedAt: completedAt.Add(-time.Minute), CompletedAt: &completedAt,
	}}
	recorder := &durableVideoAuditRecorder{}
	service := &Service{mediaJobs: repository, audits: recorder}
	if err := service.RecoverVideoJobs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if recorder.last.AccountID != nil || recorder.last.AccountName != "deleted account" {
		t.Fatalf("detached account audit = %#v", recorder.last)
	}
}

func TestVideoQueueIsBoundedAndDeduplicated(t *testing.T) {
	service := &Service{}
	service.ConfigureMedia(&videoUsageRepository{}, 1)
	capacity := cap(service.mediaQueue)
	for index := range capacity {
		if !service.enqueueVideoJob(fmt.Sprintf("video_%d", index)) {
			t.Fatalf("enqueue %d failed before capacity", index)
		}
	}
	if !service.enqueueVideoJob("video_0") {
		t.Fatal("duplicate queued job should be treated as accepted")
	}
	if service.enqueueVideoJob("video_overflow") {
		t.Fatal("queue accepted a job beyond its capacity")
	}
}

func TestPersistRemoteVideoRetriesSameResultWithoutRegeneration(t *testing.T) {
	adapter := &videoPersistAdapter{failures: 1}
	store := &videoAssetStoreStub{}
	service := &Service{mediaAssets: store}
	credential := account.Credential{ID: 42, Provider: account.ProviderWeb}
	result, err := service.persistRemoteVideo(context.Background(), "video_job", adapter, credential, provider.VideoResult{URL: "https://assets.grok.com/video.mp4", ContentType: "video/mp4"})
	if err != nil {
		t.Fatal(err)
	}
	if adapter.generateCalls != 0 || adapter.downloadCalls != 2 || adapter.lastCredentialID != credential.ID {
		t.Fatalf("generate=%d download=%d credential=%d", adapter.generateCalls, adapter.downloadCalls, adapter.lastCredentialID)
	}
	if store.saveCalls != 1 || result.AssetID != "vid_local" || result.ContentType != "video/mp4" {
		t.Fatalf("store calls=%d result=%#v", store.saveCalls, result)
	}
}

type videoPersistAdapter struct {
	failures         int
	generateCalls    int
	downloadCalls    int
	lastCredentialID uint64
}

type videoRetryAdapter struct {
	failAccountID uint64
	accountIDs    []uint64
}

func (a *videoRetryAdapter) Provider() account.Provider { return account.ProviderWeb }

func (a *videoRetryAdapter) Definition() provider.Definition {
	return provider.Definition{
		Provider: account.ProviderWeb, ModelNamespace: account.ProviderWeb.ModelNamespace(), ModelCatalog: provider.ModelCatalogStatic,
		ModelCapabilities: []modeldomain.Capability{modeldomain.CapabilityVideo},
		Credential:        provider.CredentialSurface{AuthType: account.AuthTypeSSO}, Media: provider.MediaSurface{VideoGeneration: true},
		Inference: provider.InferencePolicy{Usage: provider.UsageEstimated, RetryForbiddenAsEgress: true},
	}
}

func (a *videoRetryAdapter) TierOrder(string) []account.WebTier {
	return []account.WebTier{account.WebTierSuper, account.WebTierHeavy}
}

func (a *videoRetryAdapter) QuotaMode(string) string { return "" }

func (a *videoRetryAdapter) GenerateVideo(_ context.Context, request provider.VideoRequest) (provider.VideoResult, error) {
	a.accountIDs = append(a.accountIDs, request.Credential.ID)
	if request.Credential.ID == a.failAccountID {
		return provider.VideoResult{}, videoRetryStatusError{status: http.StatusForbidden}
	}
	return provider.VideoResult{AssetID: "video_asset_retry", ContentType: "video/mp4"}, nil
}

type videoRetryStatusError struct{ status int }

func (e videoRetryStatusError) Error() string       { return fmt.Sprintf("upstream returned %d", e.status) }
func (e videoRetryStatusError) HTTPStatusCode() int { return e.status }

type videoExecutionRepository struct{ videoUsageRepository }

func (r *videoExecutionRepository) UpdateMediaJob(_ context.Context, job media.Job) error {
	r.job = job
	return nil
}

func (a *videoPersistAdapter) Provider() account.Provider { return account.ProviderWeb }

func (a *videoPersistAdapter) GenerateVideo(context.Context, provider.VideoRequest) (provider.VideoResult, error) {
	a.generateCalls++
	return provider.VideoResult{}, errors.New("must not regenerate")
}

func (a *videoPersistAdapter) DownloadVideo(_ context.Context, credential account.Credential, _ string) (io.ReadCloser, string, int64, error) {
	a.downloadCalls++
	a.lastCredentialID = credential.ID
	if a.downloadCalls <= a.failures {
		return nil, "", 0, errors.New("temporary download failure")
	}
	return io.NopCloser(strings.NewReader("video")), "video/mp4", 5, nil
}

type videoAssetStoreStub struct {
	saveCalls   int
	images      map[string]storedVideoInputImage
	deleted     []string
	saves       int
	saveErrorAt int
}

func (s *videoAssetStoreStub) SaveVideo(_ context.Context, jobID, contentType string, body io.Reader) (media.Asset, error) {
	s.saveCalls++
	if jobID != "video_job" {
		return media.Asset{}, fmt.Errorf("job ID = %s", jobID)
	}
	if contentType != "video/mp4" {
		return media.Asset{}, fmt.Errorf("content type = %s", contentType)
	}
	data, err := io.ReadAll(body)
	if err != nil || string(data) != "video" {
		return media.Asset{}, fmt.Errorf("video body = %q: %w", data, err)
	}
	return media.Asset{ID: "vid_local", Kind: "video", MIMEType: "video/mp4", SizeBytes: int64(len(data))}, nil
}

func (*videoAssetStoreStub) OpenVideo(context.Context, string) (media.Asset, io.ReadCloser, error) {
	return media.Asset{}, nil, errors.New("not implemented")
}

type durableVideoAuditRecorder struct {
	failures int
	calls    int
	last     audit.Record
}

func (r *durableVideoAuditRecorder) Create(context.Context, audit.Record) error { return nil }

func (r *durableVideoAuditRecorder) CreateDurable(_ context.Context, value audit.Record) error {
	r.calls++
	r.last = value
	if r.calls <= r.failures {
		return errors.New("database unavailable")
	}
	return nil
}

type videoUsageRepository struct{ job media.Job }

type storedVideoInputImage struct {
	asset media.Asset
	data  []byte
}

func newVideoAssetStoreStub() *videoAssetStoreStub {
	return &videoAssetStoreStub{images: make(map[string]storedVideoInputImage)}
}

func (s *videoAssetStoreStub) SaveVideoInputImage(_ context.Context, data []byte) (media.Asset, error) {
	s.saves++
	if s.saveErrorAt == s.saves {
		return media.Asset{}, errors.New("save failed")
	}
	id := fmt.Sprintf("img_input_%d", s.saves)
	mimeType := "image/png"
	asset := media.Asset{ID: id, Kind: "image", MIMEType: mimeType, SizeBytes: int64(len(data))}
	s.images[id] = storedVideoInputImage{asset: asset, data: append([]byte(nil), data...)}
	return asset, nil
}

func (s *videoAssetStoreStub) DeleteVideoInputImage(_ context.Context, id string) error {
	if _, ok := s.images[id]; !ok {
		return errors.New("not found")
	}
	delete(s.images, id)
	s.deleted = append(s.deleted, id)
	return nil
}

func (s *videoAssetStoreStub) OpenInternalImage(_ context.Context, id string) (media.Asset, io.ReadCloser, error) {
	stored, ok := s.images[id]
	if !ok {
		return media.Asset{}, nil, errors.New("not found")
	}
	return stored.asset, io.NopCloser(bytes.NewReader(stored.data)), nil
}

func (r *videoUsageRepository) CreateMediaJob(context.Context, media.Job) error { return nil }

func (r *videoUsageRepository) GetMediaJob(context.Context, string, uint64) (media.Job, error) {
	return r.job, nil
}

func (r *videoUsageRepository) GetMediaJobsByIDs(context.Context, []string) ([]media.Job, error) {
	return []media.Job{r.job}, nil
}

func (r *videoUsageRepository) UpdateMediaJob(context.Context, media.Job) error { return nil }

func (r *videoUsageRepository) DeleteMediaJob(context.Context, string) error { return nil }

func (r *videoUsageRepository) ListMediaJobs(context.Context, repository.MediaJobListQuery) ([]media.Job, int64, error) {
	return nil, 0, nil
}

func (r *videoUsageRepository) SummarizeMediaJobs(context.Context) (repository.MediaJobStats, error) {
	return repository.MediaJobStats{}, nil
}

func (r *videoUsageRepository) ListRecoverableMediaJobs(context.Context, int) ([]media.Job, error) {
	return nil, nil
}

func (r *videoUsageRepository) ListUnrecordedTerminalMediaJobs(context.Context, int) ([]media.Job, error) {
	if r.job.UsageRecordedAt != nil || (r.job.Status != media.StatusCompleted && r.job.Status != media.StatusFailed) {
		return nil, nil
	}
	return []media.Job{r.job}, nil
}

func (r *videoUsageRepository) TryClaimMediaJob(context.Context, string, time.Time, time.Time, string) (media.Job, bool, error) {
	return media.Job{}, false, nil
}
func (r *videoUsageRepository) MarkMediaJobUsageRecorded(_ context.Context, _ string, recordedAt time.Time) error {
	r.job.UsageRecordedAt = &recordedAt
	return nil
}
