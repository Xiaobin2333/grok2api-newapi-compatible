package relational

import (
	"context"
	"testing"
	"time"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	mediadomain "github.com/chenyme/grok2api/backend/internal/domain/media"
	"github.com/chenyme/grok2api/backend/internal/repository"
)

func TestInitializeSchemaBackfillsVideoInputAssetPurpose(t *testing.T) {
	ctx := context.Background()
	database := openTestDatabase(t)
	accountValue, _, err := NewAccountRepository(database).UpsertByIdentity(ctx, accountdomain.Credential{
		Provider: accountdomain.ProviderWeb, AuthType: accountdomain.AuthTypeSSO, WebTier: accountdomain.WebTierBasic,
		Name: "media-purpose-account", SourceKey: "media-purpose-account", EncryptedAccessToken: testEncryptedToken,
		AuthStatus: accountdomain.AuthStatusActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	key := clientKeyModel{
		Name: "media-purpose-key", Prefix: "media-purpose-key", SecretHash: testSecretHash,
		EncryptedSecret: testEncryptedToken, Enabled: true, RPMLimit: 60, MaxConcurrent: 4,
	}
	if err := database.db.WithContext(ctx).Create(&key).Error; err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	linked := testMediaAsset("img_legacy_input_aaaaaaaa", "media/legacy-input.png", now)
	unlinked := testMediaAsset("img_legacy_output_aaaaaaa", "media/legacy-output.png", now.Add(time.Second))
	assets := NewMediaAssetRepository(database)
	for _, asset := range []mediadomain.Asset{linked, unlinked} {
		if err := assets.CreateMediaAsset(ctx, asset); err != nil {
			t.Fatal(err)
		}
	}
	job := testMediaJob("media_job_legacy_input", accountValue.ID, key.ID, mediadomain.StatusCompleted, now)
	if err := NewMediaJobRepository(database).CreateMediaJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := database.db.WithContext(ctx).Create(&mediaJobInputAssetModel{JobID: job.ID, Position: 0, AssetID: linked.ID}).Error; err != nil {
		t.Fatal(err)
	}

	for attempt := 0; attempt < 2; attempt++ {
		if err := database.InitializeSchema(ctx); err != nil {
			t.Fatal(err)
		}
	}
	linkedAfter, err := assets.GetMediaAsset(ctx, linked.ID)
	if err != nil {
		t.Fatal(err)
	}
	if linkedAfter.Purpose != mediadomain.AssetPurposeVideoInput {
		t.Fatalf("linked purpose = %q", linkedAfter.Purpose)
	}
	unlinkedAfter, err := assets.GetMediaAsset(ctx, unlinked.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unlinkedAfter.Purpose != mediadomain.AssetPurposeOutput {
		t.Fatalf("unlinked purpose = %q", unlinkedAfter.Purpose)
	}
	listed, total, err := assets.ListMediaAssets(ctx, repository.MediaAssetListQuery{Page: repository.PageQuery{Limit: 10}})
	if err != nil || total != 1 || len(listed) != 1 || listed[0].ID != unlinked.ID {
		t.Fatalf("listed=%#v total=%d err=%v", listed, total, err)
	}
}
