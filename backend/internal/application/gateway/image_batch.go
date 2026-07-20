package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
)

type liteImageUnitResult struct {
	index      int
	item       json.RawMessage
	credential accountdomain.Credential
	quotaMode  string
	response   *provider.Response
	err        error
}

type liteImageBatchCoordinator struct {
	wake chan struct{}
}

func newLiteImageBatchCoordinator(count int) *liteImageBatchCoordinator {
	return &liteImageBatchCoordinator{wake: make(chan struct{}, max(1, count))}
}

func (c *liteImageBatchCoordinator) release(lease *accountLease) {
	if lease == nil {
		return
	}
	lease.Release()
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (s *Service) executeLiteImageBatch(
	ctx context.Context,
	providerValue accountdomain.Provider,
	upstreamModel string,
	quotaMode string,
	count int,
	policy accountAttemptPolicy,
	execute imageExecution,
	failureAttempts *failureAttemptRecorder,
) (*provider.Response, []imageAccountUsage, *accountdomain.Credential, error) {
	count = max(1, count)
	batchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	coordinator := newLiteImageBatchCoordinator(count)
	results := make(chan liteImageUnitResult, count)
	initiallySelected := make(map[uint64]bool, count)
	launched := 0

	for index := 0; index < count; index++ {
		lease, err := s.acquireLiteImageLease(batchCtx, providerValue, upstreamModel, quotaMode, initiallySelected, coordinator)
		if err != nil {
			lease, err = s.acquireLiteImageLease(batchCtx, providerValue, upstreamModel, quotaMode, nil, coordinator)
		}
		if err != nil {
			cancel()
			for range launched {
				result := <-results
				if result.response != nil {
					_ = result.response.Body.Close()
				}
			}
			return nil, nil, nil, fmt.Errorf("%w: %w", ErrNoAvailableAccount, err)
		}
		initiallySelected[lease.Credential.ID] = true
		launched++
		go func(index int, initial *accountLease) {
			results <- s.executeLiteImageUnit(batchCtx, providerValue, upstreamModel, quotaMode, index, initial, policy, execute, coordinator, failureAttempts)
		}(index, lease)
	}

	items := make([]json.RawMessage, count)
	usagesByAccount := make(map[uint64]imageAccountUsage, count)
	var primary *accountdomain.Credential
	var failure *liteImageUnitResult
	for range count {
		result := <-results
		if result.index == 0 {
			value := result.credential
			primary = &value
		}
		if result.err != nil || result.response != nil {
			if failure == nil {
				value := result
				failure = &value
				cancel()
			} else if result.response != nil {
				_ = result.response.Body.Close()
			}
			continue
		}
		items[result.index] = result.item
		usage := usagesByAccount[result.credential.ID]
		usage.credential = result.credential
		usage.quotaMode = result.quotaMode
		usage.units++
		usagesByAccount[result.credential.ID] = usage
	}
	if failure != nil {
		failedCredential := failure.credential
		if failure.response != nil {
			return failure.response, nil, &failedCredential, nil
		}
		return nil, nil, &failedCredential, failure.err
	}

	body, err := json.Marshal(struct {
		Created int64             `json:"created"`
		Data    []json.RawMessage `json:"data"`
	}{Created: time.Now().Unix(), Data: items})
	if err != nil {
		return nil, nil, primary, fmt.Errorf("合并 Lite 图片响应: %w", err)
	}
	usages := make([]imageAccountUsage, 0, len(usagesByAccount))
	for _, usage := range usagesByAccount {
		usages = append(usages, usage)
	}
	s.logger.Info("image_lite_batch_completed", "images", count, "accounts", len(usages))
	return &provider.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
		QuotaUnits: count,
	}, usages, primary, nil
}

func (s *Service) acquireLiteImageLease(
	ctx context.Context,
	providerValue accountdomain.Provider,
	upstreamModel string,
	quotaMode string,
	excluded map[uint64]bool,
	coordinator *liteImageBatchCoordinator,
) (*accountLease, error) {
	for {
		lease, err := s.selector.Acquire(ctx, providerValue, upstreamModel, quotaMode, "", excluded, false)
		if err == nil {
			return lease, nil
		}
		var unavailable *SelectionUnavailableError
		if !errors.As(err, &unavailable) || unavailable.Reason != SelectionSaturated {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-coordinator.wake:
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (s *Service) executeLiteImageUnit(
	ctx context.Context,
	providerValue accountdomain.Provider,
	upstreamModel string,
	quotaMode string,
	index int,
	initial *accountLease,
	policy accountAttemptPolicy,
	execute imageExecution,
	coordinator *liteImageBatchCoordinator,
	failureAttempts *failureAttemptRecorder,
) liteImageUnitResult {
	excluded := make(map[uint64]bool)
	lease := initial
	var lastCredential accountdomain.Credential
	var lastErr error
	var lastResponse *provider.Response
	for attempt := 0; policy.allows(attempt); attempt++ {
		if lease == nil {
			var err error
			lease, err = s.acquireLiteImageLease(ctx, providerValue, upstreamModel, quotaMode, excluded, coordinator)
			if err != nil {
				if lastResponse != nil {
					return liteImageUnitResult{index: index, credential: lastCredential, response: lastResponse}
				}
				if lastErr == nil {
					lastErr = err
				}
				break
			}
		}
		excluded[lease.Credential.ID] = true
		credentialStartedAt := time.Now()
		credential, err := s.accounts.EnsureCredential(ctx, lease.Credential, false)
		if err != nil {
			failureAttempts.captureCredentialFailure(lease.Credential, credentialStartedAt, false, err)
			lastResponse = nil
			lastCredential = lease.Credential
			lastErr = err
			coordinator.release(lease)
			lease = nil
			continue
		}
		lastCredential = credential
		attemptStartedAt := time.Now()
		response, err := execute(ctx, providerValue, credential, upstreamModel, 1)
		err = failureAttempts.captureResponse(credential, attemptStartedAt, response, err)
		if err != nil {
			lastErr = err
			if isSSOCredentialRejected(err, credential) {
				s.markSSOCredentialRejected(ctx, credential, fmt.Sprintf("%s SSO credential rejected", credential.Provider))
			} else if ctx.Err() == nil && !provider.IsMediaPostProcessingError(err) {
				s.selector.MarkFailure(ctx, credential, 0, 0)
			}
			coordinator.release(lease)
			lease = nil
			if provider.IsMediaPostProcessingError(err) || !policy.hasNext(attempt) {
				break
			}
			continue
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			item, parseErr := readLiteImageItem(response.Body)
			_ = response.Body.Close()
			coordinator.release(lease)
			lease = nil
			if parseErr == nil {
				return liteImageUnitResult{index: index, item: item, credential: credential, quotaMode: quotaMode}
			}
			lastErr = parseErr
			failureAttempts.captureProviderFailure(credential, attemptStartedAt, "response_decode", parseErr)
			s.selector.MarkFailure(ctx, credential, http.StatusBadGateway, 0)
			if !policy.hasNext(attempt) {
				break
			}
			continue
		}

		retryable := liteImageStatusRetryable(response.StatusCode)
		if response.StatusCode == http.StatusUnauthorized && credential.AuthType == accountdomain.AuthTypeSSO {
			s.markSSOCredentialRejected(ctx, credential, fmt.Sprintf("%s SSO credential rejected", credential.Provider))
			retryable = true
		}
		if response.StatusCode == http.StatusTooManyRequests && quotaMode != "" {
			retryAfter := parseRetryAfter(response.Header.Get("Retry-After"), time.Now().UTC())
			_, _ = s.accounts.ReconcileWebRateLimit(ctx, credential.ID, quotaMode, retryAfter)
			s.selector.MarkQuotaStateChanged(credential.Provider)
		} else if retryable {
			s.selector.MarkFailure(ctx, credential, response.StatusCode, 0)
		}
		if retryable && policy.hasNext(attempt) {
			body, _ := readRetryableBody(response.Body)
			lastResponse = cloneLiteImageFailureResponse(response, body)
			coordinator.release(lease)
			lease = nil
			lastErr = fmt.Errorf("Lite 图片账号 %d 返回 %d", credential.ID, response.StatusCode)
			continue
		}
		coordinator.release(lease)
		lease = nil
		return liteImageUnitResult{index: index, credential: credential, response: response}
	}
	if lease != nil {
		coordinator.release(lease)
	}
	if lastErr == nil {
		lastErr = ErrNoAvailableAccount
	}
	if lastResponse != nil {
		return liteImageUnitResult{index: index, credential: lastCredential, response: lastResponse}
	}
	return liteImageUnitResult{index: index, credential: lastCredential, err: lastErr}
}

func cloneLiteImageFailureResponse(response *provider.Response, body []byte) *provider.Response {
	if response == nil {
		return nil
	}
	return &provider.Response{
		StatusCode: response.StatusCode,
		Status:     response.Status,
		Header:     response.Header.Clone(),
		Body:       io.NopCloser(bytes.NewReader(body)),
		Diagnostic: response.Diagnostic,
	}
}

func liteImageStatusRetryable(status int) bool {
	return status == http.StatusForbidden || status == http.StatusRequestTimeout || status == http.StatusTooEarly ||
		status == http.StatusTooManyRequests || status >= http.StatusInternalServerError
}

func readLiteImageItem(body io.Reader) (json.RawMessage, error) {
	var value struct {
		Data []json.RawMessage `json:"data"`
	}
	decoder := json.NewDecoder(io.LimitReader(body, 64<<20))
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("解析 Lite 图片响应: %w", err)
	}
	if len(value.Data) != 1 || len(value.Data[0]) == 0 {
		return nil, fmt.Errorf("Lite 图片响应缺少单张结果")
	}
	return value.Data[0], nil
}
