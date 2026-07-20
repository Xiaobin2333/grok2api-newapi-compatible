package inference

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/png"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/chenyme/grok2api/backend/internal/application/gateway"
	clientkeydomain "github.com/chenyme/grok2api/backend/internal/domain/clientkey"
	mediadomain "github.com/chenyme/grok2api/backend/internal/domain/media"
	"github.com/chenyme/grok2api/backend/internal/transport/http/middleware"
	"github.com/gin-gonic/gin"
)

type recordingImageGateway struct {
	generationInputs []gateway.ImageGenerationInput
	editInputs       []gateway.ImageEditInput
	editErr          error
}

func (g *recordingImageGateway) GenerateImage(_ context.Context, input gateway.ImageGenerationInput) (*gateway.Result, error) {
	g.generationInputs = append(g.generationInputs, input)
	return imageHandlerTestResult(input.Streaming, input.ResponseFormat), nil
}

func (g *recordingImageGateway) EditImage(_ context.Context, input gateway.ImageEditInput) (*gateway.Result, error) {
	g.editInputs = append(g.editInputs, input)
	if g.editErr != nil {
		return nil, g.editErr
	}
	return imageHandlerTestResult(input.Streaming, input.ResponseFormat), nil
}

func imageHandlerTestResult(streaming bool, responseFormat string) *gateway.Result {
	body := "{\"created\":1,\"data\":[{\"url\":\"https://example.com/output.png\"}]}"
	if responseFormat == "b64_json" {
		body = "{\"created\":1,\"data\":[{\"b64_json\":\"aW1hZ2U=\"}]}"
	}
	if streaming {
		body = "event: image_generation.completed\ndata: {\"type\":\"image_generation.completed\"}\n\ndata: [DONE]\n\n"
	}
	return &gateway.Result{
		StatusCode: http.StatusOK,
		Status:     http.StatusText(http.StatusOK),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Finalize:   func(gateway.Usage, string, string) {},
	}
}

func imageHandlerTestRouter(images imageGateway) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(middleware.ClientKey, clientkeydomain.Key{ID: 7})
		c.Set(middleware.RequestIDKey, "req-image-route")
		c.Next()
	})
	handler := NewHandler(nil, nil, 1<<20)
	handler.images = images
	handler.Register(router.Group("/v1"))
	return router
}

func TestVideoGenerationUsesOfficialXAIEndpointsAndFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	NewHandler(nil, nil, 1<<20).Register(router.Group("/v1"))

	for _, test := range []struct {
		name string
		body string
	}{
		{name: "unsupported seconds", body: `{"model":"grok-imagine-video","prompt":"test","seconds":8}`},
		{name: "unsupported nested image url", body: `{"model":"grok-imagine-video","image":{"image_url":"https://example.com/input.png"}}`},
		{name: "unsupported size", body: `{"model":"grok-imagine-video","prompt":"test","size":"16:9"}`},
		{name: "unsupported quality", body: `{"model":"grok-imagine-video","prompt":"test","quality":"720p"}`},
		{name: "unsupported input reference", body: `{"model":"grok-imagine-video","input_reference":"https://example.com/input.png"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/videos/generations", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "unknown field") {
				t.Fatalf("unsupported field status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}

	invalidDuration := httptest.NewRequest(http.MethodPost, "/v1/videos/generations", strings.NewReader(`{"model":"grok-imagine-video","prompt":"test","duration":16}`))
	invalidDuration.Header.Set("Content-Type", "application/json")
	invalidRecorder := httptest.NewRecorder()
	router.ServeHTTP(invalidRecorder, invalidDuration)
	if invalidRecorder.Code != http.StatusBadRequest || !strings.Contains(invalidRecorder.Body.String(), "1 到 15") {
		t.Fatalf("invalid duration status=%d body=%s", invalidRecorder.Code, invalidRecorder.Body.String())
	}

	valid := httptest.NewRequest(http.MethodPost, "/v1/videos/generations", strings.NewReader(`{
		"model":"grok-imagine-video","prompt":"test","duration":"8",
		"aspect_ratio":"16:9","resolution":"720p","user":"end_user_1"
	}`))
	valid.Header.Set("Content-Type", "application/json")
	validRecorder := httptest.NewRecorder()
	router.ServeHTTP(validRecorder, valid)
	if validRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("official generation shape status=%d body=%s", validRecorder.Code, validRecorder.Body.String())
	}

	imageOnly := httptest.NewRequest(http.MethodPost, "/v1/videos/generations", strings.NewReader(`{
		"model":"grok-imagine-video","image":{"url":"https://example.com/input.png"}
	}`))
	imageOnly.Header.Set("Content-Type", "application/json")
	imageRecorder := httptest.NewRecorder()
	router.ServeHTTP(imageRecorder, imageOnly)
	if imageRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("image-only generation status=%d body=%s", imageRecorder.Code, imageRecorder.Body.String())
	}

	wrongContentType := httptest.NewRequest(http.MethodPost, "/v1/videos/generations", strings.NewReader(`{"model":"grok-imagine-video","prompt":"test"}`))
	wrongContentType.Header.Set("Content-Type", "text/plain")
	wrongContentTypeRecorder := httptest.NewRecorder()
	router.ServeHTTP(wrongContentTypeRecorder, wrongContentType)
	if wrongContentTypeRecorder.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("wrong content type status=%d body=%s", wrongContentTypeRecorder.Code, wrongContentTypeRecorder.Body.String())
	}

	compatible := httptest.NewRequest(http.MethodPost, "/v1/videos", strings.NewReader(`{
		"model":"grok-imagine-video","prompt":"test","seconds":"8",
		"size":"1280x720","quality":"720p","input_reference":"https://example.com/input.png"
	}`))
	compatible.Header.Set("Content-Type", "application/json")
	compatibleRecorder := httptest.NewRecorder()
	router.ServeHTTP(compatibleRecorder, compatible)
	if compatibleRecorder.Code != http.StatusUnauthorized || !strings.Contains(compatibleRecorder.Body.String(), "invalid_api_key") {
		t.Fatalf("compatible video endpoint status=%d body=%s", compatibleRecorder.Code, compatibleRecorder.Body.String())
	}
	contentRecorder := httptest.NewRecorder()
	router.ServeHTTP(contentRecorder, httptest.NewRequest(http.MethodGet, "/v1/videos/request_1/content", nil))
	if contentRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("video content endpoint status=%d", contentRecorder.Code)
	}
}

func TestWriteVideoContentRejectsDeclaredOversizeMedia(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	writeVideoContent(context, strings.NewReader("ignored"), "video/mp4", maxMediaResponseTransferBytes+1)
	if recorder.Code != http.StatusBadGateway || !strings.Contains(recorder.Body.String(), "media_too_large") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestVideoContentURLUsesConfiguredPublicAPIBase(t *testing.T) {
	handler := NewHandler(nil, nil, 1<<20, "https://api.example.com/grok2api/")
	response := videoGenerationResponse(mediadomain.Job{ID: "video_request_1", Status: mediadomain.StatusCompleted, UpstreamURL: "https://assets.grok.com/source.mp4"}, handler.videoContentURL("video_request_1"))
	video, ok := response["video"].(gin.H)
	if !ok || video["url"] != "https://api.example.com/grok2api/v1/videos/video_request_1/content" {
		t.Fatalf("response = %#v", response)
	}
}

func TestVideoContentURLFollowsRuntimePublicAPIBase(t *testing.T) {
	baseURL := "https://old.example.com"
	handler := NewHandler(nil, nil, 1<<20, "https://static.example.com").SetPublicAPIBaseURLResolver(func() string {
		return baseURL
	})
	if got := handler.videoContentURL("video_request_1"); got != "https://old.example.com/v1/videos/video_request_1/content" {
		t.Fatalf("initial URL = %q", got)
	}
	baseURL = "https://new.example.com/api/"
	if got := handler.videoContentURL("video_request_2"); got != "https://new.example.com/api/v1/videos/video_request_2/content" {
		t.Fatalf("updated URL = %q", got)
	}
}

func TestGatewayErrorDoesNotExposeInternalDetails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/", func(c *gin.Context) {
		writeGatewayError(c, errors.New("dial postgres://secret@internal:5432 failed"))
	})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusBadGateway || strings.Contains(recorder.Body.String(), "postgres") || !strings.Contains(recorder.Body.String(), "上游服务暂不可用") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestGatewayVideoInputTooLargeReturnsRequestError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	writeGatewayError(context, gateway.ErrVideoInputTooLarge)
	if recorder.Code != http.StatusRequestEntityTooLarge || !strings.Contains(recorder.Body.String(), `"code":"request_too_large"`) {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestGatewayErrorHidesUpstreamCredentialStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	openAIRouter := gin.New()
	openAIRouter.GET("/", func(c *gin.Context) {
		writeGatewayError(c, &gateway.UpstreamFailure{
			HTTPStatus: http.StatusForbidden, Code: "upstream_forbidden", PublicMessage: "上游拒绝了该请求",
			UpstreamCode: "permission-denied",
			Cause:        errors.New("secret upstream response"),
		})
	})
	openAIRecorder := httptest.NewRecorder()
	openAIRouter.ServeHTTP(openAIRecorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if openAIRecorder.Code != http.StatusServiceUnavailable || !strings.Contains(openAIRecorder.Body.String(), `"code":"permission-denied"`) || !strings.Contains(openAIRecorder.Body.String(), "上游服务暂不可用，聊天端点访问被拒绝") || strings.Contains(openAIRecorder.Body.String(), "secret") || strings.Contains(openAIRecorder.Body.String(), "上游拒绝了该请求") {
		t.Fatalf("OpenAI status=%d body=%s", openAIRecorder.Code, openAIRecorder.Body.String())
	}

	anthropicRouter := gin.New()
	anthropicRouter.GET("/", func(c *gin.Context) {
		writeGatewayAnthropicError(c, &gateway.UpstreamFailure{
			HTTPStatus: http.StatusTooManyRequests, Code: "upstream_rate_limited", PublicMessage: "上游请求频率受限",
		})
	})
	anthropicRecorder := httptest.NewRecorder()
	anthropicRouter.ServeHTTP(anthropicRecorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if anthropicRecorder.Code != http.StatusTooManyRequests || !strings.Contains(anthropicRecorder.Body.String(), `"type":"rate_limit_error"`) {
		t.Fatalf("Anthropic status=%d body=%s", anthropicRecorder.Code, anthropicRecorder.Body.String())
	}

	credentialRouter := gin.New()
	credentialRouter.GET("/", func(c *gin.Context) {
		writeGatewayAnthropicError(c, &gateway.UpstreamFailure{
			HTTPStatus: http.StatusUnauthorized, Code: "upstream_unauthorized", PublicMessage: "上游账号认证失败",
		})
	})
	credentialRecorder := httptest.NewRecorder()
	credentialRouter.ServeHTTP(credentialRecorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if credentialRecorder.Code != http.StatusServiceUnavailable || !strings.Contains(credentialRecorder.Body.String(), `"type":"overloaded_error"`) || strings.Contains(credentialRecorder.Body.String(), "认证") {
		t.Fatalf("Anthropic credential status=%d body=%s", credentialRecorder.Code, credentialRecorder.Body.String())
	}
}

func TestDirectUpstreamCredentialResponsesAreRewritten(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewHandler(nil, nil, 1<<20)
	for _, tc := range []struct {
		name      string
		status    int
		anthropic bool
		media     bool
		body      string
		wantCode  string
	}{
		{name: "openai unauthorized", status: http.StatusUnauthorized, body: `{"error":"secret upstream credential detail"}`, wantCode: "upstream_unavailable"},
		{name: "anthropic forbidden", status: http.StatusForbidden, anthropic: true, body: `{"code":"permission-denied","error":"secret upstream credential detail"}`, wantCode: "permission-denied"},
		{name: "media forbidden", status: http.StatusForbidden, media: true, body: `{"code":"permission-denied","error":"secret upstream credential detail"}`, wantCode: "permission-denied"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			finalCode := ""
			result := &gateway.Result{
				StatusCode: tc.status,
				Header:     http.Header{"Content-Type": {"application/json"}},
				Body:       io.NopCloser(strings.NewReader(tc.body)),
				Finalize: func(_ gateway.Usage, _, code string) {
					finalCode = code
				},
			}
			router := gin.New()
			router.GET("/", func(c *gin.Context) {
				switch {
				case tc.media:
					handler.writeMediaResult(c, result)
				case tc.anthropic:
					handler.writeAnthropicResult(c, result, false)
				default:
					handler.writeResult(c, result, false, streamProtocolResponses)
				}
			})
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
			if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"`+tc.wantCode+`"`) || strings.Contains(recorder.Body.String(), "secret") || finalCode != "upstream_unavailable" {
				t.Fatalf("status=%d body=%s finalize=%s", recorder.Code, recorder.Body.String(), finalCode)
			}
			if tc.wantCode == "permission-denied" && !strings.Contains(recorder.Body.String(), "上游服务暂不可用，聊天端点访问被拒绝") {
				t.Fatalf("permission message missing: %s", recorder.Body.String())
			}
		})
	}
}

func TestMessagesEndpointUsesAnthropicContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	NewHandler(nil, nil, 1<<20).Register(router.Group("/v1"))

	missingVersion := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"grok-4.5","max_tokens":128,"messages":[{"role":"user","content":"hi"}]}`))
	missingVersion.Header.Set("Content-Type", "application/json")
	missingRecorder := httptest.NewRecorder()
	router.ServeHTTP(missingRecorder, missingVersion)
	if missingRecorder.Code != http.StatusBadRequest || !strings.Contains(missingRecorder.Body.String(), `"type":"error"`) {
		t.Fatalf("missing version status=%d body=%s", missingRecorder.Code, missingRecorder.Body.String())
	}

	valid := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"grok-4.5","max_tokens":128,"messages":[{"role":"user","content":"hi"}]}`))
	valid.Header.Set("Content-Type", "application/json")
	valid.Header.Set("anthropic-version", "2023-06-01")
	validRecorder := httptest.NewRecorder()
	router.ServeHTTP(validRecorder, valid)
	if validRecorder.Code != http.StatusUnauthorized || !strings.Contains(validRecorder.Body.String(), `"type":"authentication_error"`) {
		t.Fatalf("valid shape status=%d body=%s", validRecorder.Code, validRecorder.Body.String())
	}

	zeroTokens := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"grok-4.5","max_tokens":0,"messages":[{"role":"user","content":"hi"}]}`))
	zeroTokens.Header.Set("Content-Type", "application/json")
	zeroTokens.Header.Set("anthropic-version", "2023-06-01")
	zeroRecorder := httptest.NewRecorder()
	router.ServeHTTP(zeroRecorder, zeroTokens)
	if zeroRecorder.Code != http.StatusBadRequest {
		t.Fatalf("zero max_tokens status=%d body=%s", zeroRecorder.Code, zeroRecorder.Body.String())
	}
}

func TestJSONInferenceEndpointsRejectWrongMediaTypeAndTrailingDocument(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	NewHandler(nil, nil, 1<<20).Register(router.Group("/v1"))

	for _, path := range []string{"/v1/responses", "/v1/images/generations"} {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"model":"test","prompt":"test"}`))
		request.Header.Set("Content-Type", "text/plain")
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusUnsupportedMediaType {
			t.Fatalf("%s status=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}

	for _, test := range []struct {
		path string
		body string
	}{
		{path: "/v1/images/generations", body: `{"model":"grok-imagine-image","prompt":"test"}{}`},
		{path: "/v1/images/edits", body: `{"model":"grok-imagine-image-edit","prompt":"test","image":{"url":"https://example.com/input.png"}}{}`},
	} {
		request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
		request.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("%s status=%d body=%s", test.path, recorder.Code, recorder.Body.String())
		}
	}
}

func TestVideoDurationSupportsOpenAICompatibleSeconds(t *testing.T) {
	if value, err := parseVideoDuration(nil); err != nil || value != 8 {
		t.Fatalf("default duration=%d err=%v", value, err)
	}
	if value, err := parseVideoDuration(json.RawMessage(`"6"`)); err != nil || value != 6 {
		t.Fatalf("duration=%d err=%v", value, err)
	}
	normalized, err := normalizeOpenAIVideoRequest(openAIVideoGenerationRequest{Seconds: json.RawMessage(`"9"`)})
	if err != nil || string(normalized.Duration) != `"9"` || normalized.AspectRatio != "9:16" || normalized.ResponseSize != "720x1280" {
		t.Fatalf("normalized duration=%s err=%v", normalized.Duration, err)
	}
	defaultRequest, err := normalizeOpenAIVideoRequest(openAIVideoGenerationRequest{})
	if err != nil || string(defaultRequest.Duration) != `4` || defaultRequest.AspectRatio != "9:16" {
		t.Fatalf("default request=%#v err=%v", defaultRequest, err)
	}
	landscape, err := normalizeOpenAIVideoRequest(openAIVideoGenerationRequest{AspectRatio: "16:9", Resolution: "720p"})
	if err != nil || landscape.ResponseSize != "1280x720" {
		t.Fatalf("landscape request=%#v err=%v", landscape, err)
	}
	portrait1080, err := normalizeOpenAIVideoRequest(openAIVideoGenerationRequest{AspectRatio: "9:16", Resolution: "1080p"})
	if err != nil || portrait1080.ResponseSize != "1080x1920" {
		t.Fatalf("portrait request=%#v err=%v", portrait1080, err)
	}
}

func TestOpenAIVideoRequestCompatibility(t *testing.T) {
	normalized, err := normalizeOpenAIVideoRequest(openAIVideoGenerationRequest{
		Model: "grok-imagine-video", Prompt: "test", Seconds: json.RawMessage(`8`), Size: "1280x720",
		Image:  json.RawMessage(`"https://example.com/image.png"`),
		Images: []string{"https://example.com/second.png"}, InputReference: "https://example.com/third.png",
	})
	if err != nil {
		t.Fatal(err)
	}
	if normalized.AspectRatio != "16:9" || normalized.Resolution != "720p" || normalized.ResponseSize != "1280x720" || normalized.Image == nil || normalized.Image.URL != "https://example.com/image.png" {
		t.Fatalf("normalized request=%#v", normalized)
	}
	if len(normalized.ReferenceImages) != 2 || normalized.ReferenceImages[0].URL != "https://example.com/third.png" || normalized.ReferenceImages[1].URL != "https://example.com/second.png" {
		t.Fatalf("references=%#v", normalized.ReferenceImages)
	}
	if _, _, err := resolveOpenAIVideoSize("", "", "unsupported", ""); err == nil {
		t.Fatal("unsupported size was accepted")
	}
}

func TestOpenAIVideoMultipartCompatibility(t *testing.T) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("model", "grok-imagine-video")
	_ = writer.WriteField("prompt", "test")
	_ = writer.WriteField("seconds", "8")
	_ = writer.WriteField("size", "1280x720")
	part, err := writer.CreateFormFile("input_reference", "reference.png")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write([]byte("\x89PNG\r\n\x1a\n"))
	_ = writer.Close()

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", &body)
	context.Request.Header.Set("Content-Type", writer.FormDataContentType())
	request, err := parseOpenAIVideoMultipart(context, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := normalizeOpenAIVideoRequest(request)
	if err != nil || normalized.AspectRatio != "16:9" || normalized.Resolution != "720p" || len(normalized.ReferenceImages) != 1 || !bytes.Equal(normalized.ReferenceImages[0].Data, []byte("\x89PNG\r\n\x1a\n")) || normalized.ReferenceImages[0].URL != "" {
		t.Fatalf("normalized=%#v err=%v", normalized, err)
	}
}

func TestOpenAIVideoMultipartLargeReferenceStaysRaw(t *testing.T) {
	for _, size := range []int{900 << 10, 5 << 20} {
		t.Run(fmt.Sprintf("%d_bytes", size), func(t *testing.T) {
			raw := makeVideoTestPNG(t, size)
			var body bytes.Buffer
			writer := multipart.NewWriter(&body)
			_ = writer.WriteField("model", "grok-imagine-video")
			part, err := writer.CreateFormFile("input_reference", "reference.png")
			if err != nil {
				t.Fatal(err)
			}
			_, _ = part.Write(raw)
			_ = writer.Close()

			recorder := httptest.NewRecorder()
			context, _ := gin.CreateTestContext(recorder)
			context.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", &body)
			context.Request.Header.Set("Content-Type", writer.FormDataContentType())
			request, err := parseOpenAIVideoMultipart(context, 8<<20)
			if err != nil {
				t.Fatal(err)
			}
			normalized, err := normalizeOpenAIVideoRequest(request)
			if err != nil || len(normalized.ReferenceImages) != 1 || !bytes.Equal(normalized.ReferenceImages[0].Data, raw) || len(raw) < size || normalized.ReferenceImages[0].URL != "" {
				t.Fatalf("normalized data=%d err=%v", len(normalized.ReferenceImages[0].Data), err)
			}
		})
	}
}

func TestOpenAIVideoMultipartOversizeReturns413(t *testing.T) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("model", "grok-imagine-video")
	part, err := writer.CreateFormFile("input_reference", "reference.png")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write(makeVideoTestPNG(t, 2<<20))
	_ = writer.Close()

	gin.SetMode(gin.TestMode)
	router := gin.New()
	NewHandler(nil, nil, 1<<20).Register(router.Group("/v1"))
	request := httptest.NewRequest(http.MethodPost, "/v1/videos", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusRequestEntityTooLarge || !strings.Contains(recorder.Body.String(), `"code":"request_too_large"`) {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func makeVideoTestPNG(t *testing.T, minimumBytes int) []byte {
	t.Helper()
	const width = 1024
	height := max(1, (minimumBytes+width-1)/width)
	value := image.NewGray(image.Rect(0, 0, width, height))
	for index := range value.Pix {
		value.Pix[index] = byte(index*31 + index/251)
	}
	var encoded bytes.Buffer
	encoder := png.Encoder{CompressionLevel: png.NoCompression}
	if err := encoder.Encode(&encoded, value); err != nil {
		t.Fatal(err)
	}
	if _, err := png.Decode(bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatalf("generated PNG is invalid: %v", err)
	}
	return encoded.Bytes()
}

func testPNGBytes(t *testing.T, width, height int) []byte {
	t.Helper()
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, image.NewGray(image.Rect(0, 0, width, height))); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func TestVideoGenerationResponseKeepsNativePollingShape(t *testing.T) {
	pending := videoGenerationResponse(mediadomain.Job{Model: "grok-imagine-video", Status: mediadomain.StatusInProgress, Progress: 42})
	if pending["status"] != "pending" || pending["progress"] != 42 || pending["model"] != "grok-imagine-video" {
		t.Fatalf("pending response=%#v", pending)
	}
	done := videoGenerationResponse(mediadomain.Job{Model: "grok-imagine-video", Status: mediadomain.StatusCompleted, Seconds: 8, UpstreamURL: "https://assets.grok.com/video.mp4"})
	if done["status"] != "done" || done["progress"] != 100 {
		t.Fatalf("done response=%#v", done)
	}
}

func TestOpenAIVideoGenerationResponseMatchesNewAPIPollingShape(t *testing.T) {
	now := time.Now().UTC()
	pending := openAIVideoGenerationResponse(mediadomain.Job{Model: "grok-imagine-video", Status: mediadomain.StatusInProgress, Progress: 42})
	if pending["status"] != "in_progress" || pending["progress"] != 42 || pending["model"] != "grok-imagine-video" || pending["object"] != "video" || pending["video"] != nil {
		t.Fatalf("pending response=%#v", pending)
	}
	done := openAIVideoGenerationResponse(mediadomain.Job{ID: "video_oai_test", Model: "grok-imagine-video", Status: mediadomain.StatusCompleted, Progress: 100, Seconds: 8, Size: "16:9", InputJSON: `{"response_protocol":"openai","response_size":"1280x720"}`, UpstreamURL: "https://assets.grok.com/video.mp4", CreatedAt: now.Add(-time.Minute), CompletedAt: &now})
	if done["id"] != "video_oai_test" || done["task_id"] != nil || done["status"] != "completed" || done["progress"] != 100 || done["completed_at"] != now.Unix() || done["size"] != "1280x720" || done["video"] != nil {
		t.Fatalf("done response=%#v", done)
	}
	failed := openAIVideoGenerationResponse(mediadomain.Job{Status: mediadomain.StatusFailed, ErrorCode: "account_unavailable", ErrorMessage: "try later"})
	errorValue, ok := failed["error"].(gin.H)
	if failed["status"] != "failed" || !ok || errorValue["code"] != "service_unavailable" {
		t.Fatalf("failed response=%#v", failed)
	}
}

func TestImageGenerationEndpointValidatesXAIContractBeforeRouting(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	NewHandler(nil, nil, 1<<20).Register(router.Group("/v1"))

	for _, test := range []struct {
		name string
		body string
		want string
	}{
		{name: "zero n", body: `{"model":"grok-imagine-image","prompt":"test","n":0}`, want: "n 必须在 1 到 10 之间"},
		{name: "large n", body: `{"model":"grok-imagine-image","prompt":"test","n":11}`, want: "n 必须在 1 到 10 之间"},
		{name: "storage options", body: `{"model":"grok-imagine-image","prompt":"test","storage_options":{"filename":"test.jpg"}}`, want: "不支持 storage_options"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), test.want) {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/image", strings.NewReader(`{}`)))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("singular image endpoint status = %d", recorder.Code)
	}
}

func TestResolveOpenAIImageResolution(t *testing.T) {
	for _, test := range []struct {
		name       string
		resolution string
		quality    string
		want       string
		wantErr    bool
	}{
		{name: "default", want: ""},
		{name: "standard", quality: "standard", want: "1k"},
		{name: "auto", quality: "AUTO", want: "1k"},
		{name: "high", quality: "high", want: "2k"},
		{name: "hd", quality: "HD", want: "2k"},
		{name: "explicit resolution wins", resolution: "2K", quality: "low", want: "2k"},
		{name: "unsupported", quality: "ultra", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveOpenAIImageResolution(test.resolution, test.quality)
			if (err != nil) != test.wantErr || got != test.want {
				t.Fatalf("resolution=%q err=%v, want=%q wantErr=%t", got, err, test.want, test.wantErr)
			}
		})
	}
}

func TestImageGenerationAcceptsOpenAIQuality(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	NewHandler(nil, nil, 1<<20).Register(router.Group("/v1"))

	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{
		"model":"grok-imagine-image-quality","prompt":"cat","n":1,
		"size":"1024x1024","quality":"high","response_format":"url"
	}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized || !strings.Contains(recorder.Body.String(), "invalid_api_key") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestImageGenerationAutomaticallyRoutesReferencesToEdit(t *testing.T) {
	images := &recordingImageGateway{}
	router := imageHandlerTestRouter(images)

	textRequest := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{
		"model":"grok-imagine-image","prompt":"draw","n":2,"size":"1536x1024",
		"aspect_ratio":"16:9","resolution":"1k","response_format":"b64_json"
	}`))
	textRequest.Header.Set("Content-Type", "application/json")
	textRecorder := httptest.NewRecorder()
	router.ServeHTTP(textRecorder, textRequest)
	if textRecorder.Code != http.StatusOK || len(images.generationInputs) != 1 || len(images.editInputs) != 0 {
		t.Fatalf("text route status=%d generations=%#v edits=%#v body=%s", textRecorder.Code, images.generationInputs, images.editInputs, textRecorder.Body.String())
	}
	if input := images.generationInputs[0]; input.PublicModel != "grok-imagine-image" || input.RequestedModel != "grok-imagine-image" || input.EffectiveModel != "grok-imagine-image" || input.AutoRouted || input.Count != 2 || input.Size != "1536x1024" || input.AspectRatio != "16:9" || input.Resolution != "1k" || input.ResponseFormat != "b64_json" {
		t.Fatalf("generation input = %#v", input)
	}

	editRequest := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{
		"model":"grok-imagine-image","prompt":"combine 图片1 and image 2","n":1,
		"image_urls":["https://example.com/scene.png","https://example.com/shared.png"],
		"image":{"url":"https://example.com/character.png"},
		"images":["https://example.com/shared.png",{"url":"https://example.com/style.png"}],
		"size":"1536x1024","aspect_ratio":"16:9","resolution":"1k","response_format":"url",
		"stream":true,"partial_images":2
	}`))
	editRequest.Header.Set("Content-Type", "application/json")
	editRecorder := httptest.NewRecorder()
	router.ServeHTTP(editRecorder, editRequest)
	if editRecorder.Code != http.StatusOK || len(images.editInputs) != 1 {
		t.Fatalf("edit route status=%d edits=%#v body=%s", editRecorder.Code, images.editInputs, editRecorder.Body.String())
	}
	wantURLs := []string{
		"https://example.com/scene.png",
		"https://example.com/shared.png",
		"https://example.com/character.png",
		"https://example.com/style.png",
	}
	input := images.editInputs[0]
	if input.PublicModel != "grok-imagine-image-edit" || input.RequestedModel != "grok-imagine-image" || input.EffectiveModel != "grok-imagine-image-edit" || !input.AutoRouted || !slices.Equal(input.ImageURLs, wantURLs) || input.Count != 1 || input.Size != "1536x1024" || input.AspectRatio != "16:9" || input.Resolution != "1k" || input.ResponseFormat != "url" || !input.Streaming || input.PartialImages != 2 {
		t.Fatalf("edit input = %#v", input)
	}
	b64Request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{
		"model":"grok-imagine-image-edit","prompt":"edit","image":"https://example.com/input.png","response_format":"b64_json"
	}`))
	b64Request.Header.Set("Content-Type", "application/json")
	b64Recorder := httptest.NewRecorder()
	router.ServeHTTP(b64Recorder, b64Request)
	if b64Recorder.Code != http.StatusOK || !strings.Contains(b64Recorder.Body.String(), `"b64_json":"aW1hZ2U="`) || len(images.editInputs) != 2 || images.editInputs[1].AutoRouted || images.editInputs[1].EffectiveModel != "grok-imagine-image-edit" {
		t.Fatalf("b64 edit status=%d inputs=%#v body=%s", b64Recorder.Code, images.editInputs, b64Recorder.Body.String())
	}

	missingReferences := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{
		"model":"grok-imagine-image-edit","prompt":"edit"
	}`))
	missingReferences.Header.Set("Content-Type", "application/json")
	missingRecorder := httptest.NewRecorder()
	router.ServeHTTP(missingRecorder, missingReferences)
	if missingRecorder.Code != http.StatusBadRequest || !strings.Contains(missingRecorder.Body.String(), "需要至少一张参考图") {
		t.Fatalf("missing references status=%d body=%s", missingRecorder.Code, missingRecorder.Body.String())
	}
}

func TestParseImageReferencesSupportsAllShapesAndStableDeduplication(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    []string
	}{
		{name: "image string", payload: `{"image":"https://example.com/a.png"}`, want: []string{"https://example.com/a.png"}},
		{name: "image object", payload: `{"image":{"url":"https://example.com/a.png"}}`, want: []string{"https://example.com/a.png"}},
		{name: "mixed images", payload: `{"images":["https://example.com/a.png",{"url":"https://example.com/b.png"}]}`, want: []string{"https://example.com/a.png", "https://example.com/b.png"}},
		{
			name: "field appearance order and deduplication",
			payload: `{
				"image_urls":["https://example.com/c.png","https://example.com/a.png"],
				"images":[{"url":"https://example.com/b.png"},"https://example.com/c.png"],
				"image":"https://example.com/a.png"
			}`,
			want: []string{"https://example.com/c.png", "https://example.com/a.png", "https://example.com/b.png"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseImageReferences([]byte(test.payload))
			if err != nil || !slices.Equal(got, test.want) {
				t.Fatalf("references=%#v error=%#v want=%#v", got, err, test.want)
			}
		})
	}

	if _, err := parseImageReferences([]byte(`{"image":{"file_id":"file_123"}}`)); err == nil || err.Code != "unsupported_parameter" {
		t.Fatalf("file_id error = %#v", err)
	}
}

func TestImageEditRouteAndUnavailableErrorRemainCompatible(t *testing.T) {
	images := &recordingImageGateway{}
	router := imageHandlerTestRouter(images)
	request := httptest.NewRequest(http.MethodPost, "/v1/images/edits", strings.NewReader(`{
		"model":"grok-imagine-image","prompt":"edit","image":"https://example.com/a.png"
	}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || len(images.editInputs) != 1 || images.editInputs[0].PublicModel != "grok-imagine-image-edit" || images.editInputs[0].RequestedModel != "grok-imagine-image" || images.editInputs[0].EffectiveModel != "grok-imagine-image-edit" || !images.editInputs[0].AutoRouted {
		t.Fatalf("explicit edit status=%d inputs=%#v body=%s", recorder.Code, images.editInputs, recorder.Body.String())
	}
	explicitRequest := httptest.NewRequest(http.MethodPost, "/v1/images/edits", strings.NewReader(`{
		"model":"grok-imagine-image-edit","prompt":"edit","image":{"url":"https://example.com/b.png"}
	}`))
	explicitRequest.Header.Set("Content-Type", "application/json")
	explicitRecorder := httptest.NewRecorder()
	router.ServeHTTP(explicitRecorder, explicitRequest)
	if explicitRecorder.Code != http.StatusOK || len(images.editInputs) != 2 || images.editInputs[1].PublicModel != "grok-imagine-image-edit" || images.editInputs[1].RequestedModel != "grok-imagine-image-edit" || images.editInputs[1].EffectiveModel != "grok-imagine-image-edit" || images.editInputs[1].AutoRouted {
		t.Fatalf("explicit edit model status=%d inputs=%#v body=%s", explicitRecorder.Code, images.editInputs, explicitRecorder.Body.String())
	}

	images.editErr = errors.New("missing image edit route")
	unavailable := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{
		"model":"grok-imagine-image","prompt":"edit","image":{"url":"https://example.com/a.png"}
	}`))
	unavailable.Header.Set("Content-Type", "application/json")
	unavailableRecorder := httptest.NewRecorder()
	router.ServeHTTP(unavailableRecorder, unavailable)
	if unavailableRecorder.Code != http.StatusBadGateway || !strings.Contains(unavailableRecorder.Body.String(), "upstream_unavailable") || len(images.generationInputs) != 0 {
		t.Fatalf("unavailable status=%d body=%s", unavailableRecorder.Code, unavailableRecorder.Body.String())
	}
}

func TestImageEditAcceptsOfficialJSONShape(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	NewHandler(nil, nil, 1<<20).Register(router.Group("/v1"))

	missingImage := httptest.NewRequest(http.MethodPost, "/v1/images/edits", strings.NewReader(`{
		"model":"grok-imagine-image-edit","prompt":"变成黑色 白字","n":1
	}`))
	missingImage.Header.Set("Content-Type", "application/json")
	missingRecorder := httptest.NewRecorder()
	router.ServeHTTP(missingRecorder, missingImage)
	if missingRecorder.Code != http.StatusBadRequest || !strings.Contains(missingRecorder.Body.String(), "image 或 images") {
		t.Fatalf("missing image status=%d body=%s", missingRecorder.Code, missingRecorder.Body.String())
	}

	validShape := httptest.NewRequest(http.MethodPost, "/v1/images/edits", strings.NewReader(`{
		"model":"grok-imagine-image-edit","prompt":"变成黑色 白字","n":1,"resolution":"1k",
		"image":{"url":"https://example.com/input.png"},"aspect_ratio":"1:1",
		"stream":true,"partial_images":1
	}`))
	validShape.Header.Set("Content-Type", "application/json")
	validRecorder := httptest.NewRecorder()
	router.ServeHTTP(validRecorder, validShape)
	if validRecorder.Code != http.StatusUnauthorized || strings.Contains(validRecorder.Body.String(), "multipart") {
		t.Fatalf("valid JSON shape status=%d body=%s", validRecorder.Code, validRecorder.Body.String())
	}

	invalidResolution := httptest.NewRequest(http.MethodPost, "/v1/images/edits", strings.NewReader(`{
		"model":"grok-imagine-image-edit","prompt":"test","resolution":"2k",
		"image":{"url":"https://example.com/input.png"}
	}`))
	invalidResolution.Header.Set("Content-Type", "application/json")
	invalidResolutionRecorder := httptest.NewRecorder()
	router.ServeHTTP(invalidResolutionRecorder, invalidResolution)
	if invalidResolutionRecorder.Code != http.StatusBadRequest || !strings.Contains(invalidResolutionRecorder.Body.String(), "仅支持 resolution=1k") {
		t.Fatalf("invalid resolution status=%d body=%s", invalidResolutionRecorder.Code, invalidResolutionRecorder.Body.String())
	}

	invalidCount := httptest.NewRequest(http.MethodPost, "/v1/images/edits", strings.NewReader(`{
		"model":"grok-imagine-image-edit","prompt":"test","n":2,
		"image":{"url":"https://example.com/input.png"}
	}`))
	invalidCount.Header.Set("Content-Type", "application/json")
	invalidCountRecorder := httptest.NewRecorder()
	router.ServeHTTP(invalidCountRecorder, invalidCount)
	if invalidCountRecorder.Code != http.StatusBadRequest || !strings.Contains(invalidCountRecorder.Body.String(), "仅支持 n=1") {
		t.Fatalf("invalid count status=%d body=%s", invalidCountRecorder.Code, invalidCountRecorder.Body.String())
	}

	for _, test := range []struct {
		name string
		body string
	}{
		{name: "negative partial images", body: `{"model":"grok-imagine-image-edit","prompt":"test","stream":true,"partial_images":-1,"image":{"url":"https://example.com/input.png"}}`},
		{name: "too many partial images", body: `{"model":"grok-imagine-image-edit","prompt":"test","stream":true,"partial_images":4,"image":{"url":"https://example.com/input.png"}}`},
		{name: "partial images require stream", body: `{"model":"grok-imagine-image-edit","prompt":"test","partial_images":1,"image":{"url":"https://example.com/input.png"}}`},
		{name: "invalid aspect ratio", body: `{"model":"grok-imagine-image-edit","prompt":"test","aspect_ratio":"7:5","image":{"url":"https://example.com/input.png"}}`},
		{name: "invalid size", body: `{"model":"grok-imagine-image-edit","prompt":"test","size":"512x512","image":{"url":"https://example.com/input.png"}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/images/edits", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}

	unsupportedRequest := httptest.NewRequest(http.MethodPost, "/v1/images/edits", strings.NewReader("ignored"))
	unsupportedRequest.Header.Set("Content-Type", "text/plain")
	unsupportedRecorder := httptest.NewRecorder()
	router.ServeHTTP(unsupportedRecorder, unsupportedRequest)
	if unsupportedRecorder.Code != http.StatusUnsupportedMediaType || !strings.Contains(unsupportedRecorder.Body.String(), "multipart/form-data") {
		t.Fatalf("unsupported status=%d body=%s", unsupportedRecorder.Code, unsupportedRecorder.Body.String())
	}
}

func TestImageEditAcceptsOpenAIMultipartFileUpload(t *testing.T) {
	images := &recordingImageGateway{}
	router := imageHandlerTestRouter(images)
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for field, value := range map[string]string{
		"model": "grok-imagine-image", "prompt": "保持角色，改为夜景", "n": "1",
		"size": "1536x1024", "aspect_ratio": "16:9", "resolution": "1k", "response_format": "b64_json",
	} {
		if err := writer.WriteField(field, value); err != nil {
			t.Fatal(err)
		}
	}
	part, err := writer.CreateFormFile("image", "character.png")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(testPNGBytes(t, 600, 900)); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteField("image_urls", "https://example.com/scene.png"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/images/edits", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"b64_json":"aW1hZ2U="`) {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if len(images.editInputs) != 1 {
		t.Fatalf("edit inputs=%#v", images.editInputs)
	}
	input := images.editInputs[0]
	if input.RequestedModel != "grok-imagine-image" || input.EffectiveModel != "grok-imagine-image-edit" || !input.AutoRouted || input.Count != 1 || input.Size != "1536x1024" || input.AspectRatio != "16:9" || input.Resolution != "1k" || input.ResponseFormat != "b64_json" {
		t.Fatalf("input=%#v", input)
	}
	if len(input.ImageURLs) != 2 || !strings.HasPrefix(input.ImageURLs[0], "data:image/png;base64,") || input.ImageURLs[1] != "https://example.com/scene.png" {
		t.Fatalf("image URLs=%#v", input.ImageURLs)
	}
}

func TestImageEditMultipartRejectsNonImageAndMask(t *testing.T) {
	for _, test := range []struct {
		name      string
		field     string
		filename  string
		content   []byte
		wantCode  string
		wantError string
	}{
		{name: "non image", field: "image", filename: "reference.txt", content: []byte("not an image"), wantCode: "invalid_request", wantError: "必须是图片"},
		{name: "mask", field: "mask", filename: "mask.png", content: testPNGBytes(t, 1, 1), wantCode: "unsupported_parameter", wantError: "不支持 mask"},
	} {
		t.Run(test.name, func(t *testing.T) {
			router := imageHandlerTestRouter(&recordingImageGateway{})
			var body bytes.Buffer
			writer := multipart.NewWriter(&body)
			_ = writer.WriteField("model", "grok-imagine-image")
			_ = writer.WriteField("prompt", "edit")
			part, err := writer.CreateFormFile(test.field, test.filename)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := part.Write(test.content); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/v1/images/edits", &body)
			request.Header.Set("Content-Type", writer.FormDataContentType())
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), test.wantCode) || !strings.Contains(recorder.Body.String(), test.wantError) {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestImageGenerationValidatesOpenAIPartialImages(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	NewHandler(nil, nil, 1<<20).Register(router.Group("/v1"))

	for _, test := range []struct {
		name string
		body string
	}{
		{name: "negative", body: `{"model":"grok-imagine-image-quality","prompt":"cat","stream":true,"partial_images":-1}`},
		{name: "too many", body: `{"model":"grok-imagine-image-quality","prompt":"cat","stream":true,"partial_images":4}`},
		{name: "requires stream", body: `{"model":"grok-imagine-image-quality","prompt":"cat","partial_images":1}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "partial_images") {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}

	invalidStreamingCount := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{
		"model":"grok-imagine-image-quality","prompt":"cat","n":2,"stream":true
	}`))
	invalidStreamingCount.Header.Set("Content-Type", "application/json")
	invalidStreamingCountRecorder := httptest.NewRecorder()
	router.ServeHTTP(invalidStreamingCountRecorder, invalidStreamingCount)
	if invalidStreamingCountRecorder.Code != http.StatusBadRequest {
		t.Fatalf("stream n status=%d body=%s", invalidStreamingCountRecorder.Code, invalidStreamingCountRecorder.Body.String())
	}
	var payload map[string]any
	if json.Unmarshal(invalidStreamingCountRecorder.Body.Bytes(), &payload) != nil {
		t.Fatalf("stream n body=%s", invalidStreamingCountRecorder.Body.String())
	}
	errorValue, _ := payload["error"].(map[string]any)
	if errorValue["message"] != "Streaming is only supported with n=1." || errorValue["type"] != "image_generation_user_error" || errorValue["param"] != "input" || errorValue["code"] != "unsupported_parameter" {
		t.Fatalf("stream n error=%#v", errorValue)
	}

	valid := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{
		"model":"grok-imagine-image-quality","prompt":"cat","n":1,"stream":true,"partial_images":1
	}`))
	valid.Header.Set("Content-Type", "application/json")
	validRecorder := httptest.NewRecorder()
	router.ServeHTTP(validRecorder, valid)
	if validRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("valid status=%d body=%s", validRecorder.Code, validRecorder.Body.String())
	}
}

func TestExtractUsageFromCompletedEvent(t *testing.T) {
	metadata := extractMetadata([]byte(`{"type":"response.completed","response":{"id":"resp_1","model":"grok-4.5-build-free","usage":{"input_tokens":10,"input_tokens_details":{"cached_tokens":4},"output_tokens":5,"output_tokens_details":{"reasoning_tokens":2},"total_tokens":15,"cost_in_usd_ticks":158500,"num_sources_used":1,"num_server_side_tools_used":2,"context_details":{"input_tokens":9,"output_tokens":4}}}}`))
	usage := metadata.Usage
	if usage.InputTokens != 10 || usage.OutputTokens != 5 || usage.TotalTokens != 15 {
		t.Fatalf("usage = %#v", usage)
	}
	if usage.CachedInputTokens != 4 || usage.ReasoningTokens != 2 || metadata.ResponseID != "resp_1" {
		t.Fatalf("metadata = %#v", metadata)
	}
	if usage.CostInUSDTicks != 158500 || usage.NumSourcesUsed != 1 || usage.NumServerSideToolsUsed != 2 || usage.ContextInputTokens != 9 || usage.ContextOutputTokens != 4 || usage.ResponseModel != "grok-4.5-build-free" {
		t.Fatalf("observed usage = %#v", usage)
	}
}

func TestExtractUsageFromAnthropicMessagesCaches(t *testing.T) {
	// Anthropic Messages 协议用 cache_read_input_tokens，不得再记为 0。
	metadata := extractMetadata([]byte(`{"id":"msg_1","type":"message","role":"assistant","model":"grok-4.5","usage":{"input_tokens":100,"output_tokens":20,"cache_creation_input_tokens":0,"cache_read_input_tokens":80,"cost_in_usd_ticks":1000}}`))
	if metadata.Usage.CachedInputTokens != 80 || metadata.Usage.InputTokens != 100 || metadata.Usage.OutputTokens != 20 {
		t.Fatalf("anthropic usage = %#v", metadata.Usage)
	}
}

func TestExtractUsageFromChatCompletionsCaches(t *testing.T) {
	// OpenAI Chat Completions 用 prompt_tokens_details.cached_tokens。
	metadata := extractMetadata([]byte(`{"id":"chatcmpl_1","object":"chat.completion","model":"grok-4.5","usage":{"prompt_tokens":50,"completion_tokens":10,"total_tokens":60,"prompt_tokens_details":{"cached_tokens":30},"completion_tokens_details":{"reasoning_tokens":5}}}`))
	if metadata.Usage.CachedInputTokens != 30 || metadata.Usage.InputTokens != 50 || metadata.Usage.OutputTokens != 10 || metadata.Usage.ReasoningTokens != 5 || metadata.Usage.TotalTokens != 60 {
		t.Fatalf("chat usage = %#v", metadata.Usage)
	}
}

func TestExtractUsagePrefersResponsesCachedTokensOverAnthropicField(t *testing.T) {
	// 同时存在时优先 Responses 字段（正常路径不会并存，防回归）。
	metadata := extractMetadata([]byte(`{"usage":{"input_tokens":10,"output_tokens":1,"input_tokens_details":{"cached_tokens":7},"cache_read_input_tokens":99}}`))
	if metadata.Usage.CachedInputTokens != 7 {
		t.Fatalf("prefer responses cached = %#v", metadata.Usage)
	}
}

func TestStreamInspectorMergesCachedTokensAcrossFrames(t *testing.T) {
	// 模拟流式：先到 input/output，后到带 cache 的 usage 帧。
	inspector := &responseInspector{protocol: streamProtocolAnthropic}
	inspector.Inspect([]byte("data: {\"type\":\"message_delta\",\"usage\":{\"input_tokens\":100,\"output_tokens\":20}}\n\n"))
	inspector.Inspect([]byte("data: {\"type\":\"message_delta\",\"usage\":{\"cache_read_input_tokens\":80}}\n\n"))
	inspector.Inspect([]byte("data: {\"type\":\"message_stop\"}\n\n"))
	inspector.Finish()
	usage := inspector.Metadata().Usage
	if usage.InputTokens != 100 || usage.OutputTokens != 20 || usage.CachedInputTokens != 80 {
		t.Fatalf("merged stream usage = %#v", usage)
	}
}

func TestStreamInspectorAcceptsChatCachedOnlyFrame(t *testing.T) {
	inspector := &responseInspector{protocol: streamProtocolChat}
	inspector.Inspect([]byte("data: {\"usage\":{\"prompt_tokens\":40,\"completion_tokens\":5,\"total_tokens\":45,\"prompt_tokens_details\":{\"cached_tokens\":25}}}\n\n"))
	inspector.Inspect([]byte("data: [DONE]\n\n"))
	inspector.Finish()
	usage := inspector.Metadata().Usage
	if usage.CachedInputTokens != 25 || usage.InputTokens != 40 || usage.TotalTokens != 45 {
		t.Fatalf("chat stream cached usage = %#v", usage)
	}
}

func TestUsageInspectorHandlesChunkedSSE(t *testing.T) {
	inspector := &responseInspector{}
	inspector.Inspect([]byte("data: {\"response\":{\"id\":\"resp_stream\",\"usage\":{\"input_tokens\":2,"))
	inspector.Inspect([]byte("\"output_tokens\":3}}}\n\n"))
	metadata := inspector.Metadata()
	usage := metadata.Usage
	if usage.TotalTokens != 5 {
		t.Fatalf("usage = %#v", usage)
	}
	if metadata.ResponseID != "resp_stream" {
		t.Fatalf("response ID = %q", metadata.ResponseID)
	}
}

func TestUsageInspectorHandlesFinalEventWithoutNewline(t *testing.T) {
	inspector := &responseInspector{}
	inspector.Inspect([]byte(`data: {"response":{"id":"resp_final","usage":{"input_tokens":7,"output_tokens":4}}}`))
	inspector.Finish()
	metadata := inspector.Metadata()
	if metadata.ResponseID != "resp_final" || metadata.Usage.TotalTokens != 11 {
		t.Fatalf("metadata = %#v", metadata)
	}
}

func TestCopyStreamRequiresProtocolTerminalEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name           string
		protocol       streamProtocol
		body           string
		wantErr        error
		wantDiagnostic bool
	}{
		{
			name: "responses completed", protocol: streamProtocolResponses,
			body: `data: {"type":"response.completed","response":{"id":"resp_ok","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}` + "\n\n",
		},
		{
			name: "responses eof before completed", protocol: streamProtocolResponses,
			body:    `data: {"type":"response.created","response":{"id":"resp_cut"}}` + "\n\n",
			wantErr: errUpstreamStreamIncomplete,
		},
		{
			name: "responses failed", protocol: streamProtocolResponses,
			body:    `data: {"type":"response.failed","response":{"id":"resp_failed","status":"failed","error":{"code":"upstream_error","message":"failed"},"output":[{"type":"reasoning","encrypted_content":"must-not-be-audited"}]}}` + "\n\n",
			wantErr: errUpstreamStreamFailed, wantDiagnostic: true,
		},
		{name: "chat done", protocol: streamProtocolChat, body: "data: [DONE]\n\n"},
		{name: "chat error", protocol: streamProtocolChat, body: `data: {"type":"error","error":{"code":"server_error","message":"chat failed"}}` + "\n\n", wantErr: errUpstreamStreamFailed, wantDiagnostic: true},
		{name: "anthropic stop", protocol: streamProtocolAnthropic, body: `data: {"type":"message_stop"}` + "\n\n"},
		{name: "anthropic error", protocol: streamProtocolAnthropic, body: `data: {"type":"error","error":{"type":"api_error","message":"messages failed"}}` + "\n\n", wantErr: errUpstreamStreamFailed, wantDiagnostic: true},
		{name: "image generation completed", protocol: streamProtocolImage, body: `data: {"type":"image_generation.completed"}` + "\n\n"},
		{name: "image edit completed", protocol: streamProtocolImage, body: `event: image_edit.completed` + "\n" + `data: {"type":"image_edit.completed"}` + "\n\n"},
		{name: "image edit failed", protocol: streamProtocolImage, body: `data: {"type":"image_edit.failed","error":{"code":"image_edit_failed","message":"edit failed"}}` + "\n\n", wantErr: errUpstreamStreamFailed, wantDiagnostic: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			context, _ := gin.CreateTestContext(recorder)
			metadata, err := copyStream(context.Writer, strings.NewReader(test.body), test.protocol)
			if test.wantErr == nil && err != nil {
				t.Fatal(err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %#v, want %v", err, test.wantErr)
			}
			if test.name == "responses completed" && (metadata.ResponseID != "resp_ok" || metadata.Usage.TotalTokens != 5) {
				t.Fatalf("metadata = %#v", metadata)
			}
			if test.wantDiagnostic {
				if metadata.StreamFailure == nil || !strings.Contains(string(metadata.StreamFailure.Body), "failed") || strings.Contains(string(metadata.StreamFailure.Body), "must-not-be-audited") {
					t.Fatalf("stream failure diagnostic = %#v", metadata.StreamFailure)
				}
			} else if metadata.StreamFailure != nil {
				t.Fatalf("unexpected stream failure diagnostic = %#v", metadata.StreamFailure)
			}
			if recorder.Body.String() != test.body {
				t.Fatalf("forwarded = %q", recorder.Body.String())
			}
		})
	}
}

func TestWriteResultRecordsStreamFailureDiagnostic(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewHandler(nil, nil, 1<<20)
	stream := `data: {"type":"response.failed","response":{"status":"failed","error":{"code":"server_error","message":"upstream failed"}}}` + "\n\n"
	var finalCode string
	var diagnostic *gateway.StreamFailureDiagnostic
	result := &gateway.Result{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(stream)),
		RecordStreamFailure: func(value gateway.StreamFailureDiagnostic) {
			diagnostic = &value
		},
		Finalize: func(_ gateway.Usage, _, code string) {
			finalCode = code
		},
	}
	router := gin.New()
	router.GET("/", func(c *gin.Context) {
		handler.writeResult(c, result, true, streamProtocolResponses)
	})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK || recorder.Body.String() != stream || finalCode != "upstream_stream_error" {
		t.Fatalf("status=%d body=%q final=%q", recorder.Code, recorder.Body.String(), finalCode)
	}
	if diagnostic == nil || !strings.Contains(string(diagnostic.Body), `"code":"server_error"`) {
		t.Fatalf("diagnostic = %#v", diagnostic)
	}
}

func TestProjectStreamFailureDiagnosticBoundsErrorMessage(t *testing.T) {
	diagnostic := projectStreamFailureDiagnostic([]byte(`{"type":"error","error":{"code":"server_error","message":"` + strings.Repeat("错误", maxStreamFailureDiagnosticBytes) + `"},"output":"must-not-be-audited"}`))
	if !diagnostic.BodyTruncated || len(diagnostic.Body) > maxStreamFailureDiagnosticBytes || len(diagnostic.Body) == 0 || !utf8.Valid(diagnostic.Body) || strings.Contains(string(diagnostic.Body), "must-not-be-audited") {
		t.Fatalf("diagnostic length=%d truncated=%v", len(diagnostic.Body), diagnostic.BodyTruncated)
	}
}

func TestExtractMetadataPreservesLargeCostTicks(t *testing.T) {
	metadata := extractMetadata([]byte(`{"id":"resp_cost","model":"grok-4.5","usage":{"input_tokens":1,"output_tokens":1,"cost_in_usd_ticks":9007199254740993}}`))
	if metadata.Usage.CostInUSDTicks != 9_007_199_254_740_993 {
		t.Fatalf("cost ticks = %d", metadata.Usage.CostInUSDTicks)
	}
}

func TestCopyHeadersFiltersHopByHopAndUpstreamCookies(t *testing.T) {
	source := http.Header{
		"Connection":          {"X-Upstream-Internal"},
		"Content-Type":        {"application/json"},
		"Set-Cookie":          {"upstream_session=secret"},
		"X-Request-Id":        {"req_123"},
		"X-Upstream-Internal": {"hidden"},
	}
	destination := make(http.Header)

	copyHeaders(destination, source)

	if destination.Get("Content-Type") != "application/json" || destination.Get("X-Request-Id") != "req_123" {
		t.Fatalf("forwarded headers = %#v", destination)
	}
	if destination.Get("Set-Cookie") != "" || destination.Get("X-Upstream-Internal") != "" || destination.Get("Connection") != "" {
		t.Fatalf("filtered headers leaked = %#v", destination)
	}
}

func TestCopyJSONForwardsBodyBeyondMetadataInspectionLimit(t *testing.T) {
	payload := make([]byte, 0, maxJSONMetadataInspectionBytes+1024)
	payload = append(payload, []byte(`{"padding":"`)...)
	payload = append(payload, bytes.Repeat([]byte("a"), maxJSONMetadataInspectionBytes)...)
	payload = append(payload, []byte(`"}`)...)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)

	metadata, err := copyJSON(context.Writer, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(recorder.Body.Bytes(), payload) {
		t.Fatalf("forwarded body size = %d, want %d", recorder.Body.Len(), len(payload))
	}
	if metadata.ResponseID != "" || metadata.Usage.TotalTokens != 0 {
		t.Fatalf("metadata should be skipped after inspection limit: %#v", metadata)
	}
}

func TestCopyMediaRejectsUnknownLengthOverflowWithoutWritingPastLimit(t *testing.T) {
	payload := bytes.Repeat([]byte("v"), 33)
	var destination bytes.Buffer
	err := copyMedia(&destination, bytes.NewReader(payload), 32)
	if !errors.Is(err, errResponseTransferLimit) {
		t.Fatalf("copy error = %v", err)
	}
	if destination.Len() != 32 {
		t.Fatalf("forwarded media size = %d", destination.Len())
	}
}

func TestCopyMediaAllowsExactLimit(t *testing.T) {
	payload := bytes.Repeat([]byte("v"), 32)
	var destination bytes.Buffer
	if err := copyMedia(&destination, bytes.NewReader(payload), 32); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(destination.Bytes(), payload) {
		t.Fatalf("forwarded media = %q", destination.Bytes())
	}
}

func TestSelectionErrorResponseDistinguishesCoolingAndSaturation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		name       string
		failure    *gateway.SelectionUnavailableError
		status     int
		code       string
		retryAfter string
	}{
		{name: "cooling", failure: &gateway.SelectionUnavailableError{Reason: gateway.SelectionCooling, RetryAfter: 1500 * time.Millisecond}, status: http.StatusTooManyRequests, code: "upstream_cooling", retryAfter: "2"},
		{name: "model cooling", failure: &gateway.SelectionUnavailableError{Reason: gateway.SelectionModelCooling, RetryAfter: time.Second}, status: http.StatusTooManyRequests, code: "upstream_model_cooling", retryAfter: "1"},
		{name: "saturated", failure: &gateway.SelectionUnavailableError{Reason: gateway.SelectionSaturated, RetryAfter: time.Second}, status: http.StatusServiceUnavailable, code: "upstream_saturated", retryAfter: "1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			context, _ := gin.CreateTestContext(recorder)
			status, code, _ := selectionErrorResponse(context, test.failure)
			if status != test.status || code != test.code || recorder.Header().Get("Retry-After") != test.retryAfter {
				t.Fatalf("status=%d code=%q retry-after=%q", status, code, recorder.Header().Get("Retry-After"))
			}
		})
	}
}
