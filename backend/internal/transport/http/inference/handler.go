package inference

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	clientkeyapp "github.com/chenyme/grok2api/backend/internal/application/clientkey"
	"github.com/chenyme/grok2api/backend/internal/application/gateway"
	mediaapp "github.com/chenyme/grok2api/backend/internal/application/media"
	modelapp "github.com/chenyme/grok2api/backend/internal/application/model"
	clientkeydomain "github.com/chenyme/grok2api/backend/internal/domain/clientkey"
	mediadomain "github.com/chenyme/grok2api/backend/internal/domain/media"
	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
	"github.com/chenyme/grok2api/backend/internal/transport/http/middleware"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	gateway          *gateway.Service
	images           imageGateway
	models           *modelapp.Service
	maxBodyBytes     int64
	publicAPIBaseURL string
	publicBaseURL    func() string
}

type imageGateway interface {
	GenerateImage(context.Context, gateway.ImageGenerationInput) (*gateway.Result, error)
	EditImage(context.Context, gateway.ImageEditInput) (*gateway.Result, error)
}

const (
	responseCopyBufferBytes          = 32 << 10
	openAIImageEditMultipartMemory   = 1 << 20
	openAIImageEditReferenceMaxBytes = int64(32 << 20)
	openAIVideoMultipartMemory       = 1 << 20
	openAIVideoReferenceMaxBytes     = int64(32 << 20)
	maxJSONMetadataInspectionBytes   = 8 << 20
	maxStreamEventInspectionBytes    = 8 << 20
	maxStreamFailureDiagnosticBytes  = 64 << 10
	maxCredentialErrorInspectBytes   = 64 << 10
	maxJSONResponseTransferBytes     = 128 << 20
	maxStreamResponseTransferBytes   = 256 << 20
	maxMediaResponseTransferBytes    = int64(2) << 30
	responseWriteTimeout             = 30 * time.Second
)

var (
	errResponseTransferLimit    = errors.New("响应超过代理安全上限")
	errUpstreamStreamIncomplete = errors.New("上游流在终止事件前结束")
	errUpstreamStreamFailed     = errors.New("上游流返回失败终止事件")
	errUpstreamStreamRead       = errors.New("读取上游流失败")
	errImageEditMaskUnsupported = errors.New("当前 Grok Web 图片编辑不支持 mask")
)

type streamProtocol uint8

const (
	streamProtocolResponses streamProtocol = iota
	streamProtocolChat
	streamProtocolAnthropic
	streamProtocolImage
)

const mediaTransferErrorTrailer = "X-Grok2API-Transfer-Error"

func NewHandler(gatewayService *gateway.Service, models *modelapp.Service, maxBodyBytes int64, publicAPIBaseURL ...string) *Handler {
	baseURL := ""
	if len(publicAPIBaseURL) > 0 {
		baseURL = strings.TrimRight(strings.TrimSpace(publicAPIBaseURL[0]), "/")
	}
	var images imageGateway
	if gatewayService != nil {
		images = gatewayService
	}
	return &Handler{gateway: gatewayService, images: images, models: models, maxBodyBytes: maxBodyBytes, publicAPIBaseURL: baseURL}
}

// SetPublicAPIBaseURLResolver 让视频内容 URL 跟随运行设置热更新。
// 应在 Register 前设置；请求处理期间只读取该函数。
func (h *Handler) SetPublicAPIBaseURLResolver(resolve func() string) *Handler {
	h.publicBaseURL = resolve
	return h
}

func (h *Handler) Register(router *gin.RouterGroup) {
	router.GET("/models", h.listModels)
	router.POST("/responses", h.createResponse)
	router.POST("/chat/completions", h.createChatCompletion)
	router.POST("/messages", h.createMessage)
	router.POST("/images/generations", h.generateImage)
	router.POST("/images/edits", h.editImage)
	router.POST("/videos", h.generateOpenAIVideo)
	router.POST("/videos/generations", h.generateVideo)
	router.GET("/videos/:requestId", h.getVideo)
	router.GET("/videos/:requestId/content", h.getVideoContent)
	router.POST("/responses/compact", h.compactResponse)
	router.GET("/responses/:responseId", h.getResponse)
	router.DELETE("/responses/:responseId", h.deleteResponse)
}

type responsesRequest struct {
	Model              string `json:"model"`
	Stream             bool   `json:"stream"`
	PromptCacheKey     string `json:"prompt_cache_key"`
	PreviousResponseID string `json:"previous_response_id"`
}

type chatCompletionRequest struct {
	Model          string `json:"model"`
	Stream         bool   `json:"stream"`
	PromptCacheKey string `json:"prompt_cache_key"`
}

type messagesRequest struct {
	Model          string          `json:"model"`
	MaxTokens      *int            `json:"max_tokens"`
	Messages       json.RawMessage `json:"messages"`
	Stream         bool            `json:"stream"`
	PromptCacheKey string          `json:"prompt_cache_key"`
}

type imageGenerationRequest struct {
	Model          string          `json:"model"`
	Prompt         string          `json:"prompt"`
	Count          *int            `json:"n"`
	PartialImages  *int            `json:"partial_images"`
	Size           string          `json:"size"`
	Quality        string          `json:"quality"`
	AspectRatio    string          `json:"aspect_ratio"`
	Resolution     string          `json:"resolution"`
	ResponseFormat string          `json:"response_format"`
	StorageOptions json.RawMessage `json:"storage_options"`
	Stream         bool            `json:"stream"`
}

type imageEditJSONImage struct {
	URL    string `json:"url"`
	FileID string `json:"file_id"`
}

type imageEditJSONRequest struct {
	Model          string          `json:"model"`
	Prompt         string          `json:"prompt"`
	Count          *int            `json:"n"`
	Size           string          `json:"size"`
	AspectRatio    string          `json:"aspect_ratio"`
	Resolution     string          `json:"resolution"`
	ResponseFormat string          `json:"response_format"`
	StorageOptions json.RawMessage `json:"storage_options"`
	Stream         bool            `json:"stream"`
	PartialImages  *int            `json:"partial_images"`
}

type videoGenerationImage struct {
	URL    string `json:"url"`
	FileID string `json:"file_id"`
	Data   []byte `json:"-"`
}

type videoGenerationRequest struct {
	Model           string                 `json:"model"`
	Prompt          string                 `json:"prompt"`
	User            *string                `json:"user"`
	Duration        json.RawMessage        `json:"duration"`
	AspectRatio     string                 `json:"aspect_ratio"`
	Resolution      string                 `json:"resolution"`
	Image           *videoGenerationImage  `json:"image"`
	ReferenceImages []videoGenerationImage `json:"reference_images"`
	Output          json.RawMessage        `json:"output"`
	StorageOptions  json.RawMessage        `json:"storage_options"`
	ResponseSize    string                 `json:"-"`
}

type openAIVideoGenerationRequest struct {
	Model           string                 `json:"model"`
	Prompt          string                 `json:"prompt"`
	User            *string                `json:"user"`
	Duration        json.RawMessage        `json:"duration"`
	Seconds         json.RawMessage        `json:"seconds"`
	Size            string                 `json:"size"`
	Quality         string                 `json:"quality"`
	AspectRatio     string                 `json:"aspect_ratio"`
	Resolution      string                 `json:"resolution"`
	Image           json.RawMessage        `json:"image"`
	Images          []string               `json:"images"`
	InputReference  string                 `json:"input_reference"`
	ReferenceImages []videoGenerationImage `json:"reference_images"`
	Output          json.RawMessage        `json:"output"`
	StorageOptions  json.RawMessage        `json:"storage_options"`
	ImageFile       *videoGenerationImage  `json:"-"`
	InputFile       *videoGenerationImage  `json:"-"`
}

type modelListItem struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

func (h *Handler) listModels(c *gin.Context) {
	values, err := h.models.ListEnabled(c.Request.Context())
	if err != nil {
		writeOpenAIError(c, http.StatusInternalServerError, "model_list_failed", "读取模型列表失败")
		return
	}
	c.JSON(http.StatusOK, gin.H{"object": "list", "data": newModelListItems(values)})
}

// newModelListItems 按下游公开名称去重，隐藏仅用于内部选路的 Provider 前缀。
func newModelListItems(values []modeldomain.Route) []modelListItem {
	data := make([]modelListItem, 0, len(values))
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		publicID := modeldomain.ExternalPublicID(value.Provider, value.PublicID)
		if seen[publicID] {
			continue
		}
		seen[publicID] = true
		data = append(data, modelListItem{ID: publicID, Object: "model", Created: value.CreatedAt.Unix(), OwnedBy: "grok2api"})
	}
	return data
}

func (h *Handler) createResponse(c *gin.Context) {
	h.handleCreate(c, false)
}

func (h *Handler) compactResponse(c *gin.Context) {
	h.handleCreate(c, true)
}

func (h *Handler) createChatCompletion(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, h.maxBodyBytes)
	if !isJSONRequest(c) {
		writeOpenAIError(c, http.StatusUnsupportedMediaType, "invalid_request", "Chat Completions only supports application/json")
		return
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		writeOpenAIError(c, http.StatusRequestEntityTooLarge, "request_too_large", "请求体超过限制")
		return
	}
	var request chatCompletionRequest
	if json.Unmarshal(body, &request) != nil || strings.TrimSpace(request.Model) == "" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "Chat Completions 请求缺少有效 model")
		return
	}
	clientValue, exists := c.Get(middleware.ClientKey)
	clientKey, ok := clientValue.(clientkeydomain.Key)
	if !exists || !ok {
		writeOpenAIError(c, http.StatusUnauthorized, "invalid_api_key", "客户端 API Key 无效")
		return
	}
	requestID, _ := c.Get(middleware.RequestIDKey)
	requestIDValue, _ := requestID.(string)
	result, err := h.gateway.CreateChatCompletion(c.Request.Context(), gateway.Input{
		RequestID: requestIDValue, ClientKey: clientKey, PublicModel: request.Model,
		Body: body, Streaming: request.Stream, PromptCacheKey: request.PromptCacheKey,
		PromptCacheSeed: extractPromptCacheSeed(c.Request.Header, body),
		GrokTurnIndex:   c.GetHeader("x-grok-turn-idx"),
	})
	if err != nil {
		writeGatewayError(c, err)
		return
	}
	h.writeResult(c, result, request.Stream, streamProtocolChat)
}

func (h *Handler) createMessage(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, h.maxBodyBytes)
	if !isJSONRequest(c) {
		writeAnthropicError(c, http.StatusUnsupportedMediaType, "invalid_request_error", "Messages only supports application/json")
		return
	}
	if strings.TrimSpace(c.GetHeader("anthropic-version")) == "" {
		writeAnthropicError(c, http.StatusBadRequest, "invalid_request_error", "anthropic-version header is required")
		return
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		writeAnthropicError(c, http.StatusRequestEntityTooLarge, "invalid_request_error", "request body exceeds the configured limit")
		return
	}
	var request messagesRequest
	if json.Unmarshal(body, &request) != nil || strings.TrimSpace(request.Model) == "" || request.MaxTokens == nil || *request.MaxTokens <= 0 || len(bytes.TrimSpace(request.Messages)) == 0 {
		writeAnthropicError(c, http.StatusBadRequest, "invalid_request_error", "model, max_tokens, and messages are required")
		return
	}
	clientValue, exists := c.Get(middleware.ClientKey)
	clientKey, ok := clientValue.(clientkeydomain.Key)
	if !exists || !ok {
		writeAnthropicError(c, http.StatusUnauthorized, "authentication_error", "invalid API key")
		return
	}
	requestID, _ := c.Get(middleware.RequestIDKey)
	requestIDValue, _ := requestID.(string)
	result, err := h.gateway.CreateMessage(c.Request.Context(), gateway.Input{
		RequestID: requestIDValue, ClientKey: clientKey, PublicModel: request.Model,
		Body: body, Streaming: request.Stream, PromptCacheKey: request.PromptCacheKey,
		PromptCacheSeed: extractPromptCacheSeed(c.Request.Header, body),
		GrokTurnIndex:   c.GetHeader("x-grok-turn-idx"),
	})
	if err != nil {
		writeGatewayAnthropicError(c, err)
		return
	}
	h.writeAnthropicResult(c, result, request.Stream)
}

func (h *Handler) generateImage(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, h.maxBodyBytes)
	if !isJSONRequest(c) {
		writeOpenAIError(c, http.StatusUnsupportedMediaType, "invalid_request", "图片生成仅支持 application/json")
		return
	}
	var request imageGenerationRequest
	payload, decodeErr := decodeImageJSONRequest(c.Request.Body, &request)
	if decodeErr != nil || strings.TrimSpace(request.Model) == "" || strings.TrimSpace(request.Prompt) == "" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "图片请求缺少有效 model 或 prompt")
		return
	}
	imageURLs, referenceErr := parseImageReferences(payload)
	if referenceErr != nil {
		writeOpenAIError(c, http.StatusBadRequest, referenceErr.Code, referenceErr.Message)
		return
	}
	route, routeErr := normalizeGrokImageRoute(grokImageGenerationsEndpoint, request.Model, imageURLs)
	if routeErr != nil {
		writeOpenAIError(c, http.StatusBadRequest, routeErr.Code, routeErr.Message)
		return
	}
	if value := bytes.TrimSpace(request.StorageOptions); len(value) > 0 && !bytes.Equal(value, []byte("null")) {
		writeOpenAIError(c, http.StatusBadRequest, "unsupported_parameter", "当前 Grok Web Provider 不支持 storage_options")
		return
	}
	resolution, err := resolveOpenAIImageResolution(request.Resolution, request.Quality)
	if err != nil {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", err.Error())
		return
	}
	count := 1
	if request.Count != nil {
		if *request.Count < 1 || *request.Count > 10 {
			writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "n 必须在 1 到 10 之间")
			return
		}
		count = *request.Count
	}
	if route.Capability == modeldomain.CapabilityImageEdit {
		options, validationErr := resolveImageEditOptions(request.Count, request.Size, request.AspectRatio, resolution, request.Stream, request.PartialImages)
		if validationErr != nil {
			writeOpenAIError(c, http.StatusBadRequest, validationErr.Code, validationErr.Message)
			return
		}
		clientKey, requestID, ok := requestIdentity(c)
		if !ok {
			return
		}
		result, err := h.images.EditImage(c.Request.Context(), gateway.ImageEditInput{
			RequestID: requestID, ClientKey: clientKey, PublicModel: route.EffectiveModel, Prompt: strings.TrimSpace(request.Prompt),
			RequestedModel: route.RequestedModel, EffectiveModel: route.EffectiveModel, AutoRouted: route.AutoRouted,
			ImageURLs: imageURLs, Count: options.Count, Size: options.Size, AspectRatio: options.AspectRatio,
			Resolution: options.Resolution, ResponseFormat: request.ResponseFormat,
			Streaming: request.Stream, PartialImages: options.PartialImages,
		})
		if err != nil {
			writeGatewayError(c, err)
			return
		}
		h.writeResult(c, result, request.Stream, streamProtocolImage)
		return
	}
	if request.Stream && count != 1 {
		writeImageGenerationUserError(c, "unsupported_parameter", "input", "Streaming is only supported with n=1.")
		return
	}
	partialImages := 0
	if request.PartialImages != nil {
		if *request.PartialImages < 0 || *request.PartialImages > 3 {
			writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "partial_images 必须在 0 到 3 之间")
			return
		}
		partialImages = *request.PartialImages
		if partialImages > 0 && !request.Stream {
			writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "partial_images 仅可在 stream=true 时使用")
			return
		}
	}
	clientKey, requestID, ok := requestIdentity(c)
	if !ok {
		return
	}
	result, err := h.images.GenerateImage(c.Request.Context(), gateway.ImageGenerationInput{
		RequestID: requestID, ClientKey: clientKey, PublicModel: route.EffectiveModel, Prompt: request.Prompt,
		RequestedModel: route.RequestedModel, EffectiveModel: route.EffectiveModel, AutoRouted: route.AutoRouted,
		Count: count, Size: request.Size, AspectRatio: request.AspectRatio,
		Resolution: resolution, ResponseFormat: request.ResponseFormat,
		Streaming: request.Stream, PartialImages: partialImages,
	})
	if err != nil {
		writeGatewayError(c, err)
		return
	}
	h.writeResult(c, result, request.Stream, streamProtocolImage)
}

type imageRequestError struct {
	Code    string
	Message string
}

type imageEditOptions struct {
	Count         int
	Size          string
	AspectRatio   string
	Resolution    string
	PartialImages int
}

type grokImageEndpoint uint8

const (
	grokImageGenerationsEndpoint grokImageEndpoint = iota
	grokImageEditsEndpoint
	grokImageGenerationModel = "grok-imagine-image"
	grokImageEditModel       = "grok-imagine-image-edit"
)

type grokImageRoute struct {
	RequestedModel string
	EffectiveModel string
	Capability     modeldomain.Capability
	HasReferences  bool
	AutoRouted     bool
}

func normalizeGrokImageRoute(endpoint grokImageEndpoint, requestedModel string, referenceImages []string) (grokImageRoute, *imageRequestError) {
	requestedModel = strings.TrimSpace(requestedModel)
	route := grokImageRoute{
		RequestedModel: requestedModel,
		EffectiveModel: requestedModel,
		HasReferences:  len(referenceImages) > 0,
	}
	if endpoint == grokImageEditsEndpoint || route.HasReferences {
		route.Capability = modeldomain.CapabilityImageEdit
		if requestedModel == grokImageGenerationModel {
			route.EffectiveModel = grokImageEditModel
			route.AutoRouted = true
		}
		return route, nil
	}
	route.Capability = modeldomain.CapabilityImage
	if requestedModel == grokImageEditModel {
		return grokImageRoute{}, &imageRequestError{Code: "invalid_request", Message: "grok-imagine-image-edit 需要至少一张参考图"}
	}
	return route, nil
}

func decodeImageJSONRequest(body io.Reader, destination any) ([]byte, error) {
	payload, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	if err := decodeSingleJSON(bytes.NewReader(payload), destination, false); err != nil {
		return nil, err
	}
	return payload, nil
}

func parseImageReferences(payload []byte) ([]string, *imageRequestError) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, &imageRequestError{Code: "invalid_request", Message: "图片请求 JSON 无效"}
	}
	values := make([]string, 0, 8)
	seen := make(map[string]struct{}, 8)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, &imageRequestError{Code: "invalid_request", Message: "图片请求 JSON 无效"}
		}
		key, _ := keyToken.(string)
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, &imageRequestError{Code: "invalid_request", Message: "图片请求 JSON 无效"}
		}
		if key != "image" && key != "images" && key != "image_urls" {
			continue
		}
		fieldValues, parseErr := parseImageReferenceField(raw, key != "image")
		if parseErr != nil {
			return nil, parseErr
		}
		for _, value := range fieldValues {
			if _, exists := seen[value]; exists {
				continue
			}
			seen[value] = struct{}{}
			values = append(values, value)
		}
	}
	if len(values) > 8 {
		return nil, &imageRequestError{Code: "invalid_request", Message: "image、images 和 image_urls 去重后数量必须在 1 到 8 之间"}
	}
	return values, nil
}

func parseImageReferenceField(raw json.RawMessage, plural bool) ([]string, *imageRequestError) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	if !plural {
		value, err := parseImageReferenceValue(raw)
		if err != nil {
			return nil, err
		}
		return []string{value}, nil
	}
	var items []json.RawMessage
	if json.Unmarshal(raw, &items) != nil {
		return nil, &imageRequestError{Code: "invalid_request", Message: "images 和 image_urls 必须是参考图数组"}
	}
	values := make([]string, 0, len(items))
	for _, item := range items {
		value, err := parseImageReferenceValue(item)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func parseImageReferenceValue(raw json.RawMessage) (string, *imageRequestError) {
	var urlValue string
	if json.Unmarshal(raw, &urlValue) != nil {
		var value imageEditJSONImage
		if json.Unmarshal(raw, &value) != nil {
			return "", &imageRequestError{Code: "invalid_request", Message: "参考图必须是 URL 字符串或包含 url 的对象"}
		}
		if strings.TrimSpace(value.FileID) != "" {
			return "", &imageRequestError{Code: "unsupported_parameter", Message: "当前暂不支持 image.file_id，请使用 image.url"}
		}
		urlValue = value.URL
	}
	urlValue = strings.TrimSpace(urlValue)
	if urlValue == "" {
		return "", &imageRequestError{Code: "invalid_request", Message: "每个参考图都必须提供有效 url"}
	}
	return urlValue, nil
}

func resolveImageEditOptions(countValue *int, sizeValue, aspectRatioValue, resolutionValue string, stream bool, partialImagesValue *int) (imageEditOptions, *imageRequestError) {
	options := imageEditOptions{
		Count:       1,
		Size:        strings.ToLower(strings.TrimSpace(sizeValue)),
		AspectRatio: strings.ToLower(strings.TrimSpace(aspectRatioValue)),
		Resolution:  strings.ToLower(strings.TrimSpace(resolutionValue)),
	}
	if countValue != nil {
		options.Count = *countValue
	}
	if options.Count != 1 {
		return imageEditOptions{}, &imageRequestError{Code: "invalid_parameter", Message: "Grok Web 图片编辑当前仅支持 n=1"}
	}
	if partialImagesValue != nil {
		options.PartialImages = *partialImagesValue
		if options.PartialImages < 0 || options.PartialImages > 3 {
			return imageEditOptions{}, &imageRequestError{Code: "invalid_parameter", Message: "partial_images 必须在 0 到 3 之间"}
		}
		if options.PartialImages > 0 && !stream {
			return imageEditOptions{}, &imageRequestError{Code: "invalid_parameter", Message: "partial_images 仅可在 stream=true 时使用"}
		}
	}
	if options.AspectRatio != "" && !validImageAspectRatio(options.AspectRatio) {
		return imageEditOptions{}, &imageRequestError{Code: "invalid_parameter", Message: "aspect_ratio 不受支持"}
	}
	if options.Size != "" && !validImageEditSize(options.Size) {
		return imageEditOptions{}, &imageRequestError{Code: "invalid_parameter", Message: "size 必须是 auto、1024x1024、1024x1536 或 1536x1024"}
	}
	if options.Resolution == "" {
		options.Resolution = "1k"
	}
	if options.Resolution != "1k" {
		return imageEditOptions{}, &imageRequestError{Code: "invalid_parameter", Message: "Grok Web 图片编辑当前仅支持 resolution=1k"}
	}
	return options, nil
}

func resolveOpenAIImageResolution(resolution, quality string) (string, error) {
	resolution = strings.ToLower(strings.TrimSpace(resolution))
	if resolution != "" {
		return resolution, nil
	}
	switch strings.ToLower(strings.TrimSpace(quality)) {
	case "":
		return "", nil
	case "auto", "standard", "medium", "low":
		return "1k", nil
	case "high", "hd":
		return "2k", nil
	default:
		return "", fmt.Errorf("quality 必须是 auto、standard、low、medium、high 或 hd")
	}
}

func (h *Handler) writeMediaResult(c *gin.Context, result *gateway.Result) {
	errorCode := ""
	defer result.Body.Close()
	defer func() { result.Finalize(gateway.Usage{}, "", errorCode) }()
	if isUpstreamCredentialStatus(result.StatusCode) {
		errorCode = "upstream_unavailable"
		clientCode := readCredentialErrorCode(result.StatusCode, result.Body)
		writeOpenAIError(c, http.StatusServiceUnavailable, clientCode, credentialErrorMessage(clientCode))
		return
	}
	contentLength, contentLengthErr := strconv.ParseInt(result.Header.Get("Content-Length"), 10, 64)
	if contentLengthErr == nil && contentLength > maxMediaResponseTransferBytes {
		errorCode = "response_too_large"
		writeOpenAIError(c, http.StatusBadGateway, "media_too_large", "上游媒体超过 2 GiB 安全上限")
		return
	}
	copyHeaders(c.Writer.Header(), result.Header)
	if contentLengthErr == nil && contentLength >= 0 {
		c.Header("Content-Length", strconv.FormatInt(contentLength, 10))
	} else {
		c.Header("Trailer", mediaTransferErrorTrailer)
	}
	c.Status(result.StatusCode)
	if err := copyMedia(responseDeadlineWriter{ResponseWriter: c.Writer}, result.Body, maxMediaResponseTransferBytes); err != nil {
		if errors.Is(err, errResponseTransferLimit) {
			errorCode = "response_too_large"
		} else {
			errorCode = "stream_interrupted"
		}
		if contentLengthErr != nil {
			c.Header(mediaTransferErrorTrailer, errorCode)
		}
	}
}

type responseDeadlineWriter struct{ http.ResponseWriter }

func (w responseDeadlineWriter) Write(payload []byte) (int, error) {
	if err := setResponseWriteDeadline(w.ResponseWriter); err != nil {
		return 0, err
	}
	return w.ResponseWriter.Write(payload)
}

func setResponseWriteDeadline(writer http.ResponseWriter) error {
	err := http.NewResponseController(writer).SetWriteDeadline(time.Now().Add(responseWriteTimeout))
	if errors.Is(err, http.ErrNotSupported) {
		return nil
	}
	return err
}

func copyMedia(writer io.Writer, source io.Reader, limit int64) error {
	buffer := make([]byte, 64<<10)
	var transferred int64
	for {
		n, readErr := source.Read(buffer)
		if n > 0 {
			remaining := limit - transferred
			if remaining <= 0 {
				return errResponseTransferLimit
			}
			writeSize := n
			if int64(writeSize) > remaining {
				writeSize = int(remaining)
			}
			written, writeErr := writer.Write(buffer[:writeSize])
			transferred += int64(written)
			if writeErr != nil {
				return writeErr
			}
			if written != writeSize {
				return io.ErrShortWrite
			}
			if writeSize != n {
				return errResponseTransferLimit
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}

func (h *Handler) editImage(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, h.maxBodyBytes)
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || (!strings.EqualFold(mediaType, "application/json") && !strings.EqualFold(mediaType, "multipart/form-data")) {
		writeOpenAIError(c, http.StatusUnsupportedMediaType, "invalid_request", "图片编辑仅支持 application/json 或 multipart/form-data")
		return
	}
	var request imageEditJSONRequest
	var imageURLs []string
	if strings.EqualFold(mediaType, "multipart/form-data") {
		request, imageURLs, err = parseImageEditMultipart(c, h.maxBodyBytes)
	} else {
		var payload []byte
		payload, err = decodeImageJSONRequest(c.Request.Body, &request)
		if err == nil {
			var referenceErr *imageRequestError
			imageURLs, referenceErr = parseImageReferences(payload)
			if referenceErr != nil {
				writeOpenAIError(c, http.StatusBadRequest, referenceErr.Code, referenceErr.Message)
				return
			}
		}
	}
	if err != nil {
		var maxBytesError *http.MaxBytesError
		switch {
		case errors.As(err, &maxBytesError):
			writeOpenAIError(c, http.StatusRequestEntityTooLarge, "request_too_large", "图片编辑参考图超过请求大小限制")
		case errors.Is(err, errImageEditMaskUnsupported):
			writeOpenAIError(c, http.StatusBadRequest, "unsupported_parameter", err.Error())
		default:
			writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "图片编辑请求无效: "+err.Error())
		}
		return
	}
	if value := bytes.TrimSpace(request.StorageOptions); len(value) > 0 && !bytes.Equal(value, []byte("null")) {
		writeOpenAIError(c, http.StatusBadRequest, "unsupported_parameter", "当前 Grok Web Provider 不支持 storage_options")
		return
	}
	model := strings.TrimSpace(request.Model)
	prompt := strings.TrimSpace(request.Prompt)
	if len(imageURLs) == 0 {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "image 或 images（以及 image_urls）数量必须在 1 到 8 之间")
		return
	}
	route, routeErr := normalizeGrokImageRoute(grokImageEditsEndpoint, model, imageURLs)
	if routeErr != nil {
		writeOpenAIError(c, http.StatusBadRequest, routeErr.Code, routeErr.Message)
		return
	}
	if model == "" || prompt == "" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "图片编辑缺少有效 model 或 prompt")
		return
	}
	options, validationErr := resolveImageEditOptions(request.Count, request.Size, request.AspectRatio, request.Resolution, request.Stream, request.PartialImages)
	if validationErr != nil {
		writeOpenAIError(c, http.StatusBadRequest, validationErr.Code, validationErr.Message)
		return
	}
	clientKey, requestID, ok := requestIdentity(c)
	if !ok {
		return
	}
	result, err := h.images.EditImage(c.Request.Context(), gateway.ImageEditInput{
		RequestID: requestID, ClientKey: clientKey, PublicModel: route.EffectiveModel, Prompt: prompt,
		RequestedModel: route.RequestedModel, EffectiveModel: route.EffectiveModel, AutoRouted: route.AutoRouted,
		ImageURLs: imageURLs, Count: options.Count, Size: options.Size, AspectRatio: options.AspectRatio,
		Resolution: options.Resolution, ResponseFormat: request.ResponseFormat,
		Streaming: request.Stream, PartialImages: options.PartialImages,
	})
	if err != nil {
		writeGatewayError(c, err)
		return
	}
	h.writeResult(c, result, request.Stream, streamProtocolImage)
}

func parseImageEditMultipart(c *gin.Context, maxBodyBytes int64) (imageEditJSONRequest, []string, error) {
	memoryBytes := min(maxBodyBytes, int64(openAIImageEditMultipartMemory))
	if err := c.Request.ParseMultipartForm(memoryBytes); err != nil {
		return imageEditJSONRequest{}, nil, err
	}
	form := c.Request.MultipartForm
	defer form.RemoveAll()
	if len(form.File["mask"]) > 0 || strings.TrimSpace(firstMultipartValue(form, "mask")) != "" {
		return imageEditJSONRequest{}, nil, errImageEditMaskUnsupported
	}
	request := imageEditJSONRequest{
		Model: strings.TrimSpace(firstMultipartValue(form, "model")), Prompt: strings.TrimSpace(firstMultipartValue(form, "prompt")),
		Size: strings.TrimSpace(firstMultipartValue(form, "size")), AspectRatio: strings.TrimSpace(firstMultipartValue(form, "aspect_ratio")),
		Resolution: strings.TrimSpace(firstMultipartValue(form, "resolution")), ResponseFormat: strings.TrimSpace(firstMultipartValue(form, "response_format")),
	}
	var err error
	if request.Count, err = optionalMultipartInt(form, "n"); err != nil {
		return imageEditJSONRequest{}, nil, err
	}
	if request.PartialImages, err = optionalMultipartInt(form, "partial_images"); err != nil {
		return imageEditJSONRequest{}, nil, err
	}
	if request.Stream, err = optionalMultipartBool(form, "stream"); err != nil {
		return imageEditJSONRequest{}, nil, err
	}
	if value := strings.TrimSpace(firstMultipartValue(form, "storage_options")); value != "" {
		request.StorageOptions = json.RawMessage(value)
	}

	references := make([]string, 0, 8)
	seen := make(map[string]struct{}, 8)
	appendReference := func(value string) error {
		value = strings.TrimSpace(value)
		if value == "" {
			return errors.New("参考图字段为空")
		}
		if _, exists := seen[value]; exists {
			return nil
		}
		if len(references) >= 8 {
			return errors.New("image 或 images（以及 image_urls）数量必须在 1 到 8 之间")
		}
		seen[value] = struct{}{}
		references = append(references, value)
		return nil
	}
	for _, field := range []string{"image", "image[]", "images", "images[]"} {
		for _, value := range form.Value[field] {
			if err := appendReference(value); err != nil {
				return imageEditJSONRequest{}, nil, err
			}
		}
		for _, header := range form.File[field] {
			value, err := multipartImageDataURI(header, maxBodyBytes)
			if err != nil {
				return imageEditJSONRequest{}, nil, err
			}
			if err := appendReference(value); err != nil {
				return imageEditJSONRequest{}, nil, err
			}
		}
	}
	for _, field := range []string{"image_urls", "image_urls[]"} {
		for _, value := range form.Value[field] {
			if err := appendReference(value); err != nil {
				return imageEditJSONRequest{}, nil, err
			}
		}
	}
	return request, references, nil
}

func firstMultipartValue(form *multipart.Form, field string) string {
	if values := form.Value[field]; len(values) > 0 {
		return values[0]
	}
	return ""
}

func optionalMultipartInt(form *multipart.Form, field string) (*int, error) {
	value := strings.TrimSpace(firstMultipartValue(form, field))
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return nil, fmt.Errorf("%s 必须是整数", field)
	}
	return &parsed, nil
}

func optionalMultipartBool(form *multipart.Form, field string) (bool, error) {
	value := strings.TrimSpace(firstMultipartValue(form, field))
	if value == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s 必须是布尔值", field)
	}
	return parsed, nil
}

func multipartImageDataURI(header *multipart.FileHeader, maxBodyBytes int64) (string, error) {
	file, err := header.Open()
	if err != nil {
		return "", err
	}
	defer file.Close()
	fileLimit := min(maxBodyBytes, openAIImageEditReferenceMaxBytes)
	raw, err := io.ReadAll(io.LimitReader(file, fileLimit+1))
	if err != nil {
		return "", err
	}
	if int64(len(raw)) > fileLimit {
		return "", &http.MaxBytesError{Limit: fileLimit}
	}
	mimeType := strings.TrimSpace(strings.Split(http.DetectContentType(raw), ";")[0])
	if !strings.HasPrefix(strings.ToLower(mimeType), "image/") {
		return "", errors.New("上传文件必须是图片")
	}
	switch strings.ToLower(mimeType) {
	case "image/jpeg", "image/png", "image/webp", "image/gif":
	default:
		return "", errors.New("图片格式必须是 JPEG、PNG、WebP 或 GIF")
	}
	return "data:" + mimeType + ";base64," + base64.StdEncoding.EncodeToString(raw), nil
}

func requestIdentity(c *gin.Context) (clientkeydomain.Key, string, bool) {
	clientValue, exists := c.Get(middleware.ClientKey)
	clientKey, ok := clientValue.(clientkeydomain.Key)
	if !exists || !ok {
		writeOpenAIError(c, http.StatusUnauthorized, "invalid_api_key", "客户端 API Key 无效")
		return clientkeydomain.Key{}, "", false
	}
	requestID, _ := c.Get(middleware.RequestIDKey)
	requestIDValue, _ := requestID.(string)
	return clientKey, requestIDValue, true
}

func (h *Handler) generateVideo(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, h.maxBodyBytes)
	if !isJSONRequest(c) {
		writeOpenAIError(c, http.StatusUnsupportedMediaType, "invalid_request", "视频生成仅支持 application/json")
		return
	}
	var request videoGenerationRequest
	if err := decodeSingleJSON(c.Request.Body, &request, true); err != nil {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "视频生成 JSON 请求无效: "+err.Error())
		return
	}
	h.createVideo(c, request, false)
}

func (h *Handler) generateOpenAIVideo(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, h.maxBodyBytes)
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || (!strings.EqualFold(mediaType, "application/json") && !strings.EqualFold(mediaType, "multipart/form-data")) {
		writeOpenAIError(c, http.StatusUnsupportedMediaType, "invalid_request", "OpenAI 兼容视频生成仅支持 application/json 或 multipart/form-data")
		return
	}
	var request openAIVideoGenerationRequest
	if strings.EqualFold(mediaType, "multipart/form-data") {
		request, err = parseOpenAIVideoMultipart(c, h.maxBodyBytes)
	} else {
		err = decodeSingleJSON(c.Request.Body, &request, false)
	}
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeOpenAIError(c, http.StatusRequestEntityTooLarge, "request_too_large", "视频参考图片超过请求大小限制")
			return
		}
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "视频生成请求无效: "+err.Error())
		return
	}
	normalized, err := normalizeOpenAIVideoRequest(request)
	if err != nil {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	h.createVideo(c, normalized, true)
}

func parseOpenAIVideoMultipart(c *gin.Context, maxBodyBytes int64) (openAIVideoGenerationRequest, error) {
	memoryBytes := min(maxBodyBytes, int64(openAIVideoMultipartMemory))
	if err := c.Request.ParseMultipartForm(memoryBytes); err != nil {
		return openAIVideoGenerationRequest{}, err
	}
	form := c.Request.MultipartForm
	defer form.RemoveAll()
	request := openAIVideoGenerationRequest{
		Model: c.PostForm("model"), Prompt: c.PostForm("prompt"), Size: c.PostForm("size"),
		Quality: c.PostForm("quality"), AspectRatio: c.PostForm("aspect_ratio"), Resolution: c.PostForm("resolution"),
	}
	if value := strings.TrimSpace(c.PostForm("seconds")); value != "" {
		request.Seconds = json.RawMessage(strconv.Quote(value))
	}
	if value := strings.TrimSpace(c.PostForm("duration")); value != "" {
		request.Duration = json.RawMessage(strconv.Quote(value))
	}
	if value := strings.TrimSpace(c.PostForm("input_reference")); value != "" {
		request.InputReference = value
	}
	if values := form.Value["images"]; len(values) > 0 {
		request.Images = append([]string(nil), values...)
	}
	for _, field := range []string{"image", "input_reference"} {
		files := form.File[field]
		if len(files) == 0 {
			continue
		}
		file, err := files[0].Open()
		if err != nil {
			return openAIVideoGenerationRequest{}, err
		}
		fileLimit := min(maxBodyBytes, openAIVideoReferenceMaxBytes)
		raw, readErr := io.ReadAll(io.LimitReader(file, fileLimit+1))
		_ = file.Close()
		if readErr != nil || int64(len(raw)) > fileLimit {
			return openAIVideoGenerationRequest{}, &http.MaxBytesError{Limit: fileLimit}
		}
		mimeType := strings.TrimSpace(strings.Split(http.DetectContentType(raw), ";")[0])
		if !strings.HasPrefix(strings.ToLower(mimeType), "image/") {
			return openAIVideoGenerationRequest{}, fmt.Errorf("input_reference 必须是图片")
		}
		if field == "image" {
			request.ImageFile = &videoGenerationImage{Data: raw}
		} else {
			request.InputFile = &videoGenerationImage{Data: raw}
		}
	}
	return request, nil
}

func (h *Handler) createVideo(c *gin.Context, request videoGenerationRequest, openAICompatible bool) {
	if hasJSONValue(request.Output) {
		writeOpenAIError(c, http.StatusBadRequest, "unsupported_parameter", "当前 Grok Web Provider 不支持 output.upload_url")
		return
	}
	if hasJSONValue(request.StorageOptions) {
		writeOpenAIError(c, http.StatusBadRequest, "unsupported_parameter", "当前 Grok Web Provider 不支持 storage_options")
		return
	}
	duration, err := parseVideoDuration(request.Duration)
	if err != nil {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	model := strings.TrimSpace(request.Model)
	prompt := strings.TrimSpace(request.Prompt)
	if model == "" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "视频生成缺少有效 model")
		return
	}
	aspectRatio := strings.TrimSpace(request.AspectRatio)
	if aspectRatio == "" {
		aspectRatio = "16:9"
	}
	if !validVideoAspectRatio(aspectRatio) {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "aspect_ratio 必须是 1:1、16:9、9:16、4:3、3:4、3:2 或 2:3")
		return
	}
	resolution := strings.ToLower(strings.TrimSpace(request.Resolution))
	if resolution == "" {
		resolution = "720p"
	}
	if resolution != "480p" && resolution != "720p" && resolution != "1080p" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "resolution 必须是 480p、720p 或 1080p")
		return
	}
	inputs := append([]videoGenerationImage(nil), request.ReferenceImages...)
	if request.Image != nil {
		inputs = append([]videoGenerationImage{*request.Image}, inputs...)
	}
	if len(inputs) > 8 {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "image 与 reference_images 合计不能超过 8 张")
		return
	}
	references := make([]gateway.VideoReference, 0, len(inputs))
	for _, input := range inputs {
		if strings.TrimSpace(input.FileID) != "" {
			writeOpenAIError(c, http.StatusBadRequest, "unsupported_parameter", "当前暂不支持 image.file_id，请使用 image.url")
			return
		}
		urlValue := strings.TrimSpace(input.URL)
		switch {
		case urlValue != "" && len(input.Data) != 0:
			writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "每个 image 只能提供 url 或文件内容")
			return
		case urlValue != "":
			references = append(references, gateway.VideoReference{URL: urlValue})
		case len(input.Data) != 0:
			references = append(references, gateway.VideoReference{Data: input.Data})
		default:
			writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "每个 image 都必须提供有效 url")
			return
		}
	}
	if prompt == "" && len(references) == 0 {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "文本生视频必须提供 prompt；图片生视频可以省略 prompt")
		return
	}
	clientKey, requestID, ok := requestIdentity(c)
	if !ok {
		return
	}
	job, err := h.gateway.CreateVideo(c.Request.Context(), gateway.VideoInput{
		RequestID: requestID, ClientKey: clientKey, PublicModel: model,
		Prompt: prompt, Duration: duration, AspectRatio: aspectRatio, Resolution: resolution,
		References: references, OpenAICompatible: openAICompatible, ResponseSize: request.ResponseSize,
	})
	if err != nil {
		writeGatewayError(c, err)
		return
	}
	if openAICompatible {
		c.JSON(http.StatusOK, openAIVideoGenerationResponse(job))
		return
	}
	c.JSON(http.StatusOK, gin.H{"request_id": job.ID})
}

func normalizeOpenAIVideoRequest(request openAIVideoGenerationRequest) (videoGenerationRequest, error) {
	duration := request.Duration
	if !hasJSONValue(duration) {
		duration = request.Seconds
	}
	if !hasJSONValue(duration) {
		duration = json.RawMessage(`4`)
	}
	responseSize := strings.ToLower(strings.TrimSpace(request.Size))
	aspectRatio, resolution, err := resolveOpenAIVideoSize(request.AspectRatio, request.Resolution, responseSize, request.Quality)
	if err != nil {
		return videoGenerationRequest{}, err
	}
	if responseSize == "" {
		responseSize = openAIVideoResponseSize(aspectRatio, resolution)
	}
	image, err := parseOpenAIVideoImage(request.Image)
	if err != nil {
		return videoGenerationRequest{}, err
	}
	if request.ImageFile != nil {
		image = request.ImageFile
	}
	references := append([]videoGenerationImage(nil), request.ReferenceImages...)
	if request.InputFile != nil {
		references = append(references, *request.InputFile)
	} else if value := strings.TrimSpace(request.InputReference); value != "" {
		references = append(references, videoGenerationImage{URL: value})
	}
	for _, value := range request.Images {
		if value = strings.TrimSpace(value); value != "" {
			references = append(references, videoGenerationImage{URL: value})
		}
	}
	return videoGenerationRequest{
		Model: request.Model, Prompt: request.Prompt, User: request.User, Duration: duration,
		AspectRatio: aspectRatio, Resolution: resolution, Image: image, ReferenceImages: references,
		Output: request.Output, StorageOptions: request.StorageOptions, ResponseSize: responseSize,
	}, nil
}

func openAIVideoResponseSize(aspectRatio, resolution string) string {
	aspectRatio = strings.ToLower(strings.TrimSpace(aspectRatio))
	resolution = strings.ToLower(strings.TrimSpace(resolution))
	if resolution == "" {
		resolution = "720p"
	}
	if value := map[string]string{
		"16:9/480p": "854x480", "9:16/480p": "480x854",
		"16:9/720p": "1280x720", "9:16/720p": "720x1280",
		"16:9/1080p": "1920x1080", "9:16/1080p": "1080x1920",
		"1:1/480p": "480x480", "1:1/720p": "720x720", "1:1/1080p": "1024x1024",
	}[aspectRatio+"/"+resolution]; value != "" {
		return value
	}
	return aspectRatio
}

func parseOpenAIVideoImage(raw json.RawMessage) (*videoGenerationImage, error) {
	if !hasJSONValue(raw) {
		return nil, nil
	}
	var value string
	if json.Unmarshal(raw, &value) == nil {
		if value = strings.TrimSpace(value); value != "" {
			return &videoGenerationImage{URL: value}, nil
		}
		return nil, nil
	}
	var image videoGenerationImage
	if json.Unmarshal(raw, &image) != nil || (strings.TrimSpace(image.URL) == "" && strings.TrimSpace(image.FileID) == "") {
		return nil, fmt.Errorf("image 必须是图片 URL 或包含 url 的对象")
	}
	return &image, nil
}

func resolveOpenAIVideoSize(aspectRatio, resolution, size, quality string) (string, string, error) {
	aspectRatio = strings.ToLower(strings.TrimSpace(aspectRatio))
	resolution = strings.ToLower(strings.TrimSpace(resolution))
	quality = strings.ToLower(strings.TrimSpace(quality))
	if resolution == "" && quality != "" {
		resolution = quality
	}
	if size = strings.ToLower(strings.TrimSpace(size)); size != "" {
		type videoSpec struct{ aspectRatio, resolution string }
		specs := map[string]videoSpec{
			"1:1": {"1:1", ""}, "16:9": {"16:9", ""}, "9:16": {"9:16", ""}, "4:3": {"4:3", ""}, "3:4": {"3:4", ""}, "3:2": {"3:2", ""}, "2:3": {"2:3", ""},
			"854x480": {"16:9", "480p"}, "480x854": {"9:16", "480p"},
			"1280x720": {"16:9", "720p"}, "720x1280": {"9:16", "720p"},
			"1792x1024": {"16:9", "1080p"}, "1024x1792": {"9:16", "1080p"},
			"1920x1080": {"16:9", "1080p"}, "1080x1920": {"9:16", "1080p"}, "1024x1024": {"1:1", "1080p"},
		}
		spec, ok := specs[size]
		if !ok {
			return "", "", fmt.Errorf("size 不受支持")
		}
		if aspectRatio == "" {
			aspectRatio = spec.aspectRatio
		}
		if resolution == "" {
			resolution = spec.resolution
		}
	}
	if aspectRatio == "" {
		aspectRatio = "9:16"
	}
	return aspectRatio, resolution, nil
}

func (h *Handler) getVideo(c *gin.Context) {
	clientKey, _, ok := requestIdentity(c)
	if !ok {
		return
	}
	job, err := h.gateway.GetVideo(c.Request.Context(), strings.TrimSpace(c.Param("requestId")), clientKey)
	if err != nil {
		writeGatewayError(c, err)
		return
	}
	contentURL := h.videoContentURL(job.ID)
	if openAICompatible, _ := gateway.VideoJobResponseMetadata(job); openAICompatible {
		c.JSON(http.StatusOK, openAIVideoGenerationResponse(job))
		return
	}
	c.JSON(http.StatusOK, videoGenerationResponse(job, contentURL))
}

func (h *Handler) videoContentURL(jobID string) string {
	path := "/v1/videos/" + url.PathEscape(jobID) + "/content"
	baseURL := h.publicAPIBaseURL
	if h.publicBaseURL != nil {
		baseURL = strings.TrimRight(strings.TrimSpace(h.publicBaseURL()), "/")
	}
	if baseURL == "" {
		return path
	}
	return baseURL + path
}

func (h *Handler) getVideoContent(c *gin.Context) {
	clientKey, _, ok := requestIdentity(c)
	if !ok {
		return
	}
	body, contentType, size, err := h.gateway.OpenVideoContent(c.Request.Context(), strings.TrimSpace(c.Param("requestId")), clientKey)
	if err != nil {
		writeGatewayError(c, err)
		return
	}
	defer func() { _ = body.Close() }()
	writeVideoContent(c, body, contentType, size)
}

func writeVideoContent(c *gin.Context, body io.Reader, contentType string, size int64) {
	if size > maxMediaResponseTransferBytes {
		writeOpenAIError(c, http.StatusBadGateway, "media_too_large", "上游媒体超过 2 GiB 安全上限")
		return
	}
	c.Header("Content-Type", contentType)
	c.Header("Content-Disposition", "inline")
	c.Header("Cache-Control", "private, no-store")
	c.Header("X-Content-Type-Options", "nosniff")
	if size >= 0 {
		c.Header("Content-Length", strconv.FormatInt(size, 10))
	} else {
		c.Header("Trailer", mediaTransferErrorTrailer)
	}
	c.Status(http.StatusOK)
	if err := copyMedia(responseDeadlineWriter{ResponseWriter: c.Writer}, body, maxMediaResponseTransferBytes); err != nil && size < 0 {
		errorCode := "stream_interrupted"
		if errors.Is(err, errResponseTransferLimit) {
			errorCode = "response_too_large"
		}
		c.Header(mediaTransferErrorTrailer, errorCode)
	}
}

func parseVideoDuration(durationRaw json.RawMessage) (int, error) {
	duration, hasDuration, err := parseOptionalVideoInteger(durationRaw)
	if err != nil {
		return 0, fmt.Errorf("duration 必须是整数或整数字符串")
	}
	value := 8
	if hasDuration {
		value = duration
	}
	if value < 1 || value > 15 {
		return 0, fmt.Errorf("duration 必须在 1 到 15 秒之间")
	}
	return value, nil
}

func parseOptionalVideoInteger(raw json.RawMessage) (int, bool, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, false, nil
	}
	var number int
	if json.Unmarshal(raw, &number) != nil {
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return 0, true, errors.New("必须是整数或整数字符串")
		}
		parsed, err := strconv.Atoi(strings.TrimSpace(text))
		if err != nil {
			return 0, true, errors.New("必须是整数或整数字符串")
		}
		number = parsed
	}
	return number, true, nil
}

func hasJSONValue(value json.RawMessage) bool {
	trimmed := bytes.TrimSpace(value)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
}

func validVideoAspectRatio(value string) bool {
	switch value {
	case "1:1", "16:9", "9:16", "4:3", "3:4", "3:2", "2:3":
		return true
	default:
		return false
	}
}

func validImageAspectRatio(value string) bool {
	switch value {
	case "auto", "1:1", "16:9", "9:16", "4:3", "3:4", "3:2", "2:3", "2:1", "1:2", "19.5:9", "9:19.5", "20:9", "9:20":
		return true
	default:
		return false
	}
}

func validImageEditSize(value string) bool {
	switch value {
	case "auto", "1024x1024", "1024x1536", "1536x1024":
		return true
	default:
		return false
	}
}

func videoGenerationResponse(job mediadomain.Job, contentURLs ...string) gin.H {
	switch job.Status {
	case mediadomain.StatusCompleted:
		videoURL := job.UpstreamURL
		if len(contentURLs) > 0 && contentURLs[0] != "" {
			videoURL = contentURLs[0]
		}
		return gin.H{
			"status": "done", "model": job.Model, "progress": 100,
			"video": gin.H{"url": videoURL, "duration": job.Seconds, "respect_moderation": true},
		}
	case mediadomain.StatusFailed:
		return gin.H{
			"status": "failed",
			"error":  gin.H{"code": officialVideoErrorCode(job.ErrorCode), "message": job.ErrorMessage},
		}
	default:
		return gin.H{"status": "pending", "model": job.Model, "progress": min(99, max(0, job.Progress))}
	}
}

func openAIVideoGenerationResponse(job mediadomain.Job) gin.H {
	_, responseSize := gateway.VideoJobResponseMetadata(job)
	if responseSize == "" {
		responseSize = job.Size
	}
	response := gin.H{
		"id": job.ID, "object": "video", "model": job.Model,
		"progress": min(100, max(0, job.Progress)), "created_at": job.CreatedAt.Unix(),
		"seconds": strconv.Itoa(job.Seconds), "size": responseSize,
	}
	switch job.Status {
	case mediadomain.StatusCompleted:
		response["status"], response["progress"] = "completed", 100
		if job.CompletedAt != nil {
			response["completed_at"] = job.CompletedAt.Unix()
		}
	case mediadomain.StatusFailed:
		response["status"] = "failed"
		response["error"] = gin.H{"code": officialVideoErrorCode(job.ErrorCode), "message": job.ErrorMessage}
	case mediadomain.StatusInProgress:
		response["status"] = "in_progress"
	default:
		response["status"] = "queued"
		response["progress"] = min(99, max(0, job.Progress))
	}
	return response
}

func officialVideoErrorCode(value string) string {
	switch value {
	case "account_unavailable", "provider_unavailable":
		return "service_unavailable"
	case "model_not_found":
		return "invalid_argument"
	default:
		return "internal_error"
	}
}

func (h *Handler) handleCreate(c *gin.Context, compact bool) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, h.maxBodyBytes)
	if !isJSONRequest(c) {
		writeOpenAIError(c, http.StatusUnsupportedMediaType, "invalid_request", "Responses only supports application/json")
		return
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		writeOpenAIError(c, http.StatusRequestEntityTooLarge, "request_too_large", "请求体超过限制")
		return
	}
	var request responsesRequest
	if err := json.Unmarshal(body, &request); err != nil || strings.TrimSpace(request.Model) == "" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "Responses 请求缺少有效 model")
		return
	}
	if compact {
		body, err = forceJSONBoolean(body, "stream", false)
		if err != nil {
			writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "Compact 请求格式无效")
			return
		}
		request.Stream = false
	}
	clientValue, exists := c.Get(middleware.ClientKey)
	clientKey, ok := clientValue.(clientkeydomain.Key)
	if !exists || !ok {
		writeOpenAIError(c, http.StatusUnauthorized, "invalid_api_key", "客户端 API Key 无效")
		return
	}
	requestID, _ := c.Get(middleware.RequestIDKey)
	requestIDValue, _ := requestID.(string)
	input := gateway.Input{
		RequestID: requestIDValue, ClientKey: clientKey, PublicModel: request.Model,
		Body: body, Streaming: request.Stream, PromptCacheKey: request.PromptCacheKey,
		PromptCacheSeed: extractPromptCacheSeed(c.Request.Header, body), PreviousResponseID: request.PreviousResponseID,
		GrokTurnIndex: c.GetHeader("x-grok-turn-idx"),
	}
	var result *gateway.Result
	if compact {
		result, err = h.gateway.CompactResponse(c.Request.Context(), input)
	} else {
		result, err = h.gateway.CreateResponse(c.Request.Context(), input)
	}
	if err != nil {
		writeGatewayError(c, err)
		return
	}
	h.writeResult(c, result, request.Stream && !compact, streamProtocolResponses)
}

func isJSONRequest(c *gin.Context) bool {
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	return err == nil && strings.EqualFold(mediaType, "application/json")
}

func decodeSingleJSON(reader io.Reader, target any, disallowUnknown bool) error {
	decoder := json.NewDecoder(reader)
	if disallowUnknown {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("请求体只能包含一个 JSON 对象")
		}
		return err
	}
	return nil
}

func (h *Handler) getResponse(c *gin.Context) {
	h.handleOwnedResource(c, false)
}

func (h *Handler) deleteResponse(c *gin.Context) {
	h.handleOwnedResource(c, true)
}

func (h *Handler) handleOwnedResource(c *gin.Context, deleteResource bool) {
	clientValue, exists := c.Get(middleware.ClientKey)
	clientKey, ok := clientValue.(clientkeydomain.Key)
	if !exists || !ok {
		writeOpenAIError(c, http.StatusUnauthorized, "invalid_api_key", "客户端 API Key 无效")
		return
	}
	input := gateway.ResourceInput{ClientKey: clientKey, ResponseID: strings.TrimSpace(c.Param("responseId")), RawQuery: c.Request.URL.RawQuery}
	if input.ResponseID == "" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "response_id 不能为空")
		return
	}
	var result *gateway.Result
	var err error
	if deleteResource {
		result, err = h.gateway.DeleteResponse(c.Request.Context(), input)
	} else {
		result, err = h.gateway.GetResponse(c.Request.Context(), input)
	}
	if err != nil {
		writeGatewayError(c, err)
		return
	}
	h.writeResult(c, result, false, streamProtocolResponses)
}

func (h *Handler) writeResult(c *gin.Context, result *gateway.Result, stream bool, protocol streamProtocol) {
	h.writeProtocolResult(c, result, stream, false, protocol)
}

func (h *Handler) writeAnthropicResult(c *gin.Context, result *gateway.Result, stream bool) {
	h.writeProtocolResult(c, result, stream, true, streamProtocolAnthropic)
}

func (h *Handler) writeProtocolResult(c *gin.Context, result *gateway.Result, stream, anthropic bool, protocol streamProtocol) {
	usage := gateway.Usage{}
	responseID := ""
	errorCode := ""
	defer result.Body.Close()
	defer func() { result.Finalize(usage, responseID, errorCode) }()
	if isUpstreamCredentialStatus(result.StatusCode) {
		errorCode = "upstream_unavailable"
		clientCode := readCredentialErrorCode(result.StatusCode, result.Body)
		if anthropic {
			writeAnthropicError(c, http.StatusServiceUnavailable, "overloaded_error", credentialErrorMessage(clientCode), clientCode)
		} else {
			writeOpenAIError(c, http.StatusServiceUnavailable, clientCode, credentialErrorMessage(clientCode))
		}
		return
	}
	transferLimit := int64(maxJSONResponseTransferBytes)
	if stream {
		transferLimit = maxStreamResponseTransferBytes
	}
	if contentLength, parseErr := strconv.ParseInt(result.Header.Get("Content-Length"), 10, 64); parseErr == nil && contentLength > transferLimit {
		errorCode = "response_too_large"
		writeOpenAIError(c, http.StatusBadGateway, "response_too_large", "上游响应超过代理安全上限")
		return
	}
	copyHeaders(c.Writer.Header(), result.Header)
	c.Status(result.StatusCode)
	if result.StatusCode >= 400 {
		errorCode = "upstream_error"
	}
	var err error
	if stream {
		metadata, copyErr := copyStream(c.Writer, result.Body, protocol)
		usage, responseID, err = metadata.Usage, metadata.ResponseID, copyErr
		if metadata.StreamFailure != nil && result.RecordStreamFailure != nil {
			result.RecordStreamFailure(*metadata.StreamFailure)
		}
	} else {
		metadata, copyErr := copyJSON(c.Writer, result.Body)
		usage, responseID, err = metadata.Usage, metadata.ResponseID, copyErr
	}
	if err != nil {
		switch {
		case errors.Is(err, errResponseTransferLimit):
			errorCode = "response_too_large"
		case errors.Is(err, errUpstreamStreamFailed):
			errorCode = "upstream_stream_error"
		case errors.Is(err, errUpstreamStreamIncomplete):
			errorCode = "upstream_stream_incomplete"
		case errors.Is(err, errUpstreamStreamRead):
			errorCode = "upstream_stream_interrupted"
		default:
			errorCode = "stream_interrupted"
		}
	}
}

type responseMetadata struct {
	Usage         gateway.Usage
	ResponseID    string
	Model         string
	StreamFailure *gateway.StreamFailureDiagnostic
}

func copyStream(writer gin.ResponseWriter, source io.Reader, protocol streamProtocol) (responseMetadata, error) {
	inspector := &responseInspector{protocol: protocol}
	buffer := make([]byte, responseCopyBufferBytes)
	transferred := 0
	for {
		n, readErr := source.Read(buffer)
		if n > 0 {
			if transferred+n > maxStreamResponseTransferBytes {
				return inspector.Metadata(), fmt.Errorf("%w: 流式响应超过 %d MiB", errResponseTransferLimit, maxStreamResponseTransferBytes>>20)
			}
			chunk := buffer[:n]
			inspector.Inspect(chunk)
			if err := setResponseWriteDeadline(writer); err != nil {
				return inspector.Metadata(), err
			}
			if _, err := writer.Write(chunk); err != nil {
				return inspector.Metadata(), err
			}
			writer.Flush()
			transferred += n
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				inspector.Finish()
				return inspector.Metadata(), inspector.TerminalError()
			}
			if inspector.terminalSuccess {
				return inspector.Metadata(), nil
			}
			return inspector.Metadata(), fmt.Errorf("%w: %v", errUpstreamStreamRead, readErr)
		}
	}
}

func copyJSON(writer gin.ResponseWriter, source io.Reader) (responseMetadata, error) {
	buffer := make([]byte, responseCopyBufferBytes)
	metadataBody := make([]byte, 0, responseCopyBufferBytes)
	metadataComplete := true
	transferred := 0
	for {
		n, readErr := source.Read(buffer)
		if n > 0 {
			if transferred+n > maxJSONResponseTransferBytes {
				return responseMetadata{}, fmt.Errorf("%w: 非流式响应超过 %d MiB", errResponseTransferLimit, maxJSONResponseTransferBytes>>20)
			}
			chunk := buffer[:n]
			if err := setResponseWriteDeadline(writer); err != nil {
				return responseMetadata{}, err
			}
			if _, err := writer.Write(chunk); err != nil {
				return responseMetadata{}, err
			}
			transferred += n
			if metadataComplete {
				if len(metadataBody)+len(chunk) <= maxJSONMetadataInspectionBytes {
					metadataBody = append(metadataBody, chunk...)
				} else {
					metadataBody = nil
					metadataComplete = false
				}
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				if metadataComplete {
					return extractMetadata(metadataBody), nil
				}
				return responseMetadata{}, nil
			}
			return responseMetadata{}, readErr
		}
	}
}

type responseInspector struct {
	protocol        streamProtocol
	pending         []byte
	metadata        responseMetadata
	terminalSuccess bool
	terminalFailure bool
}

func (i *responseInspector) Inspect(chunk []byte) {
	i.pending = append(i.pending, chunk...)
	for {
		index := bytes.IndexByte(i.pending, '\n')
		if index < 0 {
			if len(i.pending) > maxStreamEventInspectionBytes {
				i.pending = nil
			}
			return
		}
		line := bytes.TrimSpace(i.pending[:index])
		i.pending = i.pending[index+1:]
		if bytes.HasPrefix(line, []byte("data:")) {
			value := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
			i.observeTerminal(value)
			if !bytes.Equal(value, []byte("[DONE]")) {
				metadata := extractMetadata(value)
				if hasUsageSignal(metadata.Usage) {
					if metadata.Usage.ResponseModel == "" {
						metadata.Usage.ResponseModel = i.metadata.Model
					}
					i.metadata.Usage = mergeGatewayUsage(i.metadata.Usage, metadata.Usage)
				}
				if metadata.ResponseID != "" {
					i.metadata.ResponseID = metadata.ResponseID
				}
				if metadata.Model != "" {
					i.metadata.Model = metadata.Model
					i.metadata.Usage.ResponseModel = metadata.Model
				}
			}
		}
	}
}

func (i *responseInspector) Metadata() responseMetadata { return i.metadata }

func (i *responseInspector) TerminalError() error {
	if i.terminalFailure {
		return errUpstreamStreamFailed
	}
	if !i.terminalSuccess {
		return errUpstreamStreamIncomplete
	}
	return nil
}

func (i *responseInspector) observeTerminal(data []byte) {
	if bytes.Equal(data, []byte("[DONE]")) {
		if i.protocol == streamProtocolChat {
			i.terminalSuccess = true
		}
		return
	}
	var payload struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(data, &payload) != nil {
		return
	}
	switch i.protocol {
	case streamProtocolResponses:
		switch payload.Type {
		case "response.completed":
			i.terminalSuccess = true
		case "response.failed", "response.incomplete", "response.error", "error":
			i.markTerminalFailure(data)
		}
	case streamProtocolChat:
		if payload.Type == "error" {
			i.markTerminalFailure(data)
		}
	case streamProtocolAnthropic:
		switch payload.Type {
		case "message_stop":
			i.terminalSuccess = true
		case "error":
			i.markTerminalFailure(data)
		}
	case streamProtocolImage:
		switch payload.Type {
		case "image_generation.completed", "image_edit.completed":
			i.terminalSuccess = true
		case "image_generation.failed", "image_edit.failed", "error":
			i.markTerminalFailure(data)
		}
	}
}

func (i *responseInspector) markTerminalFailure(data []byte) {
	i.terminalFailure = true
	if i.metadata.StreamFailure != nil {
		return
	}
	diagnostic := projectStreamFailureDiagnostic(data)
	if len(diagnostic.Body) > 0 {
		i.metadata.StreamFailure = &diagnostic
	}
}

func projectStreamFailureDiagnostic(data []byte) gateway.StreamFailureDiagnostic {
	var root map[string]json.RawMessage
	if json.Unmarshal(data, &root) != nil {
		return gateway.StreamFailureDiagnostic{}
	}
	projected := make(map[string]json.RawMessage)
	copySafeDiagnosticFields(projected, root, "type", "status", "code", "message", "param")
	if raw := projectSafeErrorValue(root["error"]); len(raw) > 0 {
		projected["error"] = raw
	}
	if responseRaw := root["response"]; len(responseRaw) > 0 {
		var response map[string]json.RawMessage
		if json.Unmarshal(responseRaw, &response) == nil {
			safeResponse := make(map[string]json.RawMessage)
			copySafeDiagnosticFields(safeResponse, response, "id", "status", "code", "message")
			if raw := projectSafeErrorValue(response["error"]); len(raw) > 0 {
				safeResponse["error"] = raw
			}
			if raw := projectSafeErrorValue(response["incomplete_details"]); len(raw) > 0 {
				safeResponse["incomplete_details"] = raw
			}
			if len(safeResponse) > 0 {
				if encoded, err := json.Marshal(safeResponse); err == nil {
					projected["response"] = encoded
				}
			}
		}
	}
	if len(projected) == 0 {
		return gateway.StreamFailureDiagnostic{}
	}
	encoded, err := json.Marshal(projected)
	if err != nil {
		return gateway.StreamFailureDiagnostic{}
	}
	diagnostic := gateway.StreamFailureDiagnostic{Body: encoded}
	if len(diagnostic.Body) > maxStreamFailureDiagnosticBytes {
		bounded := diagnostic.Body[:maxStreamFailureDiagnosticBytes]
		for len(bounded) > 0 && !utf8.Valid(bounded) {
			bounded = bounded[:len(bounded)-1]
		}
		diagnostic.Body = append([]byte(nil), bounded...)
		diagnostic.BodyTruncated = true
	} else {
		diagnostic.Body = append([]byte(nil), diagnostic.Body...)
	}
	return diagnostic
}

func copySafeDiagnosticFields(destination, source map[string]json.RawMessage, fields ...string) {
	for _, field := range fields {
		if raw := projectSafeScalar(source[field]); len(raw) > 0 {
			destination[field] = raw
		}
	}
}

func projectSafeErrorValue(raw json.RawMessage) json.RawMessage {
	if scalar := projectSafeScalar(raw); len(scalar) > 0 {
		return scalar
	}
	var value map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil {
		return nil
	}
	projected := make(map[string]json.RawMessage)
	copySafeDiagnosticFields(projected, value, "type", "status", "code", "message", "param", "reason")
	if len(projected) == 0 {
		return nil
	}
	encoded, err := json.Marshal(projected)
	if err != nil {
		return nil
	}
	return encoded
}

func projectSafeScalar(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return nil
	}
	switch value.(type) {
	case nil, string, bool, float64:
		return append(json.RawMessage(nil), raw...)
	default:
		return nil
	}
}

func (i *responseInspector) Finish() {
	if len(i.pending) == 0 {
		return
	}
	i.pending = append(i.pending, '\n')
	i.Inspect(nil)
}

func extractMetadata(data []byte) responseMetadata {
	var root responsePayloadDTO
	if json.Unmarshal(data, &root) != nil {
		return responseMetadata{}
	}
	metadata := responseMetadata{ResponseID: root.ID, Model: root.Model}
	usage := root.Usage
	if root.Response != nil {
		if metadata.ResponseID == "" {
			metadata.ResponseID = root.Response.ID
		}
		if metadata.Model == "" {
			metadata.Model = root.Response.Model
		}
		if usage == nil {
			usage = root.Response.Usage
		}
	}
	if usage == nil {
		return metadata
	}
	metadata.Usage = usage.toGatewayUsage(metadata.Model)
	return metadata
}

type responsePayloadDTO struct {
	ID       string              `json:"id"`
	Model    string              `json:"model"`
	Usage    *responseUsageDTO   `json:"usage"`
	Response *responsePayloadDTO `json:"response"`
}

type responseUsageDTO struct {
	InputTokens            int64 `json:"input_tokens"`
	InputTokensCamel       int64 `json:"inputTokens"`
	OutputTokens           int64 `json:"output_tokens"`
	OutputTokensCamel      int64 `json:"outputTokens"`
	TotalTokens            int64 `json:"total_tokens"`
	TotalTokensCamel       int64 `json:"totalTokens"`
	CostInUSDTicks         int64 `json:"cost_in_usd_ticks"`
	NumSourcesUsed         int64 `json:"num_sources_used"`
	NumServerSideToolsUsed int64 `json:"num_server_side_tools_used"`
	// Responses 协议：input_tokens_details.cached_tokens
	InputTokensDetails responseInputDetailsDTO `json:"input_tokens_details"`
	// OpenAI Chat Completions 协议：prompt_tokens_details.cached_tokens
	PromptTokensDetails responseInputDetailsDTO `json:"prompt_tokens_details"`
	// Anthropic Messages 协议：顶层 cache_read_input_tokens
	CacheReadInputTokens     int64                    `json:"cache_read_input_tokens"`
	CacheCreationInputTokens int64                    `json:"cache_creation_input_tokens"`
	OutputTokensDetails      responseOutputDetailsDTO `json:"output_tokens_details"`
	// OpenAI Chat Completions 协议：completion_tokens_details.reasoning_tokens
	CompletionTokensDetails responseOutputDetailsDTO  `json:"completion_tokens_details"`
	ContextDetails          responseContextDetailsDTO `json:"context_details"`
	PromptTokens            int64                     `json:"prompt_tokens"`
	CompletionTokens        int64                     `json:"completion_tokens"`
}

type responseInputDetailsDTO struct {
	CachedTokens int64 `json:"cached_tokens"`
}

type responseOutputDetailsDTO struct {
	ReasoningTokens int64 `json:"reasoning_tokens"`
}

type responseContextDetailsDTO struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

func (value responseUsageDTO) toGatewayUsage(responseModel string) gateway.Usage {
	input := value.InputTokens
	if input == 0 {
		input = value.InputTokensCamel
	}
	if input == 0 {
		input = value.PromptTokens
	}
	output := value.OutputTokens
	if output == 0 {
		output = value.OutputTokensCamel
	}
	if output == 0 {
		output = value.CompletionTokens
	}
	total := value.TotalTokens
	if total == 0 {
		total = value.TotalTokensCamel
	}
	if total == 0 {
		total = input + output
	}
	// 统一缓存命中：Responses / Chat Completions / Anthropic Messages
	cached := value.InputTokensDetails.CachedTokens
	if cached == 0 {
		cached = value.PromptTokensDetails.CachedTokens
	}
	if cached == 0 {
		cached = value.CacheReadInputTokens
	}
	reasoning := value.OutputTokensDetails.ReasoningTokens
	if reasoning == 0 {
		reasoning = value.CompletionTokensDetails.ReasoningTokens
	}
	return gateway.Usage{
		InputTokens: input, CachedInputTokens: cached,
		OutputTokens: output, ReasoningTokens: reasoning,
		TotalTokens: total, CostInUSDTicks: value.CostInUSDTicks,
		NumSourcesUsed: value.NumSourcesUsed, NumServerSideToolsUsed: value.NumServerSideToolsUsed,
		ContextInputTokens: value.ContextDetails.InputTokens, ContextOutputTokens: value.ContextDetails.OutputTokens,
		ResponseModel: responseModel,
	}
}

func hasUsageSignal(usage gateway.Usage) bool {
	return usage.InputTokens > 0 || usage.OutputTokens > 0 || usage.TotalTokens > 0 ||
		usage.CachedInputTokens > 0 || usage.ReasoningTokens > 0 || usage.CostInUSDTicks > 0 ||
		usage.NumSourcesUsed > 0 || usage.NumServerSideToolsUsed > 0 ||
		usage.ContextInputTokens > 0 || usage.ContextOutputTokens > 0
}

// mergeGatewayUsage 合并流式多帧 usage：非零字段覆盖，避免后到半截帧抹掉已解析缓存命中。
func mergeGatewayUsage(base, next gateway.Usage) gateway.Usage {
	if next.InputTokens > 0 {
		base.InputTokens = next.InputTokens
	}
	if next.OutputTokens > 0 {
		base.OutputTokens = next.OutputTokens
	}
	if next.TotalTokens > 0 {
		base.TotalTokens = next.TotalTokens
	}
	if next.CachedInputTokens > 0 {
		base.CachedInputTokens = next.CachedInputTokens
	}
	if next.ReasoningTokens > 0 {
		base.ReasoningTokens = next.ReasoningTokens
	}
	if next.CostInUSDTicks > 0 {
		base.CostInUSDTicks = next.CostInUSDTicks
	}
	if next.NumSourcesUsed > 0 {
		base.NumSourcesUsed = next.NumSourcesUsed
	}
	if next.NumServerSideToolsUsed > 0 {
		base.NumServerSideToolsUsed = next.NumServerSideToolsUsed
	}
	if next.ContextInputTokens > 0 {
		base.ContextInputTokens = next.ContextInputTokens
	}
	if next.ContextOutputTokens > 0 {
		base.ContextOutputTokens = next.ContextOutputTokens
	}
	if next.ResponseModel != "" {
		base.ResponseModel = next.ResponseModel
	}
	if base.TotalTokens == 0 && (base.InputTokens > 0 || base.OutputTokens > 0) {
		base.TotalTokens = base.InputTokens + base.OutputTokens
	}
	return base
}

func copyHeaders(destination, source http.Header) {
	excluded := map[string]struct{}{
		"connection": {}, "content-length": {}, "keep-alive": {}, "proxy-authenticate": {},
		"proxy-authorization": {}, "set-cookie": {}, "te": {}, "trailer": {},
		"transfer-encoding": {}, "upgrade": {},
	}
	for _, value := range source.Values("Connection") {
		for name := range strings.SplitSeq(value, ",") {
			name = strings.ToLower(strings.TrimSpace(name))
			if name != "" {
				excluded[name] = struct{}{}
			}
		}
	}
	for name, values := range source {
		lower := strings.ToLower(name)
		if _, skip := excluded[lower]; skip {
			continue
		}
		for _, value := range values {
			destination.Add(name, value)
		}
	}
}

func writeOpenAIError(c *gin.Context, status int, code, message string) {
	errorType := "invalid_request_error"
	switch {
	case status == http.StatusUnauthorized:
		errorType = "authentication_error"
	case status == http.StatusTooManyRequests:
		errorType = "rate_limit_error"
	case status >= 500:
		errorType = "server_error"
	}
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"message": message, "type": errorType, "code": code, "param": nil}})
}

func writeImageGenerationUserError(c *gin.Context, code, param, message string) {
	c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": gin.H{
		"message": message, "type": "image_generation_user_error", "param": param, "code": code,
	}})
}

func writeGatewayError(c *gin.Context, err error) {
	status, code := http.StatusBadGateway, "upstream_unavailable"
	message := "上游服务暂不可用"
	var upstreamFailure *gateway.UpstreamFailure
	var selectionFailure *gateway.SelectionUnavailableError
	switch {
	case errors.Is(err, clientkeyapp.ErrBillingLimit):
		status, code = http.StatusTooManyRequests, "billing_limit_exceeded"
		message = clientkeyapp.ErrBillingLimit.Error()
	case errors.Is(err, gateway.ErrModelNotFound):
		status, code = http.StatusNotFound, "model_not_found"
		message = "模型不存在"
	case errors.Is(err, gateway.ErrVideoInputTooLarge):
		status, code = http.StatusRequestEntityTooLarge, "request_too_large"
		message = "视频参考图片元数据超过任务持久化上限"
	case errors.Is(err, mediaapp.ErrImageTooLarge):
		status, code = http.StatusRequestEntityTooLarge, "request_too_large"
		message = "视频参考图片超过媒体大小限制"
	case errors.Is(err, mediaapp.ErrInvalidImage):
		status, code = http.StatusBadRequest, "invalid_request"
		message = "视频参考图片无效"
	case errors.Is(err, gateway.ErrResponseNotFound):
		status, code = http.StatusNotFound, "response_not_found"
		message = "Response 不存在或已过期"
	case errors.Is(err, gateway.ErrResponseStateUnsupported), errors.Is(err, gateway.ErrConversationUnsupported):
		status, code = http.StatusBadRequest, "unsupported_parameter"
		message = err.Error()
	case errors.As(err, &upstreamFailure):
		if isUpstreamCredentialStatus(upstreamFailure.HTTPStatus) {
			code = upstreamFailure.ClientCredentialErrorCode()
			status, message = http.StatusServiceUnavailable, credentialErrorMessage(code)
		} else {
			status, code, message = upstreamFailure.HTTPStatus, upstreamFailure.Code, upstreamFailure.PublicMessage
		}
		if !isUpstreamCredentialStatus(upstreamFailure.HTTPStatus) && upstreamFailure.RetryAfter > 0 {
			c.Header("Retry-After", strconv.FormatInt(max(1, int64(upstreamFailure.RetryAfter.Round(time.Second)/time.Second)), 10))
		}
	case errors.As(err, &selectionFailure):
		status, code, message = selectionErrorResponse(c, selectionFailure)
	case errors.Is(err, gateway.ErrResponseAccountUnavailable), errors.Is(err, gateway.ErrNoAvailableAccount):
		status, code = http.StatusServiceUnavailable, "upstream_unavailable"
		message = "当前没有可用的上游账号"
	}
	writeOpenAIError(c, status, code, message)
}

func writeGatewayAnthropicError(c *gin.Context, err error) {
	status, errorType := http.StatusBadGateway, "api_error"
	message := "上游服务暂不可用"
	clientCode := ""
	var upstreamFailure *gateway.UpstreamFailure
	var selectionFailure *gateway.SelectionUnavailableError
	switch {
	case errors.Is(err, clientkeyapp.ErrBillingLimit):
		status, errorType = http.StatusTooManyRequests, "rate_limit_error"
		message = clientkeyapp.ErrBillingLimit.Error()
	case errors.Is(err, gateway.ErrModelNotFound):
		status, errorType = http.StatusNotFound, "not_found_error"
		message = "模型不存在"
	case errors.Is(err, gateway.ErrResponseStateUnsupported), errors.Is(err, gateway.ErrConversationUnsupported):
		status, errorType = http.StatusBadRequest, "invalid_request_error"
		message = err.Error()
	case errors.As(err, &upstreamFailure):
		if isUpstreamCredentialStatus(upstreamFailure.HTTPStatus) {
			clientCode = upstreamFailure.ClientCredentialErrorCode()
			status, errorType, message = http.StatusServiceUnavailable, "overloaded_error", credentialErrorMessage(clientCode)
		} else {
			status, message = upstreamFailure.HTTPStatus, upstreamFailure.PublicMessage
		}
		if !isUpstreamCredentialStatus(upstreamFailure.HTTPStatus) && upstreamFailure.RetryAfter > 0 {
			c.Header("Retry-After", strconv.FormatInt(max(1, int64(upstreamFailure.RetryAfter.Round(time.Second)/time.Second)), 10))
		}
		if status == http.StatusTooManyRequests {
			errorType = "rate_limit_error"
		}
	case errors.As(err, &selectionFailure):
		status, _, message = selectionErrorResponse(c, selectionFailure)
		if status == http.StatusTooManyRequests {
			errorType = "rate_limit_error"
		} else {
			errorType = "overloaded_error"
		}
	case errors.Is(err, gateway.ErrResponseAccountUnavailable), errors.Is(err, gateway.ErrNoAvailableAccount):
		status, errorType = http.StatusServiceUnavailable, "overloaded_error"
		message = "当前没有可用的上游账号"
	}
	writeAnthropicError(c, status, errorType, message, clientCode)
}

func isUpstreamCredentialStatus(status int) bool {
	return status == http.StatusUnauthorized || status == http.StatusForbidden
}

func selectionErrorResponse(c *gin.Context, failure *gateway.SelectionUnavailableError) (int, string, string) {
	status, code, message := http.StatusServiceUnavailable, "upstream_unavailable", "当前没有可用的上游账号"
	if failure == nil {
		return status, code, message
	}
	switch failure.Reason {
	case gateway.SelectionCooling:
		status, code, message = http.StatusTooManyRequests, "upstream_cooling", "上游账号正在冷却"
	case gateway.SelectionModelCooling:
		status, code, message = http.StatusTooManyRequests, "upstream_model_cooling", "上游账号的目标模型正在冷却"
	case gateway.SelectionQuotaExhausted:
		status, code, message = http.StatusTooManyRequests, "upstream_quota_exhausted", "上游账号额度等待恢复"
	case gateway.SelectionSaturated:
		code, message = "upstream_saturated", "上游账号当前均达到并发上限"
	case gateway.SelectionUnsupportedModel:
		code, message = "upstream_model_unavailable", "当前账号池不支持该模型"
	}
	if failure.RetryAfter > 0 {
		seconds := max(int64(1), int64((failure.RetryAfter+time.Second-1)/time.Second))
		c.Header("Retry-After", strconv.FormatInt(seconds, 10))
	}
	return status, code, message
}

func writeAnthropicError(c *gin.Context, status int, errorType, message string, errorCode ...string) {
	errorPayload := gin.H{"type": errorType, "message": message}
	if len(errorCode) > 0 && errorCode[0] != "" && errorCode[0] != "upstream_unavailable" {
		errorPayload["code"] = errorCode[0]
	}
	c.AbortWithStatusJSON(status, gin.H{"type": "error", "error": errorPayload})
}

func readCredentialErrorCode(status int, source io.Reader) string {
	body, err := io.ReadAll(io.LimitReader(source, maxCredentialErrorInspectBytes+1))
	if err != nil || len(body) > maxCredentialErrorInspectBytes {
		return "upstream_unavailable"
	}
	return gateway.ClientCredentialErrorCodeFromBody(status, body)
}

func credentialErrorMessage(code string) string {
	if code == "permission-denied" {
		return "上游服务暂不可用，聊天端点访问被拒绝"
	}
	return "上游服务暂不可用"
}

func forceJSONBoolean(body []byte, key string, value bool) ([]byte, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	payload[key] = json.RawMessage("false")
	if value {
		payload[key] = json.RawMessage("true")
	}
	return json.Marshal(payload)
}
