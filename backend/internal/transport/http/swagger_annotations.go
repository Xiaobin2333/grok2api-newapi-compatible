package httpserver

// SwaggerMessage 表示 Chat Completions 请求中的一条消息。
type SwaggerMessage struct {
	Role    string `json:"role" example:"user"`
	Content any    `json:"content"`
}

// SwaggerResponsesRequest 表示最小 Responses 请求。
type SwaggerResponsesRequest struct {
	Model              string `json:"model" example:"grok-chat-auto"`
	Input              any    `json:"input"`
	Stream             bool   `json:"stream" example:"false"`
	Store              bool   `json:"store" example:"false"`
	PreviousResponseID string `json:"previous_response_id,omitempty"`
	PromptCacheKey     string `json:"prompt_cache_key,omitempty"`
}

// SwaggerChatRequest 表示最小 Chat Completions 请求。
type SwaggerChatRequest struct {
	Model    string           `json:"model" example:"grok-chat-fast"`
	Messages []SwaggerMessage `json:"messages"`
	Stream   bool             `json:"stream" example:"false"`
}

// SwaggerMessagesRequest 表示最小 Anthropic Messages 请求。
type SwaggerMessagesRequest struct {
	Model     string           `json:"model" example:"grok-chat-expert"`
	MaxTokens int              `json:"max_tokens" example:"1024"`
	Messages  []SwaggerMessage `json:"messages"`
	Stream    bool             `json:"stream" example:"false"`
}

// SwaggerImageGenerationRequest 表示图片生成请求。
type SwaggerImageGenerationRequest struct {
	Model  string `json:"model" example:"grok-imagine-image"`
	Prompt string `json:"prompt" example:"Place image 1 in the setting from image 2"`
	// Image 接受 URL 字符串或包含 url 的对象。
	Image any `json:"image,omitempty"`
	// Images 接受 URL 字符串和包含 url 的对象组成的混合数组。
	Images []any `json:"images,omitempty"`
	// ImageURLs 接受 URL 字符串数组。
	ImageURLs      []string       `json:"image_urls,omitempty" example:"https://example.com/character.png,https://example.com/scene.png"`
	N              int            `json:"n" example:"1"`
	Size           string         `json:"size,omitempty" example:"1536x1024"`
	Quality        string         `json:"quality,omitempty" example:"high"`
	AspectRatio    string         `json:"aspect_ratio,omitempty" example:"16:9"`
	Resolution     string         `json:"resolution,omitempty" example:"2k"`
	ResponseFormat string         `json:"response_format,omitempty" example:"url"`
	Stream         bool           `json:"stream,omitempty" example:"false"`
	PartialImages  int            `json:"partial_images,omitempty" example:"0"`
	StorageOptions map[string]any `json:"storage_options,omitempty"`
}

// SwaggerImageEditRequest 表示图片编辑请求。
type SwaggerImageEditRequest struct {
	Model  string `json:"model" example:"grok-imagine-image"`
	Prompt string `json:"prompt" example:"Change the background to black"`
	// Image 接受 URL 字符串或包含 url 的对象。
	Image any `json:"image,omitempty"`
	// Images 接受 URL 字符串和包含 url 的对象组成的混合数组。
	Images []any `json:"images,omitempty"`
	// ImageURLs 接受 URL 字符串数组。
	ImageURLs      []string       `json:"image_urls,omitempty" example:"https://example.com/source.png"`
	N              int            `json:"n" example:"1"`
	Size           string         `json:"size,omitempty" example:"1024x1024"`
	AspectRatio    string         `json:"aspect_ratio,omitempty" example:"1:1"`
	Resolution     string         `json:"resolution,omitempty" example:"1k"`
	ResponseFormat string         `json:"response_format,omitempty" example:"url"`
	Stream         bool           `json:"stream,omitempty" example:"false"`
	PartialImages  int            `json:"partial_images,omitempty" example:"0"`
	StorageOptions map[string]any `json:"storage_options,omitempty"`
}

// SwaggerVideoGenerationRequest 表示视频生成请求。
type SwaggerVideoGenerationRequest struct {
	Model       string `json:"model" example:"grok-imagine-video"`
	Prompt      string `json:"prompt" example:"A cinematic tracking shot in the rain"`
	Duration    int    `json:"duration" example:"8"`
	AspectRatio string `json:"aspect_ratio,omitempty" example:"16:9"`
	Resolution  string `json:"resolution,omitempty" example:"720p"`
}

// SwaggerOpenAIVideoGenerationRequest 表示 OpenAI / NewAPI 兼容视频请求。
type SwaggerOpenAIVideoGenerationRequest struct {
	Model          string `json:"model" example:"grok-imagine-video"`
	Prompt         string `json:"prompt" example:"A cinematic tracking shot in the rain"`
	Seconds        string `json:"seconds,omitempty" example:"8"`
	Size           string `json:"size,omitempty" example:"1280x720"`
	InputReference string `json:"input_reference,omitempty" example:"https://example.com/source.png"`
}

// swaggerHealth godoc
// @Summary 存活检查
// @Tags System
// @Produce json
// @Success 200 {object} map[string]bool
// @Router /healthz [get]
func swaggerHealth() {}

// swaggerReady godoc
// @Summary 就绪检查
// @Tags System
// @Produce json
// @Success 200 {object} map[string]bool
// @Failure 503 {object} map[string]bool
// @Router /readyz [get]
func swaggerReady() {}

// swaggerModels godoc
// @Summary 获取可用模型
// @Tags Models
// @Security BearerAuth
// @Produce json
// @Success 200 {object} map[string]any
// @Failure 401 {object} map[string]any
// @Router /v1/models [get]
func swaggerModels() {}

// swaggerResponses godoc
// @Summary 创建 Response
// @Description 支持 JSON 与 SSE；stream=true 时返回 text/event-stream。
// @Tags Responses
// @Security BearerAuth
// @Accept json
// @Produce json
// @Param request body SwaggerResponsesRequest true "请求"
// @Success 200 {object} map[string]any
// @Failure 400 {object} map[string]any
// @Failure 401 {object} map[string]any
// @Router /v1/responses [post]
func swaggerResponses() {}

// swaggerCompactResponse godoc
// @Summary 压缩 Response 上下文
// @Tags Responses
// @Security BearerAuth
// @Accept json
// @Produce json
// @Param request body SwaggerResponsesRequest true "请求"
// @Success 200 {object} map[string]any
// @Router /v1/responses/compact [post]
func swaggerCompactResponse() {}

// swaggerGetResponse godoc
// @Summary 查询 Response
// @Tags Responses
// @Security BearerAuth
// @Produce json
// @Param response_id path string true "Response ID"
// @Success 200 {object} map[string]any
// @Failure 404 {object} map[string]any
// @Router /v1/responses/{response_id} [get]
func swaggerGetResponse() {}

// swaggerDeleteResponse godoc
// @Summary 删除 Response
// @Tags Responses
// @Security BearerAuth
// @Produce json
// @Param response_id path string true "Response ID"
// @Success 200 {object} map[string]any
// @Failure 404 {object} map[string]any
// @Router /v1/responses/{response_id} [delete]
func swaggerDeleteResponse() {}

// swaggerChat godoc
// @Summary 创建 Chat Completion
// @Description 支持 JSON 与 SSE、图片输入和函数工具。
// @Tags Chat
// @Security BearerAuth
// @Accept json
// @Produce json
// @Param request body SwaggerChatRequest true "请求"
// @Success 200 {object} map[string]any
// @Failure 400 {object} map[string]any
// @Router /v1/chat/completions [post]
func swaggerChat() {}

// swaggerMessages godoc
// @Summary 创建 Anthropic Message
// @Tags Messages
// @Security BearerAuth
// @Accept json
// @Produce json
// @Param anthropic-version header string true "Anthropic API version" default(2023-06-01)
// @Param request body SwaggerMessagesRequest true "请求"
// @Success 200 {object} map[string]any
// @Failure 400 {object} map[string]any
// @Router /v1/messages [post]
func swaggerMessages() {}

// swaggerGenerateImage godoc
// @Summary 生成图片
// @Description grok-imagine-image 在没有参考图时执行图片生成；提供 image、images 或 image_urls 后自动按 grok-imagine-image-edit 执行图片编辑。grok-imagine-image-edit 必须提供至少一张参考图。
// @Description image 接受 URL 字符串或 {"url":"..."} 对象；images 接受二者混合数组；image_urls 接受 URL 字符串数组。多字段按请求中出现顺序合并、去重。
// @Description 自动路由不改变 OpenAI Images 响应格式，url、b64_json 与扩展 SSE 流均保持兼容。
// @Tags Images
// @Security BearerAuth
// @Accept json
// @Produce json
// @Param request body SwaggerImageGenerationRequest true "请求"
// @Success 200 {object} map[string]any
// @Failure 400 {object} map[string]any
// @Router /v1/images/generations [post]
func swaggerGenerateImage() {}

// swaggerEditImage godoc
// @Summary 编辑图片
// @Description 接受 grok-imagine-image 或 grok-imagine-image-edit；前者会在渠道选择、计费和审计前规范为 grok-imagine-image-edit。请求必须提供至少一张参考图。
// @Description image 接受 URL 字符串或 {"url":"..."} 对象；images 接受二者混合数组；image_urls 接受 URL 字符串数组。多字段按请求中出现顺序合并、去重。
// @Description 同时接受 OpenAI SDK 使用的 multipart/form-data；image、image[]、images 或 images[] 可上传图片文件，image_urls 可继续提供 URL。mask 当前不支持。
// @Tags Images
// @Security BearerAuth
// @Accept json,mpfd
// @Produce json
// @Param request body SwaggerImageEditRequest true "请求"
// @Success 200 {object} map[string]any
// @Failure 400 {object} map[string]any
// @Router /v1/images/edits [post]
func swaggerEditImage() {}

// swaggerGetImage godoc
// @Summary 获取归档图片
// @Tags Images
// @Produce image/png
// @Param asset_id path string true "Asset ID"
// @Success 200 {file} binary
// @Failure 404
// @Router /v1/media/images/{asset_id} [get]
func swaggerGetImage() {}

// swaggerGenerateVideo godoc
// @Summary 创建异步视频任务
// @Tags Videos
// @Security BearerAuth
// @Accept json
// @Produce json
// @Param request body SwaggerVideoGenerationRequest true "请求"
// @Success 200 {object} map[string]string
// @Failure 400 {object} map[string]any
// @Router /v1/videos/generations [post]
func swaggerGenerateVideo() {}

// swaggerGenerateOpenAIVideo godoc
// @Summary 创建 OpenAI / NewAPI 兼容视频任务
// @Tags Videos
// @Security BearerAuth
// @Accept json,mpfd
// @Produce json
// @Param request body SwaggerOpenAIVideoGenerationRequest true "请求"
// @Success 200 {object} map[string]any
// @Failure 400 {object} map[string]any
// @Router /v1/videos [post]
func swaggerGenerateOpenAIVideo() {}

// swaggerGetVideo godoc
// @Summary 查询异步视频任务
// @Tags Videos
// @Security BearerAuth
// @Produce json
// @Param request_id path string true "Request ID"
// @Success 200 {object} map[string]any
// @Failure 404 {object} map[string]any
// @Router /v1/videos/{request_id} [get]
func swaggerGetVideo() {}

// swaggerGetVideoContent godoc
// @Summary 下载已完成视频内容
// @Tags Videos
// @Security BearerAuth
// @Produce video/mp4
// @Param request_id path string true "Request ID"
// @Success 200 {file} binary
// @Failure 404 {object} map[string]any
// @Router /v1/videos/{request_id}/content [get]
func swaggerGetVideoContent() {}
