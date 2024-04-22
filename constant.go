package natsq

const (
	HeaderPublishId = "PublishId"
)

type ContextKey string

const (
	ContextStartedAt = ContextKey("startedAt")
	ContextPublishId = ContextKey("publishId")
)
