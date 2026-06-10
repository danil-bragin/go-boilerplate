package kafka

// Standard record-header names shared by every producer and consumer in this
// repository. These are WIRE VALUES: records already in flight (and in DLTs)
// carry them, so the strings must never change — add new headers instead.
const (
	// HeaderMessageID carries the stable message identity used for inbox
	// deduplication. The outbox publisher sets it to the outbox row id;
	// consumers fall back to "topic:partition:offset" when absent.
	HeaderMessageID = "message-id"

	// HeaderEventType carries the versioned event-type name (e.g.
	// "orders.OrderCreated.v1") used for consumer-side dispatch.
	HeaderEventType = "event-type"
)

// Dead-letter diagnostic headers written by WithRetry (the blocking
// in-process retry → DLT path). The tiered retry escalator uses the separate
// "retry-*" vocabulary — see platform/messaging/retry (HeaderAttempt,
// HeaderOrigTopic, HeaderDueAt, HeaderLastError). Both vocabularies are wire
// values and must never change.
const (
	// HeaderDLTError is the final handler error that dead-lettered the record.
	HeaderDLTError = "x-error"

	// HeaderDLTAttempts is the number of in-process attempts performed.
	HeaderDLTAttempts = "x-attempts"

	// HeaderDLTOriginalTopic is the topic the record was consumed from before
	// dead-lettering; cmd/redrive republishes to it.
	HeaderDLTOriginalTopic = "x-original-topic"

	// HeaderDLTErrorCode is the apperr code of the error that dead-lettered
	// the record (set by both WithRetry and the tiered-retry escalator when
	// the failure chain contains an apperr.Error). Lets operators triage DLTs
	// by code without parsing x-error strings.
	HeaderDLTErrorCode = "x-error-code"
)
