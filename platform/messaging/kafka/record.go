package kafka

// Record is the transport-level message passed to Producer.Produce and
// returned from consumer handlers. It is intentionally broker-agnostic so
// callers do not need to import franz-go directly.
type Record struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string

	// Partition and Offset identify the record's position in the source
	// topic. They are populated on the CONSUME side only (zero on produce —
	// the broker assigns them) and give consumers a stable fallback message
	// id ("topic:partition:offset") when no message-id header is present.
	Partition int32
	Offset    int64
}
