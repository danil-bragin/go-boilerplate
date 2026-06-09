package kafka

// Record is the transport-level message passed to Producer.Produce and
// returned from consumer handlers. It is intentionally broker-agnostic so
// callers do not need to import franz-go directly.
type Record struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}
