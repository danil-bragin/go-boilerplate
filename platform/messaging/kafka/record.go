package kafka

import "github.com/twmb/franz-go/pkg/kgo"

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

// RecordFromKGO converts a *kgo.Record to the broker-agnostic Record type,
// flattening headers into a map. Shared by the consumer run loop, the
// transactional pipeline, and the tiered retry consumer — exported so callers
// that poll franz-go directly (e.g. custom consumers, tooling) do not have to
// reimplement the mapping.
func RecordFromKGO(rec *kgo.Record) Record {
	return Record{
		Topic:     rec.Topic,
		Key:       rec.Key,
		Value:     rec.Value,
		Headers:   HeadersFromKGO(rec.Headers),
		Partition: rec.Partition,
		Offset:    rec.Offset,
	}
}

// HeadersFromKGO converts a kgo header slice to the map form used by Record.
// Duplicate keys collapse to the last value.
func HeadersFromKGO(hs []kgo.RecordHeader) map[string]string {
	m := make(map[string]string, len(hs))
	for _, h := range hs {
		m[h.Key] = string(h.Value)
	}
	return m
}
