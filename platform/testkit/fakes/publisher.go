package fakes

import (
	"context"
	"errors"
	"sync"

	"go-boilerplate/platform/outbox"
)

// Publisher is an in-memory fake implementing both outbox.Publisher and
// outbox.BatchPublisher.
//
// FailNext, when set to true, causes the next call to Publish or PublishBatch
// to return an error; the flag is reset to false afterwards.
type Publisher struct {
	mu       sync.Mutex
	messages []outbox.Message
	FailNext bool
}

var (
	_ outbox.Publisher      = (*Publisher)(nil)
	_ outbox.BatchPublisher = (*Publisher)(nil)
)

// NewPublisher returns an initialised *Publisher ready for use.
func NewPublisher() *Publisher {
	return &Publisher{}
}

// Publish appends msg to the internal slice. If FailNext is true it returns an
// error and clears FailNext without storing the message.
func (p *Publisher) Publish(_ context.Context, msg outbox.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.FailNext {
		p.FailNext = false
		return errors.New("fakes: Publisher.Publish: injected failure")
	}
	p.messages = append(p.messages, msg)
	return nil
}

// PublishBatch appends all msgs. If FailNext is true it returns an error and
// clears FailNext without storing any message.
func (p *Publisher) PublishBatch(_ context.Context, msgs []outbox.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.FailNext {
		p.FailNext = false
		return errors.New("fakes: Publisher.PublishBatch: injected failure")
	}
	p.messages = append(p.messages, msgs...)
	return nil
}

// Messages returns a copy of all collected messages.
func (p *Publisher) Messages() []outbox.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make([]outbox.Message, len(p.messages))
	copy(cp, p.messages)
	return cp
}
