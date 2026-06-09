// Package fakes provides hand-written in-memory test doubles for platform
// interfaces: Cache, ObjectStore, Publisher/BatchPublisher, and Verifier.
// All fakes are safe for concurrent use.
package fakes

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"go-boilerplate/platform/auth"
	"go-boilerplate/platform/blob"
	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/outbox"
)

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

// Cache is an in-memory fake implementing cqrs.Cache.
// TTL values passed to Set are silently ignored.
type Cache struct {
	mu   sync.RWMutex
	data map[string][]byte
}

var _ cqrs.Cache = (*Cache)(nil)

// NewCache returns an initialised *Cache ready for use.
func NewCache() *Cache {
	return &Cache{data: make(map[string][]byte)}
}

// Get returns a copy of the stored value and true, or nil and false on a miss.
func (c *Cache) Get(_ context.Context, key string) ([]byte, bool) {
	c.mu.RLock()
	v, ok := c.data[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, true
}

// Set stores a copy of value under key. ttl is accepted but ignored.
func (c *Cache) Set(_ context.Context, key string, value []byte, _ time.Duration) {
	cp := make([]byte, len(value))
	copy(cp, value)
	c.mu.Lock()
	c.data[key] = cp
	c.mu.Unlock()
}

// ---------------------------------------------------------------------------
// ObjectStore
// ---------------------------------------------------------------------------

// ObjectStore is an in-memory fake implementing blob.ObjectStore.
type ObjectStore struct {
	mu           sync.RWMutex
	objects      map[string][]byte
	contentTypes map[string]string
}

var _ blob.ObjectStore = (*ObjectStore)(nil)

// NewObjectStore returns an initialised *ObjectStore ready for use.
func NewObjectStore() *ObjectStore {
	return &ObjectStore{
		objects:      make(map[string][]byte),
		contentTypes: make(map[string]string),
	}
}

// Put reads r fully and stores the bytes under key together with contentType.
// size is accepted but not validated.
func (s *ObjectStore) Put(_ context.Context, key string, r io.Reader, _ int64, contentType string) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("fakes: ObjectStore.Put read: %w", err)
	}
	s.mu.Lock()
	s.objects[key] = data
	s.contentTypes[key] = contentType
	s.mu.Unlock()
	return nil
}

// Get returns an io.ReadCloser over the stored bytes, or an error if the key
// does not exist.
func (s *ObjectStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	s.mu.RLock()
	data, ok := s.objects[key]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("fakes: ObjectStore.Get: key %q not found", key)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return io.NopCloser(bytes.NewReader(cp)), nil
}

// Delete removes the object at key. Deleting a non-existent key is a no-op.
func (s *ObjectStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	delete(s.objects, key)
	delete(s.contentTypes, key)
	s.mu.Unlock()
	return nil
}

// Exists reports whether an object exists under key.
func (s *ObjectStore) Exists(_ context.Context, key string) (bool, error) {
	s.mu.RLock()
	_, ok := s.objects[key]
	s.mu.RUnlock()
	return ok, nil
}

// PresignGet returns "https://fake-blob.local/<key>" regardless of TTL.
// It returns an error if the key does not exist.
func (s *ObjectStore) PresignGet(_ context.Context, key string, _ time.Duration) (string, error) {
	s.mu.RLock()
	_, ok := s.objects[key]
	s.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("fakes: ObjectStore.PresignGet: key %q not found", key)
	}
	return "https://fake-blob.local/" + key, nil
}

// List returns all keys whose names start with prefix.
func (s *ObjectStore) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var keys []string
	for k := range s.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

// ---------------------------------------------------------------------------
// Publisher
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Verifier
// ---------------------------------------------------------------------------

// Verifier is an in-memory fake implementing auth.Verifier.
//
// By default Verify returns the Principal stored in the Principal field.
// Set RejectToken to true to make Verify return auth.ErrInvalidToken.
// An empty rawToken always returns auth.ErrInvalidToken regardless of
// RejectToken.
type Verifier struct {
	Principal   auth.Principal
	RejectToken bool
}

var _ auth.Verifier = (*Verifier)(nil)

// NewVerifier returns a *Verifier pre-configured with a sensible default
// principal (Subject: "test-subject", Username: "test", Roles: ["user"]).
func NewVerifier() *Verifier {
	return &Verifier{
		Principal: auth.Principal{
			Subject:  "test-subject",
			Username: "test",
			Roles:    []string{"user"},
		},
	}
}

// Verify returns the configured Principal, or auth.ErrInvalidToken when
// rawToken is empty or RejectToken is true.
func (v *Verifier) Verify(_ context.Context, rawToken string) (auth.Principal, error) {
	if rawToken == "" || v.RejectToken {
		return auth.Principal{}, auth.ErrInvalidToken
	}
	return v.Principal, nil
}
