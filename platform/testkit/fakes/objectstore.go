package fakes

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"go-boilerplate/platform/blob"
)

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
