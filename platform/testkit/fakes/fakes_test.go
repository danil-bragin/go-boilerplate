package fakes_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"go-boilerplate/platform/auth"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/testkit/fakes"

	"github.com/google/uuid"
)

var ctx = context.Background()

// ---------------------------------------------------------------------------
// Cache tests
// ---------------------------------------------------------------------------

func TestCache_SetGet(t *testing.T) {
	c := fakes.NewCache()
	c.Set(ctx, "k", []byte("hello"), time.Minute)
	got, ok := c.Get(ctx, "k")
	if !ok {
		t.Fatal("expected hit, got miss")
	}
	if string(got) != "hello" {
		t.Fatalf("expected %q, got %q", "hello", got)
	}
}

func TestCache_Miss(t *testing.T) {
	c := fakes.NewCache()
	_, ok := c.Get(ctx, "missing")
	if ok {
		t.Fatal("expected miss, got hit")
	}
}

func TestCache_GetReturnsCopy(t *testing.T) {
	c := fakes.NewCache()
	original := []byte("data")
	c.Set(ctx, "k", original, time.Minute)

	got, _ := c.Get(ctx, "k")
	// Mutate the returned slice.
	got[0] = 'X'

	// A second Get must still return the original value.
	got2, ok := c.Get(ctx, "k")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(got2) != "data" {
		t.Fatalf("mutation leaked into cache: got %q", got2)
	}
}

func TestCache_SetStoresCopy(t *testing.T) {
	c := fakes.NewCache()
	original := []byte("data")
	c.Set(ctx, "k", original, time.Minute)

	// Mutate the original slice after Set.
	original[0] = 'Z'

	got, _ := c.Get(ctx, "k")
	if string(got) != "data" {
		t.Fatalf("Set did not copy: got %q", got)
	}
}

// ---------------------------------------------------------------------------
// ObjectStore tests
// ---------------------------------------------------------------------------

func TestObjectStore_PutGet(t *testing.T) {
	s := fakes.NewObjectStore()
	content := []byte("blob content")
	if err := s.Put(ctx, "obj/a", bytes.NewReader(content), int64(len(content)), "text/plain"); err != nil {
		t.Fatal(err)
	}
	rc, err := s.Get(ctx, "obj/a")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("content mismatch: want %q got %q", content, got)
	}
}

func TestObjectStore_GetMissing(t *testing.T) {
	s := fakes.NewObjectStore()
	_, err := s.Get(ctx, "no-such-key")
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestObjectStore_Exists(t *testing.T) {
	s := fakes.NewObjectStore()
	ok, err := s.Exists(ctx, "k")
	if err != nil || ok {
		t.Fatalf("expected false/nil, got %v/%v", ok, err)
	}
	_ = s.Put(ctx, "k", strings.NewReader("v"), 1, "text/plain")
	ok, err = s.Exists(ctx, "k")
	if err != nil || !ok {
		t.Fatalf("expected true/nil, got %v/%v", ok, err)
	}
}

func TestObjectStore_Delete(t *testing.T) {
	s := fakes.NewObjectStore()
	_ = s.Put(ctx, "k", strings.NewReader("v"), 1, "text/plain")
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	ok, _ := s.Exists(ctx, "k")
	if ok {
		t.Fatal("key should be gone after Delete")
	}
	// Deleting again must not error.
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatal("second delete should be a no-op, got:", err)
	}
}

func TestObjectStore_ListByPrefix(t *testing.T) {
	s := fakes.NewObjectStore()
	for _, k := range []string{"a/1", "a/2", "b/1"} {
		_ = s.Put(ctx, k, strings.NewReader("x"), 1, "text/plain")
	}
	list, err := s.List(ctx, "a/")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(list)
	if len(list) != 2 || list[0] != "a/1" || list[1] != "a/2" {
		t.Fatalf("unexpected list: %v", list)
	}
}

func TestObjectStore_Presign(t *testing.T) {
	s := fakes.NewObjectStore()
	_ = s.Put(ctx, "img/photo.jpg", strings.NewReader("data"), 4, "image/jpeg")
	url, err := s.PresignGet(ctx, "img/photo.jpg", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(url, "https://fake-blob.local/") {
		t.Fatalf("unexpected presign URL: %q", url)
	}
}

func TestObjectStore_PresignMissing(t *testing.T) {
	s := fakes.NewObjectStore()
	_, err := s.PresignGet(ctx, "ghost", time.Hour)
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

// ---------------------------------------------------------------------------
// Publisher tests
// ---------------------------------------------------------------------------

func newMsg(eventType string) outbox.Message {
	return outbox.Message{
		ID:            uuid.New(),
		AggregateType: "test.entity",
		AggregateID:   "1",
		EventType:     eventType,
		Payload:       []byte(`{}`),
	}
}

func TestPublisher_Publish(t *testing.T) {
	p := fakes.NewPublisher()
	m := newMsg("created")
	if err := p.Publish(ctx, m); err != nil {
		t.Fatal(err)
	}
	msgs := p.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].EventType != "created" {
		t.Fatalf("wrong event type: %q", msgs[0].EventType)
	}
}

func TestPublisher_PublishBatch(t *testing.T) {
	p := fakes.NewPublisher()
	batch := []outbox.Message{newMsg("a"), newMsg("b"), newMsg("c")}
	if err := p.PublishBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if got := len(p.Messages()); got != 3 {
		t.Fatalf("expected 3, got %d", got)
	}
}

func TestPublisher_MessagesReturnsCopy(t *testing.T) {
	p := fakes.NewPublisher()
	_ = p.Publish(ctx, newMsg("x"))
	msgs := p.Messages()
	msgs[0].EventType = "mutated"
	// The internal store must be unaffected.
	if p.Messages()[0].EventType != "x" {
		t.Fatal("Messages() did not return a copy")
	}
}

func TestPublisher_FailNext_Publish(t *testing.T) {
	p := fakes.NewPublisher()
	p.FailNext = true
	if err := p.Publish(ctx, newMsg("e")); err == nil {
		t.Fatal("expected error")
	}
	// FailNext must be cleared.
	if err := p.Publish(ctx, newMsg("e2")); err != nil {
		t.Fatal("second publish should succeed:", err)
	}
	if len(p.Messages()) != 1 {
		t.Fatalf("expected 1 message after failed+success, got %d", len(p.Messages()))
	}
}

func TestPublisher_FailNext_PublishBatch(t *testing.T) {
	p := fakes.NewPublisher()
	p.FailNext = true
	if err := p.PublishBatch(ctx, []outbox.Message{newMsg("e")}); err == nil {
		t.Fatal("expected error")
	}
	// FailNext must be cleared.
	if err := p.PublishBatch(ctx, []outbox.Message{newMsg("e2")}); err != nil {
		t.Fatal("second batch should succeed:", err)
	}
	if len(p.Messages()) != 1 {
		t.Fatalf("expected 1 message after failed+success, got %d", len(p.Messages()))
	}
}

// ---------------------------------------------------------------------------
// Verifier tests
// ---------------------------------------------------------------------------

func TestVerifier_Default(t *testing.T) {
	v := fakes.NewVerifier()
	p, err := v.Verify(ctx, "some-token")
	if err != nil {
		t.Fatal(err)
	}
	if p.Subject != "test-subject" || p.Username != "test" {
		t.Fatalf("unexpected principal: %+v", p)
	}
	if len(p.Roles) != 1 || p.Roles[0] != "user" {
		t.Fatalf("unexpected roles: %v", p.Roles)
	}
}

func TestVerifier_EmptyToken(t *testing.T) {
	v := fakes.NewVerifier()
	_, err := v.Verify(ctx, "")
	if err == nil {
		t.Fatal("expected error for empty token")
	}
}

func TestVerifier_RejectToken(t *testing.T) {
	v := fakes.NewVerifier()
	v.RejectToken = true
	_, err := v.Verify(ctx, "valid-looking-token")
	if err == nil {
		t.Fatal("expected error when RejectToken=true")
	}
}

func TestVerifier_RejectTokenIsErrInvalidToken(t *testing.T) {
	v := fakes.NewVerifier()
	v.RejectToken = true
	_, err := v.Verify(ctx, "tok")
	if !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("expected auth.ErrInvalidToken, got: %v", err)
	}
}

func TestVerifier_CustomPrincipal(t *testing.T) {
	v := fakes.NewVerifier()
	v.Principal = auth.Principal{Subject: "admin-42", Roles: []string{"admin"}}
	p, err := v.Verify(ctx, "tok")
	if err != nil {
		t.Fatal(err)
	}
	if p.Subject != "admin-42" {
		t.Fatalf("expected custom subject, got %q", p.Subject)
	}
}
