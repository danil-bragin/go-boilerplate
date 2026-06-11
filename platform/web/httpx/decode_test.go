package httpx_test

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go-boilerplate/platform/web/httpx"

	"github.com/stretchr/testify/require"
)

type createReq struct {
	Name  string `json:"name" validate:"required"`
	Email string `json:"email" validate:"required,email"`
}

func TestDecode_ValidPayload(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"a","email":"a@b.com"}`))
	got, err := httpx.Decode[createReq](r)
	require.NoError(t, err)
	require.Equal(t, "a", got.Name)
}

func TestDecode_InvalidPayloadReturnsValidationError(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"","email":"nope"}`))
	_, err := httpx.Decode[createReq](r)
	require.Error(t, err)

	var ve *httpx.ValidationError
	require.ErrorAs(t, err, &ve)
	require.Contains(t, ve.Fields, "name")
	require.Contains(t, ve.Fields, "email")
}

func TestDecode_MalformedJSON(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{`))
	_, err := httpx.Decode[createReq](r)
	require.Error(t, err)
}

type snakeReq struct {
	UserName string `json:"user_name" validate:"required"`
}

func TestDecode_ValidationErrorUsesJSONTagNames(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"user_name":""}`))
	_, err := httpx.Decode[snakeReq](r)
	require.Error(t, err)

	var ve *httpx.ValidationError
	require.ErrorAs(t, err, &ve)
	require.Contains(t, ve.Fields, "user_name") // JSON tag name, not "UserName"
	require.NotContains(t, ve.Fields, "UserName")
}

// FIX 1 — nil Body must return an error, not panic.
func TestDecode_NilBody(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	r.Body = nil
	_, err := httpx.Decode[createReq](r)
	require.Error(t, err)
}

// FIX 2 — trailing data after a valid JSON value must be rejected.
func TestDecode_TrailingData(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"a","email":"a@b.com"}EXTRA`))
	r.Header.Set("Content-Type", "application/json")
	_, err := httpx.Decode[createReq](r)
	require.Error(t, err)
}

// FIX 3 — bodies larger than MaxBodyBytes must be rejected.
func TestDecode_BodyTooLarge(t *testing.T) {
	// Build a JSON object with a "name" field containing just over 1 MiB of data.
	// The JSON value itself must be syntactically valid up to the limit.
	padding := bytes.Repeat([]byte("a"), int(httpx.MaxBodyBytes)+100)
	body := fmt.Sprintf(`{"name":%q,"email":"a@b.com"}`, string(padding))
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	_, err := httpx.Decode[createReq](r)
	require.Error(t, err)
}

// FIX 4 — Content-Type enforcement.
func TestDecode_ContentTypeTextPlain_Rejected(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"a","email":"a@b.com"}`))
	r.Header.Set("Content-Type", "text/plain")
	_, err := httpx.Decode[createReq](r)
	require.Error(t, err)
	require.True(t, errors.Is(err, httpx.ErrUnsupportedMediaType))
}

func TestDecode_ContentTypeJSONWithCharset_Accepted(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"a","email":"a@b.com"}`))
	r.Header.Set("Content-Type", "application/json; charset=utf-8")
	got, err := httpx.Decode[createReq](r)
	require.NoError(t, err)
	require.Equal(t, "a", got.Name)
}

func TestDecode_NoContentType_Accepted(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"a","email":"a@b.com"}`))
	// explicitly no Content-Type header
	r.Header.Del("Content-Type")
	got, err := httpx.Decode[createReq](r)
	require.NoError(t, err)
	require.Equal(t, "a", got.Name)
}
