package httpx_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"go-boilerplate/platform/httpx"
)

type createReq struct {
	Name  string `json:"name" validate:"required"`
	Email string `json:"email" validate:"required,email"`
}

func TestDecode_ValidPayload(t *testing.T) {
	r := httptest.NewRequest("POST", "/", strings.NewReader(`{"name":"a","email":"a@b.com"}`))
	got, err := httpx.Decode[createReq](r)
	require.NoError(t, err)
	require.Equal(t, "a", got.Name)
}

func TestDecode_InvalidPayloadReturnsValidationError(t *testing.T) {
	r := httptest.NewRequest("POST", "/", strings.NewReader(`{"name":"","email":"nope"}`))
	_, err := httpx.Decode[createReq](r)
	require.Error(t, err)

	var ve *httpx.ValidationError
	require.ErrorAs(t, err, &ve)
	require.Contains(t, ve.Fields, "Name")
	require.Contains(t, ve.Fields, "Email")
}

func TestDecode_MalformedJSON(t *testing.T) {
	r := httptest.NewRequest("POST", "/", strings.NewReader(`{`))
	_, err := httpx.Decode[createReq](r)
	require.Error(t, err)
}
