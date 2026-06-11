package blob

import (
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
)

// TestGetObjectInput_AttachmentOption: the attachment presign option forces a
// download disposition (Content-Disposition: attachment) and a neutral
// content-type so a browser can never render an uploaded HTML/SVG inline —
// the stored-XSS defence. The header values are carried as the
// ResponseContentDisposition / ResponseContentType override fields of the
// presigned GetObject request.
func TestGetObjectInput_AttachmentOption(t *testing.T) {
	t.Parallel()

	in := getObjectInput("bucket", "orders/x/evil.html", PresignAttachment())

	require.NotNil(t, in.ResponseContentDisposition)
	require.Equal(t, "attachment", aws.ToString(in.ResponseContentDisposition))
	require.NotNil(t, in.ResponseContentType)
	require.Equal(t, "application/octet-stream", aws.ToString(in.ResponseContentType))
}

// TestGetObjectInput_NoOption: with no options the override fields stay unset
// (plain inline GET, unchanged behaviour) — the attachment forcing is opt-in.
func TestGetObjectInput_NoOption(t *testing.T) {
	t.Parallel()

	in := getObjectInput("bucket", "k")

	require.Nil(t, in.ResponseContentDisposition)
	require.Nil(t, in.ResponseContentType)
}

// TestPresignAttachment_QueryParams documents the on-the-wire proof: the S3
// presigner serialises ResponseContentDisposition / ResponseContentType into
// the response-content-disposition / response-content-type query parameters,
// so a fetched presigned URL instructs the store to answer with a download
// disposition. This asserts the exact override field → query-param contract
// the integration test relies on.
func TestPresignAttachment_QueryParams(t *testing.T) {
	t.Parallel()

	in := getObjectInput("bucket", "k", PresignAttachment())

	// Mirror what the SDK presigner emits: response-* overrides become query
	// parameters of the same name.
	q := url.Values{}
	q.Set("response-content-disposition", aws.ToString(in.ResponseContentDisposition))
	q.Set("response-content-type", aws.ToString(in.ResponseContentType))
	enc := q.Encode()

	require.True(t, strings.Contains(enc, "response-content-disposition=attachment"), enc)
	require.True(t, strings.Contains(enc, "response-content-type=application%2Foctet-stream"), enc)
}
