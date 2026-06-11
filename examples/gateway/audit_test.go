package gateway_test

// Integration tests for the admin-only audit read endpoint
// GET /v1/audit?actor=&since=&limit= (the DSAR read path; D3): RBAC —
// 401 without a token, 403 for non-admin principals — and the query
// semantics (actor isolation, newest-first ordering, inclusive since,
// limit keeps the newest rows).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"go-boilerplate/platform/messaging/kafka/kafkatest"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedAudit inserts one audit_log row with an explicit created_at into the
// gateway database (the app's migrations created the table on startup).
func seedAudit(t *testing.T, dsn, actor, action, subject string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx,
		`insert into audit_log (actor, action, subject, metadata, created_at) values ($1, $2, $3, '{"src":"test"}', $4)`,
		actor, action, subject, at)
	require.NoError(t, err)
}

// getAudit GETs /v1/audit with the given token and query params, returning
// the status code and raw body.
func getAudit(t *testing.T, baseURL, token string, params url.Values) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		baseURL+"/v1/audit?"+params.Encode(), http.NoBody)
	require.NoError(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var body json.RawMessage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	return resp.StatusCode, body
}

type auditItem struct {
	Actor    string            `json:"actor"`
	Action   string            `json:"action"`
	Subject  string            `json:"subject"`
	Metadata map[string]string `json:"metadata"`
	At       string            `json:"at"`
}

func decodeAuditItems(t *testing.T, body []byte) []auditItem {
	t.Helper()
	var out struct {
		Items []auditItem `json:"items"`
	}
	require.NoError(t, json.Unmarshal(body, &out))
	return out.Items
}

func TestGateway_AuditEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)

	baseURL := startAppWithVerifier(t, broker, dsn, multiUserVerifier{})

	base := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	seedAudit(t, dsn, "alice", "order:create", "order-a1", base)
	seedAudit(t, dsn, "alice", "order:create", "order-a2", base.Add(1*time.Hour))
	seedAudit(t, dsn, "alice", "order:create", "order-a3", base.Add(2*time.Hour))
	seedAudit(t, dsn, "bob", "order:create", "order-b1", base.Add(30*time.Minute))

	q := url.Values{"actor": {"alice"}}

	t.Run("no token → 401", func(t *testing.T) {
		status, body := getAudit(t, baseURL, "", q)
		require.Equal(t, http.StatusUnauthorized, status)
		var prob struct {
			Code string `json:"code"`
		}
		require.NoError(t, json.Unmarshal(body, &prob))
		assert.Equal(t, "AUTH_UNAUTHENTICATED", prob.Code)
	})

	t.Run("non-admin → 403", func(t *testing.T) {
		status, body := getAudit(t, baseURL, "alice", q) // alice holds only "user"
		require.Equal(t, http.StatusForbidden, status)
		var prob struct {
			Code string `json:"code"`
		}
		require.NoError(t, json.Unmarshal(body, &prob))
		assert.Equal(t, "AUTH_FORBIDDEN", prob.Code)
	})

	t.Run("admin → 200, actor-isolated, newest first", func(t *testing.T) {
		status, body := getAudit(t, baseURL, "root", q)
		require.Equal(t, http.StatusOK, status)
		items := decodeAuditItems(t, body)
		require.Len(t, items, 3, "only alice's entries — bob's are invisible")
		assert.Equal(t, []string{"order-a3", "order-a2", "order-a1"},
			[]string{items[0].Subject, items[1].Subject, items[2].Subject},
			"newest first")
		assert.Equal(t, "alice", items[0].Actor)
		assert.Equal(t, "order:create", items[0].Action)
		assert.Equal(t, map[string]string{"src": "test"}, items[0].Metadata)
		assert.Equal(t, base.Add(2*time.Hour).Format(time.RFC3339), items[0].At)
	})

	t.Run("since is inclusive", func(t *testing.T) {
		qs := url.Values{
			"actor": {"alice"},
			"since": {base.Add(1 * time.Hour).Format(time.RFC3339)},
		}
		status, body := getAudit(t, baseURL, "root", qs)
		require.Equal(t, http.StatusOK, status)
		items := decodeAuditItems(t, body)
		require.Len(t, items, 2)
		assert.Equal(t, "order-a3", items[0].Subject)
		assert.Equal(t, "order-a2", items[1].Subject)
	})

	t.Run("limit keeps the newest", func(t *testing.T) {
		qs := url.Values{"actor": {"alice"}, "limit": {"1"}}
		status, body := getAudit(t, baseURL, "root", qs)
		require.Equal(t, http.StatusOK, status)
		items := decodeAuditItems(t, body)
		require.Len(t, items, 1)
		assert.Equal(t, "order-a3", items[0].Subject)
	})

	t.Run("malformed since → 400", func(t *testing.T) {
		qs := url.Values{"actor": {"alice"}, "since": {"yesterday"}}
		status, body := getAudit(t, baseURL, "root", qs)
		require.Equal(t, http.StatusBadRequest, status)
		var prob struct {
			Code string `json:"code"`
		}
		require.NoError(t, json.Unmarshal(body, &prob))
		assert.Equal(t, "GATEWAY_MALFORMED_REQUEST", prob.Code)
	})

	t.Run("missing actor → 400", func(t *testing.T) {
		status, _ := getAudit(t, baseURL, "root", url.Values{})
		require.Equal(t, http.StatusBadRequest, status)
	})
}
