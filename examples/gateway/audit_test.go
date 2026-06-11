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
		// Use bob (non-admin) here so the resulting authz.denied audit row is
		// attributed to bob, not alice — the alice read-assertions below count
		// alice's order:create rows exactly and must not see a denial row.
		status, body := getAudit(t, baseURL, "bob", q) // bob holds only "user"
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

// getVerify GETs /v1/audit/verify with the given token, returning status + body.
func getVerify(t *testing.T, baseURL, token string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		baseURL+"/v1/audit/verify", http.NoBody)
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

// auditRowsByAction reads (actor, subject) for all audit_log rows with the
// given action, ordered by id. Used by the audit-on-denial tests.
func auditRowsByAction(t *testing.T, dsn, action string) [][2]string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx,
		`select actor, subject from audit_log where action = $1 order by id`, action)
	require.NoError(t, err)
	defer rows.Close()
	var out [][2]string
	for rows.Next() {
		var actor, subject string
		require.NoError(t, rows.Scan(&actor, &subject))
		out = append(out, [2]string{actor, subject})
	}
	require.NoError(t, rows.Err())
	return out
}

// waitForAuditRow polls until at least one row with the given action exists
// (the denial/read audits are written out-of-band on a separate connection,
// so they may land a beat after the HTTP response returns).
func waitForAuditRow(t *testing.T, dsn, action string) [][2]string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows := auditRowsByAction(t, dsn, action)
		if len(rows) > 0 {
			return rows
		}
		if time.Now().After(deadline) {
			t.Fatalf("no audit_log row with action=%q appeared within deadline", action)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestGateway_AuditOnDenial proves B3: a valid principal denied by authorization
// produces an authz.denied audit row attributed to that principal, and an admin
// DSAR read produces an audit.read row — both written out-of-band.
func TestGateway_AuditOnDenial(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startAppWithVerifier(t, broker, dsn, multiUserVerifier{})

	t.Run("ownership-denied read → authz.denied audit row", func(t *testing.T) {
		// alice (non-admin) tries to read bob's audit trail → 403.
		status, _ := getAudit(t, baseURL, "alice", url.Values{"actor": {"bob"}})
		require.Equal(t, http.StatusForbidden, status)

		rows := waitForAuditRow(t, dsn, "authz.denied")
		var found bool
		for _, r := range rows {
			if r[0] == "alice" {
				found = true
			}
		}
		assert.True(t, found, "authz.denied row attributed to alice must exist; got %v", rows)
	})

	t.Run("admin read → audit.read row", func(t *testing.T) {
		status, _ := getAudit(t, baseURL, "root", url.Values{"actor": {"alice"}})
		require.Equal(t, http.StatusOK, status)

		rows := waitForAuditRow(t, dsn, "audit.read")
		var found bool
		for _, r := range rows {
			if r[0] == "root" && r[1] == "alice" {
				found = true
			}
		}
		assert.True(t, found, "audit.read row (actor=root subject=alice) must exist; got %v", rows)
	})

	t.Run("anonymous 401 is NOT audited", func(t *testing.T) {
		before := len(auditRowsByAction(t, dsn, "authz.denied"))
		status, _ := getAudit(t, baseURL, "", url.Values{"actor": {"alice"}})
		require.Equal(t, http.StatusUnauthorized, status)
		// Give any (erroneous) out-of-band write a chance to land.
		time.Sleep(300 * time.Millisecond)
		after := len(auditRowsByAction(t, dsn, "authz.denied"))
		assert.Equal(t, before, after, "anonymous 401 must NOT produce an authz.denied audit row")
	})
}

// TestGateway_AuditVerifyEndpoint covers the admin-only audit-chain integrity
// endpoint: RBAC (401/403) and that an untampered chain (here: an empty fresh
// DB) verifies OK. Deep chain/tamper correctness is proven at the platform
// level (platform/security/audit chain_test).
func TestGateway_AuditVerifyEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, _ := kafkatest.Shared(t)
	dsn := pgtest.SharedDSN(t)
	baseURL := startAppWithVerifier(t, broker, dsn, multiUserVerifier{})

	t.Run("no token → 401", func(t *testing.T) {
		status, _ := getVerify(t, baseURL, "")
		require.Equal(t, http.StatusUnauthorized, status)
	})

	t.Run("non-admin → 403", func(t *testing.T) {
		status, _ := getVerify(t, baseURL, "alice")
		require.Equal(t, http.StatusForbidden, status)
	})

	t.Run("admin → 200, chain verifies", func(t *testing.T) {
		status, body := getVerify(t, baseURL, "root")
		require.Equal(t, http.StatusOK, status)
		var res struct {
			Ok       bool   `json:"ok"`
			Verified int    `json:"verified"`
			BreakID  *int64 `json:"break_id"`
		}
		require.NoError(t, json.Unmarshal(body, &res))
		assert.True(t, res.Ok, "fresh chain must verify")
		assert.Nil(t, res.BreakID)
	})
}
