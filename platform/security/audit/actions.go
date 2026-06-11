package audit

// Action codes for the security-event audit entries emitted on the denial /
// admin-read / attachment-access paths (round-8 B3). Command-success audits use
// domain-specific actions (e.g. "order:create"); these are the cross-cutting
// security events recorded out-of-band via RecordOutOfBand.
const (
	// ActionAuthzDenied is recorded when a VALID principal is denied by an
	// authorization check (RBAC role gate or resource ownership). Anonymous
	// 401s are NOT audited here — only a present-but-insufficient principal —
	// so an unauthenticated request flood cannot fill the audit log.
	ActionAuthzDenied = "authz.denied"

	// ActionAuditRead is recorded when an admin reads the audit trail
	// (GET /v1/audit — the DSAR / forensics read path). The read audits
	// itself: who looked at whose trail, and when.
	ActionAuditRead = "audit.read"

	// ActionAuditVerify is recorded when an admin runs the chain-integrity
	// check (GET /v1/audit/verify).
	ActionAuditVerify = "audit.verify"

	// ActionAttachmentUpload / ActionAttachmentDownload are recorded on
	// successful attachment access so the audit trail covers object I/O, not
	// just command writes.
	ActionAttachmentUpload   = "attachment.upload"
	ActionAttachmentDownload = "attachment.download"
)
