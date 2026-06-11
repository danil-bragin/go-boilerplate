package httpx

import "context"

// ProblemLocalizer translates a stable error code (+ its params) into a
// human-readable title and detail for the request's negotiated locale.
// ok=false means "no translation" — the caller keeps the developer message.
//
// This is the localization seam: httpx OWNS the context key and the lookup
// so that it never imports an i18n library; platform/i18n.Middleware
// installs an implementation per request. Localization only ever touches
// the human-readable members (title/detail and, for VALIDATION_FAILED, the
// legacy per-field errors map via "validation.<rule>" keys) — code and
// params are the machine-readable contract and stay locale-independent.
type ProblemLocalizer func(code string, params map[string]any) (title, detail string, ok bool)

type problemLocalizerKey struct{}

// WithProblemLocalizer installs l into ctx (used by i18n middleware).
func WithProblemLocalizer(ctx context.Context, l ProblemLocalizer) context.Context {
	return context.WithValue(ctx, problemLocalizerKey{}, l)
}

// ProblemLocalizerFrom returns the request's ProblemLocalizer, if any.
func ProblemLocalizerFrom(ctx context.Context) (ProblemLocalizer, bool) {
	l, ok := ctx.Value(problemLocalizerKey{}).(ProblemLocalizer)
	return l, ok && l != nil
}

// localizeFieldErrors renders the per-field messages of a VALIDATION_FAILED
// problem through the localizer using the "validation.<rule>" catalog keys
// (params: field/rule/param from Params["fields"], the structured contract).
// Fields whose rule has no translation keep their English developer string.
// The input map is never mutated — it aliases ValidationError.Fields.
func localizeFieldErrors(loc ProblemLocalizer, errs map[string]string, params map[string]any) map[string]string {
	fields, ok := params["fields"].([]map[string]any)
	if !ok || len(errs) == 0 {
		return errs
	}
	out := make(map[string]string, len(errs))
	for k, v := range errs {
		out[k] = v
	}
	for _, f := range fields {
		field, _ := f["field"].(string)
		rule, _ := f["rule"].(string)
		if field == "" || rule == "" {
			continue
		}
		if _, detail, ok := loc("validation."+rule, f); ok && detail != "" {
			out[field] = detail
		}
	}
	return out
}
