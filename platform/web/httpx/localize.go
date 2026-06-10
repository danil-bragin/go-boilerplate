package httpx

import "context"

// ProblemLocalizer translates a stable error code (+ its params) into a
// human-readable title and detail for the request's negotiated locale.
// ok=false means "no translation" — the caller keeps the developer message.
//
// This is the localization seam: httpx OWNS the context key and the lookup
// so that it never imports an i18n library; platform/i18n.Middleware
// installs an implementation per request. Localization only ever touches
// the human-readable members (title/detail) — code and params are the
// machine-readable contract and stay locale-independent.
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
