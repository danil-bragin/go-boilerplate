// Package i18n provides server-side localization of error messages built on
// go-i18n v2 and golang.org/x/text language matching.
//
// The contract with API clients is ALWAYS the machine-readable pair
// (code, params) on problem+json responses — server-side localization of
// title/detail is a courtesy for humans, never something clients parse.
//
// Message IDs are apperr error codes ("VALIDATION_FAILED", "ORDERS_…") plus
// "validation.<rule>" keys for per-field validator rules. Catalogs are TOML
// files in go-i18n format; templates use Go-template {{.param}} refs bound
// to apperr params. An optional "<code>.title" message overrides the
// problem title (the status text is kept when absent).
//
// Wiring: i18n.Middleware negotiates Accept-Language against the supported
// tags, stores the locale + cached localizer in the request context, and
// installs an httpx.ProblemLocalizer — httpx stays free of i18n imports
// (it owns the seam; this package provides the implementation).
package i18n

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"sync"

	"go-boilerplate/platform/web/httpx"

	"github.com/BurntSushi/toml"
	goi18n "github.com/nicksnyder/go-i18n/v2/i18n"
	"golang.org/x/text/language"
)

// catalogFS embeds the platform default catalogs: en (base) localizes every
// platform-registered code plus the common validation rules; ru is the demo
// translation. Services embed their own catalogs for their own codes.
//
//go:embed catalog/*.toml
var catalogFS embed.FS

// Bundle wraps a go-i18n bundle with a per-language localizer cache.
// Safe for concurrent use after construction.
type Bundle struct {
	bundle *goi18n.Bundle
	tags   []language.Tag // default (en) first, then load order

	mu         sync.RWMutex
	localizers map[language.Tag]*goi18n.Localizer
}

// New builds a Bundle from TOML catalog files in fsys. The language of each
// file is inferred from its name ("en.toml", "ru.toml"). language.English is
// the default/fallback language and should be the complete base catalog.
func New(fsys fs.FS, paths ...string) (*Bundle, error) {
	b := goi18n.NewBundle(language.English)
	b.RegisterUnmarshalFunc("toml", toml.Unmarshal)
	tags := []language.Tag{language.English}
	seen := map[language.Tag]bool{language.English: true}
	for _, p := range paths {
		f, err := b.LoadMessageFileFS(fsys, p)
		if err != nil {
			return nil, fmt.Errorf("i18n: load %s: %w", p, err)
		}
		if !seen[f.Tag] {
			seen[f.Tag] = true
			tags = append(tags, f.Tag)
		}
	}
	return &Bundle{
		bundle:     b,
		tags:       tags,
		localizers: make(map[language.Tag]*goi18n.Localizer, len(tags)),
	}, nil
}

// Default builds a Bundle from the embedded platform catalogs (en + ru).
// Services that own additional codes should ship their own catalog files
// and merge them via Load.
func Default() (*Bundle, error) {
	return New(catalogFS, "catalog/en.toml", "catalog/ru.toml")
}

// Load merges additional catalog files (e.g. a service's own codes) into
// the bundle. Not safe to call concurrently with request traffic — load
// everything during startup.
func (b *Bundle) Load(fsys fs.FS, paths ...string) error {
	for _, p := range paths {
		f, err := b.bundle.LoadMessageFileFS(fsys, p)
		if err != nil {
			return fmt.Errorf("i18n: load %s: %w", p, err)
		}
		known := false
		for _, t := range b.tags {
			if t == f.Tag {
				known = true
				break
			}
		}
		if !known {
			b.tags = append(b.tags, f.Tag)
		}
	}
	return nil
}

// Tags returns the bundle's languages, default (en) first.
func (b *Bundle) Tags() []language.Tag {
	out := make([]language.Tag, len(b.tags))
	copy(out, b.tags)
	return out
}

// localizer returns the cached localizer for tag, creating it on first use.
func (b *Bundle) localizer(tag language.Tag) *goi18n.Localizer {
	b.mu.RLock()
	loc, ok := b.localizers[tag]
	b.mu.RUnlock()
	if ok {
		return loc
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if loc, ok = b.localizers[tag]; ok {
		return loc
	}
	loc = goi18n.NewLocalizer(b.bundle, tag.String())
	b.localizers[tag] = loc
	return loc
}

type (
	localeKey    struct{}
	localizerKey struct{}
)

// Middleware negotiates Accept-Language against the supported tags (the
// bundle's languages when none are given; the FIRST tag is the fallback) and
// stores in the request context: the matched locale (Locale), the cached
// localizer (T), and an httpx.ProblemLocalizer so httpx.WriteError emits
// localized title/detail automatically.
func Middleware(b *Bundle, supported ...language.Tag) func(http.Handler) http.Handler {
	if len(supported) == 0 {
		supported = b.Tags()
	}
	matcher := language.NewMatcher(supported)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// ParseAcceptLanguage errors (malformed header) leave tags empty
			// → matcher picks the default. q-weights are honored by Match.
			tags, _, _ := language.ParseAcceptLanguage(r.Header.Get("Accept-Language"))
			_, idx, _ := matcher.Match(tags...)
			locale := supported[idx]
			loc := b.localizer(locale)

			ctx := r.Context()
			ctx = context.WithValue(ctx, localeKey{}, locale)
			ctx = context.WithValue(ctx, localizerKey{}, loc)
			ctx = httpx.WithProblemLocalizer(ctx, problemLocalizer(loc))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// Locale returns the locale negotiated by Middleware (language.English when
// no middleware ran).
func Locale(ctx context.Context) language.Tag {
	if t, ok := ctx.Value(localeKey{}).(language.Tag); ok {
		return t
	}
	return language.English
}

// T translates the message id (an error code or "validation.<rule>" key)
// with the given template params, using the request's negotiated localizer.
// Returns "" when no localizer is in ctx or the message is missing — the
// caller falls back to the developer message. params["count"], when
// present, selects the plural form.
func T(ctx context.Context, id string, params map[string]any) string {
	loc, ok := ctx.Value(localizerKey{}).(*goi18n.Localizer)
	if !ok {
		return ""
	}
	return localize(loc, id, params)
}

// localize resolves one message; missing messages yield "".
func localize(loc *goi18n.Localizer, id string, params map[string]any) string {
	cfg := &goi18n.LocalizeConfig{MessageID: id}
	if len(params) > 0 {
		cfg.TemplateData = params
		if c, ok := params["count"]; ok {
			cfg.PluralCount = c
		}
	}
	msg, err := loc.Localize(cfg)
	if err != nil && msg == "" {
		return ""
	}
	return msg
}

// problemLocalizer adapts a localizer to the httpx seam: detail is the
// message registered under the code itself (required for ok=true), title
// the optional "<code>.title" message.
func problemLocalizer(loc *goi18n.Localizer) httpx.ProblemLocalizer {
	return func(code string, params map[string]any) (title, detail string, ok bool) {
		detail = localize(loc, code, params)
		if detail == "" {
			return "", "", false
		}
		return localize(loc, code+".title", params), detail, true
	}
}
