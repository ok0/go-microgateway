package httpcache

import (
	"net/http"
	"time"
)

// Entry is a stored response. Stored Header always carries a valid Date (see DateToAdd).
// Stale is the soft-purge mark.
type Entry struct {
	Status     int
	Header     http.Header
	Body       []byte
	InitialAge time.Duration
	Stale      bool
}

// Action is the decision for a request against the stored entry.
type Action uint8

const (
	Fresh Action = iota + 1
	ServeStale
	StaleRevalidateBackground
	Revalidate
	Miss
	Unsatisfiable
)

func (a Action) String() string {
	switch a {
	case Fresh:
		return "fresh"
	case ServeStale:
		return "serve-stale"
	case StaleRevalidateBackground:
		return "stale-revalidate-background"
	case Revalidate:
		return "revalidate"
	case Miss:
		return "miss"
	case Unsatisfiable:
		return "unsatisfiable"
	}
	return "unknown"
}

// Decide returns the action for req given the stored entry e and its current age.
func Decide(req *http.Request, e *Entry, age time.Duration, o Options) Action {
	age = clamp(age, 0, MaxDelta)
	rd := RequestDirectives(req.Header)
	a := Miss
	if e != nil {
		a = decideStored(rd, e, age, o)
	}
	if rd.OnlyIfCached {
		switch a {
		case Revalidate, Miss:
			a = Unsatisfiable
		case StaleRevalidateBackground:
			a = ServeStale
		}
	}
	return a
}

func decideStored(rd Directives, e *Entry, age time.Duration, o Options) Action {
	d := ParseCacheControl(e.Header)
	l := lifetime(d, e.Header, time.Time{}, false, o)
	swr, _ := staleWindows(d, o)
	rem := Remaining(l, age)
	switch {
	case rd.NoCache || d.NoCache || e.Stale:
		return Revalidate
	case rd.MaxAge.Present && age > rd.MaxAge.Value:
		return Revalidate
	case rd.MinFresh.Present && !(rem > 0 && rem >= rd.MinFresh.Value):
		return Revalidate
	case rem > 0:
		return Fresh
	case d.MustRevalidate || d.ProxyRevalidate:
		return Revalidate
	}
	staleAge := age - l
	switch {
	case rd.MaxStale.Present && !d.SMaxAge.Present && staleAge <= rd.MaxStale.Value:
		return ServeStale
	case staleAge < swr:
		return StaleRevalidateBackground
	}
	return Revalidate
}

// StaleOnError reports whether a stale entry may answer a failed revalidation.
func StaleOnError(e *Entry, age time.Duration, o Options) bool {
	if e == nil || e.Stale {
		return false
	}
	d := ParseCacheControl(e.Header)
	if d.NoCache || d.MustRevalidate || d.ProxyRevalidate {
		return false
	}
	l := lifetime(d, e.Header, time.Time{}, false, o)
	_, sie := staleWindows(d, o)
	return age >= l && age-l < sie
}
