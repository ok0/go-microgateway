package middleware

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/ok0/lychgate/config"
)

func stub(name string, rec *[]string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			*rec = append(*rec, name)
			next.ServeHTTP(w, r)
		})
	}
}

func stagesFor(rec *[]string, names ...string) map[string]Middleware {
	m := make(map[string]Middleware, len(names))
	for _, n := range names {
		m[n] = stub(n, rec)
	}
	return m
}

func execute(order []string, stages map[string]Middleware, rec *[]string) {
	final := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*rec = append(*rec, "final")
	})
	Chain(final, Pipeline(order, stages)...).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
}

func TestDefaultStagesMatchConfig(t *testing.T) {
	if !slices.Equal(DefaultStages, config.BuiltinStages) {
		t.Errorf("DefaultStages = %v, config.BuiltinStages = %v", DefaultStages, config.BuiltinStages)
	}
}

func TestOrderDefault(t *testing.T) {
	order := Order([]string{"a", "b"}, nil)
	want := []string{"a", "b", StageTimeout, StageAuth, StageRateLimit, StageCircuitBreaker, StageRetry}
	if !slices.Equal(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	var rec []string
	stages := stagesFor(&rec, append([]string{"a", "b"}, DefaultStages...)...)
	execute(order, stages, &rec)
	wantRec := append(slices.Clone(want), "final")
	if !slices.Equal(rec, wantRec) {
		t.Errorf("executed = %v, want %v", rec, wantRec)
	}
}

func TestOrderExplicit(t *testing.T) {
	order := Order([]string{"a", "b", "c"}, []string{StageRateLimit, "b", StageTimeout, StageRetry})
	want := []string{"a", "c", StageRateLimit, "b", StageTimeout, StageRetry}
	if !slices.Equal(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	var rec []string
	stages := stagesFor(&rec, append([]string{"a", "b", "c"}, DefaultStages...)...)
	execute(order, stages, &rec)
	wantRec := append(slices.Clone(want), "final")
	if !slices.Equal(rec, wantRec) {
		t.Errorf("executed = %v, want %v", rec, wantRec)
	}
}

func TestPipelineSkipsUnconfigured(t *testing.T) {
	var rec []string
	stages := stagesFor(&rec, "a", StageRateLimit, StageCircuitBreaker, StageRetry)
	stages[StageTimeout] = nil
	order := Order([]string{"a"}, nil)
	execute(order, stages, &rec)
	want := []string{"a", StageRateLimit, StageCircuitBreaker, StageRetry, "final"}
	if !slices.Equal(rec, want) {
		t.Errorf("executed = %v, want %v", rec, want)
	}
}
