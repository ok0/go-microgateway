package main

import (
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
)

func TestReloadCountsEncoderOutput(t *testing.T) {
	ups := upstreams{A: "http://127.0.0.1:1", B: "http://127.0.0.1:2"}
	g, err := microgateway.NewFromYAML(render(stepBaseline, ups, renderOpts{}), microgateway.WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()

	scrapeCounts := func() (float64, float64, float64) {
		t.Helper()
		rec := httptest.NewRecorder()
		g.AdminHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("metrics status %d", rec.Code)
		}
		s, f, u, err := reloadCounts(rec.Body.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		return s, f, u
	}

	if s, f, u := scrapeCounts(); s != 0 || f != 0 || u != 0 {
		t.Fatalf("initial counts = %v %v %v, want 0 0 0", s, f, u)
	}

	err = g.ReloadYAML(render(stepInvalid, ups, renderOpts{}))
	var ve *config.ValidationError
	if err == nil || !errors.As(err, &ve) {
		t.Fatalf("invalid reload error = %v, want a *config.ValidationError", err)
	}
	if s, f, _ := scrapeCounts(); s != 0 || f != 1 {
		t.Fatalf("after invalid reload success %v failure %v, want 0 1", s, f)
	}

	if err := g.ReloadYAML(render(stepRoute, ups, renderOpts{})); err != nil {
		t.Fatalf("route reload: %v", err)
	}
	if s, f, _ := scrapeCounts(); s != 1 || f != 1 {
		t.Fatalf("after route reload success %v failure %v, want 1 1", s, f)
	}
}

func TestReloadCountsLabelOrder(t *testing.T) {
	text := `# HELP microgateway_config_reloads_total Configuration reload attempts, by result.
# TYPE microgateway_config_reloads_total counter
microgateway_config_reloads_total{zone="x",result="failure"} 2
microgateway_config_reloads_total{result="success",zone="a\",b"} 3
microgateway_config_reloads_total{result="unchanged"} 4
microgateway_config_reloads_total_other{result="success"} 9
`
	s, f, u, err := reloadCounts([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	if s != 3 || f != 2 || u != 4 {
		t.Fatalf("counts = %v %v %v, want 3 2 4", s, f, u)
	}
}

func TestReloadCountsAbsent(t *testing.T) {
	cases := map[string]string{
		"empty":        "",
		"success only": `microgateway_config_reloads_total{result="success"} 1` + "\n",
		"decoy only":   `microgateway_config_reloads_total_other{result="success"} 9` + "\n",
	}
	for name, text := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, _, err := reloadCounts([]byte(text))
			if err == nil || !strings.Contains(err.Error(), "not found") {
				t.Fatalf("error = %v, want not found", err)
			}
		})
	}
}

func TestReloadCountsMalformed(t *testing.T) {
	cases := map[string]string{
		"value x":            `microgateway_config_reloads_total{result="success"} x` + "\n",
		"unterminated quote": `microgateway_config_reloads_total{result="success} 1` + "\n",
	}
	for name, text := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, _, err := reloadCounts([]byte(text))
			if err == nil {
				t.Fatal("error = nil, want a malformed-line error")
			}
		})
	}
}
