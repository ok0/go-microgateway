package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	microgateway "github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
)

const drainLimit = 5 * time.Second

type instance struct {
	name, data, admin string
}

type driverRun struct {
	name     string
	results  []StepResult
	shutdown []ShutdownObs
	problems []string
}

type logScanner struct {
	mu          sync.Mutex
	buf         []byte
	lines       []string
	data        string
	admin       string
	ready       chan struct{}
	readyClosed bool
	drainLogged bool
}

func newLogScanner() *logScanner {
	return &logScanner{ready: make(chan struct{})}
}

func (s *logScanner) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf = append(s.buf, p...)
	for {
		i := bytes.IndexByte(s.buf, '\n')
		if i < 0 {
			break
		}
		line := string(s.buf[:i])
		s.buf = s.buf[i+1:]
		s.line(line)
	}
	return len(p), nil
}

func (s *logScanner) line(text string) {
	s.lines = append(s.lines, text)
	if len(s.lines) > 20 {
		s.lines = s.lines[len(s.lines)-20:]
	}
	var e struct {
		Msg      string `json:"msg"`
		Listener string `json:"listener"`
		Address  string `json:"address"`
	}
	if json.Unmarshal([]byte(text), &e) != nil {
		return
	}
	switch {
	case e.Msg == "listening" && e.Listener == "public":
		s.data = e.Address
	case e.Msg == "listening" && e.Listener == "admin":
		s.admin = e.Address
	case e.Msg == "drain complete":
		s.drainLogged = true
	}
	if s.data != "" && s.admin != "" && !s.readyClosed {
		s.readyClosed = true
		close(s.ready)
	}
}

func (s *logScanner) addrs() (string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data, s.admin
}

func (s *logScanner) drained() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.drainLogged
}

func (s *logScanner) tail() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.lines, " | ")
}

type proc struct {
	cmd      *exec.Cmd
	logs     *logScanner
	done     chan struct{}
	exitCode int
	exited   time.Time
}

func instName(i int) string {
	return fmt.Sprintf("gw%d", i+1)
}

func startBackends() (upstreams, func()) {
	a := httptest.NewServer(backendHandler("A"))
	b := httptest.NewServer(backendHandler("B"))
	return upstreams{A: a.URL, B: b.URL}, func() {
		a.Close()
		b.Close()
	}
}

func backendHandler(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend", name)
		if r.URL.Path == "/slow" {
			select {
			case <-time.After(300 * time.Millisecond):
			case <-r.Context().Done():
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
}

func atomicWrite(dir, path string, data []byte) (err error) {
	f, err := os.CreateTemp(dir, ".reloadload-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
		}
	}()
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readyStatus(ctx context.Context, c *http.Client, admin string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+admin+"/readyz", nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

func waitReady(ctx context.Context, c *http.Client, admin string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		code, err := readyStatus(ctx, c, admin)
		if err == nil && code == http.StatusOK {
			return nil
		}
		if time.Now().After(deadline) {
			if err == nil {
				err = fmt.Errorf("status %d", code)
			}
			return fmt.Errorf("not ready within %v: %w", limit, err)
		}
		if !sleepUntil(ctx, time.Now().Add(50*time.Millisecond)) {
			return ctx.Err()
		}
	}
}

func pollReadiness(ctx context.Context, c *http.Client, t *target, admin string, errOut *string) {
	for {
		code, err := readyStatus(ctx, c, admin)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if *errOut == "" {
				*errOut = err.Error()
			}
		} else if code == http.StatusServiceUnavailable {
			t.markNotReady()
			return
		}
		if !sleepUntil(ctx, time.Now().Add(50*time.Millisecond)) {
			return
		}
	}
}

func dialRefused(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err == nil {
		_ = c.Close()
		return false
	}
	return errors.Is(err, syscall.ECONNREFUSED)
}

func sleepUntil(ctx context.Context, at time.Time) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(time.Until(at)):
		return true
	}
}

func runWindows(ctx context.Context, d *driverRun, gen *generator, insts []instance, client *http.Client, window time.Duration, trigger func(step string) (start, done map[string]time.Time, problems []string)) bool {
	specs := stepSpecs()
	for _, step := range stepOrder {
		wStart := time.Now()
		before := make(map[string]Counts, len(insts))
		for _, in := range insts {
			c, err := scrape(ctx, client, in.admin)
			if err != nil {
				d.problems = append(d.problems, fmt.Sprintf("%s before %s: %v", in.name, step, err))
			}
			before[in.name] = c
		}
		var starts, dones map[string]time.Time
		if step != stepBaseline {
			if !sleepUntil(ctx, wStart.Add(window/10)) {
				return false
			}
			var probs []string
			starts, dones, probs = trigger(step)
			d.problems = append(d.problems, probs...)
		}
		wEnd := wStart.Add(window)
		if !sleepUntil(ctx, wEnd) {
			return false
		}
		for _, in := range insts {
			c, err := scrape(ctx, client, in.admin)
			if err != nil {
				d.problems = append(d.problems, fmt.Sprintf("%s after %s: %v", in.name, step, err))
			}
			r := judgeStep(specs[step], starts[in.name], dones[in.name], between(gen.records(), in.name, wStart, wEnd), before[in.name], c)
			r.Driver = d.name
			r.Step = step
			r.Instance = in.name
			d.results = append(d.results, r)
		}
	}
	return true
}

func binaryDriver(ctx context.Context, s settings, name string, ups upstreams, dir string, client *http.Client) driverRun {
	d := driverRun{name: name}
	window := s.duration / 12
	bin, err := filepath.Abs(s.binary)
	if err != nil {
		d.problems = append(d.problems, fmt.Sprintf("binary path: %v", err))
		return d
	}
	path := filepath.Join(dir, name+".yaml")
	opts := renderOpts{signalOnly: name == "sighup"}
	if err := atomicWrite(dir, path, render(stepBaseline, ups, opts)); err != nil {
		d.problems = append(d.problems, fmt.Sprintf("write baseline: %v", err))
		return d
	}

	var procs []*proc
	var gen *generator
	defer func() {
		for _, p := range procs {
			select {
			case <-p.done:
			default:
				_ = p.cmd.Process.Kill()
				<-p.done
			}
		}
		if gen != nil {
			gen.stop()
		}
	}()

	for i := range s.instances {
		logs := newLogScanner()
		cmd := exec.Command(bin, "run", "-config", path)
		cmd.Stdout = io.Discard
		cmd.Stderr = logs
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Env = os.Environ()
		if err := cmd.Start(); err != nil {
			d.problems = append(d.problems, fmt.Sprintf("%s did not start: %v", instName(i), err))
			return d
		}
		p := &proc{cmd: cmd, logs: logs, done: make(chan struct{})}
		procs = append(procs, p)
		go func() {
			_ = cmd.Wait()
			p.exitCode = cmd.ProcessState.ExitCode()
			p.exited = time.Now()
			close(p.done)
		}()
	}

	insts := make([]instance, len(procs))
	for i, p := range procs {
		select {
		case <-p.logs.ready:
		case <-p.done:
			d.problems = append(d.problems, fmt.Sprintf("%s did not start: exited with code %d; last log: %s", instName(i), p.exitCode, p.logs.tail()))
			return d
		case <-time.After(10 * time.Second):
			d.problems = append(d.problems, fmt.Sprintf("%s did not start: no listening log within 10s; last log: %s", instName(i), p.logs.tail()))
			return d
		}
		data, admin := p.logs.addrs()
		insts[i] = instance{name: instName(i), data: data, admin: admin}
		if err := waitReady(ctx, client, admin, 10*time.Second); err != nil {
			d.problems = append(d.problems, fmt.Sprintf("%s did not start: %v; last log: %s", instName(i), err, p.logs.tail()))
			return d
		}
	}

	targets := make([]*target, len(insts))
	for i, in := range insts {
		targets[i] = &target{name: in.name, base: "http://" + in.data}
	}
	gen = newGenerator(targets, s.rate, s.concurrency)
	gen.start()

	trigger := func(step string) (map[string]time.Time, map[string]time.Time, []string) {
		starts, dones := map[string]time.Time{}, map[string]time.Time{}
		var probs []string
		if name == "sighup" {
			if err := atomicWrite(dir, path, render(step, ups, opts)); err != nil {
				probs = append(probs, fmt.Sprintf("write %s: %v", step, err))
			}
			for i, p := range procs {
				in := insts[i].name
				starts[in] = time.Now()
				if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
					probs = append(probs, fmt.Sprintf("%s SIGHUP: %v", in, err))
				}
				dones[in] = time.Now()
			}
			return starts, dones, probs
		}
		writeStart := time.Now()
		if err := atomicWrite(dir, path, render(step, ups, opts)); err != nil {
			probs = append(probs, fmt.Sprintf("write %s: %v", step, err))
		}
		writeDone := time.Now()
		for _, in := range insts {
			starts[in.name] = writeStart
			dones[in.name] = writeDone
		}
		return starts, dones, probs
	}

	if !runWindows(ctx, &d, gen, insts, client, window, trigger) {
		return d
	}
	tPhase := time.Now()

	pctx, pcancel := context.WithCancel(ctx)
	pollErrs := make([]string, len(insts))
	var pollWG sync.WaitGroup
	for i := range insts {
		pollWG.Add(1)
		go func() {
			defer pollWG.Done()
			pollReadiness(pctx, client, targets[i], insts[i].admin, &pollErrs[i])
		}()
	}
	var slowWG sync.WaitGroup
	for i := range targets {
		slowWG.Add(1)
		go func() {
			defer slowWG.Done()
			_, _ = gen.do(context.Background(), targets[i], "/slow")
		}()
	}
	time.Sleep(100 * time.Millisecond)

	sigs := make([]time.Time, len(procs))
	for i, p := range procs {
		sigs[i] = time.Now()
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			d.problems = append(d.problems, fmt.Sprintf("%s SIGTERM: %v", insts[i].name, err))
		}
	}
	for i, p := range procs {
		select {
		case <-p.done:
		case <-time.After(time.Until(sigs[i].Add(drainLimit))):
			_ = p.cmd.Process.Kill()
			<-p.done
		}
	}
	pcancel()
	pollWG.Wait()
	slowWG.Wait()
	gen.stop()
	recs := gen.records()

	for i, p := range procs {
		in := insts[i]
		d.shutdown = append(d.shutdown, ShutdownObs{
			Driver:       name,
			Instance:     in.name,
			Signal:       sigs[i],
			NotReady:     targets[i].notReadyAt(),
			PollErr:      pollErrs[i],
			Records:      between(recs, in.name, tPhase, time.Time{}),
			Refused:      dialRefused(in.data),
			ExitCode:     p.exitCode,
			Exited:       p.exited,
			DrainLogged:  p.logs.drained(),
			DrainTimeout: drainLimit,
		})
	}
	return d
}

func apiReload(step string, g *microgateway.Gateway, path string, body []byte, cfg *config.Config) error {
	switch step {
	case stepRoute:
		return g.ReloadFile(path)
	case stepWeight:
		return g.Reload(cfg)
	}
	var ve *config.ValidationError
	if err := g.ReloadYAML(body); !errors.As(err, &ve) {
		return fmt.Errorf("got %v, want a validation error", err)
	}
	return nil
}

func apiDriver(ctx context.Context, s settings, ups upstreams, dir string, client *http.Client) driverRun {
	d := driverRun{name: "api"}
	window := s.duration / 12
	path := filepath.Join(dir, "api.yaml")
	if err := atomicWrite(dir, path, render(stepBaseline, ups, renderOpts{})); err != nil {
		d.problems = append(d.problems, fmt.Sprintf("write baseline: %v", err))
		return d
	}

	var gws []*microgateway.Gateway
	var srvs []*http.Server
	var gen *generator
	defer func() {
		if gen != nil {
			gen.stop()
		}
		for _, srv := range srvs {
			_ = srv.Close()
		}
		for _, g := range gws {
			_ = g.Close()
		}
	}()

	insts := make([]instance, 0, s.instances)
	for i := range s.instances {
		g, err := microgateway.NewFromYAML(render(stepBaseline, ups, renderOpts{}), microgateway.WithLogger(slog.New(slog.DiscardHandler)))
		if err != nil {
			d.problems = append(d.problems, fmt.Sprintf("%s did not start: %v", instName(i), err))
			return d
		}
		gws = append(gws, g)
		dl, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			d.problems = append(d.problems, fmt.Sprintf("%s did not start: %v", instName(i), err))
			return d
		}
		al, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			_ = dl.Close()
			d.problems = append(d.problems, fmt.Sprintf("%s did not start: %v", instName(i), err))
			return d
		}
		dsrv := &http.Server{Handler: g, ReadHeaderTimeout: 10 * time.Second}
		asrv := &http.Server{Handler: g.AdminHandler(), ReadHeaderTimeout: 10 * time.Second}
		srvs = append(srvs, dsrv, asrv)
		go func() { _ = dsrv.Serve(dl) }()
		go func() { _ = asrv.Serve(al) }()
		insts = append(insts, instance{name: instName(i), data: dl.Addr().String(), admin: al.Addr().String()})
	}

	targets := make([]*target, len(insts))
	for i, in := range insts {
		targets[i] = &target{name: in.name, base: "http://" + in.data}
	}
	gen = newGenerator(targets, s.rate, s.concurrency)
	gen.start()

	trigger := func(step string) (map[string]time.Time, map[string]time.Time, []string) {
		starts, dones := map[string]time.Time{}, map[string]time.Time{}
		var probs []string
		body := render(step, ups, renderOpts{})
		if err := atomicWrite(dir, path, body); err != nil {
			probs = append(probs, fmt.Sprintf("write %s: %v", step, err))
		}
		for i, g := range gws {
			in := insts[i].name
			var cfg *config.Config
			if step == stepWeight {
				c, err := config.Parse(body)
				if err != nil {
					probs = append(probs, fmt.Sprintf("%s weight: parse: %v", in, err))
					continue
				}
				cfg = c
			}
			starts[in] = time.Now()
			err := apiReload(step, g, path, body, cfg)
			dones[in] = time.Now()
			if err != nil {
				probs = append(probs, fmt.Sprintf("%s %s: %v", in, step, err))
			}
		}
		return starts, dones, probs
	}

	_ = runWindows(ctx, &d, gen, insts, client, window, trigger)
	return d
}
