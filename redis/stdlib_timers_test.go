package redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
)

func TestConcurrentFirstUseNoInitWait(t *testing.T) {
	m := newMini(t)
	fake := clock.NewFake(miniStart)
	s, err := newFromConfig(&config.RedisStore{Mode: "standalone", Addresses: []string{m.Addr()}}, nil, fake)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	const callers = 32
	start := make(chan struct{})
	errs := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			_, err := s.Take(context.Background(), "first", storeTB)
			errs <- err
		}()
	}
	close(start)
	for range callers {
		if err, _ := testwait.Recv(t, "a first command to return", errs, nil); err != nil {
			t.Errorf("first command: %v", err)
		}
	}
	if d := fake.Since(miniStart); d != 0 {
		t.Fatalf("the fake clock moved by %v", d)
	}

	standalone, err := clientOptions(&config.RedisStore{Mode: "standalone", Addresses: []string{m.Addr()}}, nil, fake)
	if err != nil {
		t.Fatal(err)
	}
	if mode := standalone.(*goredis.Options).MaintNotificationsConfig.Mode; mode != maintnotifications.ModeDisabled {
		t.Errorf("standalone maintenance notifications mode %q, want %q", mode, maintnotifications.ModeDisabled)
	}

	failover, err := clientOptions(&config.RedisStore{Mode: "sentinel", Addresses: []string{m.Addr()}, MasterName: "mymaster"}, nil, fake)
	if err != nil {
		t.Fatal(err)
	}
	fc := newClient(failover).(*goredis.Client)
	t.Cleanup(func() { _ = fc.Close() })
	if mode := fc.Options().MaintNotificationsConfig.Mode; mode != maintnotifications.ModeDisabled {
		t.Errorf("failover maintenance notifications mode %q, want %q", mode, maintnotifications.ModeDisabled)
	}

	cluster, err := clientOptions(&config.RedisStore{Mode: "cluster", Addresses: []string{m.Addr()}}, nil, fake)
	if err != nil {
		t.Fatal(err)
	}
	if mode := cluster.(*goredis.ClusterOptions).MaintNotificationsConfig.Mode; mode != maintnotifications.ModeDisabled {
		t.Errorf("cluster maintenance notifications mode %q, want %q", mode, maintnotifications.ModeDisabled)
	}
}

func TestDialHostNameIndependentOfFallback(t *testing.T) {
	m := newMini(t)
	_, port, err := net.SplitHostPort(m.Addr())
	if err != nil {
		t.Fatal(err)
	}
	fake := clock.NewFake(miniStart)
	s, err := newFromConfig(&config.RedisStore{Mode: "standalone", Addresses: []string{net.JoinHostPort("localhost", port)}}, nil, fake)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	mustTake(t, s, "host", storeTB)
	if d := fake.Since(miniStart); d != 0 {
		t.Fatalf("the fake clock moved by %v", d)
	}
}

func TestClusterTimerFeaturesOff(t *testing.T) {
	m := newMini(t)
	opts, err := clientOptions(&config.RedisStore{Mode: "cluster", Addresses: []string{m.Addr()}}, nil, clock.NewFake(miniStart))
	if err != nil {
		t.Fatal(err)
	}
	o := opts.(*goredis.ClusterOptions)
	if o.ReadOnly || o.RouteByLatency || o.RouteRandomly {
		t.Fatalf("ReadOnly %v, RouteByLatency %v, RouteRandomly %v; want all false", o.ReadOnly, o.RouteByLatency, o.RouteRandomly)
	}
}

const slowNodeID = "0123456789abcdef0123456789abcdef01234567"

// startClusterNode serves one node that owns every slot and relays commands to target. Its COMMAND reply is held for
// hold before it is sent.
func startClusterNode(t *testing.T, target string, hold time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	slots := fmt.Sprintf("*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n$9\r\n127.0.0.1\r\n:%s\r\n$%d\r\n%s\r\n", port, len(slowNodeID), slowNodeID)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go serveClusterConn(c, target, slots, hold)
		}
	}()
	return ln.Addr().String()
}

func serveClusterConn(c net.Conn, target, slots string, hold time.Duration) {
	defer c.Close()
	up, err := net.Dial("tcp", target)
	if err != nil {
		return
	}
	defer up.Close()
	cr := bufio.NewReader(c)
	ur := bufio.NewReader(up)
	for {
		raw, args, err := readRequest(cr)
		if err != nil {
			return
		}
		var reply []byte
		switch name := strings.ToUpper(args[0]); {
		case name == "CLUSTER" && len(args) > 1 && strings.ToUpper(args[1]) == "SLOTS":
			reply = []byte(slots)
		case name == "COMMAND" && len(args) == 1:
			// Real stall: the held reply must take real time to exceed go-redis's 5 s lookup bound.
			time.Sleep(hold)
			reply = []byte("*0\r\n")
		default:
			if _, err := up.Write(raw); err != nil {
				return
			}
			reply, err = readReply(ur)
			if err != nil {
				return
			}
		}
		if _, err := c.Write(reply); err != nil {
			return
		}
	}
}

func TestClusterCommandInfoSlowReply(t *testing.T) {
	if testing.Short() {
		t.Skip("holds a COMMAND reply for 5 s of wall time")
	}
	m := newMini(t)
	// Real stall: go-redis bounds the COMMAND lookup by wall time (5 s), so the held reply must take real time.
	addr := startClusterNode(t, m.Addr(), 5200*time.Millisecond)
	fake := clock.NewFake(miniStart)
	s, err := newFromConfig(&config.RedisStore{Mode: "cluster", Addresses: []string{addr}}, nil, fake)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	d, err := s.Take(context.Background(), "slow", storeTB)
	if err != nil {
		t.Fatalf("Take while the COMMAND reply is held: %v", err)
	}
	if !d.Allowed {
		t.Fatal("Take while the COMMAND reply is held: Allowed = false, want true")
	}
	d, err = s.Take(context.Background(), "after", storeTB)
	if err != nil {
		t.Fatalf("Take after the COMMAND reply arrived: %v", err)
	}
	if !d.Allowed {
		t.Fatal("Take after the COMMAND reply arrived: Allowed = false, want true")
	}
}
