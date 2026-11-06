package redis

import (
	"path/filepath"
	"testing"

	microgateway "github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
)

func rootExamplesEnv(name string) (string, bool) {
	switch name {
	case "REDIS_ADDR":
		return "127.0.0.1:6379", true
	case "REDIS_PASSWORD":
		return "example-redis-password", true
	}
	return "", false
}

func TestRootExamplesValidate(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob(filepath.Join("..", "examples", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no ../examples/*.yaml files")
	}
	usedRedis := false
	for _, path := range paths {
		cfg, err := config.LoadFile(path, config.WithEnvLookup(rootExamplesEnv))
		if err != nil {
			t.Fatalf("LoadFile(%s) = %v", path, err)
		}
		for _, s := range cfg.Stores {
			if s.Type == "redis" {
				usedRedis = true
			}
		}
		if err := microgateway.Validate(cfg, Option()); err != nil {
			t.Errorf("Validate(%s) = %v", path, err)
		}
	}
	if !usedRedis {
		t.Fatal("no example file uses a redis store")
	}
}
