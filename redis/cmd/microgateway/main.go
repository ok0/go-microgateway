// Command microgateway runs, validates and reports the version of a microgateway configuration. The binary
// includes the Redis store backends.
package main

import (
	"context"
	"os"

	"github.com/ok0/go-microgateway/internal/cli"
	"github.com/ok0/go-microgateway/redis"
)

func main() {
	os.Exit(gateway(context.Background(), cli.Env{Args: os.Args[1:], Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr, LookupEnv: os.LookupEnv}))
}

// gateway runs one invocation of the command with the Redis store backends registered.
func gateway(ctx context.Context, env cli.Env) int {
	return cli.Main(ctx, env, redis.Option())
}
