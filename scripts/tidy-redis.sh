#!/bin/sh
# Checks that the Redis module's go.mod and go.sum are tidy without a published core
# module. A temporary copy of both files replaces the core with this checkout and
# go mod tidy -diff runs on the copy; any change it wants fails the check. The core
# module's own go.sum lines are left out of the copy, so the lines recorded after a
# release are not compared. Optional arguments: the go.mod and go.sum to check,
# by default redis/go.mod and redis/go.sum.
set -eu

core=github.com/ok0/go-microgateway
root=$(cd "$(dirname "$0")/.." && pwd)
gomod=${1:-$root/redis/go.mod}
gosum=${2:-$root/redis/go.sum}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cp "$gomod" "$tmp/go.mod"
grep -v "^$core " "$gosum" > "$tmp/go.sum" || [ $? -eq 1 ]
cd "$root/redis"
GOWORK=off go mod edit -modfile="$tmp/go.mod" -replace="$core=$root"
GOWORK=off go mod tidy -diff -modfile="$tmp/go.mod"
