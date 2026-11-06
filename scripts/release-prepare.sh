#!/bin/sh
# Sets the core version required by redis/go.mod and the core version replaced in go.work to the given version, together and offline.
# A refused version changes nothing, and no tag is created.
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
. "$root/scripts/release-lib.sh"

version=${1:-}
check_version "$version" || exit 1

old=$(sed -n -E 's#^replace github\.com/ok0/go-microgateway (v[^[:space:]]+) => \./?$#\1#p' "$root/go.work")
if [ -z "$old" ]; then
    echo "go.work: no replace github.com/ok0/go-microgateway vX.Y.Z => ./ line" >&2
    exit 1
fi
req=$(sed -n -E 's#^[[:space:]]*(require[[:space:]]+)?github\.com/ok0/go-microgateway[[:space:]]+(v[^[:space:]]+).*$#\2#p' "$root/redis/go.mod")
if [ "$old" = "$version" ] && [ "$req" = "$version" ]; then
    echo "redis/go.mod and go.work already use $version"
    exit 0
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cp "$root/go.work" "$tmp/go.work"
cp "$root/redis/go.mod" "$tmp/go.mod"
GOWORK=off GOTOOLCHAIN=local GOPROXY=off GOFLAGS= go mod edit -require="$core@$version" "$tmp/go.mod"
GOWORK="$tmp/go.work" GOTOOLCHAIN=local GOPROXY=off GOFLAGS= go work edit -dropreplace="$core@$old" -replace="$core@$version=./" "$tmp/go.work"

cp "$tmp/go.mod" "$root/redis/go.mod"
cp "$tmp/go.work" "$root/go.work"
echo "redis/go.mod and go.work now use $version"
