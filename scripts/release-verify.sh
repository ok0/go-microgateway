#!/bin/sh
# Installs a published release with the command from README.md in a clean environment: an empty GOPATH, empty module and build caches,
# GOFLAGS unset, GOWORK=off, outside the checkout. It checks that microgateway version prints the release version and that validate and
# run accept examples/redis.yaml. It needs the network and is opt-in: set MICROGATEWAY_HEAVY=1. The run needs ports 8080 and 9901 free and
# no Redis server, because connections are dialed on first use.
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
. "$root/scripts/release-lib.sh"

[ "${MICROGATEWAY_HEAVY:-}" = 1 ] || { echo "release-verify needs the network and is opt-in: set MICROGATEWAY_HEAVY=1" >&2; exit 1; }
version=${1:-}
check_version "$version" || exit 1
cmd=$(install_command "$root/README.md" "$version") || exit 1

tmp=$(mktemp -d)
case "$tmp/" in
"$root"/*)
    echo "temporary directory $tmp is inside the checkout" >&2
    rm -rf "$tmp"
    exit 1
    ;;
esac
pid=
cleanup() {
    if [ -n "$pid" ]; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
    chmod -R u+w "$tmp" 2>/dev/null || true
    rm -rf "$tmp"
}
trap cleanup EXIT
trap 'exit 1' INT TERM

mkdir "$tmp/gopath" "$tmp/modcache" "$tmp/cache" "$tmp/work"
cd "$tmp/work"
echo "$cmd"
env -u GOFLAGS -u GOBIN GOENV=off GOWORK=off GOPATH="$tmp/gopath" GOMODCACHE="$tmp/modcache" GOCACHE="$tmp/cache" sh -c "$cmd"

bin="$tmp/gopath/bin/microgateway"
out=$("$bin" version)
set -- $out
[ "${2:-}" = "$version" ] || { echo "microgateway version printed: $out, want $version" >&2; exit 1; }

export REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:6379}" REDIS_PASSWORD="${REDIS_PASSWORD:-example-redis-password}"
"$bin" validate -config "$root/examples/redis.yaml"

"$bin" run -config "$root/examples/redis.yaml" >"$tmp/run.log" 2>&1 &
pid=$!
listening=
i=0
while [ "$i" -lt 30 ]; do
    if grep listening "$tmp/run.log" | grep -q admin; then
        listening=1
        break
    fi
    kill -0 "$pid" 2>/dev/null || { cat "$tmp/run.log" >&2; echo "run exited before listening" >&2; exit 1; }
    sleep 1
    i=$((i + 1))
done
if [ -z "$listening" ]; then
    cat "$tmp/run.log" >&2
    echo "run did not report listening within 30 seconds" >&2
    exit 1
fi
kill -TERM "$pid"
status=0
wait "$pid" || status=$?
pid=
[ "$status" = 0 ] && grep -q stopped "$tmp/run.log" || { cat "$tmp/run.log" >&2; echo "run did not stop cleanly" >&2; exit 1; }

echo "release $version verified"
