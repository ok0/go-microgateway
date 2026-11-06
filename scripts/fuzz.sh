#!/bin/sh
set -eu

FUZZTIME="${FUZZTIME:-30s}"
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
count=0

# fuzz_target runs one Fuzz target: core targets without the workspace, redis targets from redis/ with the committed go.work.
fuzz_target() {
    module=$1 pkgdir=$2 name=$3
    if [ "$module" = "." ]; then
        GOWORK=off go test -run '^$' -fuzz "^$name\$" -fuzztime "$FUZZTIME" "./$pkgdir"
    else
        (cd redis && GOWORK="$root/go.work" go test -run '^$' -fuzz "^$name\$" -fuzztime "$FUZZTIME" "./$pkgdir")
    fi
}

run_module() {
    module=$1
    if [ "$module" = "." ]; then
        prefix="./"
        files=$(grep -rl --include='*_test.go' -E '^func Fuzz[A-Za-z0-9_]*\(' . | grep -v '^\./redis/' || true)
    else
        prefix="$module/"
        files=$(grep -rl --include='*_test.go' -E '^func Fuzz[A-Za-z0-9_]*\(' "$module" || true)
    fi
    for file in $files; do
        rel=${file#"$prefix"}
        pkgdir=$(dirname "$rel")
        names=$(grep -E -o '^func Fuzz[A-Za-z0-9_]*\(' "$file" | sed -e 's/^func //' -e 's/($//')
        for name in $names; do
            count=$((count + 1))
            echo "fuzz $module $name in ./$pkgdir for $FUZZTIME"
            fuzz_target "$module" "$pkgdir" "$name"
        done
    done
}

run_module .
run_module redis

if [ "$count" -eq 0 ]; then
    echo "no fuzz targets found" >&2
    exit 1
fi
echo "ran $count fuzz target(s)"
