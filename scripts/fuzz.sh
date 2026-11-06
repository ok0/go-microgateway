#!/bin/sh
set -eu

FUZZTIME="${FUZZTIME:-30s}"
count=0

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
            (cd "$module" && GOWORK=off go test -run '^$' -fuzz "^$name\$" -fuzztime "$FUZZTIME" "./$pkgdir")
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
