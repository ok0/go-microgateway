#!/bin/sh
# Prints the container image tags of a core release tag, one per line: X.Y.Z, then X.Y and latest unless the tag has a prerelease part.
# The argument is the release tag vX.Y.Z or vX.Y.Z-<prerelease>. Anything else, such as redis/vX.Y.Z, X.Y.Z, vX.Y or a build part, exits 1. Offline.
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
. "$root/scripts/release-lib.sh"

if [ $# -ne 1 ]; then
    echo "usage: sh scripts/image-tags.sh vX.Y.Z[-prerelease]" >&2
    exit 1
fi
check_version "$1" || exit 1

version=${1#v}
printf '%s\n' "$version"
case $version in
*-*) ;;
*) printf '%s\n%s\n' "${version%.*}" latest ;;
esac
