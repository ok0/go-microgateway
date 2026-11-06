# Shared helpers of the release scripts. This file is sourced, not executed; it only defines functions and a variable.
core=github.com/ok0/go-microgateway

# check_version returns 0 when $1 is a release version vX.Y.Z with an optional prerelease and no build part, and 1 otherwise. A pseudo-version is refused.
check_version() {
    if ! printf '%s\n' "$1" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$'; then
        echo "VERSION must be a release version vX.Y.Z, not a pseudo-version: got \"$1\"" >&2
        return 1
    fi
    case $1 in
    *-*)
        if printf '%s\n' "${1#*-}" | grep -Eq '(^|\.)[0-9]{14}-[0-9A-Za-z]+$'; then
            echo "VERSION must be a release version vX.Y.Z, not a pseudo-version: got \"$1\"" >&2
            return 1
        fi
        ;;
    esac
    return 0
}

# install_command prints the README install command with its version replaced by $2. $1 is the README path. It fails unless the README has exactly one such command.
install_command() {
    lines=$(grep -E '^go install github\.com/ok0/go-microgateway/redis/cmd/microgateway@[^[:space:]]+$' "$1" || true)
    count=$(printf '%s\n' "$lines" | grep -c . || true)
    if [ "$count" -ne 1 ]; then
        echo "$1: want exactly one line go install github.com/ok0/go-microgateway/redis/cmd/microgateway@<version>, found $count" >&2
        return 1
    fi
    printf '%s\n' "$lines" | sed -E "s/@[^[:space:]]+\$/@$2/"
}
