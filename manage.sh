#!/bin/bash
set -e

cmd="$1"

if test ! "$cmd"; then
    echo "command required."
    echo
    echo "available commands:"
    echo "  build        build project"
    echo "  release      build project for distribution"
    echo "  update-deps  update project dependencies"
    exit 1
fi

shift
rest=$*

if test "$cmd" = "build"; then
    # CGO_ENABLED=0 skips CGO and linking against glibc to build static binaries.
    # -v 'verbose'
    CGO_ENABLED=0 go build \
        -v
    exit 0

elif test "$cmd" = "release"; then
    # GOOS is 'Go OS' and is being explicit in which OS to build for.
    # CGO_ENABLED=0 skips CGO and linking against glibc to build static binaries.
    # ld -s is 'disable symbol table'
    # ld -w is 'disable DWARF generation'
    # -trimpath removes leading paths to source files
    # -v 'verbose'
    # -o 'output'
    GOOS=linux CGO_ENABLED=0 go build \
        -ldflags="-s -w" \
        -trimpath \
        -v \
        -o linux-amd64
    sha256sum linux-amd64 > linux-amd64.sha256
    echo ---
    echo "wrote linux-amd64"
    echo "wrote linux-amd64.sha256"
    exit 0

elif test "$cmd" = "update-deps"; then
    # -u 'update modules [...] to use newer minor or patch releases when available'
    go get -u
    go mod tidy
    ./manage.sh build
    exit 0

# ...

fi

echo "unknown command: $cmd"
exit 1
