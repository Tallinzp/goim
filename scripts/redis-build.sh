#!/usr/bin/env bash
# Builds the Redis the race suite runs (Plan 5b, choice #62): redis-server
# 7.4.11, the version the Tier 2 compose image runs, from the release tarball
# checked against its published SHA-256 (github.com/redis/redis-hashes). It
# needs cc, make and curl, not Docker. It does nothing if PREFIX already holds
# this version.
#
# Usage: scripts/redis-build.sh [PREFIX]   (default .cache/redis; make redis-server)
set -euo pipefail
version=7.4.11
sha256=3c266ece0abd54ed3b1c912c6eb86b7508cf382cb690ee6649d3843f018f6357

prefix=${1:-.cache/redis}
mkdir -p "$prefix"
prefix=$(cd "$prefix" && pwd)
if "$prefix/bin/redis-server" --version 2>/dev/null | grep -q "v=$version "; then
  echo "redis-server $version is built: $prefix/bin/redis-server"
  exit 0
fi
src=$(mktemp -d)
trap 'rm -rf "$src"' EXIT
curl -fsSL -o "$src/redis.tar.gz" "https://download.redis.io/releases/redis-$version.tar.gz"
echo "$sha256  $src/redis.tar.gz" | sha256sum -c --quiet
tar -xzf "$src/redis.tar.gz" -C "$src"
# libc malloc: no jemalloc build, and the suite does not measure memory.
if ! make -C "$src/redis-$version" -j"$(getconf _NPROCESSORS_ONLN)" BUILD_TLS=no MALLOC=libc >"$src/build.log" 2>&1 ||
  ! make -C "$src/redis-$version" PREFIX="$prefix" install >>"$src/build.log" 2>&1; then
  tail -n 50 "$src/build.log" >&2
  exit 1
fi
"$prefix/bin/redis-server" --version
