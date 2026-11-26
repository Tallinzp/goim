#!/usr/bin/env bash
# Fetches the pinned kind and kubectl (Plan 6d1), checked against their
# SHA-256, unless PREFIX/bin holds them at these versions. No root.
#
# Usage: scripts/k8s-tools.sh [PREFIX]   (default .cache/k8s; make k8s-tools)
set -euo pipefail
kind_version=v0.33.0
kind_sha256=aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d
kubectl_version=v1.37.0
kubectl_sha256=6129359f4e1f3848a5572ccb0b26cf28b8ca08cef38c95a765b2f64a2c961a2f

prefix=${1:-.cache/k8s}
mkdir -p "$prefix/bin"
prefix=$(cd "$prefix" && pwd)

fetch() {
  local tmp
  tmp=$(mktemp "$prefix/bin/.$1.XXXXXX")
  curl -fsSL -o "$tmp" "$2" && echo "$3  $tmp" | sha256sum -c --quiet || { rm -f "$tmp"; return 1; }
  chmod 0755 "$tmp" && mv "$tmp" "$prefix/bin/$1"
}

"$prefix/bin/kind" version 2>/dev/null | grep -q "^kind $kind_version " ||
  fetch kind "https://github.com/kubernetes-sigs/kind/releases/download/$kind_version/kind-linux-amd64" "$kind_sha256"
"$prefix/bin/kubectl" version --client 2>/dev/null | grep -q "^Client Version: $kubectl_version\$" ||
  fetch kubectl "https://dl.k8s.io/release/$kubectl_version/bin/linux/amd64/kubectl" "$kubectl_sha256"
"$prefix/bin/kind" version
"$prefix/bin/kubectl" version --client | head -n 1
