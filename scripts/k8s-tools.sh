#!/usr/bin/env bash
# Fetches the pinned kind and kubectl (Plan 6d1), checked against their
# SHA-256, unless PREFIX/bin holds them at these versions. No root.
# When the kubectl download fails and K8S_NODE_IMAGE names the pinned kind
# node image, kubectl is copied from that image instead (it holds the same
# release binary) and checked against the same SHA-256: some hosts reach
# Docker Hub but not cdn.dl.k8s.io (6d1 host run).
#
# Usage: [K8S_NODE_IMAGE=…] scripts/k8s-tools.sh [PREFIX]   (default .cache/k8s; make k8s-tools)
set -euo pipefail
kind_version=v0.33.0
kind_sha256=aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d
kubectl_version=v1.37.0
kubectl_sha256=6129359f4e1f3848a5572ccb0b26cf28b8ca08cef38c95a765b2f64a2c961a2f

prefix=${1:-.cache/k8s}
mkdir -p "$prefix/bin"
prefix=$(cd "$prefix" && pwd)

# install NAME SHA256 CMD…: runs CMD with the temp file as its last
# argument, then checks the file and moves it into place.
install() {
  local name=$1 sum=$2 tmp; shift 2
  tmp=$(mktemp "$prefix/bin/.$name.XXXXXX")
  "$@" "$tmp" && echo "$sum  $tmp" | sha256sum -c --quiet || { rm -f "$tmp"; return 1; }
  chmod 0755 "$tmp" && mv "$tmp" "$prefix/bin/$name"
}
download() { curl -fsS -L --connect-timeout 20 --max-time 600 --retry 3 --retry-all-errors -o "$2" "$1"; }
from_node_image() {
  [ -n "${K8S_NODE_IMAGE:-}" ] && command -v docker >/dev/null || return 1
  echo "k8s-tools: kubectl from $K8S_NODE_IMAGE" >&2
  local id rc
  docker pull -q "$K8S_NODE_IMAGE" >/dev/null && id=$(docker create "$K8S_NODE_IMAGE") || return 1
  docker cp "$id:/usr/bin/kubectl" "$1" >/dev/null; rc=$?
  docker rm "$id" >/dev/null
  return $rc
}

"$prefix/bin/kind" version 2>/dev/null | grep -q "^kind $kind_version " ||
  install kind "$kind_sha256" download "https://github.com/kubernetes-sigs/kind/releases/download/$kind_version/kind-linux-amd64"
"$prefix/bin/kubectl" version --client 2>/dev/null | grep -q "^Client Version: $kubectl_version\$" ||
  install kubectl "$kubectl_sha256" download "https://dl.k8s.io/release/$kubectl_version/bin/linux/amd64/kubectl" ||
  install kubectl "$kubectl_sha256" from_node_image
"$prefix/bin/kind" version
"$prefix/bin/kubectl" version --client | head -n 1
