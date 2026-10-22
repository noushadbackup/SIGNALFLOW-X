#!/usr/bin/env bash
# Build an image on a machine whose outbound traffic goes through a TLS-intercepting proxy (HTTPS_PROXY plus a
# private CA bundle). The CA is passed as a BuildKit secret, so it is never written into an image layer.
# Normal machines and CI do not need this: `docker compose build` works directly.
# Usage: scripts/docker_build_behind_proxy.sh <ca-bundle> <tag> <dockerfile> [context]
set -euo pipefail
ca="$1" tag="$2" dockerfile="$3" context="${4:-.}"
docker build --network host \
  --build-arg HTTPS_PROXY="${HTTPS_PROXY:-}" --build-arg HTTP_PROXY="${HTTP_PROXY:-}" \
  --build-arg NO_PROXY="${NO_PROXY:-}" \
  --secret id=build_ca,src="$ca" -t "$tag" -f "$dockerfile" "$context"
