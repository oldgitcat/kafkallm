#!/bin/sh
# Proof of concept: run the goose job image inside a libkrun microVM.
#
# The krun runtime ships with the crun-krun package on Fedora. This script
# checks the prerequisites, then boots the goose image in a microVM and reports
# the guest kernel, the start-up time, and the memory footprint.
#
# Usage: scripts/poc-krun.sh [image]
set -eu

IMAGE="${1:-${GOOSE_IMAGE:-kafkallm-goose-job:latest}}"
RAM_MIB="${KRAM_MIB:-2048}"
CPUS="${KCPUS:-2}"

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

echo "== prerequisites"
[ -e /dev/kvm ] || fail "/dev/kvm is missing. Enable hardware virtualization in the BIOS."
[ -r /dev/kvm ] && [ -w /dev/kvm ] || fail "/dev/kvm is not accessible for this user."
echo "  /dev/kvm: ok"

command -v podman >/dev/null 2>&1 || fail "podman is not installed."
echo "  podman: $(podman --version)"

command -v krun >/dev/null 2>&1 || fail "the krun runtime is missing. On Fedora: dnf install crun-krun"
echo "  krun runtime: $(krun --version | head -1)"

if ! podman image exists "$IMAGE"; then
    echo "== loading image into podman storage"
    podman pull "$IMAGE" || fail "could not pull $IMAGE"
fi

echo "== booting microVM (ram_mib=${RAM_MIB}, cpus=${CPUS})"
START=$(date +%s)
podman run --runtime=krun --rm \
    --annotation "krun.ram_mib=${RAM_MIB}" \
    --annotation "krun.cpus=${CPUS}" \
    "$IMAGE" \
    sh -c 'echo "guest kernel: $(uname -r)"; echo "guest user: $(id -u)"; command -v goose >/dev/null 2>&1 && goose --version || echo "goose not in this image"' || fail "the microVM run failed"
END=$(date +%s)
echo "== start-up time: $((END - START))s (includes image unpack and microVM boot)"

echo
echo "The guest kernel differs from the host kernel, so the workload runs behind a"
echo "hypervisor. Build the job image first with scripts/build-goose-image.sh."
