#!/bin/sh
# Strict host-side isolation for the job microVMs.
#
# The job microVMs run as your user through rootless Podman (TSI/passt), so
# their outbound connections leave the host as your UID. The Docker Compose
# services (Redpanda, RustFS, Phoenix, the worker) live on the Docker bridge
# networks in 172.16.0.0/12. Blocking your UID from that range removes the
# microVM's path to them while leaving the public internet (the model
# provider) reachable. The worker itself runs as root in its own network
# namespace, so it is unaffected and can still reach Redpanda and RustFS.
#
# Requires root. Run once per boot, or wire it into a systemd unit:
#   sudo scripts/host-firewall.sh
#   sudo scripts/host-firewall.sh --remove
set -eu

TARGET_UID="${SUDO_UID:-${TARGET_UID:-}}"
if [ -z "$TARGET_UID" ]; then
    TARGET_UID="$(id -u)"
fi

PRIVATE_RANGES="172.16.0.0/12"

remove() {
    for range in $PRIVATE_RANGES; do
        while iptables -C OUTPUT -d "$range" -m owner --uid-owner "$TARGET_UID" -j DROP 2>/dev/null; do
            iptables -D OUTPUT -d "$range" -m owner --uid-owner "$TARGET_UID" -j DROP
        done
    done
    echo "Removed the microVM egress block for uid $TARGET_UID"
}

if [ "${1:-}" = "--remove" ]; then
    remove
    exit 0
fi

for range in $PRIVATE_RANGES; do
    if ! iptables -C OUTPUT -d "$range" -m owner --uid-owner "$TARGET_UID" -j DROP 2>/dev/null; then
        iptables -I OUTPUT -d "$range" -m owner --uid-owner "$TARGET_UID" -j DROP
    fi
done

echo "Blocked uid $TARGET_UID from $PRIVATE_RANGES"
echo "The microVMs can still reach the internet, but not the Compose services."
