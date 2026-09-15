#!/bin/sh
# hive-worker-deploy — GitOps reconcile for hive_worker.py + its systemd units.
# POSIX sh. Runs as root. Idempotent: pulls the pinned graeae-hive ref, installs
# hive_worker.py + the zeroclaw/codex unit files only if changed, restarts only
# the units whose script or unit file changed. Sibling to zeroclaw-deploy.sh
# (gitops/zeroclaw/deploy.sh) but for the thin claim/execute worker introduced
# 2026-09-14 to replace the zc-gateway/zc-worker@ stack (GRAEAE architecture
# consult, 7-muse consensus 0.95) -- kept as a SEPARATE script rather than
# folded into deploy.sh because that script's whole design (render quadlets,
# manage the podman zeroclaw-stack image) doesn't apply here at all.
#
# Gate:   /etc/hive-worker-deploy.conf must set ENABLED=1 (default off = no-op).
# Usage:  hive-worker-deploy.sh [--dry-run]
set -eu

REPO=/var/lib/graeae-hive                          # git checkout of graeae-hive
STATE=/var/lib/hive-worker-deploy
UNITDIR=/etc/systemd/system
DEPLOY_REF=${DEPLOY_REF:-wip/studio/2026-06-15-register-dedup}
# graeae-hive is NOT git-daemon-exported on PYTHIA (unlike zeroclaw-fleet) --
# its canonical remote is the ARGONAS bare repo over SSH.
SOURCE=ssh://root@192.168.207.101/mnt/datapool/git/graeae-hive.git

DRY_RUN=0
[ "${1:-}" = "--dry-run" ] && DRY_RUN=1

ENABLED=0
GIT_FLEET_SSH_PASSWORD=""
[ -f /etc/hive-worker-deploy.conf ] && . /etc/hive-worker-deploy.conf
if [ "$ENABLED" != 1 ] && [ "$DRY_RUN" != 1 ]; then
  echo "hive-worker-deploy: ENABLED!=1, no-op (set ENABLED=1 in /etc/hive-worker-deploy.conf)"
  exit 0
fi
# root@ARGONAS git ops can hit the documented pubkey-exhaustion class of
# failure (kernel-build-checklist.md #0f) on a host whose SSH agent hasn't
# got root's key trusted, so fall back to password auth when the config
# provides one (never hardcoded here). MUST come after sourcing the config
# above -- an earlier version of this script checked the var before it was
# ever set, so the fallback silently never engaged (caught live rolling
# this out to 4 hosts, 2026-09-15: fedora/proteus/tydeus/argos all failed
# clone with plain-pubkey-style errors despite the config having the
# password set).
if [ -n "$GIT_FLEET_SSH_PASSWORD" ]; then
  # UserKnownHostsFile=/dev/null in addition to StrictHostKeyChecking=no:
  # OpenSSH disables password/keyboard-interactive auth entirely (not just
  # the host-key prompt) when a host's key in known_hosts has CHANGED, as
  # an extra MITM guard -- StrictHostKeyChecking=no alone doesn't override
  # that. Hit live on ARGOS root, 2026-09-15 (a stale known_hosts entry for
  # ARGONAS from some earlier session).
  export GIT_SSH_COMMAND="sshpass -p ${GIT_FLEET_SSH_PASSWORD} ssh -o PubkeyAuthentication=no -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
fi

log() { echo "hive-worker-deploy: $*"; }
HOST=$(hostname -s)
mkdir -p "$STATE"

# hosts/<HOST>/hive-worker.conf declares which kinds this host runs
# (KINDS="zeroclaw codex" or KINDS="zeroclaw"). No file -> not a managed host.
CONF="$REPO/hosts/$HOST/hive-worker.conf"
if [ ! -f "$REPO" ] 2>/dev/null; then :; fi
if [ ! -d "$REPO" ]; then
  git clone --quiet "$SOURCE" "$REPO"
fi
git -C "$REPO" fetch --quiet origin "$DEPLOY_REF"
git -C "$REPO" checkout --quiet --detach FETCH_HEAD

if [ ! -f "$CONF" ]; then
  log "no $CONF; not a hive-worker-managed host"
  exit 0
fi
KINDS=""
. "$CONF"
if [ -z "$KINDS" ]; then
  log "hosts/$HOST/hive-worker.conf has no KINDS set; nothing to do"
  exit 0
fi

SCRIPT_SRC="$REPO/hive_worker.py"
SCRIPT_DST="/opt/graeae/hive_worker.py"
mkdir -p /opt/graeae

CHANGED=""
if [ ! -f "$SCRIPT_DST" ] || ! cmp -s "$SCRIPT_SRC" "$SCRIPT_DST"; then
  log "changed: $SCRIPT_DST"
  if [ "$DRY_RUN" != 1 ]; then
    install -m 0755 "$SCRIPT_SRC" "$SCRIPT_DST"
  fi
  CHANGED="$CHANGED script"
fi

for KIND in $KINDS; do
  UNIT="hive-worker-$KIND.service"
  SRC="$REPO/systemd/$UNIT"
  DST="$UNITDIR/$UNIT"
  if [ ! -f "$SRC" ]; then
    log "WARNING: no template $SRC for kind=$KIND, skipping"
    continue
  fi
  if [ ! -f "$DST" ] || ! cmp -s "$SRC" "$DST"; then
    log "changed: $DST"
    if [ "$DRY_RUN" != 1 ]; then
      install -m 0644 "$SRC" "$DST"
    fi
    CHANGED="$CHANGED $UNIT"
  fi
done

if [ -n "$CHANGED" ]; then
  if [ "$DRY_RUN" = 1 ]; then
    log "DRY-RUN: would reload+restart for:$CHANGED"
    exit 0
  fi
  systemctl daemon-reload
  for KIND in $KINDS; do
    UNIT="hive-worker-$KIND.service"
    systemctl enable --now "$UNIT"
    case " $CHANGED " in
      *" script "*|*" $UNIT "*) systemctl restart "$UNIT"; log "restarted $UNIT" ;;
    esac
  done
else
  log "no changes"
fi
