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
# Usage:  hive-worker-deploy.sh [--dry-run] [--fanout-only]
set -eu

REPO=/var/lib/graeae-hive                          # git checkout of graeae-hive
STATE=/var/lib/hive-worker-deploy
UNITDIR=/etc/systemd/system
DEPLOY_REF=${DEPLOY_REF:-wip/studio/2026-06-15-register-dedup}
# graeae-hive is NOT git-daemon-exported on PYTHIA (unlike zeroclaw-fleet) --
# its canonical remote is the ARGONAS bare repo over SSH.
SOURCE=ssh://root@192.168.207.101/mnt/datapool/git/graeae-hive.git

DRY_RUN=0
FANOUT_ONLY=0
for ARG do
  case "$ARG" in
    --dry-run) DRY_RUN=1 ;;
    --fanout-only) FANOUT_ONLY=1 ;;
    *) echo "usage: $0 [--dry-run] [--fanout-only]" >&2; exit 2 ;;
  esac
done

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

# Self-update: this script's INSTALLED copy (/usr/local/bin/hive-worker-deploy.sh,
# what the systemd timer actually runs) is never touched by the reconcile logic
# below -- only hive_worker.py and the systemd unit files get pulled from the
# repo. A fix to this script itself would otherwise need a manual re-push to
# every host forever (found the hard way, 2026-09-15: a real bugfix committed
# to the repo sat there doing nothing on two hosts because their installed
# copy was still the pre-fix version, and "no changes" gave no hint why).
SELF="/usr/local/bin/hive-worker-deploy.sh"
REPO_SELF="$REPO/deploy/hive-worker-deploy.sh"
if [ -f "$REPO_SELF" ] && [ -f "$SELF" ] && ! cmp -s "$REPO_SELF" "$SELF"; then
  log "self-update: $SELF changed, installing and re-execing"
  if [ "$DRY_RUN" != 1 ]; then
    install -m 0755 "$REPO_SELF" "$SELF"
    exec "$SELF" "$@"
  fi
fi

if [ ! -f "$CONF" ]; then
  log "no $CONF; not a hive-worker-managed host"
  exit 0
fi
KINDS=""
RUN_AS=""
ZEROCLAW_FANOUT=1
. "$CONF"
if [ -z "$KINDS" ]; then
  log "hosts/$HOST/hive-worker.conf has no KINDS set; nothing to do"
  exit 0
fi
case "$ZEROCLAW_FANOUT" in
  ''|*[!0-9]*) log "invalid ZEROCLAW_FANOUT=$ZEROCLAW_FANOUT"; exit 1 ;;
esac
if [ "$ZEROCLAW_FANOUT" -lt 1 ]; then
  log "ZEROCLAW_FANOUT must be at least 1"
  exit 1
fi
if [ "$ZEROCLAW_FANOUT" -gt 1 ]; then
  case " $KINDS " in
    *" zeroclaw "*) ;;
    *) log "ZEROCLAW_FANOUT requires zeroclaw in KINDS"; exit 1 ;;
  esac
fi
# RUN_AS=root selects the *-root.service template (User=root, WorkingDirectory=
# /root) instead of the default jasonperlow one. Real bug this fixed, 2026-09-15:
# the TYPHON LXC CTs (no jasonperlow user at all -- everything there runs as
# root) had a one-off manually-installed root unit; the FIRST gitops reconcile
# silently clobbered it with the canonical jasonperlow unit and crash-looped
# the service (systemd exit 217/USER, "user does not exist"). Every 5-minute
# timer fire would have re-broken it again even after a manual re-fix.
UNIT_SUFFIX=""
[ "$RUN_AS" = "root" ] && UNIT_SUFFIX="-root"

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
  SRC="$REPO/systemd/hive-worker-$KIND$UNIT_SUFFIX.service"
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

# Keep the existing singleton as slot 1. Additional systemd instances have
# distinct agent URNs and job-ID workspaces, so a busy singleton is not
# interrupted when capacity is added.
if [ "$ZEROCLAW_FANOUT" -gt 1 ]; then
  FANOUT_SRC="$REPO/systemd/hive-worker-zeroclaw$UNIT_SUFFIX@.service"
  FANOUT_DST="$UNITDIR/hive-worker-zeroclaw@.service"
  if [ ! -f "$FANOUT_SRC" ]; then
    log "missing fanout unit $FANOUT_SRC"
    exit 1
  fi
  if [ ! -f "$FANOUT_DST" ] || ! cmp -s "$FANOUT_SRC" "$FANOUT_DST"; then
    log "changed: $FANOUT_DST"
    if [ "$DRY_RUN" != 1 ]; then
      install -m 0644 "$FANOUT_SRC" "$FANOUT_DST"
    fi
    CHANGED="$CHANGED fanout-template"
  fi
fi

# Optional host-specific, non-secret route for the zeroclaw worker. Keep
# credentials in /etc/hive-worker-env; this drop-in only pins reachable models.
ROUTE_SRC="$REPO/hosts/$HOST/hive-worker-zeroclaw-route.conf"
ROUTE_DST="$UNITDIR/hive-worker-zeroclaw.service.d/20-host-route.conf"
if [ -f "$ROUTE_SRC" ] && { [ ! -f "$ROUTE_DST" ] || ! cmp -s "$ROUTE_SRC" "$ROUTE_DST"; }; then
  log "changed: $ROUTE_DST"
  if [ "$DRY_RUN" != 1 ]; then
    mkdir -p "$(dirname "$ROUTE_DST")"
    install -m 0644 "$ROUTE_SRC" "$ROUTE_DST"
  fi
  CHANGED="$CHANGED hive-worker-zeroclaw.service"
fi

if [ -n "$CHANGED" ]; then
  if [ "$DRY_RUN" = 1 ]; then
    log "DRY-RUN: would reload+restart for:$CHANGED"
    exit 0
  fi
  systemctl daemon-reload
  if [ "$FANOUT_ONLY" != 1 ]; then
    for KIND in $KINDS; do
      UNIT="hive-worker-$KIND.service"
      systemctl enable --now "$UNIT"
      case " $CHANGED " in
        *" script "*|*" $UNIT "*) systemctl restart "$UNIT"; log "restarted $UNIT" ;;
      esac
    done
  else
    log "fanout-only: singleton service operations deferred"
  fi
else
  log "no changes"
fi

if [ "$ZEROCLAW_FANOUT" -gt 1 ]; then
  SLOT=2
  while [ "$SLOT" -le "$ZEROCLAW_FANOUT" ]; do
    UNIT="hive-worker-zeroclaw@$SLOT.service"
    if [ "$DRY_RUN" = 1 ]; then
      log "DRY-RUN: would enable $UNIT"
    else
      systemctl enable --now "$UNIT"
    fi
    SLOT=$((SLOT + 1))
  done
fi
