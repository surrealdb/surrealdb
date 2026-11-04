#!/usr/bin/env bash
# Model-check concurrent_index_build.tla across its scenarios.
#
# Every scenario runs TLC on a generated configuration and states the outcome
# it expects. A `pass` scenario must explore its whole state space without a
# violation. A `fail` scenario must violate the property it names:
#
# - a safety property, when the row turns one mechanism off or models a gap
#   the design leaves open; a pass means the model no longer exercises what
#   that mechanism protects;
# - a `Never*` reachability check (added with INV=), when the row shows that a
#   passing scenario reaches the states it claims to cover;
# - a liveness property, when the row drops the fairness one step needs or
#   the barrier's exclusion of lapsed nodes.
#
# Overrides set a constant (Name=value) or change the run:
#   INV=Name     also check the invariant Name
#   PROP=Name    also check the temporal property Name
#   SPEC=Name    check the specification Name and drop SYMMETRY, which is
#                unsound for liveness
#   SIM=n        simulate instead: n random traces per TLC worker, from a
#   DEPTH=d      fixed seed, each at most d steps long (default 100)
# A simulated row is not exhaustive. It passes when no trace it generates
# violates a property, and its output line says it was simulated.
#
# Usage: TLA2TOOLS=/path/to/tla2tools.jar doc/tla/check_concurrent_index_build.sh [name...]
# With names, only those scenarios run. Counterexamples are kept in $OUT
# (default: a fresh temporary directory, printed at the end). TLC_WORKERS sets
# TLC's worker threads (default: one per core).
set -euo pipefail

: "${TLA2TOOLS:?set TLA2TOOLS to the path of tla2tools.jar}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${OUT:-$(mktemp -d)}"
mkdir -p "$OUT"
# TLC runs from the spec's directory, so both paths are made absolute first.
OUT="$(cd "$OUT" && pwd)"
case "$TLA2TOOLS" in
	/*) ;;
	*) TLA2TOOLS="$PWD/$TLA2TOOLS" ;;
esac
if [ ! -f "$TLA2TOOLS" ]; then
	echo "TLA2TOOLS is not a file: $TLA2TOOLS" >&2
	exit 2
fi

# name | expected | property it must violate | constant overrides
#
# Shared scenario bounds.
D="MaxTxns=3"                                                    # DEFINE ... CONCURRENTLY
T="Builders={b1,b2} MaxTakeovers=1 MaxFaults=1 MaxTxns=2"        # lease takeovers
R="MaxGen=2 MaxFaults=1 MaxTxns=3"                               # a failed build, then REBUILD
RT="Builders={b1,b2} MaxGen=2 MaxTakeovers=1 MaxFaults=1 MaxTxns=2"
RM="MaxGen=2 MaxFaults=1 MaxTxns=2 MaxMuts=2"
L="MaxFaults=1 MaxTxns=3"                                        # a writer node lapses
I="DefineInTxn=TRUE MaxTxns=3"                                   # blocking build in BEGIN...COMMIT
# A builder on a release without the write barrier owns the generation.
OR="BarrierlessBuilders={b1} BeginAfterDefine=TRUE RebuildFrom={\"Error\"} $R"
OT="BarrierlessBuilders={b1} BeginAfterDefine=TRUE $T"
# A UNIQUE index over two records, so that two records can hold one value.
U="Unique=TRUE Counting=FALSE NumKeys=2 MaxTxns=2"
UR="$U MaxGen=2"                                                 # a failed build, then REBUILD
X="RetireAs=\"remove\" MaxTxns=2"                               # REMOVE INDEX during the build
XR="RetireAs=\"remove\" MaxGen=2 MaxFaults=1 MaxTxns=2"         # ... or after a failure and REBUILD
# The defining statement may roll back, die with its node, or commit with an
# unknown outcome; its lease may lapse while it is open.
A="DefineAborts=TRUE MaxFaults=1 MaxTakeovers=1 MaxTxns=2"
AI="$A DefineInTxn=TRUE"                                         # ... with the build inside it
# Liveness, under LiveSpec and without symmetry. MaxTicket is raised so the
# ticket bound never holds an admission back (TicketBoundUnreached).
LV="SPEC=LiveSpec Vals={a} MaxTxns=2 MaxTicket=4 INV=TicketBoundUnreached"
LP="PROP=BuildStarts PROP=BuildSettles PROP=AdmissionWaitEnds PROP=BarrierProceeds"
# Simulation: two records, two mutations per transaction, takeovers and a
# rebuild. SIM is the number of traces per TLC worker, DEPTH their length.
S="NumKeys=2 MaxMuts=2 MaxTxns=3 Builders={b1,b2} MaxGen=2 MaxTakeovers=1 MaxFaults=1 SIM=300000 DEPTH=200"
# The store fences catalog changes: a build that reads after its statement's
# stamp waits for no other node.
F="FencesCatalog=TRUE"
# A rebuild on that store, with one writer opening after the definition
# commits: what the definition's shortcut adds, `define-fenced` covers. The
# takeover bounds give the one fault to a crash of the only builder, which
# resumes its own generation after the shortcut recorded the proof.
FR="BeginAfterDefine=TRUE Writers={w1} HeldWriters={w1} $F $R"
FT="BeginAfterDefine=TRUE Writers={w1} HeldWriters={w1} RebuildFrom={\"Online\"} MaxGen=2 MaxFaults=1 MaxTxns=2 $F"

SCENARIOS="
define                       pass -                     $D
define-keyed                 pass -                     $D Counting=FALSE
multi-mutation               pass -                     $D MaxMuts=2
takeover                     pass -                     $T
rebuild                      pass -                     $R
rebuild-multi-mutation       pass -                     $RM
rebuild-takeover             pass -                     $RT
node-lapse                   pass -                     $L
in-txn                       pass -                     $I
old-release-rebuild          pass -                     $OR
old-release-takeover         pass -                     $OT
unique                       pass -                     $U
unique-rebuild               pass -                     $UR
unique-multi-mutation        pass -                     $U MaxMuts=2
unique-fault                 pass -                     $U MaxFaults=1
retire                       pass -                     $X
retire-multi-mutation        pass -                     $X MaxMuts=2
retire-rebuild               pass -                     $XR
retire-overwrite             pass -                     RetireAs=\"overwrite\" MaxTxns=2
retire-fenced                pass -                     $X $F
retire-orphan-queue-fenced   pass -                     $X $F INV=RetiredQueuesStayEmpty
rollback                     pass -                     $A
rollback-in-txn              pass -                     $AI
define-fenced                pass -                     $D $F
rebuild-fenced               pass -                     $FR
rebuild-takeover-fenced      pass -                     $FT

reach-define                 fail NeverQueuedWhileClosing          $D INV=NeverQueuedWhileClosing
reach-multi-mutation         fail NeverTwoQueuedInOneTxn           $D MaxMuts=2 INV=NeverTwoQueuedInOneTxn
reach-takeover               fail NeverTakenOverOnline             $T INV=NeverTakenOverOnline
reach-rebuild                fail NeverRebuiltOnline               $R INV=NeverRebuiltOnline
reach-rebuild-rotated        fail NeverFenceRotated                $R INV=NeverFenceRotated
reach-rebuild-recheck        fail NeverRecheckFailed               $RM INV=NeverRecheckFailed
reach-rebuild-takeover       fail NeverFreshRebuildTakenOverOnline $RT INV=NeverFreshRebuildTakenOverOnline
reach-node-lapse             fail NeverLapsedReservationRetiredOnline $L INV=NeverLapsedReservationRetiredOnline
reach-in-txn                 fail NeverBlindCommitThenOnline       $I INV=NeverBlindCommitThenOnline
reach-old-release-takeover   fail NeverTakenOverOnline             $OT INV=NeverTakenOverOnline
reach-old-release-rebuild    fail NeverRebuiltOnline               $OR INV=NeverRebuiltOnline
reach-unique-scan            fail NeverScanDuplicate               $U INV=NeverScanDuplicate
reach-unique-replay          fail NeverReplayDuplicate             $U INV=NeverReplayDuplicate
reach-unique-refused         fail NeverUniqueRefused               $U INV=NeverUniqueRefused
reach-unique-online          fail NeverKeyReplayedOnline           $U INV=NeverKeyReplayedOnline
reach-unique-error-refused   fail NeverErrorRefused                $U INV=NeverErrorRefused
reach-unique-error-delete    fail NeverDeleteQueuedInError         $U INV=NeverDeleteQueuedInError
reach-unique-key-after-error fail NeverKeyCommittedInError         $U INV=NeverKeyCommittedInError
reach-unique-stale-duplicate fail NeverFailedOverUniqueTable       $U INV=NeverFailedOverUniqueTable
reach-unique-rebuild         fail NeverRebuiltOnline               $UR INV=NeverRebuiltOnline
reach-unique-recover         fail NeverRebuiltAfterDuplicate       $UR INV=NeverRebuiltAfterDuplicate
reach-unique-refused-direct  fail NeverUniqueRefusedDirect         $U MaxMuts=2 INV=NeverUniqueRefusedDirect
reach-unique-error-refused-cached fail NeverErrorRefusedCached     $U MaxMuts=2 INV=NeverErrorRefusedCached
reach-unique-fault           fail NeverFailedAfterCrash            $U MaxFaults=1 INV=NeverFailedAfterCrash
reach-retire-mid-build       fail NeverRetiredMidBuild             $X INV=NeverRetiredMidBuild
reach-retire-rebuild         fail NeverRetiredRebuilt              $XR INV=NeverRetiredRebuilt
reach-retire-rebuild-mid-build fail NeverRetiredRebuildMidBuild    $XR INV=NeverRetiredRebuildMidBuild
reach-retire-rebuild-pending fail NeverRetiredRebuildPending       $XR INV=NeverRetiredRebuildPending
reach-retire-reservation     fail NeverRetiredWithReservation      $X INV=NeverRetiredWithReservation
reach-retire-admit-skip      fail NeverRetiredAdmitSkip            $X INV=NeverRetiredAdmitSkip
reach-retire-recheck-skip    fail NeverRetiredRecheckSkip          $X MaxMuts=2 INV=NeverRetiredRecheckSkip
reach-retire-overwrite       fail NeverReplacedFail                RetireAs=\"overwrite\" MaxTxns=2 INV=NeverReplacedFail
reach-rollback-mid-batch     fail NeverCleanedMidBatch             $AI INV=NeverCleanedMidBatch
reach-rollback-cluster       fail NeverClusterRetired              $AI INV=NeverClusterRetired
reach-rollback-proof-retired fail NeverClusterRetiredProof         $AI INV=NeverClusterRetiredProof
reach-retire-fence-skip      fail NeverRetiredFenceSkip            $X INV=NeverRetiredFenceSkip
reach-retire-orphan-queue-fenced fail NeverRetiredStampRefused     $X $F INV=NeverRetiredStampRefused
reach-rollback-unknown       fail NeverUnknownCleanupSkipped       $A INV=NeverUnknownCleanupSkipped
reach-define-fenced          fail NeverFencedDefineRefusedOnline   $D $F INV=NeverFencedDefineRefusedOnline
reach-rebuild-fenced         fail NeverFencedRebuildRefusedOnline  $FR INV=NeverFencedRebuildRefusedOnline
reach-rebuild-takeover-fenced fail NeverShortcutProofTrustedOnline $FT INV=NeverShortcutProofTrustedOnline

no-closing-bump              fail IndexMatchesTable     $D ClosingBumpsTicket=FALSE
no-drain                     fail IndexMatchesTable     $D DrainReservations=FALSE
no-barrier                   fail IndexMatchesTable     $D WriteBarrier=FALSE
no-markers                   fail IndexMatchesTable     $D PrimaryMarkers=FALSE
no-owner-fence               fail IndexMatchesTable     $T OwnerFence=FALSE
no-write-fence               fail IndexMatchesTable     $L WriteFence=FALSE
old-release-no-prior-drain   fail IndexMatchesTable     $OR PriorDrain=FALSE
old-release-no-marker-rebuild fail IndexMatchesTable    $OT RebuildMarkers=FALSE
unique-no-check              fail UniqueWhenQueryable   $U UniqueCheck=FALSE
unique-record-keyed          fail UniqueWhenQueryable   $U ValueKeyedEntries=FALSE
unique-no-duplicate-fail     fail DuplicateNeverPublished $U DuplicateFailsBuild=FALSE
unique-no-error-refusal      fail ErrorAddsNoKey        $U FailedBuildRefusal=FALSE
unique-no-recheck            fail ErrorAddsNoKey        $U AdmissionRecheck=FALSE
no-retire-fence              fail RetiredIndexStaysEmpty $X RetireFence=FALSE
no-retired-skip              fail NoRetiredWriteFails   $X RetiredSkips=FALSE
no-cleanup-catalog-check     fail IndexMatchesTable     $A CleanupChecksCatalog=FALSE
no-uncommitted-fence         fail IndexMatchesTable     $AI UncommittedFence=FALSE
no-fence-catalog-check       fail NoRetiredFenceFails   $X RetiredFenceSkips=FALSE
no-retire-proof-delete       fail RolledBackLeavesNoBarrierProof $AI RetireDeletesProof=FALSE
no-catalog-fence             fail IndexMatchesTable     $D $F CatalogFence=FALSE
rebuild-no-catalog-fence     fail IndexMatchesTable     $FR CatalogFence=FALSE

in-txn-cross-node            fail IndexMatchesTable     $I HeldWriters={w1}
old-release-rebuild-online   fail IndexMatchesTable     BarrierlessBuilders={b1} BeginAfterDefine=TRUE $R
in-txn-cross-node-fenced     fail IndexMatchesTable     $I HeldWriters={w1} $F
retire-orphan-queue          fail RetiredQueuesStayEmpty $X INV=RetiredQueuesStayEmpty
rollback-stranded            fail NoStrandedUncommittedBuild $A INV=NoStrandedUncommittedBuild

no-prior-drain               pass -                     $R PriorDrain=FALSE
no-recheck                   pass -                     $RM AdmissionRecheck=FALSE
old-release-no-recheck       pass -                     BarrierlessBuilders={b1} BeginAfterDefine=TRUE RebuildFrom={\"Error\"} $RM AdmissionRecheck=FALSE

live-define                  pass -                     $LV $LP
live-faults                  pass -                     $LV $LP MaxFaults=1
live-takeover                pass -                     $LV $LP Builders={b1,b2} MaxTakeovers=1 MaxTxns=1
live-rebuild                 pass -                     $LV $LP MaxGen=2 MaxFaults=1 MaxTxns=1
live-in-txn                  pass -                     $LV $LP DefineInTxn=TRUE MaxFaults=1
live-no-barrier-exclusion    fail BarrierProceeds       $LV PROP=BarrierProceeds MaxFaults=1 BarrierExcludesLapsed=FALSE
live-unfair-commit           fail BarrierProceeds       $LV PROP=BarrierProceeds Unfair={\"Commit\"}
live-unfair-release          fail AdmissionWaitEnds     $LV PROP=AdmissionWaitEnds Unfair={\"Release\"}
live-unfair-publish          fail BuildSettles          $LV PROP=BuildSettles Unfair={\"Publish\"}
live-unfair-adopt            fail BuildSettles          $LV PROP=BuildSettles MaxFaults=1 Unfair={\"Adopt\"}

sim-rebuild-takeover         pass -                     $S
sim-rebuild-takeover-keyed   pass -                     $S Counting=FALSE
sim-in-txn                   pass -                     $S DefineInTxn=TRUE
sim-reach-rebuild-takeover   fail NeverFreshRebuildTakenOverOnline $S INV=NeverFreshRebuildTakenOverOnline
sim-no-markers               fail IndexMatchesTable     $S PrimaryMarkers=FALSE
sim-rebuild-fenced           pass -                     $S $F BeginAfterDefine=TRUE
sim-reach-rebuild-fenced     fail NeverFencedRebuildRefusedOnline $S $F BeginAfterDefine=TRUE INV=NeverFencedRebuildRefusedOnline
"
# `no-markers` fails only for a counting index: replaying a mutation onto a
# key-valued index is idempotent, so the `!bp` baseline changes nothing there.
# `in-txn-cross-node` and `old-release-rebuild-online` are gaps the design
# leaves open: a write through another node during a build inside the
# defining transaction, and a rebuild that reads without the barrier while
# writers maintain the online index directly. The catalog fence leaves the
# first open (`in-txn-cross-node-fenced`) and closes the second
# (`rebuild-fenced`), as long as every writer reads the catalog version locked
# (`rebuild-no-catalog-fence`).
# The last two rows of that group are open gaps around a retired or
# uncommitted id, each named after the invariant it checks with INV=: on a
# store that does not fence catalog changes, a write queued before a
# retirement commits its `!bg` and `!bp` after it, outside the reclaimed
# prefix (a fencing store refuses it: `retire-orphan-queue-fenced`); and an uncommitted build whose node died before it queued
# compaction work is reached by no cleanup. Neither is read by a later
# definition, which draws a fresh id.
# The last three rows pass because another mechanism covers what the removed
# one protects: the barrier covers the prior-generation drain, and the two
# drains cover the admission recheck, with or without the barrier.
# `reach-unique-key-after-error` and `reach-unique-stale-duplicate` reach
# states the code allows rather than states a pass claims to cover: the
# refusal in `Error` is made at admission, not at commit, and the build judges
# a record at its baseline and the queue in ticket order, not the table as
# committed.
# Each `live-*` fail row checks only the temporal property it pins, so TLC's
# counterexample is for that property. The `sim-*` fail rows show that the
# simulation budget reaches a deep state and finds a known violation at the
# larger bounds.

config() {
	cat <<'CFG'
SPECIFICATION Spec
CONSTANTS
    NumKeys = 1
    Vals = {a, b}
    Writers = {w1, w2}
    Builders = {b1}
    MaxGen = 1
    MaxTicket = 2
    MaxTxns = 2
    MaxMuts = 1
    MaxTakeovers = 0
    MaxFaults = 0
    Counting = TRUE
    DefineInTxn = FALSE
    HeldWriters = {w1, w2}
    BarrierlessBuilders = {}
    BeginAfterDefine = FALSE
    RebuildFrom = {"Online", "Error"}
    Unique = FALSE
    FencesCatalog = FALSE
    None = None
    ClosingBumpsTicket = TRUE
    OwnerFence = TRUE
    AdmissionRecheck = TRUE
    DrainReservations = TRUE
    PriorDrain = TRUE
    RestartDrainsPrior = TRUE
    WriteBarrier = TRUE
    BarrierExcludesLapsed = TRUE
    PrimaryMarkers = TRUE
    RebuildMarkers = TRUE
    WriteFence = TRUE
    UniqueCheck = TRUE
    ValueKeyedEntries = TRUE
    DuplicateFailsBuild = TRUE
    FailedBuildRefusal = TRUE
    RetireAs = "none"
    DefineAborts = FALSE
    RetireFence = TRUE
    RetiredSkips = TRUE
    CleanupChecksCatalog = TRUE
    UncommittedFence = TRUE
    RetiredFenceSkips = TRUE
    RetireDeletesProof = TRUE
    Unfair = {}
    CatalogFence = TRUE
SYMMETRY Symmetry
INVARIANTS
    TypeOK
    DuplicateNeverPublished
    UniqueWhenQueryable
    IndexMatchesTable
    NoStrandedQueue
    UniqueSlots
    ErrorAddsNoKey
    RetiredStateStaysGone
    RetiredIndexStaysEmpty
    NoRetiredWriteFails
    RolledBackLeavesNothing
    NoRetiredFenceFails
    RolledBackLeavesNoBarrierProof
PROPERTIES
    OnlineStays
CHECK_DEADLOCK FALSE
CFG
}

names="$(printf '%s\n' "$SCENARIOS" | awk 'NF && $1 !~ /^#/ { print $1 }')"
for requested in "$@"; do
	if ! printf '%s\n' "$names" | grep -qx -- "$requested"; then
		echo "unknown scenario: $requested" >&2
		exit 2
	fi
done

failures=0
ran=0
while read -r name expect property overrides; do
	case "$name" in "" | "#"*) continue ;; esac
	if [ "$#" -gt 0 ] && ! printf '%s\n' "$@" | grep -qx -- "$name"; then
		continue
	fi
	ran=$((ran + 1))
	cfg="$OUT/$name.cfg"
	log="$OUT/$name.log"
	config > "$cfg"
	sim=()
	depth=100
	for kv in $overrides; do
		key="${kv%%=*}"
		value="${kv#*=}"
		value="${value//,/, }"
		case "$key" in
		INV) printf 'INVARIANT %s\n' "$value" >> "$cfg" ;;
		PROP) printf 'PROPERTY %s\n' "$value" >> "$cfg" ;;
		SPEC)
			# Symmetry reduction is unsound for liveness. A liveness row checks
			# only the temporal properties it names with PROP=, so a violation
			# can be told apart; the safety rows check OnlineStays.
			sed -i.bak -e "s/^SPECIFICATION .*/SPECIFICATION $value/" -e '/^SYMMETRY /d' \
				-e '/^PROPERTIES$/,/^[^ ]/{/^PROPERTIES$/d;/^    /d;}' "$cfg"
			;;
		SIM) sim=(-simulate "num=$value") ;;
		DEPTH) depth="$value" ;;
		*)
			if ! grep -q "^    $key = " "$cfg"; then
				echo "scenario $name overrides unknown constant: $key" >&2
				exit 2
			fi
			sed -i.bak "s/^    $key = .*/    $key = $value/" "$cfg"
			;;
		esac
	done
	rm -f "$cfg.bak"
	[ "${#sim[@]}" -eq 0 ] || sim+=(-depth "$depth" -seed 1)
	(cd "$HERE" && java -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC -workers "${TLC_WORKERS:-auto}" -noGenerateSpecTE \
		${sim[@]+"${sim[@]}"} -metadir "$OUT/$name.states" -config "$cfg" concurrent_index_build.tla) > "$log" 2>&1 || true
	rm -rf "$OUT/$name.states"
	if grep -qE "(Invariant|property) $property (of the behavior )?(is|was) violated" "$log"; then
		got=fail
	elif grep -q "Temporal properties were violated" "$log"; then
		# TLC releases that do not name the temporal property they violated:
		# the row's configuration must check that one property and no other.
		if [ "$(grep -c '^PROPERTY ' "$cfg")" -eq 1 ] && grep -qx "PROPERTY $property" "$cfg" \
			&& ! grep -q '^PROPERTIES$' "$cfg"; then
			got=fail
		else
			got="fail on an unnamed temporal property"
		fi
	elif grep -qE "(is|was) violated" "$log"; then
		got="fail on another property"
	elif grep -q "No error has been found" "$log"; then
		got=pass
	elif [ "${#sim[@]}" -gt 0 ] && grep -q "^Finished in" "$log" && ! grep -q "^Error" "$log"; then
		got=pass
	else
		got=error
	fi
	if [ "${#sim[@]}" -gt 0 ]; then
		traces="$(grep -oE '[0-9,]+ traces generated' "$log" | tail -1 || true)"
		states="simulated, not exhaustive: ${traces:-no traces generated} of at most $depth steps"
	else
		states="$(grep -oE '[0-9,]+ distinct states found' "$log" | tail -1 || true)"
	fi
	if [ "$got" = "$expect" ]; then
		printf 'ok    %-28s %-5s %s\n' "$name" "$got" "$states"
	else
		printf 'FAIL  %-28s expected %s, got %s (%s)\n' "$name" "$expect" "$got" "$log"
		failures=$((failures + 1))
	fi
done <<< "$SCENARIOS"

echo "logs and counterexamples: $OUT"
if [ "$ran" -eq 0 ]; then
	echo "no scenario ran" >&2
	exit 2
fi
[ "$failures" -eq 0 ]
