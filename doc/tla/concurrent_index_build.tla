------------------------ MODULE concurrent_index_build ------------------------
(***************************************************************************)
(* The cluster protocol that builds one index over one table while user    *)
(* writes continue (`surrealdb/core/src/kvs/index/`). The index counts,    *)
(* holds keys, or holds UNIQUE keys (Unique).                              *)
(*                                                                         *)
(* Durable keys, one variable each:                                        *)
(*   bs   `!bs`  build state: generation, phase, owner, scan progress      *)
(*   bt   `!bt`  per-generation writer ticket counter                      *)
(*   br   `!br`  writer reservations                                       *)
(*   bg   `!bg`  queued mutations, replayed by the builder                 *)
(*   bp   `!bp`  per-record pointer to the first queued mutation           *)
(*   bw   `!bw`  generation whose write barrier a builder has waited at    *)
(*   def  the catalog definition of the index                              *)
(*   rec  the table, idx the index entries (record key, value)             *)
(*        A UNIQUE entry is stored under its value alone and names the     *)
(*        record that holds it, so `idx` has one record at most per value. *)
(*                                                                         *)
(* The keys belong to one index id. A schema statement may retire the id   *)
(* (`REMOVE INDEX`, or `DEFINE INDEX OVERWRITE` for the id it replaces),   *)
(* and the defining statement may roll back or die with its node. A later  *)
(* definition of the name draws a fresh id, so it shares none of these     *)
(* keys and is not modelled.                                               *)
(*                                                                         *)
(* Granularity. Every builder transaction compare-and-swaps `!bs`, so      *)
(* builder transactions conflict pairwise and each is one atomic step.     *)
(* A writer runs two transactions whose reads and commit are separated by  *)
(* other steps: the admission transaction that reserves a ticket, and the  *)
(* user transaction. The user transaction reads at the snapshot it began   *)
(* with and commits under first-committer-wins on the keys it writes.      *)
(* The admission commit is a compare-and-swap on `!bt` by value.           *)
(*                                                                         *)
(* Time is not modelled. A lease lapses whenever the model chooses, up to  *)
(* MaxTakeovers times, so safety may rest only on the fences, never on     *)
(* lease timing. A writer node whose heartbeat lapses is `fenced`: the     *)
(* drain may treat its reservations as dead, the write barrier leaves it   *)
(* out, and with WriteFence its datastore refuses its commits.             *)
(*                                                                         *)
(* On a store that fences catalog changes (FencesCatalog), every write of  *)
(* the table's records reads the table's catalog version as a locked read, *)
(* and `DEFINE INDEX` and `REBUILD INDEX` stamp that version as they       *)
(* commit. A build of a generation its statement installed, which reads    *)
(* once that statement has committed, then waits for no other node.        *)
(*                                                                         *)
(* Each mechanism switch removes one protection. All are TRUE in           *)
(* concurrent_index_build.cfg. check_concurrent_index_build.sh runs the    *)
(* scenarios, turns each switch off in turn, and states which outcome TLC  *)
(* must reach for each (see README.md).                                    *)
(***************************************************************************)
EXTENDS Naturals, Integers, FiniteSets, Sequences, TLC

CONSTANTS
    NumKeys,        \* records are keys 1..NumKeys
    Vals,           \* values an indexed field can hold
    Writers,        \* user sessions, one node each
    Builders,       \* builder tasks
    MaxGen,         \* generations: the DEFINE, then REBUILDs
    MaxTicket,      \* tickets per generation
    MaxTxns,        \* user transactions in a behaviour
    MaxMuts,        \* mutations per user transaction
    MaxTakeovers,   \* lease lapses of a live owner
    MaxFaults,      \* builder crashes/failures and writer node lapses
    Counting,       \* entries are counts (COUNT, full-text statistics), not keys
    DefineInTxn,    \* the build runs inside the defining transaction (blocking, BEGIN...COMMIT)
    HeldWriters,    \* writers on the defining node, which `DefinitionHold` holds back
    BarrierlessBuilders, \* builders on a release without the write barrier; they
                         \* start generations and take none over
    BeginAfterDefine, \* every user transaction opens after the definition commits
    RebuildFrom,    \* phases a `REBUILD INDEX` may start from
    Unique,         \* the index is UNIQUE, and so key-valued
    RetireAs,       \* "none", or how the catalog answers for the id once a schema
                    \* statement retires it: "remove" (`REMOVE INDEX`) or
                    \* "overwrite" (`DEFINE INDEX OVERWRITE` gave the name a new id)
    DefineAborts,   \* the defining statement may roll back, die with its node, or
                    \* commit with an unknown outcome
    FencesCatalog,  \* the store fences catalog changes, so a build that reads after
                    \* its statement's stamp asks no other node
    None

\* Mechanism switches.
CONSTANTS
    ClosingBumpsTicket,   \* `Closing` advances `!bt`, failing in-flight admissions
    OwnerFence,           \* builder writes CAS `!bs` on (generation, owner)
    AdmissionRecheck,     \* writer re-reads `!bs` after reserving and per cached mutation
    DrainReservations,    \* `Closing` waits for every `!br` before the last replay
    PriorDrain,           \* a new generation drains older `!br` before scanning
    RestartDrainsPrior,   \* a takeover that restarts the scan drains older `!br` again
    WriteBarrier,         \* the scan waits for write transactions older than its wait
    BarrierExcludesLapsed, \* the barrier does not wait for a node whose heartbeat lapsed
    PrimaryMarkers,       \* the scan baselines a queued record from its `!bp` entry
    RebuildMarkers,       \* an initial scan first rebuilds `!bp` from the queued mutations
    WriteFence,           \* a node whose heartbeat lapsed commits nothing
    UniqueCheck,          \* a UNIQUE write is refused a value another record holds
    ValueKeyedEntries,    \* a UNIQUE entry is keyed by its value, so its writes conflict
    DuplicateFailsBuild,  \* a UNIQUE build that meets a value held by another record fails
    FailedBuildRefusal,   \* a UNIQUE index in `Error` refuses a write that adds a key
    RetireFence,          \* a builder transaction that finds `!bs` gone commits nothing
    RetiredSkips,         \* admission and the cached recheck skip an index the catalog retired
    CleanupChecksCatalog, \* the rollback cleanup leaves a build whose definition committed
    UncommittedFence,     \* retiring an uncommitted build fences its defining statement
    RetiredFenceSkips,   \* the fence that finds `!bs` gone skips an index the catalog retired
    RetireDeletesProof,   \* retiring an uncommitted build deletes its barrier proof `!bw`
    CatalogFence          \* on a fencing store, a write reads its table's catalog
                          \* version locked, so a stamp since its snapshot fails it

ASSUME Unique => ~Counting

\* Steps that LiveSpec leaves without fairness, named as in BuilderStep and
\* WriterFinish. Empty except in the liveness rows that show a property
\* rests on one step's fairness.
CONSTANT Unfair

Keys == 1..NumKeys
Gens == 1..MaxGen
Phases == {"Building", "Closing", "Online", "Error"}
Opt(S) == S \cup {None}

VARIABLES
    def, epoch, rec, idx,
    bs, bt, br, bg, bp, bw,
    defineRequested, rebuildRequested,
    \* writers
    wpc, wcur, wsnapBp, wdef, wepoch, wadm, wread, wpend,
    wrecW, wnorm, wbg, wbp, wkeys, wdirty, wseq, wmuts, wres,
    wsnapIdx, fenced,
    \* builders
    bpc, bgen, btarget,
    \* the catalog fence: a `REBUILD INDEX` whose generation is installed and
    \* whose stamp has not committed, and the builders whose build reads after
    \* its statement's stamp
    rebuildOpen, bstamp,
    \* bounds, and the events the reachability checks look for
    txns, takeovers, faults, events,
    \* retirement and rollback
    retired, reclaimed, remEpoch, cleanup, swept

storeVars   == <<def, epoch, rec, idx, bs, bt, br, bg, bp, bw,
                 defineRequested, rebuildRequested>>
writerVars  == <<wpc, wcur, wsnapBp, wdef, wepoch, wadm, wread, wpend,
                 wrecW, wnorm, wbg, wbp, wkeys, wdirty, wseq,
                 wmuts, wres, wsnapIdx, fenced>>
builderVars == <<bpc, bgen, btarget>>
boundVars   == <<txns, takeovers, faults, events>>
\* retired    a schema statement retired the id
\* reclaimed  the deferred reclaim of the retired id's index data has run
\* remEpoch   the epoch the retirement committed in
\* cleanup    the defining statement's rollback hook: "none", "pending" (it
\*            runs on the statement's node), "done", or "lost" (the node died)
\* swept      None, or the phase of the build state a rollback cleanup or a
\*            cluster-side retirement deleted ("unstaged" when there was none)
schemaVars  == <<retired, reclaimed, remEpoch, cleanup, swept>>
fenceVars   == <<rebuildOpen, bstamp>>
vars == <<storeVars, writerVars, builderVars, boundVars, schemaVars, fenceVars>>

-----------------------------------------------------------------------------
(* Key names, for first-committer-wins against open user transactions. *)
RecKey(k)    == <<"rec", k>>
\* A UNIQUE entry is keyed by its value, so two writes of one value conflict
\* whichever records they are for.
IdxKey(k, v) == IF Unique /\ ValueKeyedEntries THEN <<"idx", v>> ELSE <<"idx", k, v>>
BpKey(g, k)  == <<"bp", g, k>>
CvKey        == <<"cv">>   \* the table's catalog version

\* A commit by anyone else marks the keys it wrote on every open user
\* transaction; that transaction then fails its commit if it wrote one.
OpenTxn(w) == wpc[w] \in {"ready", "admit", "admitCommit", "fence"}
TouchBy(self, K) ==
    wdirty' = [w \in Writers |->
                 IF w # self /\ OpenTxn(w) THEN wdirty[w] \cup K ELSE wdirty[w]]
Touch(K) == TouchBy(None, K)

KeepWriters == UNCHANGED <<wpc, wcur, wsnapBp, wsnapIdx, wdef, wepoch, wadm, wread, wpend,
                           wrecW, wnorm, wbg, wbp, wkeys, wseq,
                           wmuts, wres, fenced>>

\* `DEFINE INDEX` and `REBUILD INDEX` stamp the table's catalog version as
\* they commit (`Transaction::stamp_catalog_version`), a blind write. On a
\* store that fences catalog changes it fails every open write transaction
\* that read the version; on any other store nothing reads it locked, and the
\* model leaves it out.
Stamp == IF FencesCatalog THEN Touch({CvKey}) ELSE UNCHANGED wdirty

\* The index holds a number per (record, value) entry. A key-valued index
\* writes it present or absent, so maintaining it twice is harmless; a
\* counting one adds and subtracts, so every write must land exactly once.
\* A UNIQUE index has one slot per value: a write gives the slot to its
\* record, and a delete clears it only while it names that record, as a
\* compare-and-delete does.
Entries == Keys \X Vals
NoEntries == [e \in Entries |-> 0]
Put(ix, k, v, d) ==
    IF v = None THEN ix
    ELSE IF Unique /\ d > 0
    THEN [e \in Entries |-> IF e[2] = v THEN IF e[1] = k THEN 1 ELSE 0 ELSE ix[e]]
    ELSE [ix EXCEPT ![<<k, v>>] = IF Counting THEN @ + d ELSE IF d > 0 THEN 1 ELSE 0]
Apply(ix, k, old, new) == Put(Put(ix, k, old, -1), k, new, 1)
RECURSIVE ApplyAll(_, _)
ApplyAll(ix, ms) ==
    IF ms = <<>> THEN ix ELSE ApplyAll(Apply(ix, Head(ms).k, Head(ms).old, Head(ms).new), Tail(ms))
IdxKeysOf(k, old, new) == {IdxKey(k, v) : v \in ({old, new} \ {None})}

Proj == [e \in Entries |-> IF rec[e[1]] = e[2] THEN 1 ELSE 0]

\* The slot of value `v` names a record other than `k`.
HeldByOther(ix, k, v) == v # None /\ \E k2 \in Keys \ {k} : ix[<<k2, v>>] # 0
NoDuplicates(r) == \A k1, k2 \in Keys : k1 # k2 /\ r[k1] # None => r[k1] # r[k2]

\* Whether a mutation adds a UNIQUE key (`refuses_on_failed_build`). With one
\* indexed field, every mutation that leaves the record holding a value does.
AddsKey(m) == m.new # None

\* An event recorded only for a UNIQUE index, so that the other modes keep
\* their state space.
Note(ev) == IF Unique THEN events \cup {ev} ELSE events

\* The planner uses the index when `!bs` is `Online`, or absent under a
\* committed definition (`filter_online_indexes`).
Queryable == def /\ (bs = None \/ bs.phase = "Online")

\* A reservation the drain may delete: its writer committed a queued mutation
\* under it, or its node lapsed.
Retirable(r) == (\E e \in bg : e.g = r.g /\ e.t = r.t) \/ r.w \in fenced

\* A user transaction's cached admission: None until its first indexed
\* mutation, then Normal (maintain the index in the transaction) or the
\* admitted ticket [g, t, init].
Normal == [g |-> 0, t |-> 0, init |-> TRUE]
\* A cached decision to skip an index the catalog retired.
Skip == [g |-> 0, t |-> 0, init |-> FALSE]

\* What the catalog, read now, says of this id (`catalog_index_state`). Only
\* a writer whose snapshot holds the definition asks.
CatalogNow ==
    IF def THEN "Present"
    ELSE IF retired /\ RetireAs = "overwrite" THEN "Replaced"
    ELSE "Retired"

\* A rollback cleanup or a cluster-side retirement deleted the build state.
Swept == swept # None

\* The builder staged the generation the defining statement awaits.
Staged == bs # None \/ retired \/ Swept

\* Nothing of the id is left but its barrier proof: build state, ticket
\* counters, reservations, queues and index data.
NoBuildState ==
    /\ bs = None /\ bt = [g \in Gens |-> None]
    /\ br = {} /\ bg = {} /\ bp = {} /\ idx = NoEntries

CanCommit(w) == ~(WriteFence /\ w \in fenced)
ReleaseOf(w, r) == IF CanCommit(w) THEN br \ {r} ELSE br

-----------------------------------------------------------------------------
Init ==
    /\ def = FALSE /\ epoch = 0
    /\ rec \in [Keys -> Opt(Vals)]
    /\ idx = NoEntries
    /\ bs = None
    /\ bt = [g \in Gens |-> None]
    /\ br = {} /\ bg = {} /\ bp = {} /\ bw = 0
    /\ defineRequested = FALSE /\ rebuildRequested = FALSE
    /\ wpc = [w \in Writers |-> "idle"]
    /\ wcur = [w \in Writers |-> [k \in Keys |-> None]]
    /\ wsnapBp = [w \in Writers |-> {}]
    /\ wdef = [w \in Writers |-> FALSE]
    /\ wepoch = [w \in Writers |-> 0]
    /\ wadm = [w \in Writers |-> None]
    /\ wread = [w \in Writers |-> None]
    /\ wpend = [w \in Writers |-> None]
    /\ wrecW = [w \in Writers |-> {}]
    /\ wnorm = [w \in Writers |-> <<>>]
    /\ wbg = [w \in Writers |-> {}]
    /\ wbp = [w \in Writers |-> {}]
    /\ wkeys = [w \in Writers |-> {}]
    /\ wdirty = [w \in Writers |-> {}]
    /\ wseq = [w \in Writers |-> 0]
    /\ wmuts = [w \in Writers |-> 0]
    /\ wres = [w \in Writers |-> None]
    /\ wsnapIdx = [w \in Writers |-> NoEntries]
    /\ fenced = {}
    /\ bpc = [b \in Builders |-> "idle"]
    /\ bgen = [b \in Builders |-> 0]
    /\ btarget = [b \in Builders |-> 0]
    /\ txns = 0 /\ takeovers = 0 /\ faults = 0
    /\ events = IF Unique /\ ~NoDuplicates(rec) THEN {"duplicate"} ELSE {}
    /\ retired = FALSE /\ reclaimed = FALSE /\ remEpoch = 0
    /\ cleanup = "none" /\ swept = None
    /\ rebuildOpen = FALSE
    /\ bstamp = [b \in Builders |-> FALSE]

-----------------------------------------------------------------------------
(***************************************************************************)
(* Schema statements.                                                      *)
(***************************************************************************)

\* `DEFINE INDEX ... CONCURRENTLY` starts. Its builder stages generation 1
\* (AcquireNew) before the statement's transaction commits the definition.
\* The statement draws the id, so it runs once.
DefineStart ==
    /\ ~defineRequested /\ cleanup = "none"
    /\ defineRequested' = TRUE
    /\ UNCHANGED <<def, epoch, rec, idx, bs, bt, br, bg, bp, bw,
                   rebuildRequested>>
    /\ UNCHANGED <<writerVars, builderVars, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* The statement awaits the staging of its generation, and a build inside
\* the defining transaction is awaited before that transaction commits: its
\* state reached `Online` or `Error`, possibly before a cleanup deleted it.
CommitDefinition ==
    /\ defineRequested /\ ~def /\ ~retired /\ Staged
    /\ DefineInTxn => \/ bs # None /\ bs.phase \in {"Online", "Error"}
                      \/ swept \in {"Online", "Error"}
    /\ def' = TRUE
    /\ Stamp
    /\ UNCHANGED <<epoch, rec, idx, bs, bt, br, bg, bp, bw, defineRequested,
                   rebuildRequested>>
    /\ KeepWriters /\ UNCHANGED <<builderVars, boundVars>>
    /\ UNCHANGED fenceVars

DefineCommit == CommitDefinition /\ UNCHANGED schemaVars

\* `REBUILD INDEX` of a settled or failed generation; a builder installs the
\* next generation (AcquireNew), and the statement commits after it
\* (RebuildCommit).
Rebuild ==
    /\ def /\ ~rebuildRequested /\ ~rebuildOpen
    /\ bs # None /\ bs.phase \in RebuildFrom /\ bs.gen < MaxGen
    /\ rebuildRequested' = TRUE
    /\ UNCHANGED <<def, epoch, rec, idx, bs, bt, br, bg, bp, bw,
                   defineRequested>>
    /\ UNCHANGED <<writerVars, builderVars, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* The `REBUILD INDEX` statement commits, stamping the table, at any point
\* after its builder installed the generation. Only a fencing store observes
\* when, so on any other the model commits it with the install.
RebuildCommit ==
    /\ rebuildOpen
    /\ rebuildOpen' = FALSE
    /\ Stamp
    /\ KeepWriters
    /\ UNCHANGED <<storeVars, builderVars, boundVars, bstamp>>
    /\ UNCHANGED schemaVars

-----------------------------------------------------------------------------
(***************************************************************************)
(* Writers.                                                                *)
(***************************************************************************)

ResetTxn(w) ==
    /\ wadm' = [wadm EXCEPT ![w] = None]
    /\ wread' = [wread EXCEPT ![w] = None]
    /\ wpend' = [wpend EXCEPT ![w] = None]
    /\ wrecW' = [wrecW EXCEPT ![w] = {}]
    /\ wnorm' = [wnorm EXCEPT ![w] = <<>>]
    /\ wbg' = [wbg EXCEPT ![w] = {}]
    /\ wbp' = [wbp EXCEPT ![w] = {}]
    /\ wkeys' = [wkeys EXCEPT ![w] = {}]
    /\ wseq' = [wseq EXCEPT ![w] = 0]
    /\ wmuts' = [wmuts EXCEPT ![w] = 0]

Begin(w) ==
    /\ wpc[w] = "idle" /\ w \notin fenced /\ txns < MaxTxns
    /\ BeginAfterDefine => def
    /\ txns' = txns + 1
    /\ wpc' = [wpc EXCEPT ![w] = "ready"]
    /\ wcur' = [wcur EXCEPT ![w] = rec]
    /\ wsnapBp' = [wsnapBp EXCEPT ![w] = bp]
    /\ wsnapIdx' = [wsnapIdx EXCEPT ![w] = IF Unique THEN idx ELSE NoEntries]
    /\ wdef' = [wdef EXCEPT ![w] = def]
    /\ wepoch' = [wepoch EXCEPT ![w] = epoch]
    /\ wdirty' = [wdirty EXCEPT ![w] = {}]
    /\ ResetTxn(w)
    /\ UNCHANGED <<wres, fenced, storeVars, builderVars, takeovers, faults, events>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* The record write every mutation makes.
WriteRec(w, k, nv) ==
    /\ wcur' = [wcur EXCEPT ![w][k] = nv]
    /\ wrecW' = [wrecW EXCEPT ![w] = @ \cup {k}]
    /\ wmuts' = [wmuts EXCEPT ![w] = @ + 1]

\* Maintain the index in the user transaction.
NormalOps(w, m) ==
    /\ wnorm' = [wnorm EXCEPT ![w] = Append(@, m)]
    /\ wkeys' = [wkeys EXCEPT ![w] = @ \cup {RecKey(m.k)} \cup IdxKeysOf(m.k, m.old, m.new)]

\* Queue the mutation for the build under the admitted ticket `a`, and point
\* `!bp` at it when the scan has not completed and no marker is visible.
QueueOps(w, m, a) ==
    LET e == [g |-> a.g, t |-> a.t, s |-> wseq[w], k |-> m.k, old |-> m.old, new |-> m.new]
        visible == {x \in wsnapBp[w] \cup wbp[w] : x.g = a.g /\ x.k = m.k}
        mark == ~a.init /\ visible = {}
    IN /\ wbg' = [wbg EXCEPT ![w] = @ \cup {e}]
       /\ wseq' = [wseq EXCEPT ![w] = @ + 1]
       /\ wbp' = [wbp EXCEPT ![w] =
                    IF mark THEN @ \cup {[g |-> a.g, k |-> m.k, t |-> a.t, s |-> e.s]} ELSE @]
       /\ wkeys' = [wkeys EXCEPT ![w] =
                    @ \cup {RecKey(m.k)} \cup (IF mark THEN {BpKey(a.g, m.k)} ELSE {})]

MutationsOf(w) ==
    UNION {{[k |-> k, old |-> wcur[w][k], new |-> nv] : nv \in Opt(Vals) \ {wcur[w][k]}}
           : k \in Keys}

CanMutate(w) == wpc[w] = "ready" /\ wmuts[w] < MaxMuts

\* The UNIQUE check of a write that maintains the index in the user
\* transaction (`IndexOperation::index_unique`, `move_entries`): its value's
\* entry, as the transaction reads it, names another record, so the write is
\* refused with `IndexExists`. The read is the transaction's snapshot with its
\* own writes; an entry committed past the snapshot fails the commit instead,
\* through the value-keyed entry both write.
Clashes(w, m) ==
    Unique /\ UniqueCheck /\ HeldByOther(ApplyAll(wsnapIdx[w], wnorm[w]), m.k, m.new)

\* A UNIQUE index whose build failed refuses a mutation that adds a key, at
\* every read of `!bs` that admits a write (`refuses_on_failed_build`).
RefusedInError(m) ==
    Unique /\ FailedBuildRefusal /\ AddsKey(m) /\ bs # None /\ bs.phase = "Error"

\* A mutation that adds a key, queued for a UNIQUE generation that has
\* failed.
KeyIntoError(m) == Unique /\ AddsKey(m) /\ bs # None /\ bs.phase = "Error"

\* While a build runs inside the transaction that defines its index, a write
\* through the defining node, in a transaction that opened after the build
\* began and cannot see the definition, waits for that transaction and is
\* refused once it has committed (`DefinitionHold`). An older transaction is
\* left to the write barrier.
Held(w) ==
    /\ DefineInTxn /\ w \in HeldWriters /\ defineRequested /\ ~wdef[w]
    /\ bs # None /\ wepoch[w] >= bs.epoch

\* A transaction whose snapshot predates the definition maintains nothing.
MutateUnindexed(w) ==
    /\ CanMutate(w) /\ ~wdef[w] /\ ~Held(w)
    /\ \E m \in MutationsOf(w) :
        /\ WriteRec(w, m.k, m.new)
        /\ wkeys' = [wkeys EXCEPT ![w] = @ \cup {RecKey(m.k)}]
    /\ UNCHANGED <<wpc, wsnapBp, wsnapIdx, wdef, wepoch, wadm, wread, wpend, wnorm, wbg,
                   wbp, wdirty, wseq, wres, fenced>>
    /\ UNCHANGED <<storeVars, builderVars, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

MutateNormal(w) ==
    /\ CanMutate(w) /\ wdef[w] /\ wadm[w] = Normal
    /\ \E m \in MutationsOf(w) :
        ~Clashes(w, m) /\ WriteRec(w, m.k, m.new) /\ NormalOps(w, m)
    /\ UNCHANGED <<wpc, wsnapBp, wsnapIdx, wdef, wepoch, wadm, wread, wpend, wbg, wbp,
                   wdirty, wseq, wres, fenced>>
    /\ UNCHANGED <<storeVars, builderVars, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* A later mutation under a cached ticket revalidates against `!bs`
\* (`recheck_cached_admission`); a rotated or online build fails the
\* transaction, and a failed UNIQUE build refuses a mutation that adds a key.
CachedStillValid(w) ==
    ~AdmissionRecheck \/
    (bs # None /\ bs.gen = wadm[w].g /\ bs.phase \in {"Building", "Closing", "Error"})

MutateQueued(w) ==
    /\ CanMutate(w) /\ wdef[w] /\ wadm[w] \notin {None, Normal, Skip}
    /\ CachedStillValid(w)
    /\ \E m \in MutationsOf(w) :
        /\ ~(AdmissionRecheck /\ RefusedInError(m))
        /\ WriteRec(w, m.k, m.new) /\ QueueOps(w, m, wadm[w])
        /\ events' = IF KeyIntoError(m) THEN Note("keyQueuedInError") ELSE events
    /\ UNCHANGED <<wpc, wsnapBp, wsnapIdx, wdef, wepoch, wadm, wread, wpend, wnorm,
                   wdirty, wres, fenced>>
    /\ UNCHANGED <<storeVars, builderVars, txns, takeovers, faults>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

MutateNeedsAdmission(w) ==
    /\ CanMutate(w) /\ wdef[w] /\ wadm[w] = None
    /\ \E m \in MutationsOf(w) :
        /\ WriteRec(w, m.k, m.new)
        /\ wpend' = [wpend EXCEPT ![w] = m]
    /\ wpc' = [wpc EXCEPT ![w] = "admit"]
    /\ UNCHANGED <<wsnapBp, wsnapIdx, wdef, wepoch, wadm, wread, wnorm, wbg,
                   wbp, wkeys, wdirty, wseq, wres, fenced>>
    /\ UNCHANGED <<storeVars, builderVars, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* Admission transaction, read half (`reserve_durable_admission`). `Closing`
\* waits; an online index, or a state-less one the catalog still names, is
\* maintained in the user transaction; a building or failed one reads the
\* ticket counter. On a fencing store `wread` keeps the generation the
\* transaction maintains directly, for the reachability check of
\* `rebuild-fenced`.
AdmitNormal(w) ==
    /\ wpc[w] = "admit"
    /\ \/ bs # None /\ bs.phase = "Online"
       \/ bs = None /\ CatalogNow = "Present"
    /\ ~Clashes(w, wpend[w])
    /\ wadm' = [wadm EXCEPT ![w] = Normal]
    /\ wread' = IF FencesCatalog
                THEN [wread EXCEPT ![w] = [g |-> IF bs = None THEN 0 ELSE bs.gen,
                                           t |-> 0, init |-> TRUE]]
                ELSE wread
    /\ NormalOps(w, wpend[w])
    /\ wpend' = [wpend EXCEPT ![w] = None]
    /\ wpc' = [wpc EXCEPT ![w] = "ready"]
    /\ UNCHANGED <<wcur, wsnapBp, wsnapIdx, wdef, wepoch, wrecW, wbg, wbp, wdirty,
                   wseq, wmuts, wres, fenced>>
    /\ UNCHANGED <<storeVars, builderVars, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

AdmitRead(w) ==
    /\ wpc[w] = "admit"
    /\ bs # None /\ bs.phase \in {"Building", "Error"}
    /\ ~RefusedInError(wpend[w])
    /\ bt[bs.gen] # None /\ bt[bs.gen] < MaxTicket
    /\ wread' = [wread EXCEPT ![w] = [g |-> bs.gen, t |-> bt[bs.gen], init |-> bs.init]]
    /\ wpc' = [wpc EXCEPT ![w] = "admitCommit"]
    /\ UNCHANGED <<wcur, wsnapBp, wsnapIdx, wdef, wepoch, wadm, wpend, wrecW, wnorm, wbg,
                   wbp, wkeys, wdirty, wseq, wmuts, wres, fenced>>
    /\ UNCHANGED <<storeVars, builderVars, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* Admission transaction, commit half: `!br` plus a compare-and-swap on `!bt`.
AdmitCommit(w) ==
    /\ wpc[w] = "admitCommit" /\ CanCommit(w)
    /\ LET a == wread[w] IN
       IF bt[a.g] = a.t
       THEN /\ br' = br \cup {[g |-> a.g, t |-> a.t, w |-> w]}
            /\ bt' = [bt EXCEPT ![a.g] = a.t + 1]
            /\ wres' = [wres EXCEPT ![w] = [g |-> a.g, t |-> a.t, w |-> w]]
            /\ wpc' = [wpc EXCEPT ![w] = "fence"]
       ELSE /\ wpc' = [wpc EXCEPT ![w] = "admit"]
            /\ UNCHANGED <<br, bt, wres>>
    /\ UNCHANGED <<def, epoch, rec, idx, bs, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ UNCHANGED <<wcur, wsnapBp, wsnapIdx, wdef, wepoch, wadm, wread, wpend, wrecW,
                   wnorm, wbg, wbp, wkeys, wdirty, wseq, wmuts, fenced>>
    /\ UNCHANGED <<builderVars, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* After the reservation commits, a fresh read of `!bs`
\* (`fence_durable_admission`): a rotated generation fails the transaction,
\* an online build is maintained in the user transaction, a failed UNIQUE
\* build refuses a mutation that adds a key, anything else queues.
FenceQueue(w) ==
    /\ wpc[w] = "fence"
    /\ ~AdmissionRecheck \/ (bs # None /\ bs.gen = wread[w].g
                             /\ bs.phase \in {"Building", "Closing", "Error"}
                             /\ ~RefusedInError(wpend[w]))
    /\ wadm' = [wadm EXCEPT ![w] = wread[w]]
    /\ QueueOps(w, wpend[w], wread[w])
    /\ wpend' = [wpend EXCEPT ![w] = None]
    /\ wpc' = [wpc EXCEPT ![w] = "ready"]
    /\ events' = IF KeyIntoError(wpend[w]) THEN Note("keyQueuedInError")
                 ELSE IF bs # None /\ bs.phase = "Error" THEN Note("deleteQueuedInError")
                 ELSE events
    /\ UNCHANGED <<wcur, wsnapBp, wsnapIdx, wdef, wepoch, wread, wrecW, wnorm,
                   wdirty, wmuts, wres, fenced>>
    /\ UNCHANGED <<storeVars, builderVars, txns, takeovers, faults>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

FenceOnline(w) ==
    /\ wpc[w] = "fence" /\ AdmissionRecheck
    /\ bs # None /\ bs.gen = wread[w].g /\ bs.phase = "Online"
    /\ ~Clashes(w, wpend[w])
    /\ br' = ReleaseOf(w, wres[w])
    /\ wres' = [wres EXCEPT ![w] = None]
    /\ wadm' = [wadm EXCEPT ![w] = Normal]
    /\ NormalOps(w, wpend[w])
    /\ wpend' = [wpend EXCEPT ![w] = None]
    /\ wpc' = [wpc EXCEPT ![w] = "ready"]
    /\ UNCHANGED <<def, epoch, rec, idx, bs, bt, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ UNCHANGED <<wcur, wsnapBp, wsnapIdx, wdef, wepoch, wread, wrecW, wbg, wbp, wdirty,
                   wseq, wmuts, fenced>>
    /\ UNCHANGED <<builderVars, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* The fence finds `!bs` gone and the catalog no longer naming the index
\* (`fence_durable_admission`): it releases the reservation, and the write
\* skips the index for the rest of the transaction.
FenceRetired(w) ==
    /\ wpc[w] = "fence" /\ AdmissionRecheck /\ RetiredFenceSkips
    /\ bs = None /\ CatalogNow = "Retired"
    /\ br' = ReleaseOf(w, wres[w])
    /\ wres' = [wres EXCEPT ![w] = None]
    /\ wadm' = [wadm EXCEPT ![w] = Skip]
    /\ wkeys' = [wkeys EXCEPT ![w] = @ \cup {RecKey(wpend[w].k)}]
    /\ wpend' = [wpend EXCEPT ![w] = None]
    /\ wpc' = [wpc EXCEPT ![w] = "ready"]
    /\ events' = events \cup {"retiredFenceSkip"}
    /\ UNCHANGED <<def, epoch, rec, idx, bs, bt, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ UNCHANGED <<wcur, wsnapBp, wsnapIdx, wdef, wepoch, wread, wrecW, wnorm, wbg, wbp,
                   wdirty, wseq, wmuts, fenced>>
    /\ UNCHANGED <<builderVars, txns, takeovers, faults>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* The transaction ends without committing anything, and releases its
\* reservation once it has closed.
EndTxn(w) ==
    /\ wpc' = [wpc EXCEPT ![w] = IF wres[w] # None THEN "release" ELSE "idle"]
    /\ ResetTxn(w)
    /\ UNCHANGED <<wcur, wsnapBp, wsnapIdx, wdef, wepoch, wdirty, wres, fenced>>

\* A missing `!bs` fails the transaction too, unless the catalog, read in
\* the transaction that found it missing, says the index was retired
\* (FenceRetired). A replaced index fails it, since a writer holding the old
\* id does not maintain the replacement, and so does a build that vanished
\* under a definition that still stands.
FenceRotated(w) ==
    /\ wpc[w] = "fence" /\ AdmissionRecheck
    /\ IF bs = None THEN ~(RetiredFenceSkips /\ CatalogNow = "Retired")
       ELSE bs.gen # wread[w].g
    /\ EndTxn(w)
    /\ events' = events \cup {"fenceRotated"}
                 \cup (IF bs = None /\ CatalogNow = "Retired" THEN {"retiredFenceFail"} ELSE {})
    /\ UNCHANGED <<storeVars, builderVars, txns, takeovers, faults>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

HeldRefused(w) ==
    /\ CanMutate(w) /\ Held(w) /\ def
    /\ EndTxn(w)
    /\ UNCHANGED <<storeVars, builderVars, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* A missing `!bs` under a definition the catalog retired is skipped, not
\* failed (`MutateRetired`); under one it still names or has replaced, it
\* fails, since the writer cannot reach the replacement.
RetiredSkipped == RetiredSkips /\ bs = None /\ CatalogNow = "Retired"

\* A refused write fails the transaction, which commits nothing. A fence
\* that refuses releases the reservation first (`releaseNow`); any other is
\* released once the transaction closes.
Refuse(w, ev, releaseNow) ==
    /\ br' = IF releaseNow THEN ReleaseOf(w, wres[w]) ELSE br
    /\ wres' = IF releaseNow THEN [wres EXCEPT ![w] = None] ELSE wres
    /\ wpc' = [wpc EXCEPT ![w] =
                 IF ~releaseNow /\ wres[w] # None THEN "release" ELSE "idle"]
    /\ ResetTxn(w)
    /\ events' = Note(ev)
    /\ UNCHANGED <<def, epoch, rec, idx, bs, bt, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ UNCHANGED <<wcur, wsnapBp, wsnapIdx, wdef, wepoch, wdirty, fenced>>
    /\ UNCHANGED <<builderVars, txns, takeovers, faults>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* A write that maintains a UNIQUE index in the user transaction finds its
\* value held by another record (`IndexExists`).
UniqueRefused(w) ==
    \/ /\ CanMutate(w) /\ wdef[w] /\ wadm[w] = Normal
       /\ \E m \in MutationsOf(w) : Clashes(w, m)
       /\ Refuse(w, "uniqueRefusedDirect", FALSE)
    \/ /\ wpc[w] = "admit"
       /\ \/ bs # None /\ bs.phase = "Online"
          \/ bs = None /\ CatalogNow = "Present"
       /\ Clashes(w, wpend[w])
       /\ Refuse(w, "uniqueRefused", FALSE)
    \/ /\ wpc[w] = "fence" /\ AdmissionRecheck
       /\ bs # None /\ bs.gen = wread[w].g /\ bs.phase = "Online"
       /\ Clashes(w, wpend[w])
       /\ Refuse(w, "uniqueRefused", TRUE)

\* A write that adds a key to a UNIQUE index whose build failed is refused
\* with `IndexBuildFailed` (`failed_build_refusal`): by the admission read
\* before it reserves, by the fence after, or by the recheck of a cached
\* ticket.
ErrorRefused(w) ==
    \/ /\ wpc[w] = "admit" /\ RefusedInError(wpend[w])
       /\ Refuse(w, "errorRefused", FALSE)
    \/ /\ wpc[w] = "fence" /\ AdmissionRecheck
       /\ RefusedInError(wpend[w]) /\ bs.gen = wread[w].g
       /\ Refuse(w, "errorRefused", TRUE)
    \/ /\ CanMutate(w) /\ wdef[w] /\ wadm[w] \notin {None, Normal, Skip}
       /\ AdmissionRecheck /\ CachedStillValid(w)
       /\ \E m \in MutationsOf(w) : RefusedInError(m)
       /\ Refuse(w, "errorRefusedCached", FALSE)

RecheckFails(w) ==
    /\ CanMutate(w) /\ wdef[w] /\ wadm[w] \notin {None, Normal, Skip}
    /\ ~CachedStillValid(w) /\ ~RetiredSkipped
    /\ EndTxn(w)
    /\ events' = events \cup {"recheckFailed"}
                 \cup (IF bs = None /\ CatalogNow = "Retired" THEN {"retiredRecheckFail"} ELSE {})
    /\ UNCHANGED <<storeVars, builderVars, txns, takeovers, faults>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* `!bs` is missing and the catalog, read in a transaction of its own, no
\* longer names the index (`recheck_cached_admission`): the mutation skips the
\* index, and the transaction caches the decision.
MutateRetired(w) ==
    /\ CanMutate(w) /\ wdef[w] /\ wadm[w] \notin {None, Normal, Skip}
    /\ AdmissionRecheck /\ RetiredSkipped
    /\ \E m \in MutationsOf(w) :
        /\ WriteRec(w, m.k, m.new)
        /\ wkeys' = [wkeys EXCEPT ![w] = @ \cup {RecKey(m.k)}]
    /\ wadm' = [wadm EXCEPT ![w] = Skip]
    /\ events' = events \cup {"retiredRecheckSkip"}
    /\ UNCHANGED <<wpc, wsnapBp, wsnapIdx, wdef, wepoch, wread, wpend, wnorm, wbg,
                   wbp, wdirty, wseq, wres, fenced>>
    /\ UNCHANGED <<storeVars, builderVars, txns, takeovers, faults>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

MutateSkipped(w) ==
    /\ CanMutate(w) /\ wdef[w] /\ wadm[w] = Skip
    /\ \E m \in MutationsOf(w) :
        /\ WriteRec(w, m.k, m.new)
        /\ wkeys' = [wkeys EXCEPT ![w] = @ \cup {RecKey(m.k)}]
    /\ UNCHANGED <<wpc, wsnapBp, wsnapIdx, wdef, wepoch, wadm, wread, wpend, wnorm, wbg,
                   wbp, wdirty, wseq, wres, fenced>>
    /\ UNCHANGED <<storeVars, builderVars, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* First-use admission finds `!bs` missing and reads the catalog in the same
\* transaction (`reserve_durable_admission`). A retired index is skipped. A
\* replaced one fails the write unless the user transaction already sees the
\* replacement (`write_maintains_replacement`), which a writer holding the old
\* id does not.
AdmitRetired(w) ==
    /\ wpc[w] = "admit" /\ bs = None /\ CatalogNow = "Retired"
    /\ IF RetiredSkips
       THEN /\ wadm' = [wadm EXCEPT ![w] = Skip]
            /\ wkeys' = [wkeys EXCEPT ![w] = @ \cup {RecKey(wpend[w].k)}]
            /\ wpend' = [wpend EXCEPT ![w] = None]
            /\ wpc' = [wpc EXCEPT ![w] = "ready"]
            /\ events' = events \cup {"retiredAdmitSkip"}
            /\ UNCHANGED <<wread, wrecW, wnorm, wbg, wbp, wseq, wmuts>>
       ELSE /\ EndTxn(w)
            /\ events' = events \cup {"retiredAdmitFail"}
    /\ UNCHANGED <<wcur, wsnapBp, wsnapIdx, wdef, wepoch, wdirty, wres, fenced>>
    /\ UNCHANGED <<storeVars, builderVars, txns, takeovers, faults>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

AdmitReplaced(w) ==
    /\ wpc[w] = "admit" /\ bs = None /\ CatalogNow = "Replaced"
    /\ EndTxn(w)
    /\ events' = events \cup {"replacedFail"}
    /\ UNCHANGED <<storeVars, builderVars, txns, takeovers, faults>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* The user rolls back, or its context ends while admission waits on `Closing`.
Rollback(w) ==
    /\ wpc[w] \in {"ready", "admit"}
    /\ EndTxn(w)
    /\ UNCHANGED <<storeVars, builderVars, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* On a fencing store, a transaction that writes a record first reads the
\* table's catalog version as a locked read (`WriteCtx::resolve`,
\* `Transaction::catalog_version`), so a stamp committed since its snapshot
\* fails its commit.
LockedReads(w) == IF FencesCatalog /\ CatalogFence /\ wrecW[w] # {} THEN {CvKey} ELSE {}

\* Refused by a stamp alone: without the catalog fence it would commit.
StampRefused(w) ==
    /\ wkeys[w] \cap wdirty[w] = {} /\ CanCommit(w)
    /\ CvKey \in LockedReads(w) \cap wdirty[w]

\* The transaction maintains directly a generation that a later one replaced:
\* it found the index `Online` before a rebuild.
MaintainsOlderGen(w) ==
    wadm[w] = Normal /\ wread[w] # None /\ bs # None /\ wread[w].g < bs.gen

Commit(w) ==
    /\ wpc[w] = "ready"
    /\ IF (wkeys[w] \cup LockedReads(w)) \cap wdirty[w] = {} /\ CanCommit(w)
       THEN /\ rec' = [k \in Keys |-> IF k \in wrecW[w] THEN wcur[w][k] ELSE rec[k]]
            /\ idx' = ApplyAll(idx, wnorm[w])
            /\ bg' = bg \cup wbg[w]
            /\ bp' = bp \cup wbp[w]
            /\ TouchBy(w, wkeys[w])
            /\ LET blind == ~wdef[w] /\ wrecW[w] # {} /\ bs # None
                            /\ bs.phase \in {"Building", "Closing"}
                   dup == ~NoDuplicates(rec')
                   keyInError == \E e \in wbg[w] : KeyIntoError(e) /\ e.g = bs.gen
               IN events' = (IF blind THEN events \cup {"blindCommit"} ELSE events)
                            \cup (IF Unique /\ dup THEN {"duplicate"} ELSE {})
                            \cup (IF keyInError THEN {"keyCommittedInError"} ELSE {})
       ELSE /\ UNCHANGED <<rec, idx, bg, bp, wdirty>>
            /\ events' = (IF ~StampRefused(w) THEN events
                          ELSE IF MaintainsOlderGen(w) THEN events \cup {"stampRefusedNormal"}
                          ELSE events \cup {"stampRefused"})
                         \cup (IF StampRefused(w) /\ retired /\ wbg[w] # {}
                               THEN {"retiredStampRefused"} ELSE {})
    /\ wpc' = [wpc EXCEPT ![w] = IF wres[w] # None THEN "release" ELSE "idle"]
    /\ ResetTxn(w)
    /\ UNCHANGED <<def, epoch, bs, bt, br, bw, defineRequested,
                   rebuildRequested>>
    /\ UNCHANGED <<wcur, wsnapBp, wsnapIdx, wdef, wepoch, wres, fenced>>
    /\ UNCHANGED <<builderVars, txns, takeovers, faults>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* The reservation release registered on both outcomes of the user
\* transaction: a delete-if-unchanged of its `!br`.
Release(w) ==
    /\ wpc[w] = "release" /\ CanCommit(w)
    /\ br' = br \ {wres[w]}
    /\ wres' = [wres EXCEPT ![w] = None]
    /\ wpc' = [wpc EXCEPT ![w] = "idle"]
    /\ UNCHANGED <<def, epoch, rec, idx, bs, bt, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ UNCHANGED <<wcur, wsnapBp, wsnapIdx, wdef, wepoch, wadm, wread, wpend, wrecW,
                   wnorm, wbg, wbp, wkeys, wdirty, wseq, wmuts, fenced>>
    /\ UNCHANGED <<builderVars, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* The writer's node stops heartbeating. It may still be running.
NodeLapse(w) ==
    /\ w \notin fenced /\ faults < MaxFaults
    /\ fenced' = fenced \cup {w}
    /\ faults' = faults + 1
    /\ UNCHANGED <<wpc, wcur, wsnapBp, wsnapIdx, wdef, wepoch, wadm, wread, wpend, wrecW,
                   wnorm, wbg, wbp, wkeys, wdirty, wseq, wmuts, wres>>
    /\ UNCHANGED <<storeVars, builderVars, txns, takeovers, events>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

-----------------------------------------------------------------------------
(***************************************************************************)
(* Builders. Each action is one builder transaction.                       *)
(***************************************************************************)

Owns(b) == bs # None /\ bs.gen = bgen[b] /\ (bs.owner = b \/ ~OwnerFence)
Running(b) == bpc[b] # "idle"

SetPc(b, pc) == bpc' = [bpc EXCEPT ![b] = pc]

\* Take over a building or closing generation whose owner is gone, absent,
\* or whose lease the model lets lapse. A takeover with a scanned table and
\* no barrier proof for the generation rescans (`rescan_without_barrier_proof`).
\* A scan that restarts from the start first drains older generations'
\* reservations again, since the owner that installed the generation may have
\* stopped before its own drain. Only the resume scan, which visits the indexes
\* the catalog defines, or the defining statement itself takes a build over.
\* A takeover installs no generation, so it reads against no stamp of its
\* own and raises the barrier. It does not rescan after a proof that an
\* owner recorded through the catalog-fence shortcut.
AcquireExisting(b) ==
    /\ ~Running(b) /\ b \notin BarrierlessBuilders
    /\ def \/ defineRequested
    /\ bs # None /\ bs.phase \in {"Building", "Closing"}
    /\ LET live == bs.owner # None /\ bs.owner # b /\ Running(bs.owner)
           scanned == bs.phase = "Closing" \/ bs.init \/ bs.cursor > 0
           rescan == scanned /\ bw # bs.gen
           next == IF rescan
                   THEN [bs EXCEPT !.owner = b, !.phase = "Building", !.init = FALSE, !.cursor = 0]
                   ELSE [bs EXCEPT !.owner = b]
       IN /\ live => takeovers < MaxTakeovers
          /\ takeovers' = IF live THEN takeovers + 1 ELSE takeovers
          /\ events' = (IF bs.gen > 1 /\ ~bs.init /\ bs.cursor = 0
                        THEN events \cup {"freshRebuildTakenOver"} ELSE events)
                       \cup (IF bs.gen > 1 /\ scanned /\ ~rescan /\ bs.owner # None
                                /\ bstamp[bs.owner]
                             THEN {"shortcutProofTrusted"} ELSE {})
          /\ bs' = next
          /\ SetPc(b, IF ~next.init /\ next.cursor = 0
                      THEN IF RestartDrainsPrior /\ next.gen > 1 THEN "drainPrior" ELSE "barrier"
                      ELSE IF ~next.init THEN "barrier"
                      ELSE IF next.phase = "Building" THEN "replay1"
                      ELSE "replay2")
    /\ bgen' = [bgen EXCEPT ![b] = bs.gen]
    /\ bstamp' = [bstamp EXCEPT ![b] = FALSE]
    /\ UNCHANGED <<btarget, rebuildOpen>>
    /\ UNCHANGED <<def, epoch, rec, idx, bt, br, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ UNCHANGED <<writerVars, txns, faults>>
    /\ UNCHANGED schemaVars

\* Install the next generation (`stage_new_generation`): a new `!bs`, a fresh
\* `!bt`, and the previous generation's `!bt` removed in the same commit. The
\* defining statement stages generation 1 once; a retired or cleaned-up id is
\* never staged again.
\* The builder of the statement that installs a generation reads once that
\* statement has committed, unless it builds inside the defining transaction
\* (`reads_after_its_stamp`, `AcquiredBuild::staged`). A builder without the
\* barrier has no shortcut either.
AcquireNew(b) ==
    /\ ~Running(b)
    /\ \/ bs = None /\ defineRequested /\ ~Staged
       \/ bs # None /\ bs.phase \in {"Online", "Error"} /\ rebuildRequested /\ bs.gen < MaxGen
    /\ LET g == IF bs = None THEN 1 ELSE bs.gen + 1 IN
       /\ bs' = [gen |-> g, phase |-> "Building", owner |-> b, init |-> FALSE,
                 cursor |-> 0, epoch |-> epoch + 1]
       /\ bt' = [gg \in Gens |-> IF gg = g THEN 0
                                 ELSE IF bs # None /\ gg = bs.gen THEN None
                                 ELSE bt[gg]]
       /\ bgen' = [bgen EXCEPT ![b] = g]
       /\ bstamp' = [bstamp EXCEPT ![b] = FencesCatalog /\ b \notin BarrierlessBuilders
                                          /\ ~(DefineInTxn /\ g = 1)]
       /\ rebuildOpen' = (FencesCatalog /\ g > 1)
       /\ UNCHANGED btarget
       /\ SetPc(b, IF g > 1 THEN "drainPrior" ELSE "barrier")
    /\ epoch' = epoch + 1
    /\ rebuildRequested' = FALSE
    /\ UNCHANGED <<def, rec, idx, br, bg, bp, bw, defineRequested>>
    /\ UNCHANGED <<writerVars, boundVars>>
    /\ UNCHANGED schemaVars

\* Wait out every reservation of an older generation, then wipe the stale
\* queues (`wait_for_prior_generation_reservations`, `wipe_stale_build_queues`).
DrainPrior(b) ==
    /\ bpc[b] = "drainPrior" /\ Owns(b)
    /\ PriorDrain => \A r \in br : r.g < bgen[b] => Retirable(r)
    /\ br' = {r \in br : r.g >= bgen[b]}
    /\ bg' = {e \in bg : e.g >= bgen[b]}
    /\ bp' = {m \in bp : m.g >= bgen[b]}
    /\ Touch({BpKey(m.g, m.k) : m \in bp \ bp'})
    /\ SetPc(b, "barrier")
    /\ UNCHANGED <<def, epoch, rec, idx, bs, bt, bw, defineRequested,
                   rebuildRequested>>
    /\ KeepWriters /\ UNCHANGED <<bgen, btarget, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* Before an initial scan reads, from the start or from a checkpoint, wait
\* for every write transaction that opened before the wait began, on every
\* node (`wait_for_older_writes`), and record the proof. A build that reads
\* against its definition first waits for the defining statement to commit;
\* one inside the defining transaction does not.
\*
\* On a fencing store, a build that reads after its statement's stamp waits
\* for its own node's writes alone (`WriteBarrier::start` with `stamped`): a
\* write whose snapshot predates the stamp committed before it, or fails. It
\* begins once the stamp has committed. Writers run on nodes of their own, so
\* that wait waits for none, and the proof is recorded as for the barrier.
\* CatalogFence = FALSE stands for a writer node on a release without the
\* fence that the `!wf` check missed.
Shortcut(b) == bstamp[b]

BarrierBegin(b) ==
    /\ bpc[b] = "barrier" /\ Owns(b) /\ bs.phase = "Building"
    /\ def \/ DefineInTxn
    /\ Shortcut(b) => def /\ ~rebuildOpen
    /\ epoch' = epoch + 1
    /\ btarget' = [btarget EXCEPT ![b] = epoch + 1]
    /\ SetPc(b, "barrierWait")
    /\ UNCHANGED <<def, rec, idx, bs, bt, br, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ UNCHANGED <<writerVars, bgen, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* A builder on a release without the barrier neither waits nor records the
\* proof.
BarrierSkipped(b) == ~WriteBarrier \/ b \in BarrierlessBuilders

\* The barrier leaves out a node whose heartbeat is older than
\* `exclude_after` (`answers`): its datastore already refuses its own write
\* commits (WriteFence), so it cannot commit a write the build missed, and a
\* dead node never answers.
BarrierWaitsFor(w) == ~(BarrierExcludesLapsed /\ w \in fenced)

Barrier(b) ==
    /\ bpc[b] = "barrierWait" /\ Owns(b) /\ bs.phase = "Building"
    /\ ~BarrierSkipped(b) /\ ~Shortcut(b) =>
         \A w \in Writers : OpenTxn(w) /\ BarrierWaitsFor(w) => wepoch[w] >= btarget[b]
    /\ bw' = IF BarrierSkipped(b) THEN bw ELSE bgen[b]
    /\ events' = IF Shortcut(b) /\ ~BarrierSkipped(b) /\ bgen[b] > 1
                 THEN events \cup {"rebuildShortcut"} ELSE events
    /\ SetPc(b, IF bs.cursor > 0 THEN "markers" ELSE "wipe")
    /\ UNCHANGED <<def, epoch, rec, idx, bs, bt, br, bg, bp,
                   defineRequested, rebuildRequested>>
    /\ UNCHANGED <<writerVars, bgen, btarget, txns, takeovers, faults>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

Wipe(b) ==
    /\ bpc[b] = "wipe" /\ Owns(b) /\ bs.phase = "Building"
    /\ idx' = NoEntries
    /\ Touch({IdxKey(e[1], e[2]) : e \in {x \in Entries : idx[x] # 0}})
    /\ SetPc(b, "markers")
    /\ UNCHANGED <<def, epoch, rec, bs, bt, br, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ KeepWriters /\ UNCHANGED <<bgen, btarget, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* Queued mutations of a generation, in `!bg` key order.
Before(e1, e2) == e1.t < e2.t \/ (e1.t = e2.t /\ e1.s < e2.s)
Queue(g) == {e \in bg : e.g = g}

FirstQueued(g, k) ==
    {e \in Queue(g) : e.k = k /\ \A e2 \in Queue(g) : e2.k = k => e2 = e \/ Before(e, e2)}

\* Before an initial scan reads, every record with a queued mutation of the
\* generation gets a `!bp` marker at its earliest one
\* (`rebuild_primary_appendings`). A writer admitted after the scan completed
\* wrote none, and a takeover may restart the scan.
Markers(b) ==
    /\ bpc[b] = "markers" /\ Owns(b) /\ bs.phase = "Building" /\ ~bs.init
    /\ LET old == {m \in bp : m.g = bgen[b]}
           new == {[g |-> e.g, k |-> e.k, t |-> e.t, s |-> e.s] :
                     e \in UNION {FirstQueued(bgen[b], k) : k \in Keys}}
       IN IF RebuildMarkers
          THEN /\ bp' = (bp \ old) \cup new
               /\ Touch({BpKey(m.g, m.k) : m \in old \cup new})
          ELSE /\ UNCHANGED bp
               /\ Touch({})
    /\ SetPc(b, "scan")
    /\ UNCHANGED <<def, epoch, rec, idx, bs, bt, br, bg, bw,
                   defineRequested, rebuildRequested>>
    /\ KeepWriters /\ UNCHANGED <<bgen, btarget, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* Index the next record. A record with a `!bp` marker is indexed from the
\* old value of the mutation it points at, which replay then applies.
ScanValue(b, k) ==
    LET marks == {m \in bp : m.g = bgen[b] /\ m.k = k}
        firsts == {e \in bg : \E m \in marks : e.g = m.g /\ e.t = m.t /\ e.s = m.s}
    IN IF PrimaryMarkers /\ firsts # {} THEN (CHOOSE e \in firsts : TRUE).old ELSE rec[k]

\* A UNIQUE build writes an entry with a compare-and-swap against an absent
\* one (`index_unique`), so a value whose entry names another record fails
\* the batch with `IndexExists`.
Duplicate(k, v) == Unique /\ DuplicateFailsBuild /\ HeldByOther(idx, k, v)

ScanStep(b) ==
    /\ bpc[b] = "scan" /\ Owns(b) /\ bs.phase = "Building" /\ ~bs.init
    /\ bs.cursor < NumKeys
    /\ LET k == bs.cursor + 1
           v == ScanValue(b, k)
       IN /\ ~Duplicate(k, v)
          /\ idx' = Put(idx, k, v, 1)
          /\ Touch(IF v = None THEN {} ELSE {IdxKey(k, v)})
    /\ bs' = [bs EXCEPT !.cursor = @ + 1]
    /\ UNCHANGED <<def, epoch, rec, bt, br, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ KeepWriters /\ UNCHANGED <<builderVars, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

ScanDone(b) ==
    /\ bpc[b] = "scan" /\ Owns(b) /\ bs.phase = "Building" /\ ~bs.init
    /\ bs.cursor = NumKeys
    /\ bs' = [bs EXCEPT !.init = TRUE, !.cursor = 0]
    /\ SetPc(b, "replay1")
    /\ UNCHANGED <<def, epoch, rec, idx, bt, br, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ UNCHANGED <<writerVars, bgen, btarget, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* Replay the first queued mutation of the generation, in key order, and
\* delete it with its `!bp` marker and its `!br`.

Replay(b) ==
    /\ bpc[b] \in {"replay1", "replay2", "replay3"} /\ Owns(b)
    /\ bs.phase \in {"Building", "Closing"}
    /\ \E e \in Queue(bgen[b]) :
        /\ \A e2 \in Queue(bgen[b]) : e2 = e \/ Before(e, e2)
        /\ ~Duplicate(e.k, e.new)
        /\ idx' = Apply(idx, e.k, e.old, e.new)
        /\ bg' = bg \ {e}
        /\ bp' = {m \in bp : ~(m.g = e.g /\ m.k = e.k)}
        /\ br' = {r \in br : ~(r.g = e.g /\ r.t = e.t)}
        /\ Touch(IdxKeysOf(e.k, e.old, e.new) \cup {BpKey(e.g, e.k)})
        /\ events' = IF AddsKey(e) THEN Note("keyReplayed") ELSE events
    /\ UNCHANGED <<def, epoch, rec, bs, bt, bw, defineRequested,
                   rebuildRequested>>
    /\ KeepWriters /\ UNCHANGED <<builderVars, txns, takeovers, faults>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* The scan or the replay meets a value held by another record. The batch
\* commits nothing, and the build ends in `Error` (`run_claimed`,
\* `mark_durable_error`).
DuplicateFails(b) ==
    /\ bs # None /\ Owns(b)
    /\ \/ /\ bpc[b] = "scan" /\ bs.phase = "Building" /\ ~bs.init
          /\ bs.cursor < NumKeys
          /\ Duplicate(bs.cursor + 1, ScanValue(b, bs.cursor + 1))
          /\ events' = Note("scanDuplicate")
       \/ /\ bpc[b] \in {"replay1", "replay2", "replay3"}
          /\ bs.phase \in {"Building", "Closing"}
          /\ \E e \in Queue(bgen[b]) :
              /\ \A e2 \in Queue(bgen[b]) : e2 = e \/ Before(e, e2)
              /\ Duplicate(e.k, e.new)
          /\ events' = Note("replayDuplicate")
    /\ bs' = [bs EXCEPT !.phase = "Error", !.owner = None]
    /\ SetPc(b, "idle")
    /\ UNCHANGED <<def, epoch, rec, idx, bt, br, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ UNCHANGED <<writerVars, bgen, btarget, txns, takeovers, faults>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

ReplayDone(b) ==
    /\ bpc[b] \in {"replay1", "replay2", "replay3"} /\ Owns(b)
    /\ bs.phase \in {"Building", "Closing"}
    /\ Queue(bgen[b]) = {}
    /\ SetPc(b, CASE bpc[b] = "replay1" -> "closing"
                  [] bpc[b] = "replay2" -> "drain"
                  [] bpc[b] = "replay3" -> "online")
    /\ UNCHANGED <<storeVars, writerVars, bgen, btarget, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* Enter `Closing` (`mark_durable_closing`), advancing `!bt` so an admission
\* that read `Building` fails its compare-and-swap.
Closing(b) ==
    /\ bpc[b] = "closing" /\ Owns(b) /\ bs.phase = "Building"
    /\ bs' = [bs EXCEPT !.phase = "Closing"]
    /\ bt' = IF ClosingBumpsTicket THEN [bt EXCEPT ![bgen[b]] = @ + 1] ELSE bt
    /\ SetPc(b, "replay2")
    /\ UNCHANGED <<def, epoch, rec, idx, br, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ UNCHANGED <<writerVars, bgen, btarget, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* Retire every reservation of the generation (`wait_for_durable_reservations`).
Drain(b) ==
    /\ bpc[b] = "drain" /\ Owns(b) /\ bs.phase = "Closing"
    /\ DrainReservations => \A r \in br : r.g = bgen[b] => Retirable(r)
    /\ br' = IF DrainReservations THEN {r \in br : r.g # bgen[b]} ELSE br
    /\ SetPc(b, "replay3")
    /\ UNCHANGED <<def, epoch, rec, idx, bs, bt, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ UNCHANGED <<writerVars, bgen, btarget, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

Publish(b) ==
    /\ bpc[b] = "online" /\ Owns(b) /\ bs.phase = "Closing"
    /\ bs' = [bs EXCEPT !.phase = "Online", !.owner = None]
    /\ SetPc(b, "idle")
    /\ events' = IF NoDuplicates(rec) THEN events ELSE Note("publishedOverDuplicate")
    /\ UNCHANGED <<def, epoch, rec, idx, bt, br, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ UNCHANGED <<writerVars, bgen, btarget, txns, takeovers, faults>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

Fail(b) ==
    /\ Running(b) /\ Owns(b) /\ bs.phase \in {"Building", "Closing"}
    /\ faults < MaxFaults
    /\ bs' = [bs EXCEPT !.phase = "Error", !.owner = None]
    /\ faults' = faults + 1
    /\ SetPc(b, "idle")
    /\ UNCHANGED <<def, epoch, rec, idx, bt, br, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ UNCHANGED <<writerVars, bgen, btarget, txns, takeovers, events>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* A builder stops without ending its generation: its process dies, or
\* `mark_durable_error` gives up, leaving the generation to a takeover.
Crash(b) ==
    /\ Running(b) /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ SetPc(b, "idle")
    /\ events' = IF Unique THEN events \cup {"builderCrashed"} ELSE events
    /\ UNCHANGED <<storeVars, writerVars, bgen, btarget, txns, takeovers>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* A builder whose compare-and-swap on `!bs` fails stops. Every builder
\* transaction that writes index data compare-and-swaps `!bs` first
\* (`maintain_build_ownership`, `update_build_state_in_tx`,
\* `update_owned_build_state_inner`), so one that finds it deleted stops too.
Stop(b) ==
    /\ Running(b) /\ ~Owns(b)
    /\ SetPc(b, "idle")
    /\ UNCHANGED <<storeVars, writerVars, bgen, btarget, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* Without RetireFence, a batch the builder had in flight when `!bs` was
\* deleted commits its index data anyway.
StaleBatch(b) ==
    /\ ~RetireFence /\ Running(b) /\ bs = None /\ (retired \/ Swept)
    /\ bpc[b] \in {"scan", "replay1", "replay2", "replay3"}
    /\ \E v \in Vals : idx' = Put(idx, 1, v, 1)
    /\ SetPc(b, "idle")
    /\ UNCHANGED <<def, epoch, rec, bs, bt, br, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ UNCHANGED <<writerVars, bgen, btarget, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

\* The defining statement's rollback aborts its local build, which stops at
\* its next checkpoint (`settle_uncommitted_statement`).
AbortStop(b) ==
    /\ Running(b) /\ cleanup \in {"pending", "done"} /\ ~def
    /\ SetPc(b, "idle")
    /\ UNCHANGED <<storeVars, writerVars, bgen, btarget, boundVars>>
    /\ UNCHANGED schemaVars
    /\ UNCHANGED fenceVars

-----------------------------------------------------------------------------
(***************************************************************************)
(* Retirement, and a defining statement that does not commit.              *)
(***************************************************************************)

\* The schema transaction that retires the id (`remove_index_definition`, the
\* `retire_index` lowering) deletes the definition and its name lookup,
\* `!bs`, `!bw` and every generation's `!bg`, `!bp`, `!br` and `!bt`
\* (`delete_durable_build_artifacts`), and enqueues the index data for
\* reclaim. Its range deletes remove the keys present when it commits; a user
\* transaction that commits a queued mutation after it is not in conflict.
\* The local builder is aborted once it commits (`AbortLocalBuild`); the
\* durable fence is the deleted `!bs`. Like `DEFINE INDEX` and `REBUILD INDEX`,
\* it stamps the table (`retire_index`), so on a fencing store a writer whose
\* snapshot predates it fails its commit.
Remove ==
    /\ RetireAs # "none" /\ def /\ ~retired
    /\ def' = FALSE /\ retired' = TRUE
    /\ bs' = None /\ bt' = [g \in Gens |-> None]
    /\ br' = {} /\ bg' = {} /\ bp' = {} /\ bw' = 0
    /\ epoch' = epoch + 1 /\ remEpoch' = epoch + 1
    /\ rebuildRequested' = FALSE
    /\ Touch({BpKey(m.g, m.k) : m \in bp} \cup (IF FencesCatalog THEN {CvKey} ELSE {}))
    /\ events' = events
         \cup (IF bs # None /\ bs.phase \in {"Building", "Closing"} /\ (bs.init \/ bs.cursor > 0)
                  /\ \E b \in Builders : Running(b)
               THEN {"retiredMidBuild"} ELSE {})
         \cup (IF br # {} THEN {"retiredWithReservation"} ELSE {})
         \cup (IF bs # None /\ bs.gen > 1 THEN {"retiredRebuilt"} ELSE {})
         \cup (IF bs # None /\ bs.gen > 1 /\ bs.phase \in {"Building", "Closing"}
                  /\ \E b \in Builders : Running(b)
               THEN {"retiredRebuildMidBuild"} ELSE {})
         \cup (IF rebuildRequested THEN {"retiredRebuildPending"} ELSE {})
    /\ UNCHANGED <<rec, idx, defineRequested>>
    /\ KeepWriters /\ UNCHANGED <<builderVars, txns, takeovers, faults>>
    /\ UNCHANGED <<reclaimed, cleanup, swept>>
    /\ UNCHANGED fenceVars

\* The deferred reclaim of the index data (`reclaim_tombstones`) waits a grace
\* at least the MVCC GC lifetime, by when every transaction that opened before
\* the retirement has ended. It deletes the index's data prefix only, which
\* holds none of the build keys.
Reclaim ==
    /\ retired /\ ~reclaimed
    /\ \A w \in Writers : OpenTxn(w) => wepoch[w] >= remEpoch
    /\ idx' = NoEntries
    /\ Touch({IdxKey(e[1], e[2]) : e \in {x \in Entries : idx[x] # 0}})
    /\ reclaimed' = TRUE
    /\ UNCHANGED <<def, epoch, rec, bs, bt, br, bg, bp, bw,
                   defineRequested, rebuildRequested>>
    /\ KeepWriters /\ UNCHANGED <<builderVars, boundVars>>
    /\ UNCHANGED <<retired, remEpoch, cleanup, swept>>
    /\ UNCHANGED fenceVars

\* The defining statement's transaction rolls back. Its rollback hooks run on
\* its node: `ReleaseBuildOwnership` aborts the build, and
\* `CleanUncommittedBuild` deletes what the build committed (CleanRollback).
DefineRollback ==
    /\ DefineAborts /\ defineRequested /\ ~def
    /\ defineRequested' = FALSE
    /\ cleanup' = "pending"
    /\ UNCHANGED <<def, epoch, rec, idx, bs, bt, br, bg, bp, bw, rebuildRequested>>
    /\ UNCHANGED <<writerVars, builderVars, boundVars>>
    /\ UNCHANGED <<retired, reclaimed, remEpoch, swept>>
    /\ UNCHANGED fenceVars

\* The statement's node dies before it commits: no hook runs, and the build,
\* which runs on that node, stops.
DefineNodeDies ==
    /\ DefineAborts /\ defineRequested /\ ~def /\ faults < MaxFaults
    /\ defineRequested' = FALSE
    /\ cleanup' = "lost"
    /\ bpc' = [b \in Builders |-> "idle"]
    /\ faults' = faults + 1
    /\ UNCHANGED <<def, epoch, rec, idx, bs, bt, br, bg, bp, bw, rebuildRequested>>
    /\ UNCHANGED <<writerVars, bgen, btarget, txns, takeovers, events>>
    /\ UNCHANGED <<retired, reclaimed, remEpoch, swept>>
    /\ UNCHANGED fenceVars

\* A commit whose outcome is unknown: it applied, and the rollback hooks run
\* all the same.
DefineCommitUnknown ==
    /\ DefineAborts /\ CommitDefinition
    /\ cleanup' = "pending"
    /\ UNCHANGED <<retired, reclaimed, remEpoch, swept>>
    /\ UNCHANGED fenceVars

\* `CleanUncommittedBuild`: unless the catalog holds the definition, delete
\* `!bs`, `!bw`, the queues and the index data in one transaction. It first
\* aborts the local build and waits for it to stop, within a deadline that
\* may pass while the build still runs; the build is then fenced by the
\* deleted `!bs`.
CleanRollback ==
    /\ cleanup = "pending"
    /\ cleanup' = "done"
    /\ IF def /\ CleanupChecksCatalog
       THEN /\ events' = events \cup {"unknownCleanupSkipped"}
            /\ UNCHANGED <<bs, bt, br, bg, bp, bw, idx, wdirty, swept>>
       ELSE /\ bs' = None /\ bt' = [g \in Gens |-> None]
            /\ br' = {} /\ bg' = {} /\ bp' = {} /\ bw' = 0
            /\ idx' = NoEntries
            /\ Touch({IdxKey(e[1], e[2]) : e \in {x \in Entries : idx[x] # 0}}
                     \cup {BpKey(m.g, m.k) : m \in bp})
            /\ swept' = IF bs = None THEN "unstaged" ELSE bs.phase
            /\ events' = events \cup
                 (IF \E b \in Builders : bpc[b] \in {"scan", "replay1", "replay2", "replay3"}
                  THEN {"cleanupMidBatch"} ELSE {})
    /\ UNCHANGED <<def, epoch, rec, defineRequested, rebuildRequested>>
    /\ KeepWriters /\ UNCHANGED <<builderVars, txns, takeovers, faults>>
    /\ UNCHANGED <<retired, reclaimed, remEpoch>>
    /\ UNCHANGED fenceVars

\* A build the catalog does not define whose owner still holds it
\* (`uncommitted_build_lapsed`). `Publish` clears the owner, but the code keeps
\* it until the statement that started the build closes
\* (`defer_ownership_release`), so an `Online` generation the catalog does not
\* define still names its owner.
LapsedUncommitted ==
    /\ ~def /\ bs # None
    /\ bs.phase \in {"Building", "Closing", "Online"}

\* A compaction request for the id exists. Only counting kinds queue one, and
\* only once the build has written a batch; a build no statement awaits writes
\* none before its statement closes.
CompactionRequested == Counting /\ bs # None /\ (bs.init \/ bs.cursor > 0)

\* The cluster-side retirement of a lapsed uncommitted build
\* (`retire_uncommitted_build`), run by a compaction pass that holds a
\* request for the index (`materialise.rs`). It rewrites the name lookup and
\* the table definition, so the statement, if still open, fails its commit;
\* compare-and-deletes `!bs`; and deletes the barrier proof `!bw`, the queues
\* and the index data. An owner whose statement is still open, or whose
\* builder still runs, heartbeats, so its lease lapsing counts as a takeover.
RetireUncommitted ==
    /\ DefineAborts /\ LapsedUncommitted /\ CompactionRequested
    /\ LET live == defineRequested \/ (bs.owner # None /\ Running(bs.owner)) IN
       /\ live => takeovers < MaxTakeovers
       /\ takeovers' = IF live THEN takeovers + 1 ELSE takeovers
    /\ bs' = None /\ bt' = [g \in Gens |-> None]
    /\ br' = {} /\ bg' = {} /\ bp' = {}
    /\ idx' = NoEntries
    /\ Touch({IdxKey(e[1], e[2]) : e \in {x \in Entries : idx[x] # 0}}
             \cup {BpKey(m.g, m.k) : m \in bp})
    /\ swept' = bs.phase
    /\ IF UncommittedFence /\ defineRequested
       THEN /\ defineRequested' = FALSE
            /\ cleanup' = "pending"
       ELSE UNCHANGED <<defineRequested, cleanup>>
    /\ bw' = IF RetireDeletesProof THEN 0 ELSE bw
    /\ events' = events \cup {"clusterRetired"}
                 \cup (IF bw # 0 THEN {"clusterRetiredProof"} ELSE {})
    /\ UNCHANGED <<def, epoch, rec, rebuildRequested>>
    /\ KeepWriters /\ UNCHANGED <<builderVars, txns, faults>>
    /\ UNCHANGED <<retired, reclaimed, remEpoch>>
    /\ UNCHANGED fenceVars

-----------------------------------------------------------------------------
ProtocolNext ==
    \/ DefineStart \/ DefineCommit \/ Rebuild \/ RebuildCommit
    \/ \E w \in Writers :
         \/ Begin(w) \/ MutateUnindexed(w) \/ MutateNormal(w) \/ MutateQueued(w)
         \/ MutateNeedsAdmission(w) \/ AdmitNormal(w) \/ AdmitRead(w)
         \/ AdmitCommit(w) \/ FenceQueue(w) \/ FenceOnline(w) \/ FenceRotated(w)
         \/ FenceRetired(w)
         \/ RecheckFails(w) \/ HeldRefused(w) \/ UniqueRefused(w) \/ ErrorRefused(w)
         \/ Rollback(w) \/ Commit(w) \/ Release(w)
         \/ NodeLapse(w)
         \/ AdmitRetired(w) \/ AdmitReplaced(w) \/ MutateRetired(w) \/ MutateSkipped(w)
    \/ \E b \in Builders :
         \/ AcquireExisting(b) \/ AcquireNew(b) \/ DrainPrior(b)
         \/ BarrierBegin(b) \/ Barrier(b)
         \/ Wipe(b) \/ Markers(b) \/ ScanStep(b) \/ ScanDone(b) \/ Replay(b) \/ ReplayDone(b)
         \/ Closing(b) \/ Drain(b) \/ Publish(b) \/ Fail(b) \/ Crash(b) \/ Stop(b)
         \/ DuplicateFails(b)
         \/ StaleBatch(b) \/ AbortStop(b)

SchemaNext ==
    \/ Remove \/ Reclaim
    \/ DefineRollback \/ DefineNodeDies \/ DefineCommitUnknown \/ CleanRollback
    \/ RetireUncommitted

Next == ProtocolNext \/ SchemaNext

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(***************************************************************************)
(* Fairness, for the liveness properties.                                  *)
(*                                                                         *)
(* A builder takes every builder transaction it can take, including        *)
(* adopting a generation whose owner has stopped. A takeover of a live     *)
(* owner, a failure and a crash get no fairness; MaxTakeovers and          *)
(* MaxFaults bound them.                                                   *)
(*                                                                         *)
(* A writer whose node is live finishes the transaction it opened: its     *)
(* admission steps, its commit and its reservation release are weakly      *)
(* fair. A writer whose node lapsed may never take another step. Opening   *)
(* a transaction, a mutation and a rollback are the user's choice and get  *)
(* no fairness: a fair rollback would end every admission wait by giving   *)
(* up. Node lapses get none, and neither do the `DEFINE` and `REBUILD`     *)
(* requests; the defining statement's commit is weakly fair.               *)
(***************************************************************************)
Fair(name, A) == name \notin Unfair /\ A

\* Adopting a generation whose owner is gone or absent, which is not a
\* takeover of a live owner.
Adopt(b) == AcquireExisting(b) /\ takeovers' = takeovers

BuilderStep(b) ==
    \/ Fair("Adopt", Adopt(b))           \/ Fair("AcquireNew", AcquireNew(b))
    \/ Fair("DrainPrior", DrainPrior(b)) \/ Fair("BarrierBegin", BarrierBegin(b))
    \/ Fair("Barrier", Barrier(b))       \/ Fair("Wipe", Wipe(b))
    \/ Fair("Markers", Markers(b))       \/ Fair("ScanStep", ScanStep(b))
    \/ Fair("ScanDone", ScanDone(b))     \/ Fair("Replay", Replay(b))
    \/ Fair("ReplayDone", ReplayDone(b)) \/ Fair("Closing", Closing(b))
    \/ Fair("Drain", Drain(b))           \/ Fair("Publish", Publish(b))
    \/ Fair("Stop", Stop(b))

WriterFinish(w) ==
    /\ w \notin fenced
    /\ \/ Fair("AdmitNormal", AdmitNormal(w)) \/ Fair("AdmitRead", AdmitRead(w))
       \/ Fair("AdmitCommit", AdmitCommit(w)) \/ Fair("FenceQueue", FenceQueue(w))
       \/ Fair("FenceOnline", FenceOnline(w)) \/ Fair("FenceRotated", FenceRotated(w))
       \/ Fair("FenceRetired", FenceRetired(w))
       \/ Fair("Commit", Commit(w))           \/ Fair("Release", Release(w))

\* No two of one builder's steps, or of one writer's, are enabled in the same
\* state, so weak fairness on their disjunction is weak fairness on each.
LiveSpec ==
    /\ Spec
    /\ WF_vars(Fair("DefineCommit", DefineCommit))
    /\ \A b \in Builders : WF_vars(BuilderStep(b))
    /\ \A w \in Writers : WF_vars(WriterFinish(w))

-----------------------------------------------------------------------------
(***************************************************************************)
(* Properties.                                                             *)
(***************************************************************************)

\* Whenever the planner may use the index, it holds exactly the committed
\* table's entries.
IndexMatchesTable == Queryable => idx = Proj

\* An online generation has no queued mutation left for anyone to replay.
NoStrandedQueue == (bs # None /\ bs.phase = "Online") => Queue(bs.gen) = {}

\* An online generation stays online until a later generation replaces it,
\* or the id is retired or cleaned up.
OnlineStays ==
    [][bs # None /\ bs.phase = "Online" =>
         \/ bs' = None /\ (retired' \/ swept' # None)
         \/ bs' # None /\ (bs'.gen > bs.gen \/ bs'.phase = "Online")]_vars

\* Once the retirement has committed, no build state, ticket counter or
\* reservation of the id comes back.
RetiredStateStaysGone ==
    retired => bs = None /\ bw = 0 /\ br = {} /\ \A g \in Gens : bt[g] = None

\* Once the retirement has committed and its reclaim has run, the id holds no
\* index entries: builders are fenced, and every writer that could still
\* maintain the index has ended.
RetiredIndexStaysEmpty == reclaimed => idx = NoEntries

\* Once the retirement has committed and its reclaim has run, the id holds no
\* queued mutation or `!bp` marker. Not part of the suite's invariants: the
\* `retire-orphan-queue` row shows the open gap.
RetiredQueuesStayEmpty == reclaimed => bg = {} /\ bp = {}

\* A write never fails, at first-use admission or at the cached recheck,
\* because an index it no longer has to maintain was retired.
NoRetiredWriteFails == {"retiredAdmitFail", "retiredRecheckFail"} \cap events = {}

\* The same, at the fence after the reservation commits.
NoRetiredFenceFails == "retiredFenceFail" \notin events

\* Once a rollback cleanup or a cluster-side retirement has swept a build
\* whose definition never committed, nothing of the id is left but its
\* barrier proof, and nothing writes it again (RolledBackLeavesNoBarrierProof
\* covers the proof).
RolledBackLeavesNothing == Swept /\ ~def => NoBuildState

\* The same for the barrier proof, once no rollback hook is left to run:
\* the cluster-side retirement deletes it too.
RolledBackLeavesNoBarrierProof ==
    Swept /\ ~def /\ cleanup \in {"lost", "done"} => bw = 0

\* No uncommitted build is left where nothing will ever remove it: its
\* statement's node died, so no hook runs, and no compaction request leads a
\* pass to it. Not part of the suite's invariants: the `rollback-stranded` row
\* shows the open gap.
NoStrandedUncommittedBuild ==
    ~(cleanup = "lost" /\ ~def /\ bs # None /\ ~(LapsedUncommitted /\ CompactionRequested))

\* Whenever the planner may use a UNIQUE index, no two records hold the same
\* value.
UniqueWhenQueryable == Unique /\ Queryable => NoDuplicates(rec)

\* A UNIQUE build over a table holding a duplicate ends in `Error`: no
\* generation publishes `Online` while two records hold the same value.
DuplicateNeverPublished == "publishedOverDuplicate" \notin events

\* A UNIQUE entry names one record at most.
UniqueSlots == Unique => \A v \in Vals : Cardinality({k \in Keys : idx[<<k, v>>] # 0}) <= 1

\* No write that adds a key is queued for a UNIQUE generation in `Error`. A
\* write queued before the generation failed still commits.
ErrorAddsNoKey == "keyQueuedInError" \notin events

\* Liveness, under LiveSpec.
\*
\* A `DEFINE INDEX` commits its definition and gets generation 1 staged, and
\* a `REBUILD INDEX` gets the next generation staged.
BuildStarts ==
    /\ defineRequested ~> (def /\ bs # None)
    /\ rebuildRequested ~> ~rebuildRequested

\* Every generation that starts settles `Online` or `Error`.
BuildSettles ==
    (bs # None /\ bs.phase \in {"Building", "Closing"})
        ~> (bs # None /\ bs.phase \in {"Online", "Error"})

\* A writer on a live node whose admission waits on `Closing` is eventually
\* admitted, or maintains the index in its transaction, or ends it.
AdmissionWaitEnds ==
    \A w \in Writers :
        (wpc[w] = "admit" /\ bs # None /\ bs.phase = "Closing")
            ~> (wpc[w] \notin {"admit", "admitCommit"} \/ w \in fenced)

\* A builder waiting at the write barrier proceeds, or stops.
BarrierProceeds ==
    \A b \in Builders : bpc[b] = "barrierWait" ~> bpc[b] # "barrierWait"

\* The ticket bound never holds back an admission, so a liveness violation
\* is never an artefact of MaxTicket. The liveness rows check it.
TicketBoundUnreached ==
    \A w \in Writers :
        (wpc[w] = "admit" /\ bs # None /\ bs.phase \in {"Building", "Error"})
            => bt[bs.gen] < MaxTicket

\* Reachability checks. Each `Never*` invariant is expected to be violated
\* by the scenario named beside it, which shows that scenario reaches the
\* states its passing run claims to cover.
NeverQueuedWhileClosing ==                                    \* define
    ~(bs # None /\ bs.phase = "Closing" /\ Queue(bs.gen) # {})
NeverTwoQueuedInOneTxn == ~\E w \in Writers : wseq[w] >= 2     \* multi-mutation
NeverTakenOverOnline ==                                       \* takeover
    ~(takeovers > 0 /\ bs # None /\ bs.phase = "Online")
NeverRebuiltOnline ==                                         \* rebuild
    ~(bs # None /\ bs.gen > 1 /\ bs.phase = "Online")
NeverFreshRebuildTakenOverOnline ==                           \* rebuild-takeover
    ~("freshRebuildTakenOver" \in events /\ bs # None /\ bs.phase = "Online")
NeverLapsedReservationRetiredOnline ==                        \* node-lapse
    ~(bs # None /\ bs.phase = "Online"
      /\ \E w \in fenced : wres[w] # None /\ wres[w] \notin br)
NeverBlindCommitThenOnline ==                                 \* in-txn
    ~("blindCommit" \in events /\ bs # None /\ bs.phase = "Online")
NeverRecheckFailed == "recheckFailed" \notin events           \* rebuild-multi-mutation
NeverFenceRotated == "fenceRotated" \notin events             \* rebuild
NeverScanDuplicate == "scanDuplicate" \notin events           \* unique
NeverReplayDuplicate == "replayDuplicate" \notin events       \* unique
NeverUniqueRefused == "uniqueRefused" \notin events           \* unique
NeverErrorRefused == "errorRefused" \notin events             \* unique
NeverDeleteQueuedInError == "deleteQueuedInError" \notin events \* unique
NeverKeyReplayedOnline ==                                     \* unique
    ~("keyReplayed" \in events /\ bs # None /\ bs.phase = "Online")
NeverRebuiltAfterDuplicate ==                                 \* unique-rebuild
    ~(("scanDuplicate" \in events \/ "replayDuplicate" \in events)
      /\ bs # None /\ bs.gen > 1 /\ bs.phase = "Online")
NeverKeyCommittedInError == "keyCommittedInError" \notin events \* unique
NeverFailedOverUniqueTable ==                                 \* unique
    ~("duplicate" \notin events
      /\ ("scanDuplicate" \in events \/ "replayDuplicate" \in events))
NeverRetiredMidBuild ==                                       \* retire
    ~("retiredMidBuild" \in events /\ reclaimed)
NeverRetiredRebuilt ==                                        \* retire-rebuild
    ~("retiredRebuilt" \in events /\ reclaimed)
NeverRetiredWithReservation ==                                \* retire
    "retiredWithReservation" \notin events
NeverRetiredAdmitSkip == "retiredAdmitSkip" \notin events     \* retire
NeverRetiredRecheckSkip == "retiredRecheckSkip" \notin events \* retire-multi-mutation
NeverReplacedFail == "replacedFail" \notin events             \* retire-overwrite
NeverCleanedMidBatch == "cleanupMidBatch" \notin events       \* rollback-in-txn
NeverClusterRetired == "clusterRetired" \notin events         \* rollback
NeverClusterRetiredProof ==                                   \* rollback-in-txn
    "clusterRetiredProof" \notin events
NeverRetiredFenceSkip == "retiredFenceSkip" \notin events     \* retire
NeverRetiredStampRefused ==                                   \* retire-orphan-queue-fenced
    "retiredStampRefused" \notin events
NeverRetiredRebuildMidBuild ==                                \* retire-rebuild
    "retiredRebuildMidBuild" \notin events
NeverRetiredRebuildPending ==                                 \* retire-rebuild
    "retiredRebuildPending" \notin events
NeverUniqueRefusedDirect == "uniqueRefusedDirect" \notin events \* unique-multi-mutation
NeverErrorRefusedCached == "errorRefusedCached" \notin events \* unique-multi-mutation
NeverFailedAfterCrash ==                                      \* unique-fault
    ~("builderCrashed" \in events
      /\ {"scanDuplicate", "replayDuplicate"} \cap events # {}
      /\ bs # None /\ bs.phase = "Error")
NeverUnknownCleanupSkipped ==                                 \* rollback
    "unknownCleanupSkipped" \notin events
NeverFencedDefineRefusedOnline ==                             \* define-fenced
    ~("stampRefused" \in events /\ bs # None /\ bs.phase = "Online")
NeverFencedRebuildRefusedOnline ==                            \* rebuild-fenced
    ~({"rebuildShortcut", "stampRefusedNormal"} \subseteq events
      /\ bs # None /\ bs.gen > 1 /\ bs.phase = "Online")
NeverShortcutProofTrustedOnline ==                            \* rebuild-takeover-fenced
    ~("shortcutProofTrusted" \in events /\ bs # None /\ bs.gen > 1 /\ bs.phase = "Online")

TypeOK ==
    /\ rec \in [Keys -> Opt(Vals)]
    /\ idx \in [Entries -> Int]
    /\ bs = None \/ (bs.gen \in Gens /\ bs.phase \in Phases
                     /\ bs.owner \in Opt(Builders) /\ bs.cursor \in 0..NumKeys)
    /\ wpc \in [Writers -> {"idle", "ready", "admit", "admitCommit", "fence", "release"}]
    /\ bpc \in [Builders -> {"idle", "drainPrior", "barrier", "barrierWait", "wipe", "markers",
                             "scan", "replay1", "closing", "replay2", "drain", "replay3",
                             "online"}]
    /\ cleanup \in {"none", "pending", "done", "lost"}
    /\ rebuildOpen \in BOOLEAN
    /\ bstamp \in [Builders -> BOOLEAN]

\* Writers within HeldWriters and outside it are interchangeable, as are
\* builders within BarrierlessBuilders and outside it, and values; TLC checks
\* one representative per permutation.
Symmetry ==
    Permutations(HeldWriters) \cup Permutations(Writers \ HeldWriters)
        \cup Permutations(BarrierlessBuilders) \cup Permutations(Builders \ BarrierlessBuilders)
        \cup Permutations(Vals)
=============================================================================
