# TLA+ specifications

| Spec | Models |
| --- | --- |
| `concurrent_index_build.tla` | The cluster protocol that builds an index while writes continue (`surrealdb/core/src/kvs/index/`). |
| `versioned_index.tla` | Nodes caching a remotely versioned vector index. |

## Running

TLC ships in `tla2tools.jar` from the [TLA+ releases](https://github.com/tlaplus/tlaplus/releases); it needs Java 11 or later.

```bash
TLA2TOOLS=/path/to/tla2tools.jar doc/tla/check_concurrent_index_build.sh
```

- The script runs every scenario below and compares each outcome with the one it expects.
- It exits non-zero on a mismatch.
- The `sim-*` rows run TLC in simulation mode, and their output line says `simulated, not exhaustive`. A simulated `pass` means that no generated trace found a violation. It does not mean that no violation exists.
- Pass scenario names to run a subset.
- Logs and counterexample traces stay in `$OUT`, which is a temporary directory unless you set it.
- `concurrent_index_build.cfg` is the `define` scenario, for the TLA+ Toolbox or a single `tlc2.TLC` run.

## `concurrent_index_build`

### Properties

- `IndexMatchesTable`: whenever the planner may use the index, the index holds exactly the entries of the committed table. The planner may use it when `!bs` is `Online`, or when `!bs` is absent under a committed definition.
- `NoStrandedQueue`: an `Online` generation has no queued mutation left in `!bg`.
- For a UNIQUE index (`Unique = TRUE`):
  - `UniqueWhenQueryable`: whenever the planner may use the index, no two records hold the same value. With `IndexMatchesTable`, the index then matches the table.
  - `DuplicateNeverPublished`: a build over a table that holds a duplicate ends in `Error`. No generation publishes `Online` while two records hold the same value.
  - `UniqueSlots`: an entry names one record at most. This checks the model, not the code.
  - `ErrorAddsNoKey`: no write that adds a key is queued for a generation in `Error`. The refusal is made when a write is admitted, not when it commits. A write queued while the build was still running commits after the build fails (`reach-unique-key-after-error`). The generation's queue is wiped and the table rescanned by the `REBUILD INDEX` that recovers it, so `IndexMatchesTable` does not rest on this refusal.
- `OnlineStays`: only a later generation ever replaces an `Online` generation, unless the id is retired or its uncommitted build is cleaned up.
- `RetiredStateStaysGone`: once a retirement commits, no build state, ticket counter or reservation of the id comes back.
- `RetiredIndexStaysEmpty`: once a retirement commits and its reclaim runs, the id holds no index entries.
- `NoRetiredWriteFails`: first-use admission and the cached recheck never fail a write because the index it no longer has to maintain was retired.
- `NoRetiredFenceFails`: the same at the fence after a reservation commits.
- `RolledBackLeavesNothing`: once a rollback cleanup or a cluster-side retirement deletes a build whose definition never committed, nothing of the id is left except its barrier proof, and nothing writes the id again.
- `RolledBackLeavesNoBarrierProof`: once no rollback hook is left to run, the barrier proof of such a build is gone too.
- `Never*`: reachability checks. Each is a state that a passing scenario claims to cover, so TLC is expected to violate it.
- The open-gap rows check four more invariants: `NoRetiredFenceFails`, `RetiredQueuesStayEmpty`, `NoStrandedUncommittedBuild` and `RolledBackLeavesNoBarrierProof`.

The planner never uses an index whose definition never committed, because `Queryable` requires a committed definition. A later `DEFINE INDEX` of the same name cannot inherit what a rolled-back or retired build left behind, because it draws a fresh id and every build key is keyed by the id:
- Ids come from `Sequences::next_index_id`.
- A held id is issued again (`release_catalog_id`) only when no definition confirmed it.
- A speculative run starts no build (`exec/vm/speculate.rs`), so every build runs under a confirmed id.
- The doc comment on `del_tb_index_deferred` relies on the same property.

The model therefore covers one id. The rollback properties above check what that id leaves behind and that the planner never reads it.

Liveness properties, checked under `LiveSpec`:

- `BuildStarts`: a `DEFINE INDEX` commits its definition and gets generation 1 staged, and a `REBUILD INDEX` gets the next generation staged.
- `BuildSettles`: every generation that starts reaches `Online` or `Error`.
- `AdmissionWaitEnds`: a writer on a live node whose admission waits on `Closing` is eventually admitted, maintains the index in its transaction, or ends its transaction.
- `BarrierProceeds`: a builder waiting at the write barrier proceeds or stops.
- `TicketBoundUnreached`: an invariant that the liveness rows check. It holds when the `MaxTicket` bound never holds back an admission, so no liveness violation comes from that bound.

### Fairness

`LiveSpec` is `Spec` with weak fairness on the steps that the system takes by itself:

- **Builders.** Every builder transaction is weakly fair. This includes adopting a generation whose owner has stopped (`Adopt`).
- **Writers on a live node.** The admission steps, the commit and the reservation release are weakly fair, so a writer finishes the transaction it opened.
- **The defining statement.** Its commit is weakly fair.

These steps get no fairness:

- A takeover of a live owner, a builder failure or crash, and a node lapse. `MaxTakeovers` and `MaxFaults` bound them.
- Opening a transaction, a mutation and a rollback. They are the user's choice. A fair rollback would end every admission wait by giving up, so `AdmissionWaitEnds` would hold for no reason.
- The `DEFINE` and `REBUILD` requests.
- Any step of a writer whose node lapsed. A dead node may never act again.

No two steps of one builder or one writer are enabled in the same state. So the weak fairness on each process's disjunction of steps is weak fairness on each step. The constant `Unfair` names the steps that `LiveSpec` leaves without fairness. It is empty except in the rows that show a property depends on one step.

The liveness rows do not use `SYMMETRY`, because symmetry reduction is unsound for liveness. They use their own small bounds: one record, one value, two writers, two transactions (one in `live-takeover` and `live-rebuild`), at most one fault, and `MaxTicket` raised. They leave out the barrier-less builders. Such a builder takes no generation over, so its crash leaves the build with no owner.

### Granularity

- **Builder transactions are atomic steps.** Every builder transaction compare-and-swaps `!bs`, so builder transactions conflict pairwise.
- **A writer's two transactions have separate read and commit steps.**
  - The admission transaction reads `!bs` and `!bt`, then commits `!br` with a compare-and-swap on `!bt` by value.
  - The user transaction reads at the snapshot it began with. It commits under first-committer-wins on every key it writes: the record, index entries, `!bg` and `!bp`.
- **Time is not modelled.**
  - A live owner's lease lapses whenever the model chooses, up to `MaxTakeovers` times. Safety can therefore rest only on fences.
  - A writer node whose heartbeat lapses is `fenced`. The drain may then treat its reservations as dead, the write barrier leaves it out, and with `WriteFence` its commits are refused.
- **The index is a number per (record, value) entry.**
  - With `Counting = FALSE`, a write sets the entry present or absent, as a key-valued index does.
  - With `Counting = TRUE`, writes add and subtract, as COUNT and full-text statistics do. A write maintained twice, or not at all, is then visible.
  - With `Unique = TRUE`, the index is key-valued and has one slot per value.
    - The slot is keyed by the value alone and names the record that holds it. Two writes of one value therefore conflict under first-committer-wins, whichever records they are for.
    - A write takes the slot with a compare-and-swap against an absent entry. A delete clears it only while it names the deleted record.
    - The index has one field, so a mutation adds a key exactly when it leaves the record holding a value. NONE and NULL exemptions, `NULLS NOT DISTINCT`, array fan-out and several fields are not modelled.
- **How a UNIQUE index refuses a duplicate depends on who maintains it.**
  - A writer that maintains the index in its own transaction checks the slot as its snapshot shows it. If the slot names another record, the write fails with `IndexExists` and the transaction commits nothing. If another transaction, or the build, writes the slot after that snapshot, the conflict on the value key fails the commit instead.
  - A writer that queues for a build checks nothing. Two queued writers on different records can commit a duplicate into the table.
  - The build checks every entry it writes, at the scan and at the replay. A value that names another record fails the batch, and the build ends in `Error` in the same step. The scan's parallel batches and their retries are one step per record in the model.
  - The build judges values as it indexes them, not as the table holds them. The scan indexes a record with a queued mutation at that mutation's old value, from its `!bp` marker. Replay then applies the queue in ticket order, which is the order of admission, not of commit. A build can therefore fail on a duplicate that no committed state of the table held (`reach-unique-stale-duplicate`). The index then stays out of the planner until a `REBUILD INDEX` rescans the table, so no property is at stake.
  - A UNIQUE index in `Error` refuses a write that adds a key, wherever admission reads `!bs`: the admission read, the fence after the reservation, and the recheck of a cached ticket. A write that adds no key, such as a delete, still queues.
- **The write barrier is a global wait.**
  - Before an initial scan reads, from the start or from a checkpoint, the builder waits for every write transaction that opened before the wait began, on any node.
  - The wait leaves out a `fenced` node, as `answers` in `write_barrier.rs` leaves out a node whose heartbeat is older than `exclude_after`. The write fence already refuses that node's commits, and a dead node never answers. The model makes the fence and the exclusion apply at the same moment. In the code, the exclusion comes later by a margin.
  - A builder in `BarrierlessBuilders` runs a release without the barrier. It neither waits nor records the proof. It starts generations but takes none over, because a taker on that release would follow that release's own protocol.
- **A retirement is one schema commit, followed later by a reclaim.**
  - With `RetireAs` set, the commit deletes the definition, `!bs`, `!bw` and the queues of every generation. `"remove"` and `"overwrite"` differ only in how the catalog answers for the old id afterwards: Retired or Replaced.
  - Its range deletes remove the keys present when it commits. A user transaction that commits a queued mutation after it does not conflict with it.
  - The reclaim deletes the index entries. It runs once every write transaction that was open at the retirement has ended, which is what the grace in `reclaim_tombstones` amounts to when time is not modelled.
- **A defining statement may fail to commit** (`DefineAborts`). It can roll back, die with its node, or commit with an unknown outcome, in which case its rollback hooks run all the same.
  - The local cleanup runs as one transaction. Its wait for the builder may expire while the builder still runs, even mid-batch.
  - The cluster-side retirement runs from a compaction pass. It runs only for a counting index whose build has written a batch, since that is the only case that queues a request.
  - `Publish` clears the owner, but the code keeps it until the statement closes. So an `Online` generation the catalog does not define counts as still owned.
- **The catalog fence is a store property.** `FencesCatalog = TRUE` models a store that fences catalog changes (`Transaction::fences_catalog_changes`).
  - `DEFINE INDEX` and `REBUILD INDEX` stamp the table's catalog version as they commit. A `REBUILD INDEX` commits in a step of its own (`RebuildCommit`), at any point after its builder installed the generation.
  - Every user transaction that writes a record reads the version as a locked read. A stamp that commits after its snapshot fails its commit, as a conflicting key would.
  - A build of a generation that its own statement installed, and that reads once that statement has committed, skips the cross-node wait. It still records the proof. Writers run on nodes other than the builders', so the wait for the builder's own node waits for nothing.
  - A takeover, and a build inside the defining transaction, always raise the barrier.
  - With `FencesCatalog = FALSE` nothing reads the version locked, so the model leaves the stamp out.

### Scenarios

| Scenario | Expected | Shows |
| --- | --- | --- |
| `define` | pass | `DEFINE INDEX CONCURRENTLY` against concurrent writers |
| `define-keyed` | pass | the same, for a key-valued index |
| `multi-mutation` | pass | several mutations per transaction under one cached ticket |
| `takeover` | pass | a second builder taking over a live or dead owner |
| `rebuild` | pass | a build failing to `Error`, or settling `Online`, then `REBUILD INDEX` as generation 2, under the generic barrier |
| `rebuild-multi-mutation` | pass | the same, with a cached ticket meeting the generation change |
| `rebuild-takeover` | pass | a takeover of the rebuild's generation, fresh or part-scanned |
| `node-lapse` | pass | a writer node lapsing while it holds a reservation |
| `in-txn` | pass | a blocking build inside `BEGIN…COMMIT`, with every later writer held by `DefinitionHold` |
| `old-release-rebuild` | pass | a barrier-less owner rebuilding a failed generation |
| `old-release-takeover` | pass | a barrier-less owner taken over by a builder with the barrier |
| `unique` | pass | a UNIQUE index over two records. A duplicate in the table, or one committed by queued writers, fails the build at the scan or the replay. Writers on the `Online` index are refused a value another record holds. Writes to the failed index that add a key are refused, and deletes still queue |
| `unique-rebuild` | pass | the same, then `REBUILD INDEX` of the failed or online generation, including a rebuild that reaches `Online` after a build failed on a duplicate (`reach-unique-recover`) |
| `unique-multi-mutation` | pass | two mutations per transaction: a direct writer refused a held value (`reach-unique-refused-direct`), and a queued writer whose cached ticket meets the failed build refused a key (`reach-unique-error-refused-cached`) |
| `unique-fault` | pass | a builder that stops without ending its generation, as `mark_durable_error` does when it gives up: the build is taken over and still ends in `Error` on the duplicate (`reach-unique-fault`) |
| `retire` | pass | `REMOVE INDEX` landing during the build: mid-scan, while a writer holds a reservation, or after a writer decided to maintain the index |
| `retire-multi-mutation` | pass | the same, with a cached ticket rechecked after the retirement |
| `retire-rebuild` | pass | the same, after a failed build and `REBUILD INDEX`: `REMOVE INDEX` lands while generation 2 builds (`reach-retire-rebuild-mid-build`), and while its `REBUILD INDEX` is requested but not yet installed (`reach-retire-rebuild-pending`) |
| `retire-overwrite` | pass | `DEFINE INDEX OVERWRITE` retiring the old id, with writers still holding it |
| `retire-fenced` | pass | `retire` on a store that fences catalog changes |
| `retire-orphan-queue-fenced` | pass | on a store that fences catalog changes, `REMOVE INDEX` stamps the table (`retire_index`), so a write queued before it fails its commit and leaves no `!bg` or `!bp` behind (`RetiredQueuesStayEmpty`, `reach-retire-orphan-queue-fenced`) |
| `rollback` | pass | a `DEFINE INDEX CONCURRENTLY` that rolls back, dies with its node, or commits with an unknown outcome |
| `rollback-in-txn` | pass | the same for a build inside the defining transaction, which writes before the statement closes, so the cluster-side retirement reaches it |
| `define-fenced` | pass | `define` on a store that fences catalog changes, where the build waits for no other node |
| `rebuild-fenced` | pass | `rebuild` on that store, with one writer opening after the definition commits. A writer that found generation 1 `Online` and maintains the index directly either commits before the rebuild's stamp or fails its commit |
| `rebuild-takeover-fenced` | pass | a rebuild of an `Online` generation on that store whose only builder crashes and resumes it, trusting the barrier proof that it recorded without asking any other node |
| `reach-*` | fail | each passing scenario reaches the states it claims to cover |
| `reach-unique-key-after-error` | fail | a write queued while the build was running commits its key after the build fails. The refusal in `Error` is made at admission |
| `reach-unique-stale-duplicate` | fail | a build fails on a duplicate that no committed state of the table held: the scan baselines a record at its queued mutation's old value, and replay runs in ticket order |
| `no-closing-bump` | fail | without the `!bt` advance on `Closing`, an admission that read `Building` reserves after the drain |
| `no-drain` | fail | without the reservation drain, a write admitted before `Closing` commits after `Online` |
| `no-barrier` | fail | without the write barrier, a writer that cannot see the definition commits after the scan passed its record |
| `no-markers` | fail | without the `!bp` baseline, a counting index applies a mutation the scan already saw |
| `no-owner-fence` | fail | without the owner compare-and-swap, a superseded builder writes into the generation |
| `no-write-fence` | fail | without the write fence, a lapsed node commits a queued write after its reservation was retired |
| `old-release-no-prior-drain` | fail | without the barrier, the prior-generation drain is what keeps a writer queued under the failed generation from committing after the rebuild's scan |
| `old-release-no-marker-rebuild` | fail | a takeover that rescans needs `!bp` rebuilt from `!bg`: a writer admitted after the first owner's scan completed wrote no marker |
| `unique-no-check` | fail | without the UNIQUE check, a writer on the `Online` index commits a value another record holds |
| `unique-record-keyed` | fail | with the conflict key of an entry being (record, value) rather than the value, a writer whose snapshot predates the scan's entry for its value commits that value for a second record, because its write does not conflict with the entry |
| `unique-no-duplicate-fail` | fail | without the build's check, a build over a duplicate publishes `Online` |
| `unique-no-error-refusal` | fail | without the refusal in `Error`, a write that adds a key queues for the failed generation |
| `unique-no-recheck` | fail | without the fence after the reservation, a write reserved while the build was running queues a key after the build fails |
| `no-retire-fence` | fail | without the `!bs` compare-and-swap, a batch in flight at the retirement writes index entries after the reclaim (`RetiredIndexStaysEmpty`) |
| `no-retired-skip` | fail | if admission treated a retired index as a vanished build, a write would fail because the index it no longer maintains went away (`NoRetiredWriteFails`) |
| `no-cleanup-catalog-check` | fail | without the catalog read, the cleanup after a commit with an unknown outcome deletes the build of a committed definition, which then reads as a ready, empty index |
| `no-uncommitted-fence` | fail | without the table-definition fence, an in-transaction statement whose published build a lapse retired still commits, and its index reads as ready and empty |
| `no-fence-catalog-check` | fail | if the fence after a reservation did not ask the catalog when it finds `!bs` gone, a write that reserved before `REMOVE INDEX` committed would fail with `IndexingBuildingCancelled` (`NoRetiredFenceFails`) |
| `no-retire-proof-delete` | fail | if `retire_uncommitted_build` did not delete `!bw`, an in-transaction build whose node died after its barrier would leave its proof (`RolledBackLeavesNoBarrierProof`) |
| `no-catalog-fence` | fail | without the locked read of the catalog version, a writer whose snapshot predates the definition commits after the build scanned past its record. This is a writer node on a release without the fence, which the `!wf` check exists to catch |
| `rebuild-no-catalog-fence` | fail | the same for a rebuild: a writer that found generation 1 `Online` maintains the index directly, and the rebuild counts it twice |
| `in-txn-cross-node` | fail | open gap: a writer on another node is not held, and the in-transaction build misses its write |
| `in-txn-cross-node-fenced` | fail | the catalog fence does not close that gap: the writer commits before the defining transaction stamps the table |
| `old-release-rebuild-online` | fail | open gap: a writer that found generation 1 `Online` maintains the index directly, and a rebuild without the barrier counts it twice. Both the generic barrier (`rebuild`) and the catalog fence (`rebuild-fenced`) close it |
| `retire-orphan-queue` | fail | open gap (`RetiredQueuesStayEmpty`): a write queued before the retirement commits its `!bg` and `!bp` after it. The reclaim deletes only the index's data prefix, so nothing deletes them. Nothing replays or reads them either. On a store that fences catalog changes, the retirement's stamp fails that commit instead (`retire-orphan-queue-fenced`), so the gap is only on a store that does not |
| `rollback-stranded` | fail | open gap (`NoStrandedUncommittedBuild`): a statement whose node dies after its build staged `!bs`, and before the build queued compaction work, leaves `!bs` and `!bt` that no hook, resume scan or compaction pass reaches. A build no statement awaits writes nothing before the statement closes, and a B-tree kind queues no compaction work at all, so for either this is every death after staging. No later definition reads these keys, since it draws a fresh id |
| `no-prior-drain` | pass | with the barrier, the barrier waits for every writer the prior-generation drain waits for |
| `no-recheck` | pass | for a non-unique index, the reservation drains cover a generation change or `Online` after a reservation |
| `old-release-no-recheck` | pass | the same, without the barrier |
| `live-define` | pass | under `LiveSpec`, every requested build starts and settles, and neither an admission waiting on `Closing` nor a builder waiting at the barrier waits forever |
| `live-faults` | pass | the same, with one builder crash or failure, or one writer node lapse |
| `live-takeover` | pass | the same, with a second builder taking over a live owner |
| `live-rebuild` | pass | the same, through a `REBUILD INDEX` of a settled or failed generation |
| `live-in-txn` | pass | the same, for a blocking build inside `BEGIN…COMMIT` |
| `live-no-barrier-exclusion` | fail | if the barrier waits for a lapsed node, a node that never acts again holds the builder at the barrier forever |
| `live-unfair-commit` | fail | the barrier progress rests on writers ending their transactions |
| `live-unfair-release` | fail | an unreleased reservation holds the drain, and the admission waiting on `Closing` with it |
| `live-unfair-publish` | fail | the build settling rests on the builder's steps |
| `live-unfair-adopt` | fail | after a builder crash, the build settles only because a builder adopts the ownerless generation |
| `sim-rebuild-takeover` | pass (simulated) | two records, two mutations per transaction, three transactions, two builders with a takeover, and a rebuild after one fault |
| `sim-rebuild-takeover-keyed` | pass (simulated) | the same, for a key-valued index |
| `sim-in-txn` | pass (simulated) | the same bounds, for a blocking build inside `BEGIN…COMMIT` |
| `sim-reach-rebuild-takeover` | fail (simulated) | the simulation reaches a takeover of a fresh rebuild that settles `Online` |
| `sim-no-markers` | fail (simulated) | at these bounds the simulation finds the violation that `no-markers` finds |
| `sim-rebuild-fenced` | pass (simulated) | a rebuild on a store that fences catalog changes, with two writers, two builders and a takeover, which the exhaustive `rebuild-fenced` rows do not reach |
| `sim-reach-rebuild-fenced` | fail (simulated) | at these bounds the simulation reaches the writer the rebuild's stamp refuses |

How to read a change in these outcomes:
- A `fail` row that starts passing means the model no longer exercises the mechanism it removes.
- One of the last three rows starting to fail means the covering mechanism no longer covers it.
- A takeover that restarts the scan re-runs the prior-generation drain (`RestartDrainsPrior`). With a barrier-less first owner and a taker that has the barrier, the taker's own barrier covers that drain. That scenario is outside the suite's bounds, so the suite does not re-check it.

### Spec actions and the code they model

| Action | Code |
| --- | --- |
| `AdmitNormal`, `AdmitRead`, `AdmitCommit` | `IndexBuilder::reserve_durable_admission` (`admission.rs`) |
| `FenceQueue`, `FenceOnline`, `FenceRotated`, `FenceRetired` | `IndexBuilder::fence_durable_admission` |
| `MutateQueued`, `RecheckFails` | `recheck_cached_admission`, `write_admitted_mutation` |
| `MutateNormal`, `MutateUnindexed`, `Commit`, `Rollback` | the document write path in the user transaction |
| `Release` | `IndexBuildReservationRelease` (`kvs/tx.rs`) |
| `HeldRefused` | `DefinitionHold`, `WriteCtx::wait_for_definitions` |
| `AcquireExisting` | `Building::acquire_build_state`, `rescan_without_barrier_proof` (`builder.rs`); a takeover's `AcquiredBuild::staged` is `false` |
| `AcquireNew` | `stage_new_generation`, on `acquire_build_state`'s new-generation path, whose `AcquiredBuild::staged` is `true`; `IndexBuilder::start_building` sets `reads_after_its_stamp` when the build also waits for its statement |
| `DrainPrior` | `wait_for_prior_generation_reservations` (`replay.rs`), `wipe_stale_build_queues` |
| `BarrierBegin`, `Barrier` | `wait_for_older_writes`, `kvs/write_barrier.rs`, `record_barrier_proof`; the catalog-fence shortcut is `WriteBarrier::start` with `stamped`, which `Building::reads_after_its_stamp` sets |
| `BarrierWaitsFor` | `answers` and `exclude_after` (`kvs/write_barrier.rs`) |
| `DefineCommit`, `RebuildCommit` | the statement's commit, whose `Transaction::stamp_catalog_version` stamps the table (`replace_table` in `ir_lower/ddl.rs`) |
| `LockedReads` in `Commit` | `Transaction::catalog_version`, read locked on a store that fences catalog changes, which `WriteCtx::resolve` reads before the first record write |
| `Markers` | `rebuild_primary_appendings` |
| `ScanStep`, `ScanDone` | `index_initial_batch`, `mark_durable_initial_complete` |
| `Replay`, `ReplayDone` | `index_appending_loop`, `index_durable_appending_range` |
| `Closing` | `mark_durable_closing` |
| `Drain` | `wait_for_durable_reservations` |
| `Publish` | `mark_durable_online` |
| `Fail` | `mark_durable_error` |
| `Stop` | a failed compare-and-swap, or a missing `!bs`, in `maintain_build_ownership`, `update_build_state_in_tx` or `update_owned_build_state_inner` |
| `NodeLapse` | node heartbeat lapse; `surrealdb_datastore::triggers::WriteFence` |
| `Clashes` (in `AdmitNormal`, `FenceOnline`, `MutateNormal`), `UniqueRefused` | the UNIQUE maintainer: `IndexOperation::index_unique` (`surrealdb/idx/src/index.rs`) and `move_entries` (`exec/vm/write_storage.rs`), a `put_compare_key` against an absent entry that reports `IndexExists` for another record |
| `RefusedInError`, `ErrorRefused` | `refuses_on_failed_build`, `failed_build_refusal` (`admission.rs`), in `reserve_durable_admission`, `fence_durable_admission` and `recheck_cached_admission` |
| `Duplicate`, `DuplicateFails` | `index_unique` as the build calls it from `index_initial_values` and `apply_appending` (`replay.rs`); the failed batch's error reaches `run_claimed` (`builder.rs`), which calls `mark_durable_error` |
| `Remove` | `remove_index_definition` (`after_commit.rs`) and the `retire_index` lowering (`exec/vm/ir_lower/ddl.rs`); `delete_durable_build_artifacts` (`state.rs`); `del_tb_index_deferred`; `AbortLocalBuild` |
| `Reclaim` | `Datastore::reclaim_tombstones` for a `ReclaimKind::Index` entry |
| `AdmitRetired`, `AdmitReplaced` | the missing-`!bs` branch of `reserve_durable_admission`; `catalog_state`; `write_maintains_replacement` |
| `MutateRetired`, `MutateSkipped` | the missing-`!bs` branch of `recheck_cached_admission`; `CachedIndexAdmission::Retired`, `ConsumeResult::Retired` |
| `FenceRetired`, `FenceRotated` (missing `!bs`) | the missing-state branch of `fence_durable_admission`, which reads `catalog_state` in the transaction that found `!bs` gone |
| `DefineRollback`, `DefineCommitUnknown` | the statement transaction's rollback hooks: `ReleaseBuildOwnership`, `settle_uncommitted_statement` (`builder.rs`) |
| `DefineNodeDies` | the statement's node stops; no hook runs |
| `AbortStop` | the abort raised by `settle_uncommitted_statement` and `remove_index_and_wait` |
| `CleanRollback` | `CleanUncommittedBuild`, `IndexBuildCleanup::delete_once` |
| `RetireUncommitted` | `retire_uncommitted_build`, `uncommitted_build_lapsed`, `fence_table_definition` (`state.rs`), reached from a compaction pass (`materialise.rs`) |
| `StaleBatch` | none: a batch committed without the `!bs` fence, for `no-retire-fence` |

### Not covered

- Parts of the catalog-fence shortcut:
  - The `!wf` heartbeat check is not modelled. Every writer node is taken to carry the fence; `CatalogFence = FALSE` stands for a node without it that the check missed.
  - The shortcut's wait for the builder's own node is not modelled, because no writer runs there.
  - A stamp counts against a writer from the writer's snapshot, as `Transaction::fences_catalog_changes` states. That a locked read (`getu`) on each fencing engine does this is assumed, not modelled.
- Which `REBUILD INDEX` takes the shortcut. A plain rebuild qualifies when its build waits for the statement to close: `CONCURRENTLY`, or blocking and awaited after the commit. The model's rebuild is that case. Two other cases raise the generic barrier, which the `rebuild` rows cover:
  - a blocking rebuild awaited inside its transaction;
  - a format-migrating rebuild, whose generation `stage_rebuild` stages and `claim_staged_generation` claims with `staged = false`.
- The protocol a release without the barrier follows. A barrier-less builder is modelled as this protocol with the wait removed.
- For UNIQUE indexes:
  - The writes of the transaction that defined the index. With a blocking build, `judge_write` (`unseen.rs`) refuses a change to a record the build indexed. A UNIQUE write checks its key through the view opened after the build (`Unseen::Built`, `written_by_unseen_build` in `write_storage.rs`). The model has no actor for the defining transaction's writes, for any kind of index.
  - A statement that recovers from `IndexExists` (`UPSERT`, `INSERT ... ON DUPLICATE KEY UPDATE`) by rolling back to a save point, which leaves the transaction committable. In the model a refused write fails its transaction.
  - NONE and NULL values exempt from uniqueness, `NULLS NOT DISTINCT`, array fan-out, several fields, and numbers of different kinds that share key bytes.
- Around retirement and rollback:
  - A retirement staged in the transaction that defines the index (`stop_build_of_statement`).
  - The index that replaces the old id under `DEFINE INDEX OVERWRITE`.
  - The keeper of a published in-transaction build, which cancels its statement once its heartbeat finds the ownership gone.
  - Withdrawing a lapsed build whose basis the committed catalog does not describe (`build_basis_is_stale`, `withdraw_stale_lapsed_build`). That case needs a committed definition at another format or analyzer.
  - A retirement or cleanup of the doc-id space shared with other indexes.
  - `EXPUNGE`.
- A format-migrating `REBUILD INDEX` staged ownerless in the statement's transaction.
- The release gate, and the compaction or materialisation that follows `Online`.
- The legacy `!ig`/`!ip` queues and `next_ticket` allocation.
- Liveness outside the `live-*` bounds. These include two values, more than two transactions, a takeover together with a fault, the barrier-less builders, and the open gaps. The liveness rows also say nothing about how long a wait lasts. They show only that no wait lasts forever when the fairness above holds.
- Larger bounds, exhaustively. The exhaustive scenarios use one record, two writers and up to two builders. The UNIQUE scenarios use two records, two values, two writers, one builder and two transactions, with no faults. The `sim-*` rows go beyond that with two records and two mutations per transaction. They run TLC in simulation mode: random traces of a fixed depth, a fixed number per TLC worker, from a fixed seed. A simulation row passes when none of its traces violates a property. That is evidence, not proof. A violation that only a rare interleaving reaches can stay unfound. The exhaustive fenced rebuild rows use one writer and one builder; `sim-rebuild-fenced` simulates two of each.
