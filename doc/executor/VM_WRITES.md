# How the VM runs a write

The VM runs `CREATE`, `UPDATE`, `UPSERT`, `DELETE`, `RELATE` and `INSERT` itself: the statement's
subjects are VM pipelines, and each record's order of stages is VM code, every stage lowered to
instructions of its own (`VM_WRITE_STAGES.md`). The orders and the per-table contexts are the write
path's own — `WritePlan::build`, `WriteScope` — so the VM and the write operator cannot disagree
about which stages a statement runs, in what order, against which table's definitions. `doc/adr/0001-streaming-write-path.md` is the contract; `core/src/doc/` is the
authority it transcribes.

## 1. Two halves, and why the line is there

A write statement is a program with two halves.

- **Subjects** produce the records the statement writes: the records a scan or an index yields for
  `UPDATE` and `DELETE`, the targets `CREATE`, `UPSERT` and `RELATE` name, the rows `INSERT` supplies.
  These are read pipelines like any other, and are the only part of a write that may fan out.
- **The write body** runs one record at a time, in subject order, on the task that owns the
  transaction: the per-record order, the statement gate, output and the `ONLY` rule.

The line is forced by three properties of the write path. Savepoints are a stack with no handle, so
at most one record may be inside a create attempt at once. A later record's stages must see an
earlier record's writes, through the transaction's own writes. And the statement commits once, as
one transaction the executor owns — or, across a fan-out, as the one participant that commits for
all of them. So a fan-out leg streams subjects back to the coordinator, never writes; what it may
compute ahead is only what reads nothing the statement writes.

## 2. The write body's state

The host holds, for the statement:

- the `WriteScope` (output, the per-table `WriteCtx` cache);
- the statement gate (`WriteGate`), its per-table answers memoised;
- counters: rows returned, records matched, the guaranteed `UPSERT` table;

and, for the one record in flight, its `WriteRow` and the `WriteCtx` of its table. Registers never
hold a `WriteRow`: it is not a value, and exactly one exists at a time.

## 3. Instructions

Every write step is an `Instr::Write(WriteInstr)` (`core/src/exec/vm/program.rs`); the interpreter
hands each to the host (`core/src/exec/vm/write.rs`), which answers the instruction the body
continues at. `Instr::Jump` is the one plain jump the bodies use.

| instruction | does |
|---|---|
| `Record` | make the pipeline's row, a stored record, the record in flight (`WriteRow::for_record`, relabelled by `refused_by`) |
| `Gate { skip }` | the statement gate for the record's table; jump to `skip` when it refuses. Emitted for stored records and targets, not for `INSERT` rows or `RELATE` edges |
| `Begin` | check the statement is neither cancelled nor out of time, resolve the table's `WriteCtx`, load the record's stored metadata |
| `LoadNamedSubject` | `row.load_named_subject`, for `RELATE`'s selection |
| `JumpIfNew { target }` / `JumpIfNotIterationInitial { target }` | the selections of `RELATE` (`is_new`) and `UPSERT`/`INSERT` (`is_iteration_initial`) |
| `Data` / `Schema` / `Permissions` / `Observe` / `Storage`, `ShowImage` / `SetOutput` | a stage's instructions (`VM_WRITE_STAGES.md`); one that abandons the record jumps to the order's skip label |
| `DispatchTable { site, skip }` | for a statement that names no single table: run the record's table's unit for run of stages `site`; jump to `skip` when it abandons the record |
| `SavepointPush` / `SavepointRelease` / `SavepointRollback` | the transaction's savepoint stack |
| `TryBegin { handler }` / `TryEnd` | a region whose failure — error or signal — resumes the body at `handler` with the failure held |
| `ClearMutated` | the record's writes were rolled back, so it counts as not written |
| `RethrowSignal` | propagate a held `BREAK`, `CONTINUE` or `RETURN` |
| `ClassifyCreateFailure { policy, skip }` | `classify_create_failure` on the held error: raise it, jump to `skip`, or choose the record to update and the failure to carry |
| `JumpIfDone { target }` | `is_done(None)` between a rolled-back attempt and its retry |
| `ReloadForUpdate` | read the chosen record and `reset_for_update_retry` |
| `CarryBegin` / `CarryEnd` / `RaiseCarried` | the updating retry's region: inside it a failure is replaced by the carried one; an update that abandons the record raises it |
| `End { skipped }` | count the record as affected if it was written, as matched if its condition accepted it, and hand over its output unless skipped |

An order is a straight run of each stage's instructions, emitted from `WritePlan::order` — the stage
list itself, never a transcription of it; `Stage*` below stands for such a run. `Branching::Sole` is one run. `UpFront` is `LoadNamedSubject`,
`JumpIfNew`, and the two runs. `Retry` is:

```
    JumpIfNotIterationInitial → READ
    Stage* (creating, before savepoint_after)          skip → SKIPPED
    SavepointPush
    TryBegin → HANDLER
    Stage* (creating, the rest)                        skip → ABANDONED
    TryEnd; SavepointRelease; Jump → DONE
ABANDONED:
    TryEnd; SavepointRelease; Jump → SKIPPED
HANDLER:
    SavepointRollback; ClearMutated; RethrowSignal
    ClassifyCreateFailure                              skip → SKIPPED
    JumpIfDone → SKIPPED
    ReloadForUpdate; CarryBegin
    Stage* (updating)                                  skip → UNSATISFIED
    CarryEnd; Jump → DONE
UNSATISFIED:
    RaiseCarried; CarryEnd; Jump → SKIPPED
READ:
    Stage* (updating)                                  skip → SKIPPED
DONE:    End { skipped: false }
SKIPPED: End { skipped: true }
```

A failure outside every region ends the statement, after counting the record as affected when its
writes stand, as `counts_as_affected` decides.

## 4. Subjects, per source

- **Rows** (`UPDATE`, `DELETE`): a VM read pipeline — the same mapping a `SELECT` over the source
  gets — whose sink is the write body, starting with `Record`. The `WHERE` is a stage
  (`CheckWhereCondition`), judged after the gate. `ONLY` is judged at each flush, after a batch.
- **Targets** (`CREATE`, `UPSERT`, and a `Rows` source the planner classifies as targets): each
  target is evaluated in `subject_ctx`, one at a time, and expanded by `target_subjects` into rows,
  stored-record reads, and scans of a table or a range. The guaranteed `UPSERT` record is written
  after the last target when nothing matched, outside an import, and is not counted towards `ONLY`.
- **Inserts**: `INTO` and every row are evaluated before the first write (`insert_into`,
  `insert_values`), then each row is written (`insert_row`).
- **Edges** (`RELATE`): `from` and `to` are evaluated once, `through` per edge, over the nested loop
  of their endpoints (`relate_endpoints`, `relate_row`).

A write runs in a stream of its own (`vm::write::stream`), not the read host's, so a statement
nested inside a record's write — an event, a function body, `eval::surql` — keeps a small stack per
level.

## 5. What stays behaviour-for-behaviour

Everything the write operator does, the VM does, including what looks accidental; a test
pins each before it is changed. In particular:

- the guaranteed `UPSERT` record's row is not counted toward `ONLY`, and `ONLY` then also emits
  `NONE`;
- an `INSERT`'s create attempt evaluates `ON DUPLICATE KEY UPDATE` through `ComputeInputData`;
- `RETURN VALUE` is not flattened, so a signal inside it leaves the stage;
- the `Rows` source judges `ONLY` after each batch it wrote;
- an `UPSERT` of a missing record id does not judge `WHERE`;
- `RELATE` selects by `is_new`, the retry by `is_iteration_initial`.

A behaviour found wrong is a bug in the write path, fixed there with a reproduction in
`language-tests/tests/reproductions/`; the expectation is the reference.

## 6. Order of work

`CREATE`, then `UPDATE` and `DELETE` (the `Rows` source), then `UPSERT` and `INSERT` (the retry),
then `RELATE` (the upfront selection and the edge loop). Each lands when the language suite passes and a test pins the VM's rows and the database's state
after the statement.
