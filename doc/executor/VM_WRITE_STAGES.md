# Write stages and the field view as bytecode

**Status:** every stage of a write lowers to instructions. Companions:
[`VM_WRITES.md`](VM_WRITES.md) (the write body's control flow),
[`RFC_EXPRESSION_BYTECODE.md`](RFC_EXPRESSION_BYTECODE.md).

A VM write body is control flow around the instructions each stage of the
record's order lowers to, acting on the record in flight. This document is how
the stages that evaluate SurrealQL — `output`, `data`, `schema`, `gate` — are
bytecode, and what they are built on. The stages that are storage (`store`,
`purge`, `refs`, `index`, `guard`) are native intrinsics with declared
effects. A table's live subscriptions are units of bytecode compiled before
any write reaches them (§6).

## 1. The field view

Every one of the four stages reads a record through the table's *field view*:
the table's `COMPUTED` fields derived onto it, and, for a caller whose reads
are checked, each field it may not select cut from it. `output` shows images
through it, `gate` judges them, `data` reads the reduced pre-image, `schema`
evaluates `VALUE` and `ASSERT` against it. The read path applies the same view
as a projection layer (`Plan::Layer`, today an escape hatch into
`PreparedLayer`). The view is therefore the foundation, and it is built first,
on the read path, where the layer tests already pin it.

The view of a record `r` is, in order (`materialise_fields_with_permissions`):

1. **Derive.** Each computed field, in dependency order, is evaluated against
   the record as derived so far and set at its path. The body runs in a
   *computed frame* (§2).
2. **Cut stored fields.** For a checked caller, each stored field's `PERMISSIONS
   FOR select` predicate is judged, with `$value` the field's value, against a
   snapshot of the record taken after step 1; a field it refuses is removed.
3. **Re-derive.** If step 2 cut anything, every computed field is derived again
   against the reduced record, with the reduced stored fields as what a
   self-dereference (`COMPUTED id.x`) reads.
4. **Cut computed fields.** Each computed field's own predicate is judged
   against its final value.

A table with no computed fields runs step 2 alone; a caller whose reads are not
checked runs step 1 alone.

## 2. Frames

A body is evaluated in a frame: the context the host builds for it. The VM has
permission frames (`PermissionSpec`) and call frames (`CALL_FRAME`); the view
adds **computed frames**, one per computed field:

- writes refused (`ExecutionContext::computing_field`), whatever the body calls;
- the field's `AUTH LIMIT` applied (`with_limited_auth`), so the body runs as
  its definer, never as the reader;
- fetch permissions skipped exactly when the caller's are;
- the record being computed (`ComputingRecord`: its id, and the stored-fields
  view of step 3) set from the row when the body is evaluated, so a
  self-dereference is answered from the view rather than by re-deriving.

A field predicate of step 2 or 4 runs in a permission frame, as a table
permission does, with `$value` bound for the one evaluation.

A body that signals `RETURN` answers its value; any other signal from a
computed body is an error, as in a permission frame.

## 3. Instructions

The view is lowered into the pipeline body where `Plan::Layer` stands, in place
of `Instr::ApplyLayer`:

- `SetRowField { path, src }` — a host step: the value in `src` is set at
  `path` on the row, and the row resumes after it with fresh registers, as
  `ApplyLayer` resumes. What a later instruction reads of the row is what the
  view has made of it.
- `RemoveRowPath { path }` — the same, removing the value at `path`.
- The bodies and predicates are ordinary lowered expressions, evaluated in the
  frame their spec names.

A field permission whose idiom holds a wildcard (`items[*]`, `outer.*`) expands
to the concrete paths the record has; until that expansion is an instruction, a
layer holding one keeps the escape hatch.

## 4. The record in flight

A write stage acts on the host's `WriteRow`, which holds the record's two
images and the state between stages. A bytecode stage reaches it through
instructions of `WriteInstr`, never through registers holding the row:

- `LoadImage { image, dst }` / `StoreImage { image, src }` — the pre-image
  (`initial`) or the post-image (`current`);
- `LoadInput { dst }` / `StoreInput { src }` — the evaluated data clause
  (`ComputedInput`);
- `ShowImage { image, dst }` — an image through the caller's field view (§1),
  leaving the row's reduced counterpart as `shown` does;
- `SetOutput { src }` — the row the statement returns.

A stage's outcome — continue, skip the record, fail — is control flow: a jump
to the next stage, a jump to the body's skip label, a raise.

### 4.1 Host-fulfilled storage

A bytecode stage reaches storage only through host instructions: it reads an
image, a record or an index entry, and it hands a write to the host as an
effect. That keeps one program runnable on two hosts. On the coordinator,
which holds the live transaction, the host writes. On a fan, which reads a
snapshot at or below its shard's watermark, the host appends an *intent* to the
fan's write buffer. An intent is the key, the value, and the *premise* the
write assumes: the key is absent, or it holds the version the fan read. The
coordinator applies the buffers and checks each premise. When a premise fails,
the row aborts or is run again serially.

A fan reads its snapshot with its own buffer laid over it, so it reads its own
writes. It never reads another fan's. A statement fans only when its effect
graph shows that no fan reads an extent another fan writes. A body in which one
record's write is a later record's read does not fan through that dependency:
counters, colliding `UPSERT` keys, and events that write the table being read.
Its evaluating stages (`data`, `schema`, `gate`, `output`) may still run on the
fans, as long as the storage stages apply serially at the coordinator. Unique
entries and relation endpoints are premises. Table views that aggregate are
commutative, so they apply as escrowed deltas.

## 5. The stage modules

Each group of stages lowers in a module of its own, so the groups move
independently:

| Group | Lowering | Host | Instructions | Tests |
|---|---|---|---|---|
| data | `vm/lower_write_data.rs` | `vm/write_data.rs` | `DataInstr`, as `WriteInstr::Data` | `vm/tests_write_data.rs` |
| schema | `vm/lower_write_schema.rs` | `vm/write_schema.rs` | `SchemaInstr`, as `WriteInstr::Schema` | `vm/tests_write_schema.rs` |
| gate | `vm/lower_write_gate.rs` | `vm/write_gate.rs` | `GateInstr`, as `WriteInstr::Permissions` | `vm/tests_write_gate.rs` |

- `Lowering::write_stages` matches every `StageId` without a wildcard (the
  wildcard-arm lint is denied there, so a stage with no lowering fails the
  build) and offers each stage to its group's `<group>_stage(id, skips)`,
  which emits the stage's instructions. A jump taken when the stage abandons
  the record is pushed onto `skips`. The observer stages lower to
  `ObserveInstr`s: the table's events and a view table's order are observer
  units of the statement's arena, found again by what they run and what they
  are lowered with, so a cycle of events or of views closes on the unit being
  lowered.
- The host's `write::step` hands each instruction of a group to that module's
  `step`, which answers the instruction the body continues at. A failure it
  returns goes through the write body's handlers, as a stage's failure does.
- An expression a stage evaluates is a host pipeline over one of the record's
  images, built with `Lowering::set_aside`/`restore` and run by
  `write::run_image`. It uses the frame the expression is written for: a
  permission frame for a permission, a computed frame for a `COMPUTED` body,
  and `$before`/`$after`/`$value` bound in the pipeline's frame where the
  stage binds them. What a table declares is read when the statement is
  planned, from the bound catalog, as `image_view` reads the field view; a
  statement that names no single table lowers its runs of stages for each
  table it can reach, as units its body dispatches to by the record's table
  (`WriteInstr::DispatchTable`).
- A stage's body never calls its Rust counterpart, and it reaches storage
  only through host instructions (§4.1).

## 6. Live subscriptions

A write notifies the subscriptions its table holds when the record is
written, and a subscription is not the program's to know: another session
registers and kills subscriptions at any time. The program holds one
instruction, `ObserveInstr::Lives`, which reads the subscriptions the
record's write context holds, as `ProcessTableLives` reads them. Nothing
more runs under the router engine, for a write no observer sees, for a
table with no subscription, or where no broker takes notifications.

Each subscription's work on a record is a **unit** (`vm::live`): a program
of its own, compiled for the subscriber's principal against the catalog of
the table it watches, with every parameter its frame binds (`$before`,
`$after`, `$value`, `$event`, `$auth`, `$session`, `$access`, `$token` and
the subscription's captured variables) left to the frame. Its pipelines run
in the order the table view applies them, then the subscription's own
clauses:

1. the table's `PERMISSIONS FOR select`, judged against the image as stored,
   for a subscriber whose reads are checked;
2. the table's field view for the subscriber, every `COMPUTED` field derived
   and each field it may not select cut;
3. the `WHERE`, over the reduced image;
4. the projection, or the diff between the reduced images.

`FETCH` is walked by the host from the paths the subscription compiles to.
The images are reduced in a frame binding the unreduced records, and the
subscriber's clauses run in one binding the reduced ones, as the Rust body
orders them, so a subscriber denied a field can neither read it nor use it as
a boolean oracle. A failure in a unit is its subscription's: the
subscription is told nothing, and the write is unaffected. A notification is
sent as the Rust body sends it, only once the transaction commits.

**A program compiles nothing while it runs.** Units are held by the node,
keyed by the subscription's id and the table's catalog version. A unit is
compiled when its subscription registers (the `LIVE SELECT` is itself a
statement being run), and when a write to its table is planned against a
catalog version the node holds no unit for: the subscription was registered on
another node, or the table's catalog changed since it registered. A block
whose own schema statement changes the catalog compiles its later statements
again before they run, and their units with them. A write's table context
carries the units of its subscriptions at the catalog version it read, and the
instruction looks each up. A subscription with no unit at that version — its
stored text no longer compiles, its work has no bytecode form, or neither its
registration nor the write's planning saw the version, as for a write whose
stages are lowered per table as its records arrive — is not notified.
