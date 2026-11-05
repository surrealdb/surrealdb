# The VM as a fan-out leaf

The executable classification lives in `exec/vm/leaf.rs`. It describes the
interfaces a read-only executor needs to run a plain-data `Program` against
one storage snapshot. The report is diagnostic; it does not admit work to a
worker or select an execution engine.

## Classes

| Class | Contract |
|---|---|
| `Leaf` | The interpreter or a synchronous pure builtin answers from the program, row and arguments. |
| `Kernel(seams)` | A native host runtime answers through the listed interfaces. An empty list still requires host dispatch, including async pure calls and registry entries the classifier cannot resolve. |
| `NonLeaf(bar)` | The operation cannot run on a read-only snapshot leaf. |
| `ByOperand` | The instruction or callee determines the host step's class. |

A program is leaf-clean when none of its instructions, sources, host steps or
held units is `NonLeaf`. It is host-free only when every item is `Leaf`.
Missing registry entries and async builtins remain `Kernel`; pure synchronous
calls alone qualify as `Leaf`. Effects bar writes, state advancement, schema
changes, live subscriptions, scripts, models, network access, waiting and
opaque bodies.

Every instruction and source variant is matched exhaustively. The
`leaf::SERVICES` inventory covers every method of `vm::services::Services`.
The tests check both coverage and the effects of builtin calls.

## Host interfaces

| Interface | Responsibilities |
|---|---|
| `Storage` | Records and ranges, B-tree, bitmap, full-text and vector reads, counts and adjacency at the snapshot. |
| `Catalog` | Table, field, function and parameter definitions. |
| `Permissions` | Principal, record and field permissions, and function capabilities. |
| `Clock` | Time, deadlines and cancellation. |
| `Params` | Session values, parameter bindings and saved frames. |
| `Link` | Compiled units for closures and tables reached through values. |

A leaf can link only units it holds or receives. An engine cache miss can
compile a unit for its current catalog and frame; a remote leaf must fail
when that unit is unavailable. Programs and units contain plain data,
without planned evaluators, operator handles or datastore objects.

## Native services

FETCH walks the program's field descriptors and evaluates computed indices
through precompiled host code. Record reads apply permissions and computed
fields through their VM units. Access and Each operate on values; record-link
cases use the same checked record reads.

Recursion strategies run iterative depth bodies backed by the program's
units. Repeat recursion uses the run's discovery and assembly state. No path
of expression-tree nodes is handed to a production strategy.

Index functions receive arguments read from registers and an index context
resolved for their call site. MATCHES carries an optional `FieldPath`, its
query and registration scope. A linked field's value is read by precompiled
host code; the probe contains no logical expression.

Projection functions receive evaluated arguments and produce bindings. The
VM writes those bindings through field paths, preserving aliases. Stored view
field lists execute as projection units over their bound document.

Closure values capture bindings and refer to compiled units. API middleware
calls stored functions through catalog-stamped call units. Function
capabilities, AUTH LIMIT, permission predicates, argument coercion and return
coercion apply at the call boundary.

EXPLAIN display nodes are inert. EXPLAIN ANALYZE runs the statement's program
through the VM adapter, drains it, measures the scan metadata and renders the
counters with permission redaction. The display tree executes no statement.

The native runtime still uses `ExecutionContext` and `EvalContext` for its
frame and host dependencies. Extracting those interfaces into an independent
worker runtime requires the snapshot and unit transport contract.

## Regression guards

`vm/tests_plain.rs` checks the program's reachable types, compiler entry
boundaries and the native host services' exclusion of evaluator trees.
`vm/tests_leaf.rs` checks exhaustive service classification, builtin effects,
and conservative treatment of unknown and asynchronous calls. No expression
is compiled to or evaluated as a tree.

`SURREAL_VM_LEAF=report` emits each program's bars and host interfaces. It
compares the same classification across the units the program holds.
