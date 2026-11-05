# RFC: Expressions as bytecode

Status: draft for review. Scope: how SurrealQL expressions become VM instructions, chosen
for throughput first, so that a program is self-contained bytecode — no `Clause`,
`ExecOperator` or other Rust trait object inside it — and can ship to a Worker.

Companion documents: [`PHYSICAL_PLAN_REQUIREMENTS.md`](PHYSICAL_PLAN_REQUIREMENTS.md) (laws, §9 lowering),
[`RFC_RECORD_LAYOUT_AND_MIGRATE.md`](RFC_RECORD_LAYOUT_AND_MIGRATE.md) (per-node infallibility,
casts by kind pair), [`RFC_DURABLE_BYTECODE_RUNTIME.md`](RFC_DURABLE_BYTECODE_RUNTIME.md) (effects).

---

## 1. Goals and constraints

1. **Throughput above all.** Per-row cost is the figure of merit: no heap allocation on the row
   path, and instructions dispatched once per vector of rows rather than once per row (§6).
2. **The answer does not change.** Every law of the requirements document holds: the same values,
   the same errors in the same order, the same evaluation counts. A typed or fused instruction is
   an equivalence, never an approximation.
3. **Self-contained.** A program names builtins, parameters and catalog objects symbolically, so
   it can be encoded, shipped and loaded elsewhere. What it needs from the node it runs on — storage
   reads, effects, the clock — is a host call with a declared contract.
4. **Types before instructions.** An instruction is chosen from whether its operands' kinds are
   known. Knowing more makes the program faster; knowing less never makes it wrong.

## 2. What the programs evaluate today

An expression without a native instruction fails its statement. The census
below counts, by node kind, the expressions across the language suite with the VM on that have no
native instruction when the bytecode is planned (static counts, one per compiled expression;
paging clauses and record-id sources, which are plan-time host work, excluded):

| node kind | count | | node kind | count |
|---|---|---|---|---|
| field read (`Field` under `Current`/`Doc`) | 1152 | | `Destructure` | 80 |
| graph `Lookup` | 1071 | | `Each` / `Flatten` / `Filter` | 79 / 35 / 69 |
| record-link `Access` | 625 | | `Recurse` | 65 |
| `Broadcast` (`a.*.b`) | 354 | | `Index` | 59 |
| `Param` | 300 | | closures | 45 |
| array literal | 277 | | casts (`<int>`, `<string>`) | 38 |
| subquery | 259 | | comparisons (`=`, `>`, `INSIDE`, `!=`) | ~320 |
| `Method` | 101 | | builtin calls (all names) | ~400 |

Two conclusions shape the ISA. Navigation — reading through record links, graph edges and array
paths — dominates, and much of it is a storage read, so host reads are first-class instructions
rather than an escape. Arithmetic and builtins are a long tail of modest counts; the calling
convention matters more than any one function. The suite measures breadth, not heat: hot paths
are chosen from benchmark workloads (§8), with this census as the coverage map.

Of the `SELECT` statements the VM declines outright, `EXPLAIN` (804) and `MATCHES`/KNN (301) lead;
both are outside this RFC.

## 3. Kinds in the plan

### 3.1 A value's kind is known or it is not

A register's kind is decided when the program is lowered, and it is either **known** or **not
known**; there is no third state.

- **Leaves.** A literal's kind is known. A field of a table stored in a packed layout is known,
  because the layout stores it untagged at an offset its generation assigns: reading it at all
  requires its kind. Any other field — the tagged encoding, a flexible field, a delegated row — is
  not known.
- **Propagation.** An instruction's result is known when every input it reads is known and its rule
  names the result: an operator by the language's rules (§7), a builtin by its catalog row (§3.2).
  Otherwise it is not known.
- **What knowing buys.** A known kind selects a typed instruction and removes the per-row check a
  generic instruction makes. A builtin whose answer lies in the value's slot reads the slot:
  `string::len` of a known string field reads its length word, and `string::starts_with` its
  inline bytes. A known column in a vectorized region is a plain typed slice.
- **What not knowing costs.** A generic instruction checks the kind of each value it is handed, as
  the value was fetched, and is always correct.

A reader may still take a typed fast path for a field whose kind is not known — the tagged encoding
reads an integer in place when the stored value is one and falls back when it is not — but that is
a property of the reader, invisible to the program.

The kind of a statement's response is not inferred. The output sink writes each value as it finds
it, skipping the check where the register's kind is known; a static response schema is the
analyzer's concern.

### 3.2 The builtin catalog is the shared source of truth

The engine's builtins are the catalog: each is declared once, through the `define_*_function!`
macros, with its name, `EffectSet`, argument kinds and declared return kind, and a `ReturnRule`
that names its result kind from its arguments' kinds — fixed, an argument's kind, an array or set
argument's element kind or `NONE`, or a numeric aggregate that is `int` over ints. The SurrealQL
analyzer infers with the same rules, and consumes this catalog rather than keeping its own.

A rule is a claim the planner acts on, so it is checked, not trusted: `exec/function/rule_tests.rs`
calls every builtin that has one over arguments of every kind — mixed and empty collections,
out-of-range indexes, variadic calls — and asserts that the kind the rule names admits the value
returned, exactly. A function whose result can take values from more than one argument keeps its
declared return kind until a rule that joins those arguments' kinds replaces it.

Each row gains, beside what it has, the facts execution needs:

| field | meaning | consumed by |
|---|---|---|
| `id` | a stable symbol; programs import builtins by name, resolved to a local slot at load | encoding |
| `totality` | per argument-kind combination: total, or the errors it can raise | `MIGRATE`, predicate reordering, deferred errors |
| `lanes` | fixed-width pure / variable-width pure / allocating / host | vectorized regions and SIMD (§6.1) |
| `cost` | a cost class | predicate ordering, fan-out width |
| `native` | whether a `Scalar` implementation exists, and whether it reads only a value's slot | instruction selection |

`totality` is verified by the same kind of test as the rules.

## 4. The instruction set

A register machine, as today: registers hold `Scalar`s (borrowed or inline), at most 32 per body.

### 4.1 Families

- **Loads.** `LoadField` / `LoadPath` from the row (borrowed from the stored bytes where the row is
  read in place), `LoadConst`, `LoadParam` (a parameter slot resolved once per execution),
  `LoadRow`.
- **Typed arithmetic and comparison.** `IntAdd`, `FloatMul`, `DecAdd`, `IntCmp`, `StrEq`,
  `StrCmp`, … selected by kind; each has its generic counterpart over `Scalar` that implements
  SurrealQL's cross-kind rules. Integer overflow follows the language's rule exactly (§7).
- **Control.** `Jump`, `JumpIf`, and short-circuit forms for `AND`, `OR`, `??`, `?:` and `IF`,
  written so each operand is evaluated at most once and only when the language evaluates it.
- **Casts.** One instruction per (source kind, target kind) pair the plan can prove, and a generic
  cast. A cast's totality comes from the kind-pair table of the `MIGRATE` RFC.
- **Construction.** `NewObject`, `SetField`, `NewArray`, `Push` into the row's scratch arena.
- **Navigation.** `Index`, `First`, `Last`, `Broadcast`, `Flatten` as instructions over registers;
  `Each` and `Filter` as a loop over a sub-body, not as a callback.
- **Host reads.** `FetchRecord` (a record link's target, borrowed as a row), `GraphStep` (the ids
  an edge traversal reaches, with its filter as a sub-body), `Subquery` (a nested program run with
  the correlated values as its parameters). Each is a suspension point where the host services the
  read and the body resumes, as every host step suspends the body.
- **Calls.** `Call { builtin, args: first..last, dst }` with arguments in consecutive registers.
  A builtin with a native implementation runs over `Scalar`s; one without runs through a generic
  bridge that converts to `Value`, and the census of bridged calls is the list of natives to
  write next.
- **Sinks** as today: emit, sort push, group key, accumulate.

### 4.2 Values without allocation

A register borrows from the row or the constant pool wherever it can. A value the body builds —
a concatenated string, a projected object — lives in a per-row scratch arena that is reset between
rows, and becomes owned only when a sink keeps it. The output sink writes rows straight to the
client encoding (JSON first) from registers, so an emitted row need not become a `Value`.

## 5. Optimizations over the program

Each is an equivalence under the laws; the ones that move evaluation carry the gate named.

1. **Constant folding and CSE** over proven-pure, proven-total subexpressions.
2. **Row-free hoisting**, under §9.6: a subexpression that reads nothing of the row and nothing
   the statement writes on any control path is evaluated once per execution, lazily.
3. **Typed specialization** from §3, where a register's kind is known.
4. **Superinstructions** for the hottest sequences — `LoadPath`+`Compare`+`JumpIf`,
   `LoadField`+`Call string::len`, `FetchRecord`+`LoadPath` — chosen from benchmark profiles, not
   guessed.
5. **Predicate ordering by cost and selectivity**, only among conjuncts that are pure and total for
   their proven kinds: reordering a conjunct that can raise changes which error a statement
   reports, so it is not an equivalence.
6. **Strength reduction and range tightening** where kinds are proven (`x * 2`, `x > 5 AND x > 10`).
7. **Early termination** as today: limits, and a sort bound under a literal limit.

## 6. Vectorized execution, storage, fan-out and dispatch

### 6.1 Row semantics, vectorized execution

Every instruction is specified per row, and the unit tests and the laws are stated in those
terms. The VM executes a program's pure regions over a vector of rows at once, with a selection
vector of the rows still live: an instruction runs once per vector, not once per row. This is the
default execution mode, not a later optimization, for two reasons the census and the measurements
give:

- **Dispatch.** Row at a time, an interpreter pays a dispatch per instruction per row, 1–3 ns each;
  an expression of ten instructions costs as much as the scan beneath it. Over a vector, dispatch is
  paid once per vector.
- **Storage.** Navigation dominates what the programs evaluate (§2), and over a vector a record-link
  or graph read becomes one batched read instead of one read per row (§6.2).

What that asks of the program and the host:

- **Predication in pure regions.** Within a region, `AND`, `OR`, `IF` and filters narrow the
  selection vector instead of jumping; a region is straight-line code over the vector.
- **Deferred errors.** A fallible instruction records a failure for its row instead of raising it.
  At the end of the region the failure of the lowest row that has one is raised, which is the error
  the row-at-a-time order would have raised first. A row the region drops before the failure would
  have been raised — rejected by a later filter, or past a limit — contributes none.
- **Region boundaries.** An instruction with an effect, a per-row loop (`Each`, `Filter`, a closure),
  or a nested program ends the region and runs row at a time, so effects keep their order.
- **Vector width** is the host's choice, sized to the storage chunk and the cache, and never visible
  to a program's answer.

Fixed-width pure regions over columnar or PAX chunks can additionally run as SIMD over the vector;
that is the same execution mode with a wider inner loop.

### 6.2 The storage interface

The host serves a program's storage reads through three primitives, and every read the VM makes —
scans, index iterators, record fetches, graph steps, counts — is one of them:

| primitive | contract |
|---|---|
| `Scan(range, direction, chunk)` | a resumable visitor over borrowed key/value spans, in key order |
| `GetMany(keys)` | a visitor over borrowed values, in input order, misses reported |
| `Count(range)` | the number of keys in a range |

Reads are explicit and batched rather than hinted: a vectorized program knows the keys it will read
before it reads any of them, so it issues one `GetMany` per vector and overlaps it with the previous
vector's work. That delivers what prefetching would, without a second path or a cache to keep
coherent. Readahead into a cold tier (an object-store-backed engine) stays the storage engine's
concern.

The transaction API has most of this: the zero-copy `for_each` visitor for scans, native `getm` on
TiKV and the memory engine, and `count`. What it lacks: a visitor form of `getm` that
borrows instead of copying each value; a native `getm` on SurrealKV; and chunk sizes chosen by the
caller. Every primitive reads through the transaction's own writes; where that merge happens for
each engine is to be confirmed before the VM reads below it.

A host that is not the local node — a Worker, a remote fan-out leg — implements the same three
primitives, which is all a program needs of storage.

### 6.3 Fan-out and dispatch

- **Fan-out.** A program's effect set, the union of its instructions' and builtins', decides whether
  its pipelines may run on morsels in parallel, and cost classes size the width.
- **Dispatch.** The interpreter is a `match` loop over instructions, each running over the vector.
  Translating a loaded program into a local array of closures is a host implementation detail; the
  shipped form stays bytecode, and it is adopted only if it measures faster.

## 7. Semantics the instructions must reproduce

Verified on every engine and pinned by
`language-tests/tests/language/expression/operators/operand_semantics.surql`:

- **`AND` and `OR` return an operand**, not a boolean: `1 AND 2` is `2`, `0 AND 2` is `0`,
  `'' OR 'b'` is `'b'`, `NULL AND 1` is `NULL`. The right operand is evaluated only when the left
  does not decide (`false AND <int> 'x'` is `false`, and raises nothing). So `AND` and `OR` are a
  select on truthiness, and in a vectorized region they narrow the selection for the right operand
  rather than combining two booleans.
- **`??` replaces only `NONE` and `NULL`** (`0 ?? 1` is `0`); **`?:` replaces any falsy value**
  (`0 ?: 1` is `1`).
- **Integer overflow is an error** (`Cannot perform addition with '9223372036854775807' and '1'`), for
  addition, subtraction and multiplication; there is no promotion and no wrapping.
- **Integer division truncates** (`7 / 2` is `3`); **division by zero is `NaN`**, not an error, while
  **remainder by zero is an error**.
- **Mixed numerics widen**: `1 + 1.5` is `2.5f`, `1 + 1dec` is `2dec`.
- **Arithmetic on `NONE`, `NULL` or a string is an error**, naming the operands' kinds.
- **Comparison across kinds orders by kind**: `NONE < 1` and `NULL < 1` are true, `10 < '9'` is true,
  `NONE = NULL` is false, and `1 = 1.0` is true. Strings compare bytewise (`'B' < 'a'`).
- **Field access past a missing value is `NONE`**, however deep (`{ a: 1 }.b.c`).
- **A composite value reports its first failing element**, in written order, for arrays and objects.

Still to verify, each with its own language test before its instruction is written: `COLLATE` and
`NUMERIC` ordering; record-link access through a missing record; graph steps under permissions.

## 8. Measurement

Two figures track the work: the number of expressions the suite holds that have no native
instruction (the census above, rerun per step), and ns/row on a benchmark set that covers filters, projections with builtins,
record-link and graph navigation, and aggregates, in release on 200k rows. A step lands when the
suite matches with the VM on and neither figure regresses.

## 9. Order of work

1. The shared builtin catalog, with execution facts.
2. The core ISA: loads, typed and generic comparison, arithmetic, logic and casts; the parameter
   slots; the output sink over registers; vectorized execution with predication and deferred
   errors.
3. Navigation and host reads over the storage interface of §6.2: `FetchRecord`, `GraphStep`,
   `Broadcast`, `Index`, `Each`, `Filter`, `Flatten`, `Destructure`.
4. Calls by id: natives for the builtins benchmarks show hot, the generic bridge for the rest.
5. Subqueries as nested programs.
6. Superinstructions and SIMD regions, from profiles.

## 10. Open decisions

1. **Where the catalog lives**: in the engine repository with the analyzer depending on it (as
   proposed), or in the analyzer with the engine generating from it.
2. **Program encoding and versioning**: how a shipped program names builtins and catalog objects,
   and how a node refuses a program built against definitions it does not have.
3. **Closures** (`array::map` and friends): a sub-body in the same program, or a nested program.
