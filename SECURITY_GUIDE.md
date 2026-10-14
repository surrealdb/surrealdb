# SurrealDB Security Guide

This document defines the security invariants of SurrealDB and the review triggers
that gate any change touching them. It is the canonical source of truth for what
"secure" means in this codebase.

**Audience.** Contributors and maintainers writing code, human reviewers approving
PRs, and AI tooling assisting with development or code review.

**When to consult.** Before opening or reviewing any change that touches files or
behaviours matched by a section's *Review Triggers* — authentication, sessions,
permissions, RPC/HTTP transport, the parser, function execution, storage keys,
imports, or anything in the *Cross-Cutting Concerns*.

**How to apply.** Treat each invariant as a requirement that must still hold after
the change. If a code path deviates from an invariant, it must carry an explicit
`// SECURITY:` (or equivalent) comment explaining why and why it is safe — see
*Intentional Exceptions*. Otherwise flag the deviation at the severity defined in
*Severity Triage*.

For the general (non-security) review checklist — performance, error handling,
concurrency, test coverage, dependencies — see [REVIEW.md](REVIEW.md). For reporting
a vulnerability rather than reviewing one, see [SECURITY.md](SECURITY.md).

**Maintenance.** This document describes code, so it goes stale when code moves.
Any change that renames or relocates a symbol or module named here must update the
reference in the same PR. `.github/workflows/security-guide-refs.yml` enforces the
mechanical half of that: every Rust file path and every backticked code identifier
below must resolve somewhere in the tree, or CI fails. A reference the checker
cannot resolve but that is nonetheless correct belongs in
`.github/security-guide-refs-allow.txt` with a reason.

## Module Locations

File references use short module paths and omit the crate's `src/` segment: a
reference to `gql/lower/mutation.rs` means `surrealdb/gql/src/lower/mutation.rs`,
and `dbs/options.rs` means `surrealdb/core/src/dbs/options.rs`.

Security-relevant code is spread across the workspace, not concentrated in one
crate. Resolve a module name as follows:

| Module | Primary location | Notes |
|---|---|---|
| `dbs/`, `doc/`, `ctx/`, `fnc/`, `buc/`, `exec/` | `surrealdb/core/src/` | |
| `ntw/` | `surrealdb/server/src/` | |
| `rpc/` | both `surrealdb/core/src/` and `surrealdb/server/src/` | both are security-relevant; `rpc/streaming.rs` is the server one, `rpc/protocol.rs` the core one |
| `iam/`, `kvs/`, `idx/` | `surrealdb/core/src/` | a `surrealdb/<name>/` crate of the same name also exists; the core module holds the bulk of the enforcement logic |
| `syn/`, `gql/` | `surrealdb/<name>/` crate | the same-named module under `core/src/` is a thin remnant, **not** the code to review |
| `sql/`, `expr/` | `surrealdb/<name>/` crate | no longer under `core/src/`; some legacy AST also lives in `core/src/legacy/expr/` |

When a name resolves in two places, treat both as in scope unless the table says
otherwise.

## Review Scope

When a diff touches a security-sensitive area identified by a section's Review
Triggers, the reviewer should:

1. Examine the full diff for the affected files, not just changed lines.
2. Verify that the section's invariants still hold across the changed file, including
   call sites and callers reachable from the changed code.
3. Check whether the change affects other sections (e.g. a change to error types may
   affect Error Sanitization; a change to Options propagation may affect multiple
   sections).

## Severity Triage

When multiple sections are triggered, prioritize findings as follows:

- **Critical**: auth bypass, privilege escalation, data leakage across namespace or
  database isolation boundaries, code execution via user-supplied data.
- **High**: missing or incomplete permission checks, unbounded resource consumption,
  information leakage through error messages, session isolation failures.
- **Medium**: defense-in-depth gaps, missing audit logging, incomplete cleanup,
  overly broad capability defaults.
- **Low**: hardening opportunities with no reachable exploit path, and drift
  between this document and the code (a stale symbol name, a moved module, an
  invariant whose named mechanism no longer exists). Drift is worth reporting
  rather than silently working around: an invariant nobody can locate stops being
  reviewed.

## Verification Status

An invariant is only as good as what keeps it true. Each is one of:

- **Enforced** — a type, lint, or assertion makes the violation impossible or
  loud. Nothing further is needed at review time.
- **Verified by `<test name>`** — a named regression test fails if the invariant
  breaks. Deleting or weakening that test is itself a finding.
- **Review-only** — nothing mechanical protects it; a human or AI reviewer
  applying this document is the only control.

Invariants below carry a `Verified by:` note where such a test exists. Everything
without one is review-only by default. That is the current state, not the target:
the backlog is to give the Critical and High invariants named tests, and a PR that
adds one should also add the note here.

## Intentional Exceptions

Code paths may intentionally deviate from an invariant with explicit justification.
When reviewing, do not flag a violation if the code contains an accompanying comment
(e.g. `// SAFETY:`, `// SECURITY:`) that documents why the deviation is necessary
and why it is safe. Flag the deviation if the justification is absent, unclear, or
does not address the specific invariant being violated.

Test code (`#[cfg(test)]`, files under `tests/`, `language-tests/`) may use patterns
that are forbidden in production code (e.g. unwrap, perms bypass). These are
acceptable in test context.

---

## Core Security Model

SurrealDB's security model rests on four pillars:

1. **Isolation boundaries**: Namespaces and databases are hard isolation boundaries.
   Data, sessions, and execution context must never cross these boundaries.
2. **Principal hierarchy**: Root > Namespace > Database > Record > Anonymous. Every
   operation must be authorized for the acting principal's level.
3. **Permission enforcement**: Table-level and field-level permissions gate every
   data operation. System-internal paths that suppress permissions (`perms=false`)
   must be strictly bounded.
4. **Capabilities system**: Functions, network access, scripting, and experimental
   features are gated by allow/deny lists.

---

## 1. Authentication & Session Lifecycle

**Files**: `iam/`, `sql/access*.rs`, session/token handling, JWT signing/verification

### Invariants

- Every internal algorithm identifier must map to exactly one JWT library algorithm
  for both signing and verification. The signing-side and verification-side mappings
  must be bijective.
- Roles embedded in JWT claims from access-method tokens must be validated against
  a server-defined allowlist. Absent roles must default to the least-privileged role.
- Token refresh must validate the refresh token against the stored grant before
  trusting any routing claims from the expired access token. Namespace, database,
  and access method must be confirmed by the grant record, not unverified JWT claims.
- Every session and token must have a finite expiration. Missing duration
  configuration must result in a secure default, never unbounded validity.
- Authentication errors must not contain internal query errors, table names, field
  names, or stack traces unless an explicitly-opted-in insecure mode is enabled.
- Bearer token comparison must use constant-time comparison. Grant identifiers must
  correspond to active, non-revoked, non-expired grants in the correct scope.
- JWKS cache entries must be scoped such that a JWK trusted by one
  namespace/database cannot be reused by a different namespace/database, even when
  they reference the same URL.
- SIGNIN, SIGNUP, and AUTHENTICATE clauses must be evaluated with minimum necessary
  privileges. The evaluation session must not grant write access outside the
  authentication flow's scope.
- A CONTEXT clause must be evaluated with the authenticated record's own
  permissions, never with the privileged session the other access-method clauses
  use. Its whole result is stored on the session and readable by the client as
  `$session.data` — a SIGNIN/AUTHENTICATE result never is — so a privileged
  evaluation would make every CONTEXT clause a permission-bypassing read the
  record user can print. It must also be read-only and must run only after the
  identity is settled, so an unverified credential never triggers it.
- `$session.data` is server-computed, but it is **not** unforgeable, and no
  invariant may be written as if it were. `PROTECTED_PARAM_NAMES`, with the
  names the system binds in a frame (`surrealdb_cnf::is_refused_let_name`), is
  consulted only by `LET`, `UNSET`, `db.set`, RPC `set`/`unset` and request
  variables; the constructs that
  bind a caller-chosen parameter name — `FOR $session IN [...]`, closure
  parameters, user-defined function arguments — bind it unchecked, and
  `Context::value` prefers a locally-inserted key over the parent chain. The
  same routes shadow `$auth`. A permission that reads `$session.data` is
  therefore exactly as trustworthy as one that reads `$auth` — no more, no less
  — and every record permission already depends on `$auth`. Fixing this is an
  engine-wide change to parameter binding; until then, documentation and tests
  for CONTEXT must state the shared boundary rather than claim `$session` is
  unshadowable. A plan-time fold of `$session` must respect the same
  shadowing: the streaming planner folds `$session.field` out of the context
  the plan will run in, and carries the parameter names an enclosing `FOR`,
  closure or user-defined function binds so that a read beneath one of them is
  left to the evaluator. A fold that ignored them would answer with the
  ambient session where the evaluator answers with the binding.
- A closure invoked at runtime reads whatever row it is invoked against, and
  its body is nowhere in the expression that invokes it. The streaming
  planner's demand analysis decides which fields can reach the caller and
  emits a field-permission guard column only for those, so an invocation
  (`$pred()`, `$o.check()`, an unregistered method, which calls a closure
  stored under that name on the receiver) must be read as demanding the whole
  row — `exec/logical/reads.rs::invokes_an_unseen_body`. Read as demanding
  nothing, `WHERE $pred()` over `LET $pred = || secret = 'yes'` gets no guard
  layer at all and the predicate judges the stored value, leaking a denied
  field through row membership. A registered method is not this: it
  dispatches on the receiver's type and reads by name.
- Reading a `DEFINE PARAM` the plan did not resolve is a permission check where
  the read happens: the fetch judges the definition's `PERMISSIONS` clause
  against the caller and is denied where the clause denies. A rewrite that
  drops such a read unevaluated therefore drops the denial with it, and answers
  where the statement as written is refused. The bind pass names the parameters
  whose read is a fetch (`exec/logical/params.rs::permission_checked_names`,
  classified by the same `bind_param` the resolution uses, so a
  `PERMISSIONS FULL` definition the plan already folded to its value is not one
  of them) and `exec/logical/rules/mod.rs::may_raise` reports a read of one as
  able to raise, which is what keeps every rule gated on inertness from
  dropping it — `SELECT * FROM [$secret] WHERE false` must be refused, not
  answered `[]`.
- A record user's `$auth` is the record id of the signed-in record, and a path
  read from it (`$auth.email`, `$auth.id`) dereferences that record. Where the
  record's table is not defined the record is absent and every field of it
  reads `NONE`, the id included — never a `TbNotFound`, which every other
  record id naming an undefined table raises (`exec/operators/fetch.rs::is_auth_record`).
  The exemption keys on the id `$auth` holds and widens no access: `NONE`
  satisfies no predicate that compares a field to a stored value, so a
  permission written over `$auth.<field>` denies, as it does for a record
  that exists without the field, and a rewrite must not substitute the record
  id for the `NONE` of `$auth.id`.
- Session state must only be committed after all authentication steps complete
  successfully. Partial failures must leave the session in its pre-authentication
  state.
- Refresh token revocation and reissuance must be atomic within a single
  transaction. Concurrent reuse must result in at most one successful refresh.
- Durable RPC sessions (`--durable-sessions`) must be opt-in, and every persisted
  entry must carry a finite absolute expiry, enforced both lazily on load and by
  the periodic purge task. The durable copy contains the session's authentication
  state at rest, so only client-named (attached) sessions may ever be persisted —
  never ephemeral per-request sessions. A CONTEXT payload is part of that
  authentication state and is written at rest with it, in the durable copy and in
  the frozen session snapshot each LIVE subscription carries.
- Deleting a session (`detach`, teardown) must delete its durable copy in the same
  operation, so a torn-down session cannot be rehydrated. A failed durable delete
  must fail the teardown (leaving the session intact for retry), never report
  success while the durable copy survives. A durable session must never be
  recreated by a persist/refresh (only `attach` creates it, via a conditional
  insert), so a delete on one node cannot be undone by a stale cached copy on
  another. Each node revalidates a cached durable session against storage **before**
  dispatching each request — reloading the authoritative value, or evicting it when
  the durable copy is gone — so a detach, invalidation, or auth/USE change made on
  another node takes effect on the next request routed to a stale node, instead of
  that request running on cached state or the change lasting the node's cache
  lifetime. Durable sessions target deployments that route a given session to one
  node at a time (sticky routing / one runtime per session, per the `RpcProtocol`
  durable-session seam's design). Concurrently *mutating the same session id from
  multiple nodes* is best-effort: durable writes are compare-guarded so they never
  resurrect or clobber across nodes, but under such a race a losing mutation may be
  dropped rather than merged, so security-critical revocation should be driven from
  the session's owning node.
  Verified by: `detach_deletes_the_durable_copy`,
  `detach_fails_and_keeps_the_session_when_the_durable_delete_fails`
  (`rpc/protocol.rs`).

### Review Triggers

Flag for detailed review when changes touch:

- Token claims structure, especially routing fields (NS, DB, AC, ID, RL)
- JWT library integration, algorithm enumeration, or validation defaults
- JWKS caching (key structure, expiration, scope)
- Refresh token rotation (revocation, reissuance, atomicity)
- Session expiration checking in any query execution path
- New RPC methods or HTTP endpoints interacting with session state
- The role model (new roles, new privilege levels, `is_allowed`)
- Durable RPC session storage (`/!se` keys, `DurableSession`, the
  `load_session`/`persist_session`/`forget_session` hooks and their TTL handling)

---

## 2. Access Control Enforcement

**Files**: `iam/`, `dbs/options.rs` (`Options` struct, `perms` flag, `with_import`),
`ctx/context.rs` (`check_perms`, `is_allowed`), the write path the VM lowers
from the write stages (`gate_pipeline` in `exec/vm/ir_lower/stage.rs` for the table gates,
`process_fields` in `exec/vm/ir_lower/stage.rs` for field writes,
`DataInstr::Snapshot` in `exec/vm/write_data.rs` for the pre-mutation snapshot,
the `OutputWrite` stage and `shown_view` in `exec/vm/ir_lower/stage.rs` for what a write
returns), and the streaming engine's two read paths: the bound plan
(`exec/logical/resolve.rs` writes the table permission as a `PermissionFilter`
and the computed fields and field guards as projection layers above a resolved
source; `exec/operators/permission_filter.rs`, `exec/operators/layer.rs`) and
the runtime path for sources the analyzer left unresolved (`exec/permission.rs`:
`action_is_checked` — the one role and level rule, shared by the runtime check,
`ExecutionContext::should_check_perms` (`exec/context.rs`) and the planner's View
gate — `check_permission_for_value`, `validate_record_user_access`;
`exec/operators/scan/fetch.rs`: `resolve_with_field_state`)

### Invariants

- Every code path that reads, creates, updates, or deletes data on behalf of an
  external actor must invoke permission checks before allowing the operation.
- When system-internal code paths (events, views, cascades) set `perms=false`, this
  suppression must not extend to user-supplied expressions or user-controlled data
  lookups beyond the strictly required scope.
- `OPTION IMPORT` and `OPTION FORCE` must require Editor-or-above at Database scope.
  No Record-scoped or anonymous actor may activate import mode.
- Every `USE` statement that changes namespace or database must verify the actor's
  Auth level grants access to the target scope.
- The `Permission` enum defaults to `Full`. All code constructing permission objects
  must do so intentionally. Omitting a PERMISSIONS clause results in full access.
- Field-level permissions must be enforced on both the write path
  (`process_table_fields`) and every read path, including for computed fields,
  reduced documents, and all output modes (AFTER, BEFORE, DIFF, FIELDS). A
  mutating statement's `WHERE` condition and data clause are read paths too:
  they evaluate against the pre-mutation snapshot (`DataInstr::Snapshot`,
  derived by `materialise` in `exec/vm/write_data.rs`), so a field the caller
  cannot select must be absent from it — otherwise a data clause reads the
  value and copies it into a field the caller can select. For a checked
  caller the snapshot is the record run through the table's field view, or
  through its guards on stored fields alone when there is no pre-image to
  derive `COMPUTED` fields from (`Stages::snapshot` in
  `exec/vm/ir_lower/stage.rs`: the stored-field columns of the view's layers,
  or `Enforce::Guards` for a view with no layer form). A
  COMPUTED field is the case to check: its value is derived before its own
  `PERMISSIONS FOR select` is judged and re-derived once stored fields are
  cut, except where `plan_table_derivations` (`exec/permission.rs`) decides the
  guard first. A `NONE` guard never derives the field for a checked caller, and
  a `WHERE` guard that reads stored fields alone is judged once against the
  record as stored and derives the body only when it admits. Both leave no
  trace of the body in the result, and the read-time and bind-time paths
  decide through the same function. `materialise_fields_with_permissions` in
  `exec/operators/scan/pipeline.rs` is what orders those passes for a view
  with no layer form (`Enforce::Image`), and every site that materialises
  computed fields on a reduced view has to go through it or through the
  bound layers. What a write returns — AFTER, BEFORE, DIFF and FIELDS — is
  each image put through the same view (`WriteInstr::ShowImage`, emitted by
  the `OutputWrite` stage in `exec/vm/ir_lower/stage.rs`). There
  are two read paths and a new one must join one of them, never neither:
  the streaming engine's bound plan, where a resolved source carries its table
  permission as a `PermissionFilter` and its computed fields and field guards
  as projection layers (`exec/logical/resolve.rs`, run by
  `exec/operators/permission_filter.rs` and `exec/operators/layer.rs`); and the
  streaming engine's runtime path for a source the analyzer left unresolved
  (`resolve_with_field_state` in `exec/operators/scan/fetch.rs`, backed by
  `check_permission_for_value` in `exec/permission.rs`).
  The bytecode VM (`exec/vm/`) runs every statement, on the bound-plan path.
  It runs a statement only when its source was resolved
  (`EnforcementMode::Planned`) and the bound plan holds no column outside the
  caller's frame; every other statement fails, and nothing falls back to a path
  that enforces at runtime. Each projection layer it
  applies where the plan places it, above the `PermissionFilter` and beneath
  every user predicate, as instructions that set each column on the row
  (module docs of `exec/operators/layer.rs`), so its guards and computed
  fields are judged in the frames the layer defines.
  A
  `PermissionFilter` it applies where the plan places it, beneath every user
  predicate: a predicate reading only the row's own fields and literals as
  instructions, any other evaluated in the frame `permission_frame` builds —
  the same function the permission operator evaluates in — and neither where
  `skip_fetch_perms` holds. Its scan still checks namespace and database
  confinement (`validate_record_user_access`), as every scan does: a reading
  program that is not a scalar expression checks it before it streams a row
  (`execute_untailed` in `exec/vm/host.rs`), each group of a block that reads
  rows checks it when the group starts (`start_group` in the same file), and a
  write checks it per row at `GateInstr::Tenant` (`exec/vm/write_gate.rs`).
  Verified for a record user by
  `access/record/array_field_select_permission.surql` (a field guard cuts a
  denied field, top-level and in each array element),
  `access/record/computed_field_select_permission.surql` (a computed field is
  derived, and its own guard holds wherever the statement reads it) and
  `access/record/enforced_reads_writes_and_searches.surql` (a table permission
  admits only the rows it names, through a record range, a subquery and a KNN
  search) (`language-tests/tests/`), and by
  `a_table_permission_reads_the_callers_auth` (`exec/vm/tests_principal.rs`).
  The confinement check is pinned by
  `a_record_user_is_confined_to_its_own_namespace_and_database`
  (`exec/permission.rs`), and a write by a principal outside its tenant by
  `a_principal_outside_its_tenant_is_refused_whatever_the_clause` (below); no
  test pins the refusal of such a principal's read end to end.
  Privileged frames hold none of the caller's bindings. A table permission
  predicate, a read or write guard, a gate, a layer, a `COMPUTED` field, a
  field's `VALUE`, `ASSERT`, `DEFAULT` or `PERMISSIONS` clause, a function's or
  a parameter's `PERMISSIONS` guard, an event body, and an index column are
  definer code: each runs in a frame derived by
  `ExecutionContext::privileged_frame`, whose context is isolated from the
  caller's, so no query parameter, session `LET`, block `LET`, `FOR` variable
  or closure argument of the caller is visible there, nor in any function the
  frame calls; the session's own values and the database's parameters are. No
  plan-time step carries a caller's value into such code either: lowering
  refuses a privileged frame's read of a name a block binds (`Lowerer::value` in `exec/vm/ir_lower/expr.rs`), the logical
  parameter resolution substitutes the privileged view throughout a permission
  predicate and the plans it holds (`exec/logical/params.rs`), the resolver
  takes no caller value of `FROM $p` or `VERSION $p` inside a definition body
  (`exec/logical/resolve.rs`), a block's constants are not propagated into a
  permission predicate or a layer's columns (no value reaches into a region
  isolated from above: common-subexpression and dead-code elimination merge a
  value only where `Ir::dominates` in `exec/ir/core/ir.rs` holds, which stops
  at such a region, `exec/ir/passes/rehome.rs` never moves a value into one,
  and `exec/ir/core/verify.rs` checks it after every pass in a debug build),
  and a statement
  such code holds is bound and planned against `Planner::privileged` and, run as
  a group of the program, inherits none of the block's slots
  (`Lowerer::value` in `exec/vm/ir_lower/expr.rs`). A function body is
  lexically scoped (`Context::lexical_value`): it reads its arguments, what it
  binds, the row names, the session's own values, the request's parameters and
  the database's parameters, and never a `LET` or `FOR` binding of its caller,
  at any depth of calls. A body called from a privileged frame reads no request
  parameter either, since an isolated context carries none. A body reads every
  non-argument parameter at run time and is planned in a context that holds no
  caller binding (`Context::new_call_scope`), so a compiled body is never
  served with one caller's value for another. Verified by
  `privilege/privileged_frame_record_user.surql`,
  `privilege/privileged_frame_import.surql`,
  `function/caller_bindings_ordinary_call.surql` and
  `function/body_isolation_record_user.surql`
  (`language-tests/tests/language/statements/define/`), and by
  `no_caller_binding_reaches_a_permission_however_it_is_reached`,
  `a_statement_in_definer_code_reads_no_caller_binding` and
  `a_permission_reads_its_own_let_and_no_callers`,
  `an_ordinary_call_reads_no_binding_of_its_caller` and
  `a_callers_let_is_not_folded_into_a_bodys_predicate` (`exec/vm/tests_body_scope.rs`),
  which bind the name every way a caller can against every way a read or a
  write reaches a permission, and check that a permission's own `LET` still
  reaches the statement it holds.
  A tenant check (`validate_record_user_access`, and the plan-time
  `validate_access`) runs before any table's permission clause is consulted,
  for every statement that reads or writes a table — whether the statement
  names the table or the run does, and whether or not the table exists where
  the statement runs — so a principal outside its namespace or database is
  refused whatever the clause. Verified by
  `a_principal_outside_its_tenant_is_refused_whatever_the_clause`
  (`exec/vm/tests_body_scope.rs`).
  A statement of a block that runs after a catalog change the plan cannot
  model is planned late-bound (`Planner::late_binding`): its sources are left
  unresolved and each record is put through the table's enforcement as the
  catalog stands when it is read (`Plan::Enforced`, the field state
  `exec/vm/descriptor.rs` compiles per table version, judged by
  `process_fetched_record` in `exec/operators/fetch.rs`). Its writes run
  through the bundles of their stages compiled for the frame the write runs in
  and admitted against it (`serve_stage_bundle` in `exec/vm/fault.rs`); a write over a table or record such a
  statement left unresolved names it as a target, whose statement gate
  (`WriteGate`) judges the table's permission when the write runs. Its calls
  of user-defined functions run call units compiled for the caller's frame,
  which make the capability, `AUTH LIMIT` and `PERMISSIONS` checks a call
  makes (`exec/vm/call_unit.rs`). A parameter read whose definition holds a
  `PERMISSIONS` clause other than the one the program lowered is judged by the
  clause read now (`exec/vm/param_unit.rs`), never by the stale predicate and
  never skipped. Every such unit is keyed by the frame it was compiled for
  (the principal's class and the permission contract), so no principal is
  served a unit compiled for another class. The class is not the principal:
  a kept unit is served to every principal of its class, so it reads `$auth`,
  `$session`, `$token` and `$access` when it runs and never folds the values
  of the session it was compiled for (`Planner::reading_session_values`; a
  unit planner's `bind_scoped` leaves them unbound). A `PERMISSIONS` predicate
  of a definition that such a unit carries compiled is planned the same way
  (`exec/vm/call_unit.rs`), and a verdict it keeps for the rest
  of a statement is kept per principal (`exec/guard_verdicts.rs`).
  The bound plan upholds three rules. The barrier rule (module docs of
  `surrealdb_expr::expr::logical`): a user-written expression never moves
  below a `PermissionFilter` or a join lifted from a permission predicate, and
  moves below a layer only when every field it reads is a passthrough of that
  layer; the one permitted crossing is the access-path offer beneath the
  stack (`chain_offer` in `exec/planner/fold.rs`), which reads keys, not
  record values, drops every conjunct and order key rooted in a layer column
  or reading the row opaquely (a conjunct over the column of a record-id
  component reaches it as the read of `id[i]`, which is the key, as does an
  order key over one), and hands the scan no predicate when a
  permission node stands in the chain, so the nodes above judge every row it
  yields. The layer order (module docs of `exec/operators/layer.rs`): a
  stored field's guard is judged against the record as stored, a computed
  field is derived against the reduced record the stored guards leave, and a
  guard that does not hold removes the key. Redaction: a plan compiled for a
  caller the definitions are enforced on (`Planner::compiles_for_checked_caller`)
  renders a layer's permission and computed columns as `<permission>` and
  `<computed>` in place of the definer's expression (the column's name, kind
  and frame stay) and a permission filter's predicate as `<permission>`, a
  permission join hides its right side, its table and
  its alias, an access path chosen from a permission predicate prints
  `<permission>` in place of its index, bound and record id (a key range's `key`,
  `range` and `ranges` included), and `EXPLAIN ANALYZE` reports no metrics for
  any operator beneath a permission filter, a layer or a permission join
  (`ExecOperator::hides_metrics_beneath` and `redacts_access_paths_beneath`,
  applied in `exec/operators/explain.rs`; the VM sets them on the nodes it
  renders in `exec/vm/explain.rs`).
  Two scan paths deliberately sit outside the document pipeline and carry
  `// SECURITY:` comments saying so — `exec/operators/scan/graph.rs` (graph edge
  results) and `exec/operators/scan/reference.rs` (reference scans). A change to
  either must show that the compensating check is still applied; a change that
  adds a third such path without one is a field-permission leak.
  Verified by: `skip_fetch_perms_disables_every_check`,
  `a_record_user_is_confined_to_its_own_namespace_and_database`,
  `record_users_are_always_permission_checked` (`exec/permission.rs`).
- Each declaration is enforced exactly once per row. A source the analyzer
  resolved carries the table permission, the computed fields and the field
  guards as nodes above its scan, and the scan is planned under
  `EnforcementMode::Planned`, yielding rows as stored; a table the catalog
  lacks at the source's `VERSION` stamp, whose absence the analyzer saw too,
  is scanned under the same mode and resolves the table when it runs. Such a
  scan must never enforce at runtime beneath the plan's nodes: that judges
  the rows twice, a field guard against an already-cut row. When the planner
  cannot resolve the table context — no database selected, a plan-time cycle,
  a catalog error, a `VERSION` only known at runtime — the source is still a
  table scan under `EnforcementMode::Planned`: it resolves its table when it
  runs, at the stamp it reads at, and yields rows as stored for the plan's
  nodes to enforce. A scan that cannot yield rows as stored without a
  context, such as a lightweight relation's edge scan, refuses the plan
  instead.
  Verified end to end by `reproductions/planned_source_enforced_once.surql`.
- A `PERMISSIONS` predicate that reads a COMPUTED field of the row it judges —
  a table `select`, `create`, `update` or `delete` clause, or a field `create` or
  `update` clause — reads the value that field computes to: never `NONE`, and
  never a value the statement assigned to the field's name. A computed field is
  not stored, so judged against the record as stored, a clause written to deny
  (`WHERE hidden != true`) admits every row and one written to allow admits
  none. Each enforcement point derives the fields its predicate reads, with
  what they read in turn, on a copy of the row, in the predicate's own frame:
  reads unchecked, writes refused, each body under its field's `AUTH LIMIT`.
  The copy reaches the predicate alone, and the caller receives each field as
  the caller reads it. In the VM each body runs in a `FrameMode::Derived` frame
  (`exec/vm/program.rs`), one under an `AUTH LIMIT` holding a statement or a
  user-defined function call is refused when it is lowered, with `NotYet`
  (`refuse_narrowed` and `derive_fields` in `exec/vm/ir_lower/stage.rs`). The points
  are `Body::permission_filter` for a plan's
  `PermissionFilter` and a KNN search's candidates (`PermissionFilter`'s
  `computed`, set in `exec/ir/build/logical.rs` by `computed_reads_of` from `planner::fold::computed_reads`, judged
  through `Instr::JudgeCopy`), a graph hop's table permission (judged when
  the hop runs: the lowering in `exec/vm/ir_lower/graph.rs` leaves
  `LookupHop`'s `tables` empty, so `resolve` in `exec/vm/graph.rs` hands every
  record reached to `resolve_record_batch` in
  `exec/operators/scan/common.rs`, which judges it with
  `check_permission_for_value` against the table's compiled `SELECT` guard,
  a `ComputedFieldGuard`), the write gates'
  `GateInstr::Judge` for the create, update, delete and select clauses on
  either image (`exec/vm/ir_lower/stage.rs`), a live subscriber's `select` clause
  (`lower_subscription` in `exec/vm/ir_lower/units.rs`, admission in `exec/vm/ir_lower/subscription.rs`), and a field's write
  clauses (`field_predicate_pipeline` in `exec/vm/ir_lower/stage.rs`, judged by `SchemaInstr::Permit` in `exec/vm/write_schema.rs`). Every compiled
  table `SELECT` guard the host's streaming helpers evaluate — a record link
  the program follows — is a `ComputedFieldGuard`
  (`table_select_permission_physical`). The decorrelation rule does not lift a
  permission conjunct that reads a computed field off the outer row into a
  join, which would read it off the row as stored (`reads_outer_any_of` in
  `exec/logical/rules/decorrelate.rs`). Deriving in the caller's frame instead
  is the defect this rules out twice over: a body following a record the
  caller may not select derives from `NONE`, satisfying a clause written to
  deny, and a body reading its own table re-enters that table's `SELECT`
  permission without end. Computed fields are derived in dependency order
  (`topological_sort_computed_fields`, `Lowering::guard_derivations`), so a
  field reading another reads it derived.
  Each of these points, the write path's field graph (`field_eval_order` in
  `doc/document.rs`, which orders the `DEFAULT`, `VALUE` and `ASSERT` clauses
  and materialises the computed fields they read) and the `DEFINE FIELD` cycle
  check (`define_field_statement_validate_computed_cycles`) know a field by
  `Idiom::field_name` (`expr/idiom/mod.rs`): the unescaped name a read of it
  records (`extract_computed_deps`). `Idiom::to_raw_string` backticks a name
  that needs quoting (`` `is-locked` ``), and a field keyed by that matches no
  read: it is neither derived for nor ordered before the predicates and clauses
  that read it, they read `NONE`, and a gate or `ASSERT` written to deny admits
  the write. Only a top-level field has such a name. A read of a nested path
  records its root, so a nested field is never matched by name, and the nested
  field `is.locked` never stands in for the top-level `` `is.locked` ``: not in
  those graphs, and not in the runtime path's `PermissionPass::applies_to`,
  which tells a computed field's `SELECT` permission from a stored one's by
  that name and would otherwise cut a denied nested field only after a computed
  field reading it was derived.
  A predicate that calls a user-defined, scripted, module or silo function
  names no field the function's body reads off the row, so every computed field
  is derived for it: `extract_computed_deps` marks such a call opaque, as
  `call_evaluates_against_row` does for a bound plan.
  Verified by: `a_permission_membership_reading_a_computed_field_is_not_lifted`
  (`exec/logical/rules/decorrelate.rs`),
  `live_select_permission_reads_computed_fields` (`tests/it/live.rs`),
  `a_call_running_its_own_body_marks_incomplete` (`expr/computed_deps.rs`),
  `a_quoted_computed_field_is_keyed_and_ordered_by_its_unescaped_name`,
  `the_field_graph_matches_quoted_fields_by_their_unescaped_names` and
  `the_field_graph_tells_a_quoted_field_from_a_nested_path` (`doc/document.rs`),
  `a_clause_reading_its_own_field_is_not_a_dependency` and
  `a_nested_field_reading_a_quoted_field_spelling_its_path_has_a_dependency`
  (`catalog/schema/field.rs`); end to end by
  `reproductions/a_write_permission_reads_quoted_computed_fields.surql`,
  `reproductions/field_clauses_read_quoted_fields.surql`,
  `reproductions/field_clauses_tell_a_quoted_field_from_a_nested_path.surql`,
  `reproductions/field_permissions_tell_a_quoted_computed_field_from_a_nested_path.surql`,
  `reproductions/a_table_select_permission_reads_computed_fields.surql`,
  `reproductions/a_field_write_permission_reads_computed_fields.surql`,
  `reproductions/a_write_permission_computes_fields_in_dependency_order.surql`,
  `reproductions/a_permission_derives_the_computed_fields_it_reads_unchecked.surql`,
  `reproductions/a_permission_reads_computed_fields_through_a_function.surql`,
  `reproductions/a_write_output_select_permission_reads_computed_fields.surql`
  and `reproductions/a_permission_derives_a_computed_field_under_its_auth_limit.surql`.
- Reference cascade operations (ON DELETE CASCADE, UNSET, CUSTOM) must only modify
  records reachable through explicitly defined REFERENCE relationships.
- A `SELECT` permission expression (the WHERE clause in `PERMISSIONS FOR select`)
  must not produce observable side effects (writes, deletes, event triggers).
  A read must never trigger a write — the core escalation GHSA-66r2-5gwj-gxm2
  closes — and the predicate is evaluated with permission enforcement already
  disabled, so a write reached from one bypasses every gate.

  This is enforced **at runtime only**. Predicate evaluation carries
  `skip_fetch_perms` and a `NoWriteFrame::PermissionPredicate`, so the VM refuses
  CREATE/UPDATE/DELETE/RELATE/INSERT/UPSERT and DDL — including writes reached
  through custom-function bodies, `eval`, scripts and closures arriving as data.
  The block logs a warning steering operators to `DEFINE EVENT`, the sanctioned
  definer-authority side-effect mechanism.
  Verified by: `options_for_frame` (`exec/frame.rs`), the one place the frame an
  evaluation runs in is turned into the `Options` a callee takes, with the unit
  tests beside it; end to end by `reproductions/perm_predicate_runtime_block.surql`
  (guard reached by a scan) and `reproductions/udf_frame_write.surql`
  (guard reached by a record dereference, where the frame is marked on the
  `EvalContext` alone and the function call carries it onto the body's context).
- A permission predicate a read applies reaches the caller only as its verdict.
  It runs with reads unchecked, so a value it computes must never become the
  value of the statement it guards: a `RETURN` ends the block that holds it with
  its value, which the predicate then judges, and a `BREAK` or `CONTINUE` leaving
  the predicate is the loop-barrier error (`InvalidControlFlow`), since no loop
  is written in it. A predicate lowered into a program takes the `RETURN` of
  each statement it holds at that statement's instruction; a `RETURN` that
  leaves a statement evaluated in its frame uncaught, and a loop signal that
  does, are raised as errors where the signal leaves (`escaped_signal` in
  `exec/vm/host.rs`), never as the guarded statement's value. A guard applied to
  a record read by id or dereferenced is compiled by
  `read_permission_to_physical` as a predicate unit (`ScalarUnit::predicate` in
  `exec/scalar_unit.rs`), and `check_permission_for_value` applies the same
  table. A write's gate takes a `RETURN` as its verdict, and a `COMPUTED` field
  derived for a predicate takes it as the field's value; neither hands it to
  the caller.
  Verified by: `reproductions/a_return_in_a_permission_block_is_its_value.surql`.

  **Scope: statements, not side-effecting calls.** This frame blocks statement
  forms. It does *not* block a call to a builtin that changes something without
  one — `file::put`, `http::post`, `sequence::nextval`. The Surrealism read-only
  frame does block those (`NoWriteFrame::refuses_builtin_call`), and the
  mechanism is shared, so extending it here is a one-line change.

  It is not extended here yet, and the reason is scope rather than safety: a
  `SELECT` predicate calling `http::post` is the same shape of read-triggered
  side effect the advisory is about, and closing it would be a behaviour change
  for any deployment that has such a predicate today. It wants its own change,
  with its own note in the changelog and its own reproduction — not a quiet
  widening carried in on a Surrealism PR.

  Whoever takes it: the frame already travels correctly, and both enforcement
  points ask `NoWriteFrame` for the verdict *and* for the error
  (`refuses_builtin_call`, `side_effect_error`), so widening the scoping is the
  whole change — a predicate refused through either path still raises
  `PermissionPredicateSideEffect` and still logs the GHSA warning.
  `each_frame_raises_its_own_error` (`dbs/options.rs`) pins that the frames'
  errors stay distinct, which is what makes naming one at an enforcement point
  wrong even while the scoping keeps it unreachable.

  There is deliberately **no definition-time counterpart**. A `DEFINE`/`ALTER`
  that stores a writing `SELECT` guard is accepted; it fails when a read reaches
  the write. Rejecting at definition time cannot be made sound — whether a called
  function reaches a write depends on which branch its arguments select — and a
  check that guesses either refuses working schemas or gives false assurance.
  Treat the runtime frame as the whole of the control.
  - **create/update/delete permission predicates** (table clauses, and the
    create/update field clauses) carry no such frame: they are only ever reached
    from a statement that is already writing, so their side effects run, with
    permissions bypassed as with any predicate. The frame decision is the
    clause's `GuardKind` (`Read` for `select`, `Write` otherwise), applied by
    `permission_frame` in `exec/operators/permission_filter.rs` for a read and
    by `PermissionSpec::context` in `exec/vm/program.rs` for a write gate.
- The Auth context within Options must not be mutated by user-controlled operations.
  Only system-internal mechanisms (AuthLimit) may produce derived Options with
  modified auth, and these must never broaden permissions.
- The import flag must reset at the start of each new request.
- Record-scoped auth must be verified at all permission check sites to ensure the
  record token's namespace and database match the current execution scope.
- **The components a record id names are fields the table's permissions govern,
  and a key range over them is an access path, never an enforcement point.** A
  table whose `id` names its components (`DEFINE FIELD id ON reading TYPE
  [city: string, time: datetime]`) derives each one as a read-only virtual field
  of the row, the read `id[i]` (`KeyStructure::derived_fields` in
  `catalog/schema/key.rs`; the definition carries `key_part`), with a full
  `select` permission. Whoever may read a row reads every component of its key,
  and the table's `PermissionFilter` and permission clauses judge a component as
  they judge any field. A component has no permission of its own, and none can
  be put on `id`: a `SELECT` permission on a named `id` other than `FULL` is
  refused at `DEFINE` and `ALTER` (`IdFieldSelectPermission`, raised by
  `id_field_restrictions` in `exec/field_rules.rs`), because a permission on
  `id` alone would not cover the fields that read it. A component is never
  re-derived beneath a field guard: it reads `id` alone.
  The key range (`plan_key_range` in `exec/index/key_range.rs`,
  `Plan::KeyRangeScan`) keeps five properties.
  - It is a superset. Its ranges hold every record the predicate admits and the
    predicate, the `PermissionFilter` and the layers above the scan judge every
    row it yields; no conjunct is consumed. A range that is wider than it needs
    to be costs reads, never a disclosure.
  - It is not offered where the plan cannot vouch for the read. It is not planned
    under `RestrictedPrefixes::AssumeRestricted`, `VERSION`, a `WITH` hint,
    `MATCHES` or a KNN operator, and a conjunct that reads a field the caller may
    not read is withheld from it (`cond_touches`), as it is from an index.
  - Under a permissioned offer (`AccessInputs::permissioned`, set from
    `TableAnswers` in `exec/vm/ir_catalog.rs` wherever the caller's reads are
    checked and the table's select permission is not `FULL`) only literal
    bounds are pushed (`Classifier::bound` in `exec/index/key_range.rs`). A
    deferred bound is evaluated in the caller's frame when the scan opens, and
    a permission predicate is definer code. A deferred bound anywhere reads no
    row of the table, holds no nested statement, closure or index operator and
    has no effect (`Classifier::deferrable`), and an error in evaluating it
    widens the side it bounds rather than raising.
  - Every range `record_key_ranges` builds is intersected with the table's
    record range, and an empty or inverted range is dropped, so a bound the
    caller supplies cannot take the scan out of the table (section 14).
  - For a checked caller beneath a permission, `key`, `range` and `ranges` print
    `<permission>` (`ACCESS_PATH_ATTRS` in `exec/operators/explain.rs`), as the
    index, access and record id of any access path do, and the column of a
    component prints `<computed>` where its expression stands (`layer_attrs`
    in `exec/vm/ir_lower/explain.rs`, which renders a column evaluated in a
    computed frame, a component's included, as `<computed>` for a checked
    caller).
  Verified by: `a_key_range_reads_the_range_and_keeps_the_whole_predicate`
  (`exec/ir/phys/access.rs`), `every_range_lies_within_the_table`,
  `a_bound_its_component_cannot_hold_widens_rather_than_raising`
  (`exec/operators/scan/key_range.rs`),
  `a_component_is_not_rederived_beneath_a_restricted_field`
  (`exec/logical/resolve.rs`); end to end by
  `language/planner/id_components_differential_reads.surql` (the plan that may
  read a key range returns what `WITH NOINDEX` and an unkeyed twin return),
  `language/planner/id_components_differential_permissions.surql` (the same,
  for a record user whose permission reads the components),
  `language/planner/id_components_bounds_from_params_functions_and_closures.surql`,
  `language/planner/id_components_clock_bound.surql`,
  `language/statements/select/id_components_permissions.surql`,
  `language/statements/define/field/id_components_select_permission_refused.surql`,
  `language/planner/id_components_permission_bounds.surql` (a checked caller's
  `EXPLAIN` shows `key` and `range` as `<permission>`) and
  `language/planner/id_components_permission_bounds_for_another_user.surql`.
  These pin the rows a key range yields, not which bounds it takes: no test
  pins that a permissioned offer pushes literals alone, which deferred bounds
  are pushed, or the `<computed>` text of a component's column for a checked
  caller.

### Review Triggers

Flag for detailed review when changes touch:

- `Options::check_perms`, `Options::is_allowed`, or `perms` flag propagation
- `Permission` enum, its `Default` impl, or the `Permissions` struct
- Any new code path that calls `new_with_perms(false)` or `with_import(true)`
- `DEFINE EVENT`, `DEFINE TABLE ... AS`, or `DEFINE FIELD ... REFERENCE` processing
- `Auth`, `AuthLimit`, `Actor`, or role-checking methods
- `USE` statement handling in the executor
- `process_fields`, `DataInstr::Snapshot`, `WriteInstr::ShowImage`,
  `shown_view`, `Enforce`, or `materialise_fields_with_permissions`
  (the VM's write path and the views it shows a written record through), and
  `ComputedFieldGuard` or the VM's permission derivations (the computed fields a
  permission predicate reads)
- `resolve_with_field_state`, `check_permission_for_value`, `action_is_checked`, or
  `should_check_perms` (streaming runtime read path), or any new scan operator that fetches record contents
- `exec/logical/resolve.rs` (what the analyzer writes above a source, guard
  pruning, the whole-row read classification), `exec/logical/rules/` (any rule
  that moves an expression across a `PermissionFilter`, a layer or a join),
  `chain_offer` in `exec/planner/fold.rs`, `exec/operators/layer.rs`,
  `exec/operators/permission_filter.rs`, or `compiles_for_checked_caller` and
  the `EXPLAIN` redaction it drives
- `try_resolve_table_ctx`, `EnforcementMode`, or any planner path
  that builds a scan for a source whose `schema` is `Some` (the plan enforces above it)
- `purge_references` or cascade processing logic
- How `auth_enabled` is evaluated or propagated
- The `reduced` document mechanism or computed field handling
- `plan_key_range`, `AccessInputs::permissioned`, `record_key_ranges`, the
  `KeyRangeScan` EXPLAIN attributes in `ACCESS_PATH_ATTRS`, or the derivation of
  a record-id component (`KeyStructure::derived_fields`, a `key_part` definition,
  `id_field_restrictions`)

---

## 3. Execution Context (USE / LET / RESET)

**Files**: Executor, session management, `iam/reset.rs`, context resolution

### Invariants

- Every USE statement, RPC `use` call, and HTTP header-based context selection must
  verify the authenticated principal is authorized for the target namespace and
  database before the switch takes effect.
- Protected parameters (`$auth`, `$token`, `$session`, `$access`) must not be set,
  overwritten, or removed by LET, UNSET, RPC `set`, or RPC `unset`. Protection
  must apply symmetrically to both set and unset operations.
- Session expiry must be checked before executing any statement, processing any RPC
  method, and creating any HTTP request context. No partial execution after expiry.
- Once namespace, database, and session identity are resolved for a statement, they
  must not change in a way that affects already-authorized operations.
- HTTP `surreal-ns` and `surreal-db` headers must be validated against the
  principal's authorized scope.
- Each connection must have an independent session. Session identifiers must not
  collide or be reusable across connections.
- After a `reset`, the session must be in the most restrictive state. If auth is
  required, the reset session must not permit data operations until re-authentication.
- Namespace/database auto-creation via USE in non-strict mode must require the same
  authorization as explicit DEFINE NAMESPACE/DATABASE.
- The execution context must not be observable in a partially-mutated state by any
  concurrent or nested evaluation.

### Review Triggers

Flag when changes touch:

- `USE` statement gains new semantics or session manipulation
- `Session` struct gains new security-relevant fields
- `PROTECTED_PARAM_NAMES` list changes
- RPC methods that manipulate session state
- HTTP header-based context resolution logic
- Session lifecycle (creation, expiration, reset)
- Execution context freezing/unfreezing semantics

---

## 4. Data Write Operations

**Files**: Document processing pipeline (create, update, upsert, insert, delete,
relate), Key API, field evaluation, events, views, cascades

### Invariants

- All write operations must enforce the appropriate table-level permission (create,
  update, or delete) for the authenticated principal.
- Execution context (NS/DB/session) must be resolved at write initiation and cannot
  drift during the document processing pipeline, including retry paths.
- Field-level PERMISSIONS FOR create/update must be evaluated for every field on
  every write with the correct variant based on document state (new vs existing).
- A write permission predicate, table or field, that reads a COMPUTED field reads
  the value it computes to from the record being written, not `NONE` and not a
  value the statement assigned to its name (section 2).
- Errors from write operations must not disclose record contents, restricted field
  values, secrets, or internal schema details.
- Any execution triggered during write processing (VALUE, DEFAULT, ASSERT, event
  THEN, view computation, permission WHERE) must use an explicit authority context
  that does not exceed definer intent.
- UPSERT and INSERT operations that fall back from create to update must apply the
  correct permission kind at both table and field levels.
- RELATE must validate that the caller has sufficient access to reference endpoint
  records, or that the edge table's permission policy accounts for this.
- **Key API bodies must be treated as data, never as code.** Request bodies for CRUD
  operations must be interpreted strictly as data values, not as arbitrary SurrealQL
  expressions. Body content must not permit subqueries, function calls, or statement
  execution.
- Cascade operations (ON DELETE CASCADE/UNSET/CUSTOM) must either enforce the
  caller's permissions or require the cascade policy was defined by a sufficiently
  privileged definer.
- Batch write size, transaction duration, and mutation volume must be bounded.
- Event nesting depth must be enforced. View cascade depth and fan-out must be
  limited.
- Import mode (which bypasses field/event/view processing and the `ENFORCED` relation
  endpoint-existence check) must only be settable by administrative callers. It must not
  bypass any permission check: table-level `PERMISSIONS` still gate every imported write.
- **A field a write names that is also part of the record id is verified against the
  id, and a failure discloses only what the caller wrote.** A `id` field that is not the
  record the statement addresses is `IdMismatch` (`check_special_field` in
  `exec/operators/write/stages/data.rs`, run by `CheckDataFields`). On a table whose `id`
  names its components, a field named like a component is compared with the key's element
  and cut, and is never stored: a restatement that equals the element is accepted, and
  anything else (a different value, an `UNSET` or `NONE`, a write beneath the component, a
  compound assignment) is `IdComponentMismatch` (`settle_key_fields` in the same file, run by
  `DefaultRecordData`, as `DataInstr::SettleKeyFields`). `OPTION IMPORT` is held to the same
  check, because the import path skips the schema stage. Both errors carry the value the
  caller's data clause produced (`to_sql` of it) and never the key's element itself. That
  value is the caller's input, or computed by the caller's own expression over what the
  data clause reads (`SET city = city + 'x'` echoes the result), which is no more than any
  failing expression in the same clause discloses (`SET v = <int> city`). In an `UPDATE`
  they are raised after `CheckUpdatePermissions` and `CheckWhereCondition`, and in every
  create order after `CheckPermissionsQuickCreate`. An `UPSERT` that names a record its
  update permission refuses evaluates its data clause on the record reduced by the select
  permission and can raise either error before the refusal; the error then carries only
  what the caller wrote, since the reduced record holds none of the hidden fields. What a
  mismatch tells a caller is that the value they wrote is not the record's: for
  `IdMismatch`, of the record they named; for `IdComponentMismatch`, of a record their
  update permission and `WHERE` admitted, whose components the same `WHERE` already reads
  (a component has no field permission). A new error on this path must keep both
  properties: raised after the table's gates, and carrying nothing beyond what the
  caller's own data clause reads.
  Verified by: `the_special_fields_are_stamped_and_a_keyed_table_judges_the_key`
  (`exec/vm/ir_lower/stage.rs`, which pins that a keyed table's write lowers to
  `DataInstr::SettleKeyFields`), `merge_rpc_cannot_change_a_component_of_the_record_id`
  (`rpc/protocol.rs`), and end to end by
  `language/statements/update/id_components_immutable.surql`,
  `language/write/id_components_error_precedence.surql` and
  `language/write/id_components_import.surql`.

### Review Triggers

Flag when changes touch:

- Document processing pipeline order (permission check reordering)
- Execution authority semantics for VALUE/DEFAULT/ASSERT/EVENT/VIEW processing
- Cascade/reference behavior (ON DELETE strategies)
- Key API body parsing behavior
- Import mode access control
- New secondary processing triggers (post-write hooks)
- `settle_key_fields`, `check_special_field`, or any new error raised from
  `CheckDataFields` or `DefaultRecordData` that echoes a value

---

## 5. HTTP Transport

**Files**: `ntw/` (Axum router, middleware stack), CORS, WebSocket handlers,
GraphQL, error formatting

### Invariants

- CORS must not use wildcard origin when credentialed headers are in the allow-list.
- All request body content entering the Key API must be bound as a variable, never
  passed as an executable expression to the query engine.
- Each HTTP RPC POST request must operate on an isolated session instance. The
  handler must not write per-request session state into a shared slot.
- All WebSocket upgrade paths (SQL and RPC) must apply identical message-size,
  frame-size, and buffer limits. No WebSocket path may accept unbounded messages.
- Client-IP headers must only be trusted when the immediate TCP peer is within a
  configured set of trusted proxy addresses.
- GraphQL execution must enforce configurable maximum query depth, field count, and
  complexity score. Introspection must be gated.
- Error responses must not include raw internal error messages, file paths, storage
  engine details, or stack traces. Internal details must be logged server-side only.
- WebSocket upgrade handlers must validate Origin headers against the same policy
  used by the CORS layer.
- Server identification headers must be suppressed by default.
- Per-endpoint body size limits must be proportional to the endpoint's function.
- When the router is exposed for embedding, the full middleware stack (auth, CORS,
  compression, tracing) must be applied. Individual route modules being `pub` must
  be documented with security implications.

### Review Triggers

Flag when changes touch:

- CORS configuration (origin policy, allowed headers, allowed methods)
- HTTP or WebSocket endpoint routes (add/remove/modify)
- Any path that passes user-supplied content to the query engine
- Session storage in HTTP RPC, `set_session`, or session lifecycle
- WebSocket upgrade handlers, buffer-size or message-limit config
- `client_ip` module (header sources, proxy validation)
- GraphQL service, schema construction, or executor configuration
- The `/gql` GQL endpoint (`ntw/gql.rs`: route, body-size limit, and the
  `RouteTarget::Gql` / `ExperimentalTarget::Gql` capability gates)
- Error formatting or `ResponseError` implementation
- Authentication middleware (which endpoints are gated)
- Body-size limit constants or `RequestBodyLimitLayer` application
- Response header middleware (server identification, version)
- Router composition for embedders

---

## 6. RPC Transport

**Files**: `rpc/`, WebSocket handling, CBOR/JSON/FlatBuffers deserialization,
transaction management, notification routing

### Invariants

- CBOR deserialization must enforce a maximum recursion depth during both decoding
  and value conversion. Payloads exceeding the limit must be rejected before
  processing.
- The `begin` RPC method must cap maximum concurrent transactions per WebSocket
  connection. Unbounded transaction accumulation must not be possible.
- WebSocket upgrade must validate the Origin header against an allowlist.
- Default WebSocket message size limits must be conservative (consistent with HTTP
  RPC body size limits, not orders of magnitude larger).
- The same holds for the gRPC message size (`GRPC_MAX_MESSAGE_SIZE`), which bounds
  what one caller can make the server buffer for a single message and therefore
  defaults to the HTTP RPC body size rather than to a transport-specific figure.
- The gRPC message size the capability handshake advertises
  (`Limits.max_message_bytes`) must be the size the service enforces, in both
  directions. A client sizes its own decode and encode limits from what is
  advertised, so advertising more than is enforced converts a stated limit into a
  mid-body rejection the client can only see as a stream reset.
- Every gRPC message must be bounded on its *encoded* size before it is handed to
  tonic, measured on the protobuf rather than on what compression makes of it.
  tonic weighs its send limit against the compressed payload while the receiving
  peer applies its own limit to the decompressed one, so a message bounded after
  compression can still be one the peer refuses — a failure that surfaces there as
  a corrupt stream naming no size. Responses that grow with user data must be
  framed by encoded bytes (`frame_payload_budget`), not by a record count alone;
  a payload with no smaller form must be refused with the limit named. The budget
  must hold back headroom proportional to the limit (`transport_reserve`), because
  compression is applied unconditionally and expands data that does not compress —
  a payload bounded at exactly the limit is one the transport then refuses.
- The configured gRPC message size must be held within what the transport can
  represent (`GRPC_MAX_MESSAGE_CEILING`, the four-byte length prefix), on the
  client as well as the server. Advertising a size larger than that has every
  client size its buffers to a message the route can never carry.
- Memory threshold checks must be applied consistently across all transport entry
  points before full message body deserialization.
- Protected session parameter names must be normalized (lowercase, NFC Unicode,
  trim) before protection checks. The check must apply in all code paths that set
  session variables.
- Each HTTP RPC request must operate on an isolated session copy.
- A session rehydrated from durable storage must pass the same caller-principal
  verification (`verify_caller_for_session`) as an in-memory one; rehydration
  happens inside `get_session`, so no code path may reach a rehydrated session
  without going through the ownership gate.
- When durable sessions are enabled, dispatch must be serialized per client-named
  session id (one in-flight RPC per session id), otherwise concurrent mutations can
  persist out of order and resurrect overwritten auth state (e.g. an `invalidate`
  lost to a racing `signin`).
- Live query notification delivery must verify the target WebSocket still exists and
  the live query is still registered to that connection/session.
- A `LIVE SELECT` that commits must end up either registered for notification
  delivery or deleted from the datastore, on every transport including the
  streaming one. Where registration cannot happen — the session was detached
  mid-query, the execution failed as a whole and returned no results, or the
  client abandoned the result stream before it reached the registration — the
  committed rows must be deleted, never left registered to a torn-down
  authorization and never left behind for a cleanup path that cannot find them.
  A stream registers from the execution's completed results, as the buffered path
  does. A transport whose result stream can be dropped mid-execution has to track
  the ids as it frames them, since after the drop nothing else knows them.
- Messages in the wrong frame type for the negotiated format must be rejected before
  parsing.
- All open transactions must be explicitly cancelled on WebSocket disconnect.
- Streaming queries (`query_stream`) must be capped per WebSocket connection
  (`WEBSOCKET_MAX_CONCURRENT_STREAMS`): each in-flight stream holds an executing
  query — and whatever snapshot or transaction that entails — for as long as its
  client reads.
- A streaming execution future must never be dropped mid-flight. Stopping a stream
  is always the cooperative pair of tripping its cancel handle *and* closing its
  items channel; the driver then polls the execution to completion.
- Every frame send must be raced against both the connection canceller and the
  query's wall-clock deadline.
- A transport that streams must enforce a per-frame send timeout that always
  applies, independent of any operator-configured query timeout.
- The stream registry (`query_cancel`'s lookup) must be per-connection, like the
  session map. A client must never be able to reach another connection's streams.
- Streamed rows are provisional until their statement's `Finished` frame. A
  statement or stream failure must retract them by erroring on the terminal frame,
  never abandon them silently mid-protocol. A stream that was stopped rather than
  answered (cancelled, torn down, or abandoned because frames could no longer be
  delivered) must carry an error on its terminal `End`.
- An outcome already delivered to the client can never be retracted. When a later
  failure would contradict one, the whole stream fails.
- No transport may hold a `DashMap` guard (session map, transaction map, stream
  registry) across an `await`.
- A streaming execution runs against a **snapshot** of its session, taken when the
  query starts. Authorization teardown (`invalidate`, a token revocation, `detach`)
  therefore does not stop a stream already in flight: it succeeds immediately while
  the stream finishes under the authorization it started with. Revocation that must
  take effect immediately has to close the connection.

### Why the streaming rules are shaped this way

Rationale for the invariants above, for reviewers deciding whether a proposed
change preserves them. This is background, not additional requirements.

A streaming execution owns an open transaction, so dropping its future strands
that transaction rather than releasing it. A client that stops reading parks the
driver on a full outbound channel with the execution unpolled, which means its
timeout guard never fires either; racing every send against the canceller and
deadline is what keeps a slow or hostile reader from pinning resources
indefinitely.

The executor reports abandonment as success. Without the error on the terminal
`End`, a truncated answer is indistinguishable from a complete one to the client.

`DashMap` guards are blocking locks over a whole shard, and the awaits on these
paths are sends on client-paced channels. A guard held across one lets a single
connection park worker threads server-wide, including every other transport's.

The session snapshot exists because the buffered path holds the session read guard
for the whole execution, which is fine when execution is bounded by the server but
not when a stream lives as long as its client reads: holding it that long would
block every `use`, `set`, and `signin` behind a slow client. The exposure that
buys is bounded by the stream's own lifetime, which is why the always-on per-frame
send timeout above is load-bearing rather than a performance nicety.

### Review Triggers

Flag when changes touch:

- CBOR deserialization path, JSON parsing, or FlatBuffers decoding
- WebSocket serve/read/handle_message functions
- RPC dispatch table (new methods, modified signatures)
- The `gql` RPC method (GQL: must keep `allows_query_by_subject` and the
  `ExperimentalTarget::Gql` gate in `Datastore::parse_gql`)
- Transaction methods (begin/commit/cancel) or transaction map
- The streaming driver (`rpc/streaming.rs`), the stream registry, or the
  `query_stream`/`query_cancel` methods
- Notification routing or LiveQueries map structure
- HTTP RPC handler session management
- Durable session persistence (`Http::load_session`/`persist_session`/
  `forget_session`, `SessionLocks`, the `post_handler` dispatch guard)
- `check_protected_param` or `PROTECTED_PARAM_NAMES`
- WebSocket upgrade handler (Origin validation, protocol negotiation)
- Default values for message size, buffer size, or memory threshold
- New serialization formats

---

## 7. Live Query Subscriptions

**Files**: Live query creation, notification dispatch, KILL handling, cleanup

### Invariants

- KILL must verify the requesting principal owns the target subscription before
  deleting it. KILL must be scoped by namespace and database.
- Captured auth/session snapshots must be re-validated before notification delivery.
  Expired tokens and revoked permissions must suppress notifications.
- A subscriber's table `PERMISSIONS FOR select` clause is judged on the notified
  record with the COMPUTED fields it reads derived for the clause
  (`lower_subscription`, section 2). The notified record may carry computed
  values derived as the writer for the record's observers, which say nothing
  about what the subscriber's clause reads.
- Per-principal and per-database subscription limits must be enforced.
- Notifications must be delivered exclusively to the session that created the
  subscription. Routing must be resilient to disconnect/reconnect race conditions.
- Live query WHERE clauses must be side-effect free (no writes, deletes, event
  emissions, or external calls).
- Notification backpressure must not cause write transactions to block indefinitely.
- Orphaned subscriptions must be garbage collected.
- Notification delivery failures must be observable (logged, counted).

### Review Triggers

Flag when changes touch:

- KILL authorization or ownership semantics
- Auth/session snapshot validation logic
- Subscription limits or backpressure handling
- Node-level live query key structure
- Notification channel architecture
- Cleanup or garbage collection for subscriptions

---

## 8. Schema Definition (DEFINE / ALTER / REMOVE)

**Files**: Schema statement processing, catalog writes, expression validation

### Invariants

- All schema operations must enforce `is_allowed(Action::Edit, ResourceKind, Base)`
  before execution. Schema operations must require explicit namespace/database
  context.
- Executable expressions in schema definitions (VALUE, ASSERT, COMPUTED, DEFAULT,
  EVENT THEN, PERMISSIONS WHERE) must execute under bounded computation depth and
  explicit authority context. They must not escalate beyond the defining principal's
  privileges.
- PERMISSIONS expressions must be validated syntactically and for basic safety
  before persisting.
- Schema modifications must be fully transactional (commit or rollback, never
  partial state).
- REMOVE EXPUNGE must require elevated privileges.
- ALTER operations that weaken permissions on existing resources require the same
  scrutiny as DEFINE. ALTER is a partial patch and may subtly change security
  posture.
- ALTER FUNCTION, ALTER EVENT, and ALTER API can replace executable code and must be
  treated as equivalent to persistent code injection if authorization is insufficient.
- Import mode that bypasses duplicate checks must only be accessible through
  privileged operations.
- Async event MAXDEPTH and RETRY limits must prevent unbounded background execution.
- Changefeed retention must be bounded.
- Concurrent schema modifications on the same resource must be serialized.
- **Naming the components of a record id (`DEFINE FIELD id ON reading TYPE [city:
  string, time: datetime]`, `ALTER FIELD`) must not change what a definer-written
  expression reads, and its validation scan must be bounded.** A name that an
  index column, an index `COUNT` condition or a view over the table reads as a
  stored field is refused when the id takes it (`IdComponentReadByExisting`), and a
  name that an index, a view, a table permission, a field's clause or an event reads
  is refused when the id drops or renames it (`IdComponentInUse`), as is a name the
  `id` definition's own `ASSERT`, `DEFAULT` or permissions read when that definition
  drops or renames it; a field cannot take a component's name, nor
  the id the name of a field (`IdComponentClash`, `IdComponentShadowsField`), and
  `REMOVE` or `ALTER` of a component name is `IdComponentNotAField`. A component
  name must be a plain identifier (`IdComponentNameUnsupported`,
  `KeyStructure::is_plain_name`): the paths that evaluate a record's fields for its
  events, views, `RETURN` images and `VALUE` and `ASSERT` clauses key a field by
  its raw rendering, under which an escaped name would read `NONE`. A permission
  predicate that read `city` as a stored field and now reads the component, or read
  a component and now reads `NONE`, admits the wrong rows, so the check reads every
  clause that holds an expression (`first_reader` and `record_reads` in
  `ddl/key_scan.rs`) and fails closed: a clause that reads the record by a name it
  computes, hands the record whole to a function, method, closure or cast, or calls
  a user-defined, script, module or silo function where the record is the document
  it is evaluated against, may read any name and blocks the change
  (`IdComponentReadUnresolved`, `IdComponentReadByExistingUnresolved`), and so does
  a view that selects `*` and holds rows (`IdComponentsUnderWildcardView`). Function
  bodies are not read. A path through the document (`@.city`) reads the record;
  a part of a value that holds the record (`{ r: $this }.r.city`) may read any
  name; a field's `REFERENCE ON DELETE THEN` repair is read with the referencing
  record as `$this`. A bare name in a statement over another table is counted as
  a read of this table's field of that name: fail-closed, and the refusal then
  names the clause that holds the statement. The dependents check of
  `DEFINE FIELD id` and `REMOVE FIELD id` runs when the statement runs, as
  `ALTER FIELD`'s does (`FieldCheck::KeyComponents`, `FieldCheck::IdUnread` in
  `exec/vm/program.rs`), so rows a block, transaction or function body writes
  into a view before the change are seen. What the check does not see is
  review-only: a read of a component from another record, through a record link
  or a graph hop into the table or a statement over another table that reads
  this one, by a path or by a name passed as a string to a builtin; what a
  function body reads of a record handed to it as anything but the record or
  the document; and an index column spelled `id[i]`. A change that widens what a
  clause can read extends `record_reads`. The scan that validates the table's records
  (`validate_record_keys` in `ddl/key_scan.rs`) runs in the defining transaction
  under the definer's authority, reads each record's key and, where it could hold a
  component name, its body, pages by the fuel left and charges the meter per record,
  so it honours the statement's cancellation and deadline. Its refusals
  (`IdComponentsNonConforming`, `IdComponentsBodyConflict`) name the record id, the
  component and the remedy and never a stored field value.
  Verified by: `the_records_are_checked_only_against_a_changed_structure`,
  `a_view_that_selects_every_field_reads_every_name_once_it_holds_a_row` and the
  `record_reads` tests (`ddl/key_scan.rs`), and, for the checks
  `define_field`, `alter_field` and `remove_field`
  (`exec/vm/ir_lower/ddl/field.rs`) emit to run when the statement runs, end
  to end by
  `language/statements/define/field/id_components_wildcard_view_judged_when_the_statement_runs.surql`,
  `language/statements/define/field/id_components_rename_used_by_reference_repair.surql`,
  `language/statements/define/field/id_components_rename_used_through_function.surql`,
  `language/statements/define/field/id_components_wildcard_view_rename_and_remove_refused.surql`,
  `language/statements/define/field/id_components_rename_used_by_permission.surql`,
  `language/statements/define/field/id_components_rename_used_by_event.surql`,
  `language/statements/define/field/id_components_rename_used_by_view.surql` and
  `language/statements/define/field/id_components_clash_existing_index_on_body_field.surql`.

### Review Triggers

Flag when changes touch:

- New DEFINE, ALTER, or REMOVE statement types
- Expression types in VALUE/ASSERT/PERMISSIONS
- Execution authority semantics for schema-defined expressions
- Changefeed, index, or event behavior
- `ddl/key_scan.rs`, `define_field_statement_validate_key_components`, or any
  change to what `extract_computed_deps` or `record_reads` reports as a read of a
  name

---

## 9. Custom API Dispatch

**Files**: `ntw/api.rs`, API routing, middleware, handler execution

### Invariants

- Namespace and database from the URL path (`/api/:ns/:db/:endpoint`) must be
  validated against the session's authenticated context. Mismatches must be rejected.
- Path traversal sequences (`..`, `%2e%2e`, encoded slashes) must be rejected or
  normalized before routing.
- API handlers must execute under an explicit authority context (definer or caller)
  as specified in the API definition. Context must not drift.
- Request data ($request.body, $request.params, $request.query) must not be directly
  interpolated into query strings. Use parameterized queries.
- API errors must be sanitized before response.
- Custom API routing must be gated by capabilities configuration.
- Middleware functions must not be able to modify session authentication state or
  bypass permission evaluations.
- Fallback handlers must be subject to the same permission checks as method-specific
  handlers.

### Review Triggers

Flag when changes touch:

- `ntw/api.rs` routing, path matching, or parameter extraction
- API handler execution context or authority model
- API middleware chain composition or ordering
- Request data binding into query parameters
- API error response formatting
- Capabilities checks for custom API routing
- Fallback handler registration or dispatch

---

## 10. Import / Export

**Files**: Import endpoint, export endpoint, streaming parser, CLI import

### Invariants

- Import must enforce per-statement authorization, not just endpoint-level checks.
  Statements requiring higher privileges (DEFINE USER, DEFINE ACCESS) must be denied
  if the principal lacks those specific permissions.
- OPTION IMPORT (which bypasses field-level schema enforcement) must require
  explicit elevated authorization, and its activation must be logged.
- Import body size and per-statement resource limits must be enforced. The streaming
  parser's buffer growth must be bounded by a configurable maximum.
- USE NS/DB statements within import payloads must be validated against the
  session's authorization scope.
- Import payloads must not be able to create persistent privilege escalation vectors
  (users, access grants, functions at higher privilege than the importer).
- CLI import connecting to a local datastore must not silently disable authentication.
- Export must respect table and field permissions. Secrets must be redacted.
- A dump of a table whose `id` names its components carries the `id` as written and
  no definition of a component, which is not a stored field. Importing into such a
  table is held to the same check as any write (section 4): a component field in
  the payload that differs from the key is refused under `OPTION IMPORT`, and an
  equal one is cut. A view row's key is taken to agree with its group values, which
  view maintenance guarantees and `OPTION IMPORT` does not; a key range over an
  aggregated view can miss a hand-forged row, never return one the predicate does
  not admit.

### Review Triggers

Flag when changes touch:

- Import endpoint or streaming parser buffer handling
- `OPTION IMPORT` authorization or flag propagation
- Per-statement authorization during import execution
- Export permission filtering or secret redaction
- CLI import authentication or local datastore connection logic
- USE statement validation within import payloads

---

## 11. Function Execution

**Files**: `fnc/`, `exec/planner/` (plan-time evaluation), scripting runtime
(QuickJS), HTTP client, capabilities, Surrealism/WASM

### Invariants

- JavaScript scripting must be gated by capabilities and denied by default.
- Every function invocation must be checked against the capabilities allow/deny list.
  Deny rules override allow rules.
- Plan-time evaluation is an invocation. Whenever the planner runs a function while
  building a plan — constant folding in `exec/planner/util/literals.rs`, plan-time
  `FROM` source resolution — the capability must be checked before the call, because
  the folded result replaces the expression and leaves no physical expression to check
  at evaluation. A denied call is left unfolded rather than rejected during planning,
  so it fails at evaluation with the same `FunctionNotAllowed` an unfolded call would
  raise and plan construction stays infallible for `EXPLAIN`.
- Plan-time elimination is also an invocation, of everything it discards. A fold that
  drops a subtree — `x INSIDE []`, an AND/OR short-circuit — must keep any subtree whose
  evaluation could dispatch, since no physical expression is built for what it removes.
  `evaluation_is_inert` in `exec/planner/util/literals.rs` is a fail-closed whitelist:
  unrecognised shapes are preserved, and a function call is never inert regardless of
  whether it is currently permitted. Two things outside the expression decide what a
  bare field read costs, so both are supplied by the caller: what the `FROM` sources can
  put in a row (a record id dereferences, a scalar does not), and whether reading a row
  of that table evaluates anything — a `COMPUTED` field or a field-level `SELECT`
  permission runs before the condition observes the row.
- Outbound HTTP requests must be checked against network allow/deny lists. Both the
  initial URL and all DNS-resolved IP addresses must be validated. The DNS filtering
  resolver must validate every resolution result.
- Redirect targets must be validated against capabilities.
- **WASM target exception (Cloudflare Worker / Durable Object):** on `wasm32`
  there is no synchronous DNS resolver (`ToSocketAddrs` is unsupported) and the
  `fetch` runtime follows redirects transparently with no policy hook, so the
  DNS-resolved-IP validation and per-hop redirect validation above cannot be
  performed — only the initial request URL's host is validated against
  allow/deny. This is an accepted risk, valid only while the sole wasm deployment
  target is a Cloudflare Worker, whose egress cannot reach loopback/link-local/RFC1918
  addresses (the runtime enforces the IP-level restriction). Any change that widens
  the wasm target set invalidates the acceptance and must restore per-hop validation
  first. TODO(no-issue-yet): a web-sys `redirect: "manual"` client would let wasm
  validate each hop; file and link a tracking issue when this is scheduled.
- JavaScript runtime must enforce memory, stack, and time limits per invocation.
  Each invocation must create a new runtime (no state persistence).
- Crypto compare operations must enforce cost allowance bounds.
- String-producing functions must enforce generation allocation limits.
- Method-style function invocations (e.g., `$array.map(...)`) must be correctly
  canonicalized for capability checking.
- WASM/Surrealism capabilities must be validated before instantiation. The guest's
  WASI context is built in one place, `wasi_context.rs` (`build`), and that
  construction must stay deny-by-default: no host environment is inherited, the
  filesystem is preopened read-only or not at all, and sockets are refused outright
  when the net allowlist is empty. Any builder call that inherits host state is a
  secret-exposure finding.
- Name resolution is filtered rather than switched off. `allow_ip_name_lookup` is on,
  and `surrealism/runtime/src/dns.rs` shadows `wasi:sockets/ip-name-lookup` with a
  resolver that checks the name against the module's `NetPolicy` before querying, then
  drops every special-use address the answer carries and records the rest against the
  rule whose name produced them, so the later connect is recognised only for that rule
  and only for a bounded time. Two properties must hold: the shim binds the interface
  name read off the component's own imports — a literal version would miss a guest
  built against a different WASI release and fall through to wasmtime's unfiltered
  resolver, silently — and the operator's `deny_net` reaches this path, not only the
  SurrealQL one, or a guest socket becomes the way out that the deny list does not
  cover.
- A cached Surrealism `Runtime` is stateful: it pools controllers whose WASM linear
  memory (heap, statics) survives between invocations, so unlike the JavaScript
  runtime it is not rebuilt per invocation. Every `SurrealismCacheKey` variant
  (`surrealism/cache.rs`) must therefore be scoped by namespace and database. A key
  that identifies a module only by where its bytes come from — for silo packages,
  organisation/package/version — lets two databases share one runtime, and
  module-local state written under one tenant becomes readable by another.
- A pooled controller must not carry a guest's open connection across callers. Linear
  memory is preserved deliberately; a socket is not, because one left in the store's
  `ResourceTable` stays reachable from the next caller's query through the handle
  index the guest kept. `Runtime::release_controller` retires a controller that ends
  its invocation holding a socket it did not start with. Sockets specifically, not
  table occupancy: WASI pushes the stdio streams and the instance network into that
  table lazily and never drops them, so counting entries would retire every module
  that prints.
- A `transaction` handle is lent only to a function whose manifest entry declares one,
  and revoked on every path out of `invoke`. The manifest is user-supplied and is no
  evidence about the component beside it; the property rests on the manifest being the
  *only* thing consulted, so a component whose body calls `transaction.sql` has to
  declare the parameter to be given a handle, and a manifest that hides the parameter
  also withholds it. That makes "declared none, so it cannot query" a property of the
  host rather than of the generated dispatch.
- Host calls made during one invocation are ordered by what each one carries, not by
  what the module claims about itself: a statement that may write runs alone, a read
  runs alongside other reads. They share the caller's single transaction, whose
  savepoints form a stack with no handle, so a second writer entering before the first
  has left unwinds the wrong one. The decision comes off the parsed expression
  (`Expr::read_only`, which treats every opaque call as writing) because the manifest's
  `writeable` flag is the module's own claim and is not enforced. That flag is still
  what `access_mode` reports, and so still what licenses core's per-row fan-out to
  evaluate two *invocations* side by side — which only refusing the write closes
  (`Options::no_write`).
- The `eval::surql` / `eval::gql` functions (`fnc/eval.rs`) evaluate a runtime
  query string in the caller's transaction. They must remain gated by **all** of:
  the function-family capability (enforced by the engine before dispatch), the
  arbitrary-query subject gate (`Capabilities::allows_query` — an `eval` call *is*
  an arbitrary query, so it cannot bypass the front-door gate from inside a
  `DEFINE FUNCTION` / `DEFINE API` body), and the dedicated eval subject gate
  (`Capabilities::allows_eval_query`, `EvalQueryTarget`), which defaults to denied
  for every subject even under `Capabilities::all()` / `--allow-all`. `eval::gql`
  additionally inherits the `gql` experimental gate via
  `gql::parse_with_capabilities`.
- The eval subject must be derived from the *current execution* auth. This is
  safe because `Auth::new_limited` (the auth-limiting applied to user-defined
  function bodies) never raises the subject class — a record/guest caller is
  returned unchanged and a system caller is only ever narrowed. A record-scoped
  user invoking an owner-defined function that calls `eval` is therefore still
  seen as `record` and remains denied.
- eval must reject transaction-control and session-level top-level statements
  (BEGIN/CANCEL/COMMIT/USE/LIVE/KILL/OPTION/SHOW/access), bound recursion, and
  honour `PROTECTED_PARAM_NAMES` and the names the system binds
  (`surrealdb_cnf::is_refused_let_name`) for caller bindings. The recursion bound is
  expression-nesting depth charged against `max_computation_depth`, carried in as a
  budget so a query reached through nested `eval` calls cannot reset it; there is no
  separate eval-only depth constant.
- `eval::surql` / `eval::gql` text and embedded JavaScript's `surrealdb.query()` /
  `surrealdb.value()` run as a nested query (`exec::compiles::nested_query`):
  compiled by the executor as a query of its own in the caller's transaction, with
  the caller's session, auth and `Options` (so `perms` and `no_write` carry over).
  It is the one sanctioned place a running program has code compiled for it; it
  must never recompile the running program, and a script reaches it only when
  scripting is allowed. An `eval` program is served from the compile cache
  (`exec::vm::serve`) to every principal of the frame's class, so it must read
  `$auth`, `$session`, `$token` and `$access` when it runs and never fold them, and
  it is served only while the catalog it read is unchanged.

### Review Triggers

Flag when changes touch:

- Function dispatch table (`fnc/mod.rs`)
- JavaScript runtime engine
- WASM runtime, host functions, or WASI configuration
- Scripting or network capability defaults
- HTTP client pooling, DNS resolution, or redirect handling
- New experimental features with function-level gating
- Authority model for stored function execution
- The `eval::*` functions, the `EvalQueryTarget` capability, or how the eval
  subject gates derive the subject from execution auth
- Resource limit defaults or enforcement

---

## 12. File / Bucket Storage

**Files**: `buc/`, `BucketController`, object store backends

### Invariants

- Permission must be checked before every file operation via `check_permission()`
  with the correct `BucketOperation`.
- Permission expressions must not enable access beyond the caller's session
  authority, even when evaluated with permissions disabled for recursion avoidance.
- Object keys must be validated to prevent path traversal. Storage backends must
  enforce prefix isolation.
- Backend error messages must be sanitized. Backend URLs, access keys, and internal
  paths must not appear in client-facing errors.
- Readonly buckets must reject all write operations before any I/O.
- File upload size must be bounded.
- List results must have a maximum count enforced by the system.
- Guest and record users must not be able to list bucket contents.
- Backend URLs in DEFINE BUCKET must be validated against an allowlist.
- Remote object store operations must have bounded timeouts.

### Review Triggers

Flag when changes touch:

- `buc/` module, `BucketController`, or bucket permission checking
- Object key validation or path construction in storage backends
- `DEFINE BUCKET` processing or backend URL handling
- Bucket permission expression evaluation
- File upload size enforcement or list result limits
- Error formatting in bucket operation responses

---

## 13. Metadata & Observability

**Files**: INFO statements, health/status/version endpoints, metrics

### Invariants

- Secrets must be redacted in metadata responses (INFO FOR DB, INFO FOR NS, etc.).
- Observability endpoints must be restricted to authorized principals.
- System information endpoints must not reveal internal architecture details to
  unauthenticated callers.
- Identification headers must be disabled by default.

### Review Triggers

Flag when changes touch:

- INFO statement output formatting or field selection
- Health, status, or version endpoint responses
- Metrics endpoint access control or exported labels
- Secret redaction logic in metadata paths
- Server identification header configuration

---

## 14. Storage Layer (KVS)

**Files**: `kvs/`, key encoding/decoding, transaction handling, datastore
implementations

### Invariants

- All storage keys must be scoped by namespace and database. Key encoding must
  ensure that a key constructed for one namespace/database cannot collide with or
  be interpreted as belonging to a different namespace/database.
- Transaction isolation must be maintained across concurrent operations. Read-after-
  write consistency within a transaction must hold regardless of backend.
- Storage backend errors must be propagated faithfully. Backend-specific error details
  (paths, connection strings, internal state) must be stripped before reaching client-
  facing error paths.
- Key range scans must be bounded. Prefix scans must not inadvertently cross
  namespace or database boundaries due to key encoding ordering. A key-range scan
  over a table's record keys builds its ranges from values the caller supplies
  (`record_key_ranges` in `exec/operators/scan/key_range.rs`): each bound is
  encoded as an element of an array key after the table's own record-key prefix,
  every range is intersected with the table's record range, an empty or inverted
  range is dropped, and a bound the component cannot hold widens a side inside the
  table. No bound reaches the bytes before the table's prefix.
  Verified by: `every_range_lies_within_the_table` and
  `inverted_or_empty_bounds_read_nothing` (`exec/operators/scan/key_range.rs`).
- Backend credentials and connection configuration must not be logged or included in
  error messages.
- Encryption-at-rest configuration must not be alterable by non-root principals.
- Storage operations triggered by user queries must respect the same authorization
  context as the originating query.

### Review Triggers

Flag when changes touch:

- Key encoding or decoding logic (`kvs/` key types)
- Transaction lifecycle (begin, commit, cancel, retry)
- Datastore initialization or backend selection
- Key prefix construction or range scan boundaries (including `record_key_ranges`)
- Storage-layer error types or error conversion
- New storage backends or backend configuration
- Encryption or credential handling in storage config

---

## 15. Query Parser and Front-Ends

**Files**: `surrealdb/parser-next` (the SurrealQL lexer and parser),
`surrealdb/parser-next/lower` (its lowering into the `sql` AST, the semantic
checks made there, and the entry points every caller uses),
`surrealdb/parse-common` (diagnostics and error rendering, shared with the GQL
front-end), `surrealdb/sql` and `surrealdb/expr` (AST and expression
construction), the `surrealdb/gql` crate (GQL front-end), and
`surrealdb/pgsql` and `surrealdb/mysql` (PostgreSQL and MySQL front-ends). The
same-named modules under `core/src/` are thin
remnants of the pre-crate-split layout and are not where the grammar lives.

### Invariants

- The parser must enforce maximum nesting depth for all recursive grammar
  productions. Inputs exceeding the limit must be rejected with a bounded-cost error.
- Parser memory consumption must be bounded relative to input size. Buffer growth
  during parsing must have a configurable upper limit.
- The parser must not execute or evaluate any expressions during parsing. Parse output
  must be an inert AST that is only evaluated in the execution phase under proper
  authorization context.
- Parsing untrusted input must not cause panics. All parser entry points must return
  `Result` types.
- Parser-produced AST nodes must faithfully represent the input. Ambiguous syntax
  must be resolved in a way that does not silently change query semantics.
- Wire protocol listeners (`pgwire`, `mywire`) must be capability-gated via
  `RouteTarget::Postgres` and `RouteTarget::MySql`.

### Review Triggers

Flag when changes touch:

- Parser recursion limits or stack depth configuration
- New grammar productions or expression types
- Parser buffer management or allocation strategy
- Entry points that accept untrusted input strings
- AST node construction for security-relevant statements (DEFINE, OPTION, USE)
- The GQL front-end (`gql/` parsing and lowering, the `gql` RPC method,
  the `/gql` HTTP route)
- The PostgreSQL and MySQL front-ends (`pgsql/`, `mysql/`, `pgwire/`, `mywire/`)
- Capability routing for wire protocols (`RouteTarget::Postgres`, `RouteTarget::MySql`)

---

## Cross-Cutting Concerns

### Confused Deputy Prevention

Any code path where system-internal execution (`perms=false`) processes
user-influenced expressions is a potential confused deputy. This includes:

- Event THEN clauses
- Materialized view maintenance (DEFINE TABLE ... AS)
- Reference cascade operations (ON DELETE CASCADE/UNSET/CUSTOM)
- Permission expression evaluation (WHERE clause in PERMISSIONS)
- VALUE/DEFAULT/ASSERT/COMPUTED field expressions

All such paths must have an explicit, documented authority context. The `perms=false`
scope must be minimized and must not extend to evaluating arbitrary user inputs.

Flag when changes touch `new_with_perms(false)`, authority context propagation in
event/view/cascade processing, or any path that evaluates user-defined expressions
during system-internal operations.

### Error Sanitization

All error responses visible to clients must be sanitized. Review any change to error
formatting, error types, or error propagation paths. Watch for:

- Internal paths or filenames in error messages
- Record contents or field values in error details
- Schema structure details in authorization errors
- Stack traces or panic messages
- Storage engine implementation details
- Backend URLs or credentials

Flag when changes touch error type definitions, `Display`/`ResponseError` impls,
error conversion (`From`/`Into`) chains, or any code path that formats errors for
client responses.

### Resource Bounding

Every operation that processes user-controlled input must have bounded resource
consumption. Watch for:

- Unbounded allocations (parser buffers, batch sizes, list results)
- Missing timeouts (remote calls, long-running queries)
- Amplification patterns (single input triggering multiplicative work)
- Recursion without depth limits
- Connection/transaction accumulation without caps

Flag when changes add new allocation paths for user-controlled data, remove or
increase existing limits, add recursive processing, or introduce new external
call sites without timeouts.

### Import Mode Safety

The `import` flag bypasses field validation, event processing, live query
notifications, and view computation. Any change involving:

- Setting or propagating the `import` flag
- The `OPTION IMPORT` authorization gate
- Document processing pipeline skip conditions

...must be reviewed for authorization correctness and flag lifetime scoping.

Flag when changes touch `with_import`, `OPTION IMPORT` handling, or any document
processing pipeline condition that checks the import flag.

### Namespace/Database Isolation

Every operation must be verified against the principal's authorized namespace and
database. Watch for:

- Context switches without re-authorization
- HTTP headers overriding token-based scope
- Import payloads containing USE statements
- Cross-boundary key structures (keys not scoped by NS/DB)
- Cache entries shared across security boundaries

Flag when changes touch key encoding/decoding, cache key construction, USE statement
handling, HTTP header-based context resolution, or any data structure that is
indexed or partitioned by namespace/database.

---

# Appendices

Surfaces that are experimental, feature-gated, or otherwise not part of the
default deployment. They carry the same invariant weight as the numbered
sections while enabled, but are kept apart so the numbering of the core
sections stays stable as experimental surfaces come and go.

## Appendix A. GQL (experimental ISO GQL surface)

**Files**: `gql/` (lexer, parser, `lower/`), `expr/match_plan.rs`,
the binding-table operators — `exec/operators/graph/` (`expand.rs`, `endpoint.rs`,
`path_expand.rs`, `distinct_edges.rs`), `exec/operators/join/hash_join.rs`,
`exec/operators/{bind.rs, distinct.rs}`, and the FieldState-aware fetch helper
`exec/operators/scan/fetch.rs` —
`exec/planner/match_plan.rs`, `kvs/ds.rs` (`parse_gql`, reached from
`statements_for` when the request names `Dialect::Gql`), `rpc/protocol.rs`
(`gql` method, `QueryForm::Text` carrying `Dialect::Gql`),
`ntw/gql.rs` (`/gql` route), `pgwire/conn.rs` (`select_dialect`, the gate for
`options=-c dialect=gql` and `SET dialect = 'gql'`, and the `run_gql` /
`execute_query` paths those select), `fnc/eval.rs` (`eval::gql`)

GQL is a second query language lowered to a `MatchPlan` IR and executed by
the streaming engine. The lowering constructs only a single top-level
`TopLevelStatement::Plan` holding a `LogicalOp::Match` (never an `Expr`, and never a
`sql::Ast`). It supports reads
(`MATCH … RETURN`) and the four ISO data-modifying statements (`INSERT`, `SET`,
`REMOVE`, `DELETE`), which the `MatchPlan` carries as trailing mutation stages
executed through the native document pipeline. Its security posture rests on five
invariants.

### Invariants

- **Binding-fetch equivalence (the central data-leak boundary).** Every binding
  row's contents must be exactly what a `SELECT` on that table would return for
  the same caller. All fetched node/edge bindings (Expand targets and edges,
  EndpointBind nodes, PathExpand intermediate and terminal nodes/edges) must go
  through the FieldState-aware helper in `exec/operators/scan/fetch.rs`
  (`resolve_with_field_state`), which applies, in order: table-level SELECT
  permission (deny ⇒ record dropped, neither existence nor contents leak),
  computed-field evaluation, and field-level SELECT permission (unreadable fields
  cut). The bare `scan::common::resolve_record_batch` must **not** be substituted
  — it applies only the table-level permission and skips the field-level
  machinery, which would leak restricted fields into binding rows.
- **Layered gate chain.** Reaching the GQL executor requires, in order: the
  `gql` cargo feature compiled in; the capability gating the entry surface the
  request arrived on (below); `allows_query_by_subject` for the session's auth
  subject; and a valid, non-anonymous session under the namespace/database
  authorization context. Removing or reordering any layer is a regression.

  There is no single "GQL off" capability. Each entry surface carries its own
  gate, and an operator disabling GQL must deny each one they expose:

  | Entry surface | Gate |
  | --- | --- |
  | `POST /gql` (`ntw/gql.rs`) | `RouteTarget::Gql` |
  | Postgres `options=-c dialect=gql` / `SET dialect = 'gql'` (`pgwire/conn.rs`) | `RouteTarget::Gql`, on top of `RouteTarget::Postgres` for the port |
  | RPC `gql` method (WebSocket, `/rpc`, gRPC) | `MethodTarget` for `gql`, on top of the transport's own route |
  | `eval::gql(…)` from any SurrealQL (`fnc/eval.rs`) | `allows_function_name("eval::gql")`, `allows_query`, and `allows_eval_query` — the last denies every subject by default, including under `--allow-all` |

  On the Postgres port that gate is applied in two places, and both are
  load-bearing. At dialect *selection*, `Connection::select_dialect` is the sole
  assignment to the connection's dialect, so a denied dialect can never be
  entered from any selection point; a connect-time selection is refused fatally
  and an in-session `SET` with `42501`, rather than silently leaving the
  connection in SurrealQL — which would answer the client's GQL with parse
  errors instead of the reason. And *per query*, `check_current_dialect` re-runs
  the same gate in both `simple_query` and `execute_query`, because capabilities
  are swapped atomically at runtime (`Datastore::set_capabilities`) and a
  long-lived connection must not outlive a tightening. That matches the other
  transports, where the GQL gate is evaluated per request (`/gql`) or per
  dispatch (the `gql` RPC method's `MethodTarget`). The per-query check runs
  after the `SET` interception, so a connection refused by it can still switch
  back to a dialect it may use rather than being stranded.

  `RouteTarget::Postgres` is deliberately *not* re-checked per query: it gates
  the transport, and stays connection-scoped like `RouteTarget::Rpc` at
  WebSocket upgrade.

  A new selection point that assigns the dialect directly, or an execution path
  that skips the per-query re-check, is a regression.

  One gap in the chain is known and unclosed: GQL is **not** gated on
  `ExperimentalTarget::Gql` — neither `Datastore::parse_gql` nor
  `gql::parse_with_capabilities` checks it, and a test pins that GQL stays
  reachable without it.
- **Resource bounds.** The path-expansion and join operators must enforce row-count
  ceilings: `SURREAL_GQL_MAX_PATH_ROWS` (default 1,000,000) bounds live+emitted
  rows in `PathExpand` **per source row** (the counter resets for each input row,
  so it caps the genuinely dangerous single-source combinatorial explosion and
  keeps live DFS memory at `O(longest_path × fan-out)`; the aggregate output is
  bounded by `N_source_rows × SURREAL_GQL_MAX_PATH_ROWS`, where `N_source_rows` is
  itself bounded by the anchor scan's cardinality — size the knob with that
  per-source ceiling in mind), `SURREAL_GQL_MAX_JOIN_BUILD_ROWS` (default
  1,000,000) bounds the `HashJoin` build side (and the `Distinct` seen-set), and
  `SURREAL_GQL_MAX_OUTPUT_ROWS` (default 1,000,000) bounds the cumulative rows
  *emitted* by `HashJoin` and single-hop `Expand` — a distinct axis, because a
  `Cross` product or high-fan-out join emits far more rows than either side
  holds while the build set stays small. Exceeding any must abort the query with
  an error naming the knob — quantifiers, multi-pattern joins, cartesian (no
  shared variable) joins, and variable-length paths are amplification vectors and
  must stay bounded.
- **Cancellation.** The match operators do heavy work without pulling fresh
  upstream batches (`HashJoin` fully drains its build side then fans out the
  probe; `PathExpand` runs a per-source DFS; `Expand` scans a vertex's whole
  adjacency), so upstream cancellation cannot propagate through them. Each must
  poll `ctx.cancellation()` in its hot loops (the streaming buffer/monitor
  wrappers inject none) or a long-running MATCH ignores client disconnect / query
  timeout — a DoS. Dropping a poll is a regression.
- **VM-only execution & no serialization.** The `MatchPlan` must run only under the
  bytecode VM, as a program of binding steps (`vm::plan_front_end`).
  No `Expr` variant can carry a plan, so there is no path by which one could be
  evaluated as an expression; `eval::gql` (`exec/function/builtin/eval.rs`)
  compiles the plan its text lowers to as a nested query of its own, through
  `exec::vm::serve::front_end`, and a plan the engine refuses fails. A `MatchPlan` must
  never enter a `sql::Ast`, the catalog, or `Revisioned` serialization:
  `TopLevelStatement::Plan` (`dbs/top_level.rs`) carries the plan apart from
  the surface `sql::TopLevelExpr`, so no conversion of a plan to surface syntax
  exists, and `ToSql` renders the plan directly. A
  mutation-bearing plan reports `read_only() == false` (`LogicalOp::read_only`
  via `MatchPlan::has_mutations`), so the executor opens a write transaction.
- **Mutation safety.** GQL mutations must execute through the write pipeline as a synthetic
  `Create`/`Update`/`Delete`/`Relate` statement per resolved record id, planned and run by
  `exec/operators/mutate.rs::run_write` — never by writing the KV store directly, so record/field
  permissions, field validation, events, indexes, references, and live-query notifications all
  apply exactly as for a native mutation. The mutation operators (`exec/operators/mutate.rs`)
  must stay pipeline breakers: they fully drain their input before any write (avoiding the
  Halloween problem of a still-open scan re-observing a freshly written row), then apply per
  row in textual order (row-scoped, last-write-wins on a fan-out). A `RETURN` after a mutation
  must re-bind only the permission-filtered
  post-mutation image (the value the native mutation returns), never raw fields the caller
  cannot read. `NODETACH DELETE` (the ISO default) must probe for connected edges and error if
  any exist (native `DELETE` always cascades = `DETACH`); that integrity probe (`has_connected_edges`)
  must **peek the record's graph-key range directly off the transaction** — the same
  permission-independent check the native cascade uses (`doc::purge::purge_edges`), NOT a
  SELECT/idiom read — so it reflects the record's actual adjacency, not the caller's SELECT
  visibility. Otherwise a record-scoped user who cannot see the incident edges would slip past the
  guard and trigger a silent cascade. Per-property `SET a.p` and the `SET a = {…}` surface both
  reject the reserved keys (`id`, edge `in`/`out`), since the native write path silently re-stamps
  them. Label mutations are rejected (one table per record).
- **Read-after-write ordering.** A `MATCH`/`OPTIONAL` clause that follows a mutation must observe
  that mutation's writes (read-your-writes within the one transaction), and must never race them.
  In `HashJoin`, the writing side must drain before the reading side is *constructed*. Which side
  writes depends on the fold, and both directions are load-bearing:
  `fold_mandatory` puts the mutating accumulator on the **build** side, so the probe's construction
  is deferred until the build drains; `fold_optional`'s left-join puts it on the **probe** side, so
  the probe is pre-drained (executing its writes) before the build is constructed, and its buffered
  rows replayed. A pure-read join constructs the probe eagerly and the build
  only once the probe has yielded a non-empty batch, so an empty probe leaves
  the build side unbuilt and unpolled.

#### Why construction order is the control point

The streaming engine's read-only buffering (`spawn_buffered` in `buffer.rs`) opens scan cursors
eagerly at stream-construction time for pipeline parallelism, and every read-only operator buffers
its child, so that eager read is spread throughout a subtree rather than concentrated at the join.
It therefore cannot be defeated by buffering choices at the join level alone, which is why the
requirement is phrased around when the reading side is *constructed* rather than when it is polled.
Reverting either direction to eager construction silently reintroduces stale reads: a
`MATCH`/`OPTIONAL` after `SET`/`DELETE`/`INSERT` seeing pre-write data. Pure-read joins are exempt
because both sides share one snapshot, so overlap is both safe and faster.

### Review Triggers

Flag when changes touch:

- `exec/operators/scan/fetch.rs` or any binding-fetch call site (a switch to
  `resolve_record_batch`, or a new fetch path that bypasses FieldState, is a
  field-permission leak)
- The gate chain: `ExperimentalTarget::Gql`, `RouteTarget::Gql`, the `gql`
  `MethodTarget`, `allows_query_by_subject` in the `gql` RPC method, the
  `eval::gql` gates in `fnc/eval.rs`, or the experimental check in
  `Datastore::parse_gql`
- `pgwire/conn.rs` dialect gating: `Connection::select_dialect` must stay the only
  assignment to the connection's dialect, and both selection points
  (`dialect_from_options` at startup, `SET dialect` in session) must route
  through it — a direct assignment reopens GQL on the Postgres port under a
  denied `gql` capability. `check_current_dialect` must stay on both query
  paths (`simple_query`, `execute_query`), or a connection that selected GQL
  before the capability was denied keeps serving it
- A new GQL entry surface, which needs its own row in the gate table above
- `SURREAL_GQL_MAX_PATH_ROWS` / `SURREAL_GQL_MAX_JOIN_BUILD_ROWS` /
  `SURREAL_GQL_MAX_OUTPUT_ROWS` defaults or their enforcement in `PathExpand` /
  `HashJoin` / `Expand` / `Distinct`, or removal of a `ctx.cancellation()` poll
  from any match operator's hot loop
- The `HashJoin` residual (`on`) predicate or `fold_optional`'s routing of a
  correlated OPTIONAL-block predicate into it — a correlated predicate that
  becomes a post-join `Filter` instead silently turns an OPTIONAL into an inner
  join (drops rows that must be null-filled)
- `HashJoin::execute`'s build-vs-probe construction order — both the deferred-probe
  branch (build mutates ⇒ construct the probe only after the build drains) and the
  pre-drain branch (probe mutates ⇒ drain the probe, executing its writes, before
  constructing the build) are load-bearing for read-after-write correctness in
  interleaved `MATCH … SET/DELETE/INSERT … MATCH/OPTIONAL …` programs (see Mutation
  safety, above); `fold_mandatory` vs `fold_optional` decide which side mutates,
  and the peek branch (neither side mutates ⇒ the build is constructed only after
  the probe's first row)
- The lowering's surface (any path that would let GQL construct anything other
  than a single top-level `TopLevelStatement::Plan` holding a `LogicalOp::Match`)
- The mutation pipeline: `exec/operators/mutate.rs` (a write that bypasses
  `run_write`, a dropped pipeline-breaker drain, a re-bound RETURN image that
  is not permission-filtered, or a `NODETACH` path that skips the edge probe),
  `gql/lower/mutation.rs` (a dropped rejection — label mutation, group/path or
  unbound target, reserved `id`/`in`/`out` keys), and `LogicalOp::read_only`'s
  `Match` arm in `expr/logical/mod.rs` (a mutation plan that reports read-only
  would run in a read transaction)
- The executor's `TopLevelStatement::Plan` arm (`run_logical_plan`) and
  `eval::gql`'s nested query, or any path that gives a plan a `sql::TopLevelExpr`
  form

---

## Appendix B: Wire Protocols (pgwire and mywire)

### Invariants

- **Capability Gating**: The wire protocol listeners (`pgwire` on port 5432 / `--pg-bind`, `mywire` on port 3306 / `--mysql-bind`) are capability-gated via `RouteTarget::Postgres` and `RouteTarget::MySql`. If disabled by capabilities, incoming connections are rejected immediately with an authentication error packet (`28000` / 1045) before any query processing.
- **Readiness Gating**: Incoming connections check datastore readiness (`ready.load(Ordering::SeqCst)`). If the database is still starting up, connections are rejected with code `08S01`.
- **Authentication**:
  - `pgwire` authenticates clients via SASL SCRAM-SHA-256 or cleartext password over TLS/loopback against `surrealdb-iam` basic auth.
  - `mywire` authenticates clients using the `mysql_clear_password` authentication path (via `AuthSwitchRequest`) over TLS or loopback against `surrealdb-iam` basic auth. Plaintext authentication over non-loopback insecure connections is rejected.
  - Guest access (empty username) is permitted only if `allows_query_by_subject` permits unauthenticated queries. If a username is provided, credentials must match a defined user; no guest fallbacks or hard-coded password guessing (`""`, `"root"`) are permitted.
  - `USER()` / `CURRENT_USER()` in MySQL wire protocol reports the authenticated user and remote IP (`user@ip`), never a hardcoded `"root@localhost"`.
- **Database Selection**:
  - Authentication must complete successfully before database selection (`process_use` / `set_current_db`) runs.
  - Errors from `process_use` must never be swallowed; if a database or namespace does not exist or access is denied, an error is returned to the client.
- **Connection and Resource Limits**: Each listener enforces maximum connection concurrency via a semaphore (`MAX_CONNECTIONS = 1024`), cleanly rejecting connections when capacity is exceeded.
- **Named Dialect Selection & Fail-Closed Execution**:
  - `pgwire` defaults to `Dialect::PgSql`. It does not sniff query syntax or fall back to SurrealQL on parse failure.
  - `mywire` defaults to `Dialect::MySql`. It does not fall back to SurrealQL on parse failure.
  - Dialect switching is explicit via `SET dialect = '<name>'` (or `options=-c dialect=<name>` on startup for pgwire). Accepted dialects are strictly named (`pgsql` / `postgres`, `mysql`, `gql`, `surrealql`).
  - Cross-dialect queries and polyglot fallbacks are forbidden: queries execute strictly in the connection's active dialect and fail closed with a syntax error if invalid for that dialect.
  - Dialect switching is gated by capability (`RouteTarget::Postgres`, `RouteTarget::MySql`, `RouteTarget::Gql`). If a dialect's capability is denied, switching to it is rejected with an insufficient privilege error (`42501` / `28000`).
  - In `pgwire`, `check_current_dialect` is evaluated per query, ensuring runtime capability revocation immediately halts execution in that dialect without stranding the connection.

