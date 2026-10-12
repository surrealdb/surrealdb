# ADR 0008: Named record-id components

**Status:** Accepted · **Scope:** `surrealdb/parser-next/`, `surrealdb/expr/src/parser/`,
`surrealdb/expr/src/val/key_encoding.rs`, `surrealdb/catalog/src/schema/key.rs`,
`surrealdb/core/src/ddl/`, `surrealdb/core/src/exec/` (`index/key_range.rs`,
`operators/scan/key_range.rs`, `vm/`, `logical/`)

**Relates to:** ADR 0003 (the cached physical plan), ADR 0004 and 0007 (the VM), ADR 0006 (clock
reads keep their evaluation).

## Context

A table of readings keyed by what identifies a reading, `reading:['London', d'2025-01-01T10:00:00Z']`,
is stored in key order, so the readings of one city in one window are one contiguous stretch of the
table's keys. An array record id's elements are addressable as `id[0]` and `id[1]`, and nothing
reads a predicate over them as a key range: such a table is scanned whole, or is given a second
copy of its key as an index.

Turning `WHERE city = 'London' AND time > $from` into a range over the keys needs three things the
language lacks without this decision: a name for each element, so that the predicate can be written
and read as fields; a statement of what each element holds, so that the stored encoding's order is
known to be the order of the values; and a rule that cannot return a wrong row when the encoding's
order and the values' order disagree. Datetimes are the case that decides the last one: a record key
holds a datetime as its RFC 3339 text, in which `00:00:00.500Z` sorts before `00:00:00Z`.

## Decision

**The components are named in the type of the `id` field.** `DEFINE FIELD id ON reading TYPE
[city: string, time: datetime]` and `ALTER FIELD id ... TYPE [...]` give each element of the array
id a name. The names are accepted only as the whole `TYPE` of the field `id` (the bare field,
escaped or not); in every other position `[city: string]` stays an ordinary tuple kind and keeps its
parse error. A name is an identifier or an escaped identifier, every element is named, names are
separated by commas, each is unique, and none is `id`, `in` or `out`. DDL further holds every name
to a plain identifier (`KeyStructure::is_plain_name`): ASCII letters, digits and underscores, not
starting with a digit, and neither `NaN` nor `Infinity`; a keyword is plain. The paths that evaluate
a record's fields for its events, foreign views, `RETURN` images and `VALUE` and `ASSERT` clauses key
each field by its raw rendering (`Idiom::to_raw_string`), which escapes every other name, so a
component named `` `the city` `` would be invisible to them. A plain name is exactly one whose raw
rendering is the name itself.

**Names are syntax and live in the stored kind text.** `surrealdb_types::Kind` and `expr::Kind` carry
no names. The text of the type, held in the `KindText` of `StoredFieldDefinition.field_kind`, does,
so the stored struct keeps its revision and neither `revision.lock`, the `kvs/compat` fixtures nor
the VM codec descriptors change. The statements carry the names beside the kind
(`DefineFieldStatement.key_names`, `AlterFieldStatement.key_names`), and `set_type` is the only
writer of both, so the two cannot disagree. INFO, export and `to_sql_overwrite` render the names
from the one function that turns a stored definition into a statement
(`FieldDefinition::to_runtime_definition`).

**One runtime structure describes a key.** `catalog::KeyStructure` holds a key's arity and, per
component, its position, name, declared type, `ComponentKind` and `ComponentOrder`. It is built from
the id field's type or from an aggregated view, is runtime-only, and is never stored. A definition's
structure is acted on only while it still describes the definition's type
(`FieldDefinition::live_key`). `KeyStructure::from_id_field` is strict and is used by DDL alone.
`from_stored_id_field` is lenient and is used for every catalog read: a component type outside the
allow-list reads as `ComponentKind::Unknown` (a table with such a component plans no key range), and
text whose names are invalid reads as no structure. Reading a table never fails on its key
structure, so an allow-list that narrows later disables an optimisation and bricks nothing.

**Components are derived fields, appended at the one place field definitions are built.**
`fields_from_stored` appends, directly after `id`, one virtual definition per name: read-only,
computed from `id[i]`, with a full `select` permission and `key_part = Some(i)`. A virtual definition
has no stored form (`put_tb_field` refuses one and `to_stored` asserts against it), so every
consumer of a field list decides what a `key_part` definition means to it, and a consumer that
misses one sees a read-only computed field and fails loudly where a separate resolution path would
silently read `NONE`:

- consumers that act on definitions skip them: INFO, export, the GraphQL input types (outputs keep
  them), the `INLINE` count, the mismatched-type check and the cycle graph;
- gates that exist only for speed treat a component as reading `id` alone, which every decode sets
  from the key (`fields_read_as_stored`, `scan_decode_fields`, `enforcement_is_total`);
- evaluation order derives them first and gives them no dependency edge (`field_eval_order`);
- the binder derives a component in a layer column framed by `EvalFrame::key_part()`, which `EXPLAIN`
  prints as `@key`. A component is never re-derived beneath a field guard, because its body reads
  `id` alone and the field `SELECT` permission that could cut `id` is refused (see below).

A table whose id names nothing gets the same `Arc` of definitions back, with no key column, rule or
instruction added for it.

**Predicates over components are read as predicates over `id[i]`, in two places.** In the logical
plan, the `layers` rule judges a `Filter` conjunct that reads a `key_part` column as the read of
`id[i]` (`substitute_key_parts`), so the conjunct passes the layer chain and reaches the access
path; projection pruning treats a key column as reading `id` alone, and a layer nothing reads is
dropped. In the analyzers, `plan_key_range` and the B-tree column matchers map a component name and
`id[i]` to one spelling through `KeyStructure::canonical_column`. The second place is the one that
decides: a mutation's `WHERE` never passes through the logical rule. A bare component name is never
read as a stored field of the table.

**A key range is an access path whose residual is always kept.** `plan_key_range`, a pure function
of the key structure, the conjuncts, the ordering and whether the offer is permissioned, returns the
longest run of positions pinned by an equality (or by an `IN` over a written list, the product of the
list sizes at most `MAX_KEY_RANGES`, 32) followed by an optional range on the next component. With
no pinned leading component and no range on the first one it returns nothing: the table is scanned,
and no skip scan is attempted. The ranges are a superset of the rows the predicate admits, and the
predicate stays above the scan and judges every row (`residual: yes` in `EXPLAIN`). Correctness
therefore does not depend on any subtlety of the encoding: a key that does not conform, written by
the race noted below, can at worst be missed by a range and is never returned wrongly.

- A bound is a literal the component can hold (`ComponentKind::refusal` is `None`) or a deferred
  scalar that reads no row, holds no nested statement, closure or index operator, reads no stored
  record and has an empty `EffectSet`. A deferred bound is evaluated every time the scan opens,
  never hoisted, so a bound that varies per outer row is per open. A bound that evaluates to a
  wrong kind, to `NONE` or to an error widens its side, or ends the pinned run at its position; it
  never raises and never empties the range.
- `NONE`, `NULL`, regexes, `!=`, `NOT`, `IS NONE`, `?=`, `*=`, the containment operators, a
  whole-array parameter in an `IN` and anything `NONE` could satisfy are never bounds.
- Under `VERSION` the structure the catalog holds now is not applied: the keys at a past stamp may
  have been written under another structure, or none, and were never validated against this one. No
  key range is planned, a record-id literal is read as written (`t:[5.0]` is not read as `t:[5]`),
  and a scan is not taken to deliver `ORDER BY id`. Nor is a key range planned with any `WITH` hint, with `MATCHES` or a KNN operator, or under
  `AssumeRestricted`. A conjunct that reads a field the caller may not read is not offered.
- Under a permissioned offer only literal bounds are pushed: a deferred bound would be evaluated in
  the caller's frame, and a permission predicate is definer code.
- Against an index the ranking is: a unique index pinned on every column, then an index pinning
  strictly more equality columns than the run, then the key range; with a `LIMIT`, the candidate
  that delivers the order wins ties (`IndexCandidate::outranks_key_range`).
- `AccessPath::KeyRange` maps to `Plan::KeyRangeScan`, which lowers to `Source::Scan` with
  `range: Some(ScanRange)`; every other scan has `range: None`. `UPDATE` and `DELETE` read their
  rows through the same path.
- `record_key_ranges` turns the shape and the evaluated bounds into byte ranges: the encoded pinned
  prefix, one range per combination of `IN` values (sorted in the scan's direction and
  deduplicated), every range clamped to the table's record range, every empty or inverted range
  dropped, and overlapping ranges are merged. The host reads range by range.

**Order is delivered only where the encoding orders exactly.** String, int, uuid, duration and bool
components are `ComponentOrder::Exact`: byte order is value order, proven by the property suite
`key_order` (`surrealdb/expr/src/val/key_encoding.rs`). A scan whose `ORDER BY` reads the positions
right after the pinned run, all in one direction, delivers it exactly, reads backwards for `DESC`,
and the `Sort` is removed. Every pinned position must hold one value on the rows the residual
admits, so an `IN` in the run, or a string component pinned by a deferred value (which may be a
regex that `=` matches against many strings), claims no order. A record component is `EqualityOnly`: it
pins, it does not bound or order.

**A datetime component is `CoarseSecond`, and the key encoding is not changed.** A datetime bound
widens to the 19-byte whole-second prefix of its key text (`YYYY-MM-DDTHH:MM:SS`, years 0000 to 9999
only; anything else is not a bound), the lower side and the upper side alike. An equality pins the
exact element, because equal datetimes have equal text. An `ORDER BY` that reaches a datetime component keeps its
`Sort`. Under a `LIMIT`, `CoarseCutoffSort` gives the scan the sort's threshold (`CoarseCutoff`),
and the scan stops before the first record whose datetime lies in a second past the last kept row's
(`CoarseGate`), so the page a time-descending `LIMIT` needs is read from the end of the range.
`ORDER BY id` is delivered by a scan only when every component is exact
(`KeyStructure::all_exactly_ordered`), and a table scan of a table whose id names a datetime
component never delivers it (`Plan::TableScan.id_order`). A sort key that is exactly `id[i]` reads
the element by evaluating it, not by field-path extraction, which yields `NONE` for a record id.

**An int component holds only `Number::Int`.** A record key encodes a number as its decimal buffer
followed by a kind tag, so `5`, `5.0` and `5dec` are three different keys, and a prefix that ends
in the int tag does not reach the other two. Admitting only `Int` makes an int equality exact and
an int range a range of ints. A write normalises a float or a decimal with an integral value to
`Int` (`Number::as_int_lossless`) and refuses every other number. The planner reads a literal
`5.0` or `5dec` against an int component as `5`, rounds a fractional range bound inwards, and
leaves an equality with a fractional value, a non-finite value or a value beyond `i64` unpushed.

**Components are read-only: they are written through the id.**

- A create takes its key from the first source that applies: an explicit id (the target, or `id` in
  the data), every component written as a field, then the `DEFAULT` of the field `id`; otherwise
  `IdComponentMissing` names the first absent component in position order. When every component is
  written the `DEFAULT` is not evaluated. No random id is generated for a keyed table. Each element
  is coerced to its declared type and held to what the component stores (`IdComponentCoerce`),
  and a key of the wrong length is `IdComponentArity`.
- A component is never stored. `DataInstr::SettleKeyFields`, emitted only for tables whose id names
  components, after `StampRecordFields` in every order that has a `DefaultRecordData` stage,
  compares each component-named field of the post-image with the key and cuts it. A restatement that
  equals the key's element (after the element's own coercion) is accepted; anything else, an `UNSET`
  or `NONE` of a component, a write beneath one and a compound assignment, is `IdComponentMismatch`,
  which echoes only the value the caller wrote. A stored copy that equals the key is cut by the
  record's next write, and `OPTION IMPORT` is held to the same rule, because the import path skips
  the schema stage and the instruction cuts the field itself.
- Observers see the components: events (`$before`, `$after`), live queries, changefeeds, `RETURN`
  forms and foreign views, which makes a keyed table with a view an observed table.

**The names are defined only against what exists.** DDL (and the lowering that mirrors it, so both
statement routes refuse alike) checks, in order:

- plain names: a component name that is not a plain identifier is refused
  (`IdComponentNameUnsupported`), before the type at the same position;
- the allow-list: string, int, datetime, uuid, duration, bool, and `record` or `record<t, ...>`.
  Float, decimal, number, geometry, object, bytes, arrays and sets, `option`, `any`, literal kinds
  and unions are refused (`IdComponentUnsupportedKind`), because their encoded order is not their
  value order or is not defined;
- relation tables and views (`IdComponentsOnRelation`, `IdComponentsOnView`), and the conversion of
  a table with named components into either (`IdComponentsTableConversion`);
- a field-level `SELECT` permission on `id` other than `FULL` (`IdFieldSelectPermission`): the
  components read as fields the table's permissions govern, and a permission on `id` alone would not
  cover them;
- clashes, compared on the root name (`city`, `city.x` and `city[*]` are all `city`): a field cannot
  take a component's name and the id cannot take an existing field's (`IdComponentClash`,
  `IdComponentShadowsField`); REMOVE or ALTER of a component name is `IdComponentNotAField`;
- dependents, found by `record_reads` (`ddl/key_scan.rs`) over the stored clauses, a field's
  `REFERENCE ON DELETE THEN` repair among them. A clause reads the names it spells: a bare field
  (counted inside a nested statement over another table too, which fails closed and then names a
  read of the other table's field), a static path or destructure through `$this`, `$self`,
  `$parent` or the document `@` (`$parent.city`, `@.city`, `$this.{city}`), in an event also
  through `$after`, `$before`, `$value` and `$input`, and a literal `type::field('city')`. It may read any name when, where the record is
  the document of its evaluation (outside a nested statement that does not target the record), it
  calls a user-defined, script, module or silo function, whose body is not read, or calls
  `type::field`/`type::fields` with a path it computes; and wherever the record is reachable, when
  it reads the record through a name it computes (`$this['ci' + 'ty']`) or hands the record whole
  to a function, a method, a closure or a cast (`fn::city_of($this)`, `object::keys($after)`),
  other than a builtin that only judges its type or reads its id (`type::is_*`, `record::id`,
  `record::tb`, `record::table`, `meta::id`, `meta::tb`), or reads a part of a value that holds the
  record (`{ r: $this }.r.city`). Comparing the record, copying it and walking a graph from it read
  no name. A view whose field list selects `*` reads every name
  through the rows it holds, which keep the names they were written under. Within one definition
  a name it spells is reported first, then (for a view) its `*`, then a read of any name; the
  definitions are judged in the order indexes, views, table permissions, fields, events. Adding a
  name that an index column, an index `COUNT` condition or a foreign view already reads as a
  stored field is `IdComponentReadByExisting`, or `IdComponentReadByExistingUnresolved` when the
  definition may read any name; removing or renaming a name that an index, a foreign view, a
  table permission, a field clause or an event reads is `IdComponentInUse`, or
  `IdComponentReadUnresolved` when it may read any name. In both directions a view over the table
  that selects `*` and holds at least one row is `IdComponentsUnderWildcardView`: its rows are not
  rewritten when the names change, so it is removed, the record id changed and the view defined
  again, in one transaction, which fills it again from the source. An empty view, an aggregate
  view and a view that selects named fields are not refused for their `*`. Field, permission and
  event readers are not checked when a name is added: from then on they read the component. The
  `id` definition being written is held to the same rule for its own `ASSERT`, `DEFAULT` and
  permissions: a name the new structure drops or renames is refused there
  too (`IdComponentInUse`, or `IdComponentReadUnresolved` for a clause that may read any name,
  naming "the ASSERT of the field `id`", "the DEFAULT of the field `id`" or
  "the PERMISSIONS clause of the field `id`"), since the merged `ALTER` keeps those clauses and they would read nothing. A name the new
  structure only moves still resolves, by name, in that definition. Retyping under the same names
  is allowed. `REMOVE FIELD IF EXISTS` of a component name is `IdComponentNotAField`, not a no-op:
  the component is a field of the table, which only a change of the `id`'s type removes.

**The existing records are validated in the defining transaction.** When a `DEFINE FIELD`, `DEFINE
FIELD OVERWRITE` or `ALTER FIELD` gives `id` a structure that differs from the stored one,
`validate_record_keys` scans the table once: every key must conform to the structure
(`IdComponentsNonConforming`), and no body may store a field under a component's name with a value
other than the component's (`IdComponentsBodyConflict`). An equal copy is allowed. The scan reads in
pages sized to the fuel left and charges the meter per record, so it honours cancellation and the
deadline. It is skipped when the stored structure is equal to the new one, when `IF NOT EXISTS` hits
an existing definition and in the speculative run. The first failure names the record, the
component and the remedy: copy the records into a table that has the new record id. A record whose
key holds a datetime outside the years 0000 to 9999 is stored as text that does not decode as a
datetime, so its record id cannot be read back by any statement (a limitation of the key decoder
that predates this decision); the scan refuses the table with a message that says so and names no
record.

**Operational notes.** The scan validates the records the defining transaction sees. A writer
whose statement was planned against the catalog before the definition committed does not see the
structure, and can still commit records whose keys do not conform, on any backend. The same writer
can add a row under the old record id to a view that selects `*`, one the check found empty or one
defined again after the change; refusing every such view whatever it holds would not close this,
since the remedy defines the view again and races the same way. Removing the view and defining it
again refills it from the table. Such a record is
never returned wrongly, because the residual predicate above every key range and the key decoder
judge it, but a key range can miss it. Define the names while no writer creates ids that would not
conform. Defining the same structure again does not re-check: `DEFINE FIELD OVERWRITE` or `ALTER
FIELD` with a structure equal to the stored one skips the scan. A missed record shows as a
difference between a query's answer with `WITH NOINDEX`, under which no key range is planned, and
its answer without it. Removing the names and defining them again in one transaction scans every
record again; while an index, a view, a permission, an event or a field clause reads a name, by
spelling it or by a read `record_reads` cannot resolve to names, or a view that selects `*` holds a
row, that removal is refused until the reader is removed, and the error names it. The check reads
the definitions of the table and of the views over it, and nothing else: a read of a component
from another record, through a record link or a graph hop into the table (`prev.city` where
`prev` is a `record<reading>`, `id.city`) or a statement over another table that reads this one,
by a path or by a name passed as a string to a builtin (`object::get(prev.*, 'city')`), is not
found, and goes on reading `NONE` after the name is removed; nor is what a function body reads of
a record handed to it as anything but the record or the document (`fn::f(prev)`). The dependents
of `DEFINE FIELD id`, `ALTER FIELD id` and `REMOVE FIELD id` are judged when the statement runs,
against the catalog and the rows of the views as the statements before it in the same block,
transaction or function body leave them. A `VALUE`
or `ASSERT` clause that reads the whole record (`VALUE $this`, `ASSERT fn::check($this)`) sees at
write time only the components that a field clause of the table reads by name, as it sees only
those `COMPUTED` fields of any table (`field_eval_order` in `doc/document.rs`).

**An index may hold a component.** `DEFINE INDEX by_time ON reading FIELDS time` is accepted on
`SCHEMALESS` and `SCHEMAFULL` tables, alone or beside stored columns; a path beneath a component is
refused. The column is read as `id[i]` on every write, build and replay (the replay's initial scan
splices the record id into the decoded body). A datetime component's index entries order by the whole
second only, so a range over one widens to whole seconds, the scan is never an exact answer (its
residual is kept and an index `COUNT` with a range on it is declined) and it delivers no order across
that column nor an `id` order. That widening is one function, `derived_datetime_index_needs_widening`,
and one module of consequences (`exec/index/derived_columns.rs`), which a fixed-width datetime
entry encoding removes together. An int component is an ordinary exact column.

**Aggregated views get the same rule.** The rows of `DEFINE TABLE v AS SELECT ... GROUP BY a, b` are
keyed `v:[a, b]`. `table_key` gives such a view a structure of origin `AggregateView` whose
components are addressed by the names the groups are projected under and are of unknown order, and
gives none for `GROUP ALL` or for a view table that defines a field whose root is a group alias.
Only literal bounds are pushed: equality and `IN` over a string, uuid, duration, bool, datetime or
scalar-key record, and ranges over the kinds a key orders exactly; a number is never a bound, because
equal numbers of different kinds do not share a key. A datetime range is pushed only with both sides
(a datetime outside the years 0000 to 9999 keys as signed text that sorts before every four-digit
year). A view claims no order. A view row's key is taken to agree with its group values: view
maintenance writes only agreeing rows, and a row forged through `OPTION IMPORT` may be missed by a
range.

**Clock reads keep ADR 0006's evaluation.** A `time::now()` in a `SELECT`'s `WHERE` is one value for
the statement and is pushed like any literal. In a mutation's `WHERE` it is evaluated per row and is
never a bound, so `DELETE reading WHERE time < time::now() - 30d` reads the range its pinned prefix
selects (the whole table when none is pinned) and filters. A deferred bound holding a clock, entropy,
I/O or write effect is never pushed. The retention form that reaches a range is
`LET $cut = time::now() - 30d; DELETE reading WHERE city = $city AND time < $cut;`: the bound is a
value when the statement is planned. A mutation reads a secondary index, on any table, only for a
literal bound, so an index on the time component serves a retention `DELETE` whose cutoff is written
as a literal and not one bound by `$cut`. The two positions are pinned by
`planner/id_components_clock_bound.surql` and `planner/id_components_mutation_where.surql`.

**Nothing persisted changes.** Record keys, index entries, bodies, changefeeds and view formats are
as they were: components are never stored. A 3.x database opens unchanged on a release that has this
decision, because no stored text holds names and `fields_from_stored` returns the same `Arc` for a
table with none. A 3.x binary cannot parse a named type, so a named `id` is not defined while a 3.x
node is live (`doc/UPGRADING.md`). Compiled units are in memory and keyed by `ISA_VERSION`, which
names the instruction set that holds `SettleKeyFields`, `ValidateRecordKeys` and the `range` of
`Source::Scan`.

**To admit another component kind** it joins `ComponentKind::classify`, gets a `refusal` arm that
accepts exactly the values its encoding was proven for, a `ComponentOrder` (`Exact` only where the
`key_order` suite shows that byte order is value order for every operator, `CoarseSecond`-style
widening only where the widened range is shown to contain every key the predicate admits, otherwise
`EqualityOnly`), and arms in the scan's `equality_element` and `side_element`. A kind that fails the
proof is not admitted, or is `EqualityOnly`.

## Consequences

- **A predicate over the leading components of an array id is a range read.** `reading` read by
  city and window touches that stretch of keys and no others, with no second copy of the key; a
  retention delete costs the rows it removes when its bound is a value and its prefix is pinned,
  or when an index holds the time component.
- **The key range cannot be the enforcement point.** It is an access path under the table's
  `PermissionFilter` and the layers, its ranges are clamped to the table, its bounds under a
  permissioned offer are literals, and a checked caller's `EXPLAIN` shows `key`, `range` and
  `ranges` as `<permission>` (`SECURITY_GUIDE.md`, section 2).
- **A component is a field the table's permission governs and no field permission governs.** A
  caller that may read a row reads every component of its key.
- **Tables without named components pay nothing per row, and keep their results and plans, with
  four exceptions.** A key column, a rule, an instruction or a plan node exists only where `key` is
  `Some`. The exceptions are: planning a conditioned or ordered read of any table reads its
  definition once more from the cached catalog to find a key structure; an `ORDER BY id[i]` whose
  path has no effect orders by the element on every table; `DEFINE TABLE ... TYPE RELATION`,
  `DEFINE TABLE ... AS SELECT` and `ALTER TABLE` check the stored id for names, which is a catalog
  read at definition time; and an aggregated view plans key ranges, which changes its plans and, for
  an `UPDATE` or `DELETE` of a view whose `WHERE` on a group alias selects no stored row, answers
  `[]` where a scan raised the refusal that a view is read-only.
- **The field list has a second kind of member.** Code that iterates definitions and writes them
  back, or acts on every definition, handles `key_part` explicitly.
- **Equal numbers do not interleave in a key.** Int-only components make `5.0` in an int position
  an error at the write and at the defining scan. A 3.x table can hold `5.0` in an int position
  whether or not its id is typed (3.3.0 keeps `['x', 5f]` under `TYPE [string, int]`), so naming
  such a table is refused; the recipe in `doc/UPGRADING.md` copies its records into a table whose
  id names its components, which converts each key as it is written.
- **A mismatched write has an oracle.** `IdComponentMismatch` tells the caller that the value they
  wrote differs from the key of a record the statement reached, after the table's update permission
  and the `WHERE` admitted it. It is the class of `IdMismatch` and echoes only the caller's value.
- **What the decision does not do.** It does not scan a table whose leading component is
  unconstrained (a skip scan); the answer is a different key order or an index on the component. It
  does not change the order of `ORDER BY id` over tuple ids that no field names. It does not
  validate keys for reads at a past `VERSION`, so it does not plan ranges there. It does not make a
  float an int: a float component cannot be a key component at all. The defining scan is synchronous
  inside one transaction; a table too large for that is converted by copying it.
