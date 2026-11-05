# surrealdb-sqlplan

SQL planner, binder, and bound IR for SurrealDB.

This crate provides:
- Name resolution and scopes (`QueryScope`, `ScopeStack`).
- Dialect policies (`DialectPolicy`, `NamePolicy`, `NullOrderPolicy`).
- Unified bound IR (`BoundSelect`, `BoundExpr`, `BoundTableRef`, `BoundTarget`, etc.).
- The binder skeleton for PostgreSQL and MySQL ASTs.
- `BinderCatalog` abstraction for table schema and column resolution.
- Lowering from Bound IR to SurrealDB `LogicalOp` execution plans.

## Namespaces, databases, schemas and tables

A connection selects one SurrealDB namespace and database: the PostgreSQL startup `database` as
`namespace/database`, or `options=-c namespace=<ns>` with a database name. A namespace is the level
above a database. It is the tenant a connection lives in and has no PostgreSQL counterpart; it is
not a schema.

A SurrealDB database is one PostgreSQL schema, `public`:

- `public.t` and `t` name the table `t`.
- `pg_namespace` lists `pg_catalog`, `public`, `information_schema` and `pg_toast`.
  `information_schema.tables`, `pg_class` and the other catalog views list every table of the
  selected database under `public`.
- `s.t` with any other schema `s` names the table whose name is `s.t`. No schema `s` exists as an
  object: `CREATE SCHEMA s` creates nothing, and `DROP SCHEMA s` is `42P01`.
- `db.public.t` is accepted only when `db` is the selected database.

`DROP SCHEMA public CASCADE` removes every table of the selected database, each as `REMOVE TABLE`
does: a view before the tables it reads, the statement answering once and succeeding or failing as
a whole, and the role that removing the tables one by one needs. The sequences that the serial and
auto-increment columns of those tables draw from go with them, so a table created again numbers
its rows from the start. Without `CASCADE` it is refused
(`0A000`) while the database holds a table. A session that may not view the database's tables sees
none, so the statement removes nothing for it. The schemas the front end synthesises (`pg_catalog`,
`information_schema` and `pg_toast`) cannot be dropped (`0A000`).
Tables defined by earlier statements of the same request are removed only when the database already
held a table before the request, because the catalog lists the staged tables only on top of tables
it lists.

## The `id` column

In both SQL dialects the `id` column of a base table is the table's primary key: the key of the
stored record id. Reading it (projecting it, in `RETURNING`, in the echo of a write, and through `*`
over a single base table) yields the key as the integer, string, uuid, array or object it is, and
comparing, ordering, grouping or computing over it uses the key. The PostgreSQL parser folds an
unquoted `ID` to `id` and keeps a quoted `"ID"`, which reads as another column than the key in
both reads and writes. In both dialects, `t.*` over the one
relation of a query, a table or a derived table, reads what `*` reads, and over a base table is
followed by the key as `id`. A `t.*` of a table whose columns are not known, in a query of several
tables, is refused: the rows of a join keep the fields of the tables after the first under internal
names. `*` over a join lists the stored rows as they are. A PostgreSQL `t.*` is read only as an
item of the select list of a `SELECT`, which includes the query of an `INSERT`; a view over it is
refused.

A top-level conjunct of a `WHERE` predicate that is `id = <operand>` or `id IN (<operands>)`, where
the `id` is that of the table read and an operand is an integer or string literal, a parameter, or
a column or expression, compares the stored record id with the record the operand names
(`type::record(<table>, <operand>)`), plainly. A NULL, absent or empty operand names no record. Any
other comparison, including the same one under `NOT` or `OR`, in a projection or for the `id` of a
joined table, keeps SQL's three-valued handling of NULL. Compatible stored ids compare directly;
other comparisons extract their keys as needed.

The engine reads a record directly, rather than scanning the table, for a literal key in a
`SELECT`, and for a literal or a parameter key in an `UPDATE` or `DELETE`. A parameter key in a
`SELECT` is compared correctly but read by a scan: the executor resolves a front end's parameters
only after it has chosen the access path. An `IN` of several keys reads the table unless it has an
index on its `id`.

Two `id` columns of the same table are compared as stored record ids; the `id` of a different
table is compared by key. A column or expression compared with the `id` of a base table (in a join,
a predicate, an `IN` list or a projection) may hold the key of the row, as a foreign key written
through SQL does, or a record link to it, as one written as a record id does: it is matched against
the stored `id` by the record `type::record(<table>, <value>)` names, which is the record of the key
for a plain key and for a link alike. A literal or a parameter is a key. A literal that is the key
of a record (an integer, a non-empty string) names that record, and a number with an integral value
that fits 64 bits (`3.0`) names the record of the integer key it equals, as `id = 3.0` in a `WHERE`
clause finds it. A literal that is no key (a number with a fraction, the empty string, `NULL`) names
no record and matches nothing. Comparing the `id` of a base table this way leaves the other side of
a join as an expression, so the engine reads that table by a hash of its rows rather than by an
index on the column when the `id` is on the probing side. Scalar subqueries expose record keys. Direct column projections and unambiguous wildcards
retain record-link provenance through derived tables and CTEs, so comparisons and computations
continue to read their SQL-visible keys.

A value that is a column, an expression or a call is read by the test for each kind of value that
names a record and by the call to `type::record`, as the same expression each time. A value that
reads no row, such as `abs(5)`, is folded by the engine to the record it names, so it is read
directly, or through an index on a link column, as a literal is. A volatile expression, such as
`random()`, gives a new value to each of these reads.

Grouping by the `id` of a base table groups by the stored record id. `HAVING` and an `ORDER BY`
expression read the key of a group's `id`, and the projection yields it. The groups of the
null-extended side of an outer join have no `id`: its key is absent, never an error, and
`HAVING e.id IS NULL` finds that group. `ORDER BY e.id` itself orders by the stored `id`.

Only base tables have this column. The `id` of a derived table, subquery alias, common table
expression or `VALUES` list is already a plain value and is never read as a key again.
The SurrealQL and GQL dialects do not go through this crate and keep `id` as the full record id.
A string that names a record id (`'person:1'`) is a string key, not a record id, and the key
comparison does no type coercion: `id = '1'` does not match the integer key `1`.

`UPDATE`, `DELETE` and `INSERT` (with `ON CONFLICT`, `ON DUPLICATE KEY UPDATE` and `RETURNING`)
treat `id` as the key of the table they write, and echo or return the key as the row's `id`; the
projection of a `DELETE` reads the deleted row.

## Record link fields

A column that the catalog says holds record links to exactly one table (`record<t>`,
`option<record<t>>` or `record<t> | none`, see `BinderCatalog::link_table`) is stored as the link
`t:<key>`, and is read, written and searched by its key. A column of several tables or of any table,
a collection of links (`array<record<t>>`, `set<record<t>>`) and every other type are not link
columns: a collection of links is stored and compared as it is. Direct projections and wildcards
over one source retain a field's link target through derived tables and CTEs. Computed expressions,
VALUES lists, base-table `id` and system catalog columns do not inherit link targets.

The catalog is asked about one column at a time, never about the columns of a table as a whole: for
each column the binder resolves against a base-table relation, and, in a write, for each assigned
column and each column of the target table that an expression names. A column may be asked about more
than once. A table it does not describe has no link columns. Describing link columns does not change what `get_table`
returns, so `*` still expands to the columns `get_table` lists.

A link column is found by the exact name of its field, as the lowering reads or writes that field,
so a field that differs from a link column only in case is not a link. PostgreSQL tables, columns,
aliases and CTEs retain the spelling the parser produces: unquoted identifiers are lowercase, and
quoted identifiers retain their case. MySQL binding uses its case-insensitive name policy, while
writes retain the spelling supplied by its parser.

A link column that stands alone is the link as stored, which the wire layer presents as its key: a
projected item (also the value a subquery projects), a `GROUP BY` or `ORDER BY` key, a `DISTINCT`
item, a `USING` column, the operand of `IS [NOT] NULL` and the argument of `COUNT(col)`. Anywhere
else a link column is its key, `IF type::is_record(col) THEN record::id(col) ELSE col END`, which is
the column itself when it is NULL or absent: an operand of arithmetic, negation, `||`, `LIKE`, an
ordering comparison (`<`, `<=`, `>`, `>=`) and `BETWEEN`, an argument of a function or of `SUM`,
`MIN`, `MAX`, `AVG` and `COUNT(DISTINCT col)`, the operand of a `CAST` or `::`, a `CASE` condition,
operand or result, and an argument of `COALESCE` and `NULLIF`. So `dept + 1` is `3`, `CAST(dept AS
text)` is `'2'` and `dept > 1.5` is true for the link `dept:2`: an ordering comparison compares the
key, as it compares the `id` column, with whatever it is given, a number or a string.

`=`, `<>`, `<=>`, `IS [NOT] DISTINCT FROM`, `IN` and `NOT IN` compare the link with the record of its
target table that the other side names, as the `id` column is compared, with the target table in
the place of the table of the `id`:

- An integer or non-empty string literal is the record literal `t:<key>`. A float literal with
  an integral value that fits 64 bits names the same integer key. A parameter, a column or an
  expression names the record `type::record(t, <value>)` identifies. In comparisons that observe
  inequality or NULL, a non-null value that names no record stays non-null, so the empty string
  is distinct from a missing link. Lookup and equijoin operands that name no record use NULL,
  because only matching records matter there.
- A top-level conjunct of a `WHERE` predicate over the table read, `col = <operand>` or
  `col IN (<operands>)`, is the plain comparison of the stored link with the records, which an index
  on the column can serve. Anywhere else, and for the column of a joined table, the comparison keeps
  SQL's three-valued handling of NULL. A top-level `IN` of one operand, and `IN` and `NOT IN` of one
  operand in the `WHERE` clause of a write, are an equality and an inequality.
- A link column is compared as stored, with no conversion, with the `id` of its target table and
  with another link column to the same table, in a predicate and in a join key. The `id` of another
  table is compared by its key. A join key that has a link column on the build side keeps that
  column bare, so the engine can read the build table by an index.
- A link column compared with a subquery, or with a list that holds one, compares its key with
  the scalar keys the subquery projects. Both `col IN (SELECT id FROM t)` and a subquery projecting
  another link field compare SQL-visible keys.
- `USING (col)`, `ORDER BY` and `GROUP BY` use the link as stored.

A write converts the value assigned to a link column of its target table: a literal in `INSERT ...
VALUES`, a positional item of `INSERT ... SELECT`, `UPDATE ... SET`, `ON CONFLICT DO UPDATE SET`,
`ON DUPLICATE KEY UPDATE`, `REPLACE`, and `EXCLUDED.col` and `VALUES(col)`:

- An integer or non-empty string literal is the record literal `t:<key>`.
- A value that is a record when evaluated is left as it is, even a record of another table, which
  the type check of the field refuses.
- An integer, non-empty string, uuid, array or object is the record `type::record(t, <value>)` names.
- `NULL` written to a link column whose type admits `NONE` (`option<record<t>>`, or a union with
  `none`) is stored as `NONE`, which reads back as `NULL`, as PostgreSQL stores a NULL foreign key. This
  holds for a literal, a parameter and any expression that evaluates to NULL, in `INSERT`, `UPDATE`,
  `ON CONFLICT`, `COPY` and a `DEFAULT NULL`. A link column whose type does not admit `NONE` refuses
  `NULL`: the type check fails, which the wire layer reports as `23502`.
- Anything else (`NONE`, a float, a boolean, a datetime, the empty string) is left as it is, for the
  type check of the field to accept or refuse.

```
INSERT INTO emp (id, dept) VALUES (8, $1)
  dept = IF $_1 = '' OR !(type::is_int($_1) OR type::is_string($_1) OR type::is_uuid($_1)
         OR type::is_array($_1) OR type::is_object($_1)) THEN $_1 ELSE type::record('dept', $_1) END
```

A value that is not a literal, a parameter or a column is evaluated once, bound to `$_link` in a
block. The item of an `INSERT ... SELECT` at a position is written to the column at that position:
it keeps its alias, or is named by the column when it has none; with a wildcard in the projection no
item is converted, and the other items are written by the names the projection gives them. A MySQL
`UPDATE` over a join or a derived table has no single target table and converts nothing. In the
`WHERE` clause of an `UPDATE` or a `DELETE`, a link column is compared as above. Assigning `id` in
an `UPDATE`, and the `id` of an `INSERT`, are as they are for any table.

A write reads a link column of its target table as a `SELECT` does, bare or qualified by the name of
its table (or by the alias of a MySQL `UPDATE`). It is the link as stored where it stands alone: an assigned value, an item of a
`RETURNING` list or of the `SELECT` of an `INSERT ... SELECT`, the operand of `IS [NOT] NULL`, and a
value handed to the comparison of an equality or a membership. Anywhere else it is its key: in the
`WHERE` clause, in an assigned value or a `RETURNING` item that is an expression, as an operand of
arithmetic, of an ordering comparison and of `BETWEEN`, as an argument of a function, in the
condition or the result of a `CASE`, and as a member of a list that a value other than a link column
is compared with. The `RETURNING` list of a `DELETE` reads the deleted row and converts no
comparison, so a link column that it compares is its key.

These are not converted, and keep the link as stored:

- A link projected by a derived table or CTE retains its stored representation and target
  provenance. Scalar subqueries expose its key; functions and comparisons in the enclosing query
  read it with the same key semantics as a base-table link.
- `EXCLUDED.col` and `VALUES(col)` are the value that the `INSERT` proposes for the column, a link;
  as an operand of an expression they are that link, not its key.
- A link column assigned to a column that is not a link column is stored as the link.
- `USING (col)` compares the stored links, so a link column and a column that holds the bare key of
  the row are not equal under it.

A link column compared with an ordering operator compares the key, which filters the rows read: an
index on the column serves an equality or a membership of it, and does not serve a range.

The `WHERE` clause of a write is evaluated as the engine evaluates it, without SQL's three-valued
handling of NULL, for every column: a comparison with a NULL or an absent value gives the engine's
answer, not NULL.

### SQL foreign keys

A PostgreSQL foreign key makes its column a link field. `CREATE TABLE` with a column `REFERENCES
t [(id)]` or a table `FOREIGN KEY (col) REFERENCES t [(id)]`, `ALTER TABLE ... ADD COLUMN ...
REFERENCES t` and `ALTER TABLE ... ADD [CONSTRAINT name] FOREIGN KEY (col) REFERENCES t [(id)]` define
the column as `record<t>` (`option<record<t>>` when the column may be NULL) with a `REFERENCE`, so a
SQL client reads, writes and joins the column by the key of the row, as above. `ON DELETE CASCADE`
is the `cascade` strategy, `SET NULL` is `unset`, and `NO ACTION`, `RESTRICT` and `SET DEFAULT`
are `reject`. The catalog lists the column in `pg_constraint` as `<table>_<column>_fkey` and reports
it with the SQL type of the key of the target table (the type of its `id`), which is `bigint` for
an integer key. A foreign key over several columns, one to a column other than `id`, one on the `id`
column and one over a column that is not an integer, text or uuid is refused (`0A000`).

A `REFERENCE` does not check that a link names a row, so the field also carries an `ASSERT` that
does: a write whose value names no row of the target fails as `insert or update on table "t"
violates foreign key constraint "t_col_fkey"` (`23503`). A row that keeps its value is not judged
again. A table that links to itself accepts the row that names itself. A delete that a `reject`
reference refuses is `23503` too. `DROP CONSTRAINT <table>_<column>_fkey` drops the `REFERENCE` and
the `ASSERT` and keeps the type, so the column is still a link that is written as a key.

`ADD FOREIGN KEY` over a scalar column converts the column and its rows in one statement that runs
in one transaction. It first fails with `23503` if a row holds a value that names no row of the
target, then widens the column's type to take links, rewrites the rows to hold links (NULL becomes
absent), narrows the type to the link with a migration that keeps them, and sets the `REFERENCE` and
the `ASSERT`. The rows are rewritten ahead of the narrowing because the engine does not migrate the
field of a `REFERENCE` and keeps no backlink for a row that a migration changes lazily. A column
that has a `DEFAULT` or an assertion other than `NOT NULL` is refused (`0A000`). The column is found
in the schema, or among the columns the earlier statements of the same request declare, so a
migration that creates a table and alters it in one request converts the column it created.

A nullable foreign key column accepts SQL NULL, stored as `NONE` as above; a `NOT NULL` one refuses
it (`23502`).

## Window functions

A window function call (`func(args) OVER (...)`) is computed in the select list and the `ORDER BY`
of a `SELECT`. The window functions `row_number`, `rank`, `dense_rank`, `percent_rank`,
`cume_dist`, `ntile`, `lag`, `lead`, `first_value`, `last_value` and `nth_value` are supported, and
so are `sum`, `avg`, `count` (`count(*)` counts rows), `min` and `max` over a window. `OVER w` reads
the window `w` that the `WINDOW` clause names.

Each distinct call is computed once into a hidden field of every row, `__sql_window_<i>`, which the
select list and the `ORDER BY` read. Calls over the same `PARTITION BY` and `ORDER BY` are computed
by one `Window` operator. A select without aggregation computes its windows over the rows that its
`WHERE` clause keeps, below its `DISTINCT`, `ORDER BY`, `LIMIT` and projection. A select with
`GROUP BY`, an aggregate or `HAVING` computes them over the groups that `HAVING` keeps: an aggregate
in a window call reads the output of the aggregation, which computes it under a hidden name
(`__sql_window_agg_<n>`) when the select list does not select it. `*` and `t.*` do not read the
hidden fields.

A `ROWS` or `RANGE` frame is kept as it is written; a frame with only a start ends at `CURRENT ROW`.
Without a frame, a window with an `ORDER BY` reads `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
ROW`, and one without reads its whole partition.

These are refused as unsupported (`0A000`): any other aggregate called with `OVER`, `DISTINCT` in a
window call, a `GROUPS` frame, an `EXCLUDE` clause other than `EXCLUDE NO OTHERS`, a `RANGE` frame
with an offset, a frame offset that is not a non-negative integer literal, a frame that starts after
it ends, and a window call in the `ORDER BY` of a set operation. A window call in `WHERE`, `GROUP BY`
or `HAVING`, or in the arguments of another window call, is an error (`42P20`).

## Numeric literals

A decimal integer literal that fits 64 bits is an integer, in both dialects. One that does not
(`9223372036854775808`, `123456789012345678901234567890`) is a float, as `1.5`, `3.0` and `1e3` are:
it has the type of a double (`float8` in PostgreSQL, `DOUBLE` in MySQL), and its value is the nearest
double, so a value beyond 2^53 is not kept exactly. MySQL's `BIGINT UNSIGNED` range
(2^63 to 2^64-1) is not an integer type here: those literals are floats too. A decimal integer
literal beyond the range of a double (more than about 308 digits) is a parse error, as is a
PostgreSQL integer literal in base 16, 8 or 2 (`0x`, `0o`, `0b`) that does not fit 64 bits.

A `-` that applies to the literal `9223372036854775808` alone, with space or parentheses around it,
makes the integer `-9223372036854775808`. A `-` that applies to anything else is a negation, as it is
for a literal of any other magnitude beyond 64 bits (`-9223372036854775809`) and for the literal as
the operand of a cast, which binds tighter (`-9223372036854775808::bigint` is
`-(9223372036854775808::bigint)`).

The planner folds arithmetic over constants in a `SELECT`, and folds integers exactly in 64 bits. A
result that does not fit (`9223372036854775807 + 1`, `-9223372036854775808 * -1`,
`- -9223372036854775808`, and in PostgreSQL `-9223372036854775808 / -1`, in MySQL
`-9223372036854775808 DIV -1`) is refused as `bigint out of range`, which the planner maps to
SQLSTATE `22003` for PostgreSQL and to error 1690 for MySQL. MySQL's `/` divides in floating point
and has no such result. `-9223372036854775808 % -1` is 0. MySQL's `DIV` divides exactly in 64 bits
when both operands have an integer value that fits, a float such as `1.0` included
(`9223372036854775807 DIV 1.0` is `9223372036854775807`). When an operand has a fraction the result
is the truncated double-precision quotient (`1 DIV 0.5` is 2), which can differ from MySQL's exact
decimal result, both beyond 2^53 and where rounding leaves the quotient just below an integer. A
`DIV` result that does not fit 64 bits is refused. A divisor of zero is
`division by zero` (`22012`) in PostgreSQL and NULL in MySQL.
Arithmetic that reads a column, and arithmetic in an `INSERT`, `UPDATE` or `DELETE`, is not folded:
the engine evaluates it.

## JSON operators

`->` reads a member or an element. `->>` and `#>>` give the extracted value as text: NULL when it is
absent or null, a string as itself, and anything else as its JSON text. A step gives NULL wherever
it reaches nothing.

- A text literal operand of `->` and `->>` names a member, and reads only an object: an array, a
  scalar and NULL give NULL.
- An integer literal operand of `->` and `->>` reads an element of an array, counting from the end
  when it is negative. An object gives NULL, whatever its keys, and so does an index out of range
  in either direction. A literal that is neither text, NULL nor an integer of 32 bits (a larger
  integer, a number with a fraction, a boolean) has no operator and is refused as `42883`. An
  integer literal that does not fit 64 bits is a float (see Numeric literals) and is refused in the
  same way.
- The path of `#>>` is a text array literal such as `'{a,b,0}'` of at most 64 steps, and every step
  is text. On an object a step names a member, even one that spells an integer. On an array a step
  that spells an integer (an optional sign and decimal digits that fit 32 bits) indexes the array,
  from the end when negative, and any other step gives NULL. On a scalar a step gives NULL. A path
  with a NULL element, written as the unquoted word `NULL`, gives NULL.
- The path is read as PostgreSQL reads array input. Space (ASCII, with the vertical tab) around the
  braces and around an element is dropped. A double quote starts or ends a quoted part of an
  element wherever it stands, and a comma, a brace and space inside one are text. A backslash makes
  the next character text. Space after the last character that is not space, quoted or escaped is
  not part of the element. An element that has no text, a quote that is not closed, a brace inside
  an element and text after the closing brace are malformed.
- A NULL literal operand gives NULL.

The operators of a chain such as `doc -> 'a' -> 0 ->> 'b'`, and a path over a `->`, read their
value as one place. Only the outermost operator reads an absent value as NULL or converts what it
reaches to text. An `IS [NOT] NULL` test of a JSON operator tests the place it reaches, which is
NULL or absent exactly when the operator is NULL, and so converts nothing.

A step reads its value where it lies and copies only what it extracts, except in two cases that
bind the value whole as a value and copy it: an index before the last element (`-2` and below),
which needs the length of the array, and a negative, signed or zero-padded integer step of a path
over a value that is not a place or after the first three steps of that kind. A place is a column,
a parameter or a column of an outer row, read through literal keys and indexes; a call, a subquery
and a `CASE` are not places, and each is evaluated once. The first three such steps of a path over
a place read the member and the element from it, and so read it twice. The size of a lowered chain
or path is proportional to its length.

The member that a `->` extracts is copied out of its container each time the expression is
evaluated, and the `IS NULL` test of a place evaluates it twice, so `doc -> 'big' IS NULL` copies
`big` twice for each row, where `doc -> 'big' -> 0` and `doc #>> '{big,0}'` copy only the element.
The `IS NULL` test of a value that is not a place evaluates it once. `->>` and `#>>` bind the value
they extract to convert it to text.

These differ from PostgreSQL's `jsonb`:

- `->` gives NULL for a JSON null, where PostgreSQL gives the `jsonb` value `null`.
- `->>`, `->` and `#>>` give a JSON object or array as compact JSON with its keys in alphabetical
  order, where PostgreSQL puts a space after `:` and `,` and orders keys by length, then by bytes.
  A comparison of that text with a string literal differs.
- An operand that is not a literal, such as a bound parameter or a column, is used as the index of
  the value as it evaluates: a text operand names a member, and an integer operand reads an array
  element from the start, or the member of an object that the same text names. It never counts
  from the end, and it is not checked to be text or an integer.
- A malformed path literal is not refused in an expression that the planner folds away before it
  is lowered, such as the operand of `false AND`.

A malformed path literal is refused as `22P02` (invalid text representation); `@>` and a path that
is not a literal are refused as `0A000` (feature not supported). `INSERT`, `UPDATE` and `DELETE`
refuse every JSON operator as `0A000`, in a condition, a value, a `RETURNING` list and the
`SELECT` of an `INSERT ... SELECT`, so a malformed path in one of them is reported as `0A000`
too. A `SELECT` refused in the extended protocol is reported by Parse, and any other statement
when it runs.

## PostgreSQL declared scalar types

The PostgreSQL listener reads the declared types of fields used by a `SELECT` through the session's
namespace and database, using its interactive transaction when present. Scalar type hints remain
separate from the list of known table columns: a schemaless table still accepts undeclared fields
and its wildcard is shaped from the result rows. Types attached to base-table columns survive
relation aliases and correlated references.

Boolean, bytes, datetime, decimal, duration, float, integer, string, UUID and object declarations
supply result types. An optional or nullable scalar has the same wire type as the scalar. A mixed
union, record link or collection supplies no scalar hint. A declared integer is PostgreSQL `int8`:
the stored declaration does not distinguish SQL `smallint`, `integer` and `bigint`.

During Parse, a comparison with a declared scalar column supplies the type of an unspecified
parameter. Explicit client OIDs and explicit casts take precedence. A PostgreSQL parameter whose
type remains unspecified is described and decoded as `text`; its position is retained even when
an earlier parameter has no inferred type. An undeclared `id` therefore still needs a cast or a
client-declared OID to address an integer key.

This typing applies to SELECT comparisons and to INSERT VALUES with an explicit column list,
UPDATE assignments, ON CONFLICT assignments and write predicates. Scalar target declarations
constrain direct assignment parameters, including every VALUES row. INSERT SELECT and omitted
INSERT column lists do not yet propagate target types. Derived-table output types, collection
element types and record-link key types also require additional metadata propagation.

Explicit RETURNING lists are described before execution, in projection order, using declared
scalar field types. This description is stable for an empty result and does not execute a write
when the client describes its statement or portal. Expression aliases are retained; duplicate
labels are rejected before execution because native objects cannot preserve differing values
under the same key. Wildcards remain dynamic, with the existing prepared-statement JSON fallback.
Writes without RETURNING emit only their command tag and affected-row count on both protocols.
PostgreSQL write casts now convert their operands, preserving NULL. UPDATE FROM and DELETE USING
are rejected until join-based write lowering is implemented; source tables must never become
additional mutation targets.

## Correlated subqueries

A subquery reads the columns of the query that contains it through `$parent`, the row that query is
on. The engine's `$parent` names that row and no row further up, so a column of a query two or more
levels above the subquery that reads it (the outermost table's column read from an `EXISTS` inside
an `EXISTS`) is carried down instead.

Binding a column reference finds the query it belongs to and the query one level below it, whose
parent that is. That query is entered with the row of the query above as its `$parent`, so it reads
the column there, once, and binds the value to a parameter (`__sql_outer_<level>_<n>`, `level` being
the depth of the column's query). It is lowered as a block that first binds each parameter and then
runs the select (`BoundSelect::carried`). Every query nested in it, however deep and wherever the
column stands in it (the `WHERE`, `HAVING`, `ORDER BY` or select list, a derived table or a common
table expression), reads the parameter (`BoundColumnRef::carried_as`). A column that stands in the
query directly above is read from `$parent` and binds nothing. Each column a query binds
is bound once, however many times it is read.

A carried column of a joined relation is read from the field the row of the join holds that
relation's columns under (`$parent._join0.<column>` for the first joined relation).

## `LIKE` patterns

`LIKE` and `ILIKE` read a pattern as both databases do. `%` stands for any run of characters,
including none, `_` for any one character whatever its width in bytes, and a line break is a
character like any other. A backslash makes the character after it itself: `\%`, `\_` and `\\` stand
for a percent sign, an underscore and a backslash, and a backslash before any other character is
dropped. A backslash that ends the pattern has nothing to escape and stands for itself, where
PostgreSQL raises an error once a match reaches it. There is no `ESCAPE` clause; both parsers refuse
it. PostgreSQL `LIKE` is case-sensitive and `ILIKE` is not. MySQL `LIKE` ignores case, as under its
default collation, whose equal treatment of accented letters is not reproduced.

A MySQL string literal resolves its backslash escapes before the pattern is read, as MySQL does,
except `\%` and `\_`, which keep their backslash so that the `LIKE` reads an escaped wildcard.

A pattern written as a literal is converted to a regular expression while the statement is lowered.
Any other pattern, which is every pattern a driver binds (`LIKE $1`, `LIKE ?`), a column or an
expression, is converted for each row by calls of `string::replace` that build the same expression
(`lower/sql_like.rs`), so no character of a pattern is ever read as regular expression syntax. A
pattern with no backslash and no regular expression syntax takes the shorter route of converting
its wildcards alone.

## SQL NULL predicates

PostgreSQL writes evaluate comparisons, arithmetic, NOT, AND, OR, BETWEEN and IN with SQL
three-valued logic. Missing fields and SQL NULL both represent unknown in these expressions;
IS NULL matches either. UPDATE and DELETE act only on predicates that are true. A comparison in
an assigned value or RETURNING list preserves unknown as NULL. Complex operands are bound once
inside the expression before the NULL checks run.

A positive top-level equality or membership conjunct can use the stored record directly when its
truth agrees with SQL, retaining point lookups for literal and parameter primary keys. Nested
comparisons and nullable link operands retain their NULL guards. Non-null operands that name no
record, including empty strings, remain distinct from NULL.

SELECT membership and BETWEEN also preserve unknown. A matching IN item takes precedence over
NULL members; an unmatched value with a NULL member is unknown. IN over an empty subquery is
false, and NOT IN over an empty subquery is true, including for a NULL left operand.

Numeric scalar functions return NULL for absent or NULL arguments, evaluating their arguments
once. Literal numeric arguments retain a foldable call form. HAVING and aggregate ORDER BY
resolve grouped columns inside scalar calls and IN subqueries against aggregation output, and
unprojected grouping keys are carried as hidden outputs. LIMIT and OFFSET constrain unspecified
parameters to int8.
