/**
[env]
namespace = true
database = true

[test]
reason = """
DEFINE INDEX finds the field a column's path begins with by the name that field's definition is
stored under, so a field whose name has to be quoted (`my-obj`, `a.b`) is found as a plain or
keyword-named one is. This holds whether the column is written as a path, which the statement's
program checks, or given by type::field, which is checked as the program runs. On a SCHEMAFULL
table a path beneath such a field is indexed when the field's type admits sub-fields and refused
when it does not. On either kind of table a path beneath a computed field is refused as computed,
naming that field.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ by_dash: 'DEFINE INDEX by_dash ON t FIELDS `my-obj`.x', by_dash_eval: 'DEFINE INDEX by_dash_eval ON t FIELDS `my-obj`.y', by_dot: 'DEFINE INDEX by_dot ON t FIELDS `a.b`.x', by_dot_eval: 'DEFINE INDEX by_dot_eval ON t FIELDS `a.b`.y', by_kw: 'DEFINE INDEX by_kw ON t FIELDS `select`.x', by_plain: 'DEFINE INDEX by_plain ON t FIELDS plain.x' }"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The field '`my-str`.x' does not exist"

[[test.results]]
error = "The field '`my-str`.x' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Computed fields cannot be indexed. Index: 'by_comp' - Field: '`my-comp`'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Computed fields cannot be indexed. Index: 'by_comp' - Field: '`my-comp`'"

[[test.results]]
error = "Computed fields cannot be indexed. Index: 'by_comp_eval' - Field: '`my-comp`'"

[[test.results]]
value = "{}"

*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD `my-obj` ON t TYPE object;
DEFINE FIELD `a.b` ON t TYPE object;
DEFINE FIELD `select` ON t TYPE object;
DEFINE FIELD plain ON t TYPE object;

-- Columns written as paths.
DEFINE INDEX by_dash ON t FIELDS `my-obj`.x;
DEFINE INDEX by_dot ON t FIELDS `a.b`.x;
DEFINE INDEX by_kw ON t FIELDS `select`.x;
DEFINE INDEX by_plain ON t FIELDS plain.x;

-- Columns given by type::field.
DEFINE INDEX by_dash_eval ON t FIELDS type::field('`my-obj`.y');
DEFINE INDEX by_dot_eval ON t FIELDS type::field('`a.b`.y');

(INFO FOR TABLE t).indexes;

-- A field whose type admits no sub-field covers no path beneath it.
DEFINE FIELD `my-str` ON t TYPE string;
DEFINE INDEX by_str ON t FIELDS `my-str`.x;
DEFINE INDEX by_str_eval ON t FIELDS type::field('`my-str`.x');

-- A path beneath a computed field is refused, on a SCHEMAFULL table and on a
-- schemaless one, where a path no field covers would be indexed.
DEFINE FIELD `my-comp` ON t COMPUTED { x: 1 };
DEFINE INDEX by_comp ON t FIELDS `my-comp`.x;
DEFINE FIELD `my-comp` ON s COMPUTED { x: 1 };
DEFINE INDEX by_comp ON s FIELDS `my-comp`.x;
DEFINE INDEX by_comp_eval ON s FIELDS type::field('`my-comp`.x');
(INFO FOR TABLE s).indexes;
