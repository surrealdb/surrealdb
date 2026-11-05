/**
[env]
namespace = true
database = true

[test]
reason = """
`DEFINE FIELD` refuses a set of clauses that produce each other's values in a
cycle, and a `COMPUTED` field cannot be indexed. Both judge a field by the name the
record holds it by, which for a name that has to be backticked when written is not the
text it is rendered as: a cycle through such a name is refused like any other, a
field reading itself in `VALUE` is not one, and an index on the field is refused
whichever of the two definitions comes second.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among field clauses: `a b` -> c -> `a b`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among field clauses: `m n` -> `o p` -> `m n`"

[[test.results]]
error = "Cyclic dependency detected among field clauses: `s t` -> `s t`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: {  }, fields: { "`a b`": 'DEFINE FIELD `a b` ON cy COMPUTED c PERMISSIONS FULL', "`m n`": 'DEFINE FIELD `m n` ON cy VALUE `o p` PERMISSIONS FULL', "`x y`": "DEFINE FIELD `x y` ON cy VALUE `x y` ?? 'init' PERMISSIONS FULL", plain: "DEFINE FIELD plain ON cy VALUE plain ?? 'init' PERMISSIONS FULL" }, indexes: {  }, lives: {  }, tables: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Computed fields cannot be indexed. Index: 'on_computed' - Field: '`the x`'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Computed fields cannot be indexed. Index: 'on_stored_x' - Field: '`stored x`'"
*/

DEFINE TABLE cy SCHEMALESS;

-- A cycle between computed fields.
DEFINE FIELD `a b` ON cy COMPUTED c;
DEFINE FIELD c ON cy COMPUTED `a b`;

-- A cycle between value-producing clauses.
DEFINE FIELD `m n` ON cy VALUE `o p`;
DEFINE FIELD `o p` ON cy VALUE `m n`;

-- A computed field has nothing else to derive from than itself.
DEFINE FIELD `s t` ON cy COMPUTED `s t`;

-- A `VALUE` naming its own field reads the value under consideration: not a cycle.
DEFINE FIELD `x y` ON cy VALUE `x y` ?? 'init';
DEFINE FIELD plain ON cy VALUE plain ?? 'init';

-- None of the refused definitions is left behind.
INFO FOR TABLE cy;

DEFINE TABLE ix SCHEMALESS;
DEFINE FIELD src ON ix TYPE option<string>;
DEFINE FIELD `the x` ON ix COMPUTED src;
DEFINE INDEX on_computed ON ix FIELDS `the x`;
DEFINE INDEX on_stored ON ix FIELDS src;

DEFINE TABLE jx SCHEMALESS;
DEFINE FIELD `stored x` ON jx TYPE option<string>;
DEFINE INDEX on_stored_x ON jx FIELDS `stored x`;
DEFINE FIELD `stored x` ON jx COMPUTED 'derived';
