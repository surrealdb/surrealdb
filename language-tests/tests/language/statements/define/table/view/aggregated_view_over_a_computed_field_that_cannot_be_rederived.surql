/**
[env]
namespace = true
database = true

[test]
reason = """
An aggregated view takes a record out of the group its pre-write image derives to,
so the pre-write image has to derive the value the record joined its group with.
A `COMPUTED` body that draws randomness, reads another record that has changed
since, or reads a `DEFINE PARAM` that has been redefined, derives a different value
the second time, which names a group the view never held. The write that finds no
such group fails, and the record can then be neither updated nor deleted.

A table with such a view is therefore not handed derived images on the strength of
its views: the writes succeed and the view holds no group once its records are
gone. A materialized view has no group to leave, so its row is rewritten from the
post-write image whatever the body reads, and it holds the derived value.
"""

# The tables, fields and views
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# A volatile body: create, update and delete all succeed, and no group is left.
[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

# A body reading another record, after that record changed.
[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

# A body reading a parameter, after the parameter was redefined.
[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

# A materialized view over a volatile body holds the derived value.
[[test.results]]
value = "[]"

[[test.results]]
value = "[true]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

DEFINE TABLE person SCHEMALESS;
DEFINE TABLE vol SCHEMALESS;
DEFINE FIELD r ON vol COMPUTED rand::uuid();
DEFINE TABLE vol_by_r AS SELECT count() AS n, r FROM vol GROUP BY r;
DEFINE TABLE child SCHEMALESS;
DEFINE FIELD pname ON child COMPUTED parent.name;
DEFINE TABLE child_by_pname AS SELECT count() AS n, pname FROM child GROUP BY pname;
DEFINE TABLE mat SCHEMALESS;
DEFINE FIELD r ON mat COMPUTED rand::uuid();
DEFINE TABLE mat_view AS SELECT r FROM mat;
DEFINE PARAM $group VALUE 'a';
DEFINE TABLE tied SCHEMALESS;
DEFINE FIELD g ON tied COMPUTED $group;
DEFINE TABLE tied_by_g AS SELECT count() AS n, g FROM tied GROUP BY g;

CREATE vol:1 SET x = 1 RETURN NONE;
UPDATE vol:1 SET x = 2 RETURN NONE;
DELETE vol:1 RETURN NONE;
SELECT * FROM vol_by_r;

CREATE person:1 SET name = 'a' RETURN NONE;
CREATE child:1 SET parent = person:1 RETURN NONE;
UPDATE person:1 SET name = 'b' RETURN NONE;
UPDATE child:1 SET x = 1 RETURN NONE;
DELETE child:1 RETURN NONE;
SELECT * FROM child_by_pname;

CREATE tied:1 RETURN NONE;
DEFINE PARAM OVERWRITE $group VALUE 'b';
UPDATE tied:1 SET x = 1 RETURN NONE;
DELETE tied:1 RETURN NONE;
SELECT * FROM tied_by_g;

CREATE mat:1 SET x = 1 RETURN NONE;
SELECT VALUE type::is_uuid(r) FROM mat_view;
UPDATE mat:1 SET x = 2 RETURN NONE;
DELETE mat:1 RETURN NONE;
SELECT * FROM mat_view;
