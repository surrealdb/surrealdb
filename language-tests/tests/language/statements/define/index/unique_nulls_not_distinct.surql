/**
[test]
reason = """
`UNIQUE NULLS NOT DISTINCT` makes a NULL member of a tuple take part in
uniqueness, so a second tuple equal to one holding NULL collides as a complete
tuple does, on create and on update. A tuple holding NONE still takes no part:
any number of them coexist, on a single column and beside a NULL in a compound
tuple. An empty array is keyed as NULL, so two of them collide too. Moving a
record off NULL frees the slot. The clause is spelled in any case, is shown by
INFO, and an OVERWRITE that adds it to an index already holding duplicate NULLs
is refused."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database index `solo_v` already contains NULL, with record `solo:1`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database index `solo_v` already contains NULL, with record `solo:1`"

[[test.results]]
value = "[solo:1]"

[[test.results]]
value = "[solo:3, solo:4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[solo:5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database index `pair_ab` already contains ['k', NULL], with record `pair:1`"

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
error = "Database index `pair_ab` already contains ['k', 'v'], with record `pair:7`"

[[test.results]]
value = "[pair:1, pair:3, pair:4, pair:7]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database index `tagged_tags` already contains NULL, with record `tagged:1`"

[[test.results]]
error = "Database index `tagged_tags` already contains NULL, with record `tagged:1`"

[[test.results]]
value = "{ solo_v: 'DEFINE INDEX solo_v ON solo FIELDS v UNIQUE NULLS NOT DISTINCT' }"

[[test.results]]
value = "[{ cols: ['v'], index: 'UNIQUE NULLS NOT DISTINCT', name: 'solo_v', table: 'solo' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database index `plain_v` already contains NULL, with record `plain:1`"

[[test.results]]
value = "{ plain_v: 'DEFINE INDEX plain_v ON plain FIELDS v UNIQUE' }"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Database index `plain_v` already contains NULL, with record `plain:1`"
*/

DEFINE TABLE solo SCHEMALESS;
DEFINE INDEX solo_v ON solo FIELDS v UNIQUE NULLS NOT DISTINCT;
CREATE solo:1 SET v = NULL RETURN NONE;
CREATE solo:2 SET v = NULL RETURN NONE;
CREATE solo:3 RETURN NONE;
CREATE solo:4 RETURN NONE;
CREATE solo:5 SET v = 'a' RETURN NONE;
UPDATE solo:5 SET v = NULL RETURN NONE;
SELECT VALUE id FROM solo WHERE v = NULL ORDER BY id;
SELECT VALUE id FROM solo WHERE v = NONE ORDER BY id;
-- Moving the holder off NULL frees the slot.
UPDATE solo:1 SET v = 'b' RETURN NONE;
UPDATE solo:5 SET v = NULL RETURN NONE;
SELECT VALUE id FROM solo WHERE v = NULL ORDER BY id;

DEFINE TABLE pair SCHEMALESS;
define index pair_ab on pair fields a, b unique nulls not distinct;
CREATE pair:1 SET a = 'k', b = NULL RETURN NONE;
CREATE pair:2 SET a = 'k', b = NULL RETURN NONE;
CREATE pair:3 SET a = 'k' RETURN NONE;
CREATE pair:4 SET a = 'k' RETURN NONE;
CREATE pair:5 SET a = NULL RETURN NONE;
CREATE pair:6 SET a = NULL RETURN NONE;
CREATE pair:7 SET a = 'k', b = 'v' RETURN NONE;
CREATE pair:8 SET a = 'k', b = 'v' RETURN NONE;
SELECT VALUE id FROM pair WHERE a = 'k' ORDER BY id;

DEFINE TABLE tagged SCHEMALESS;
DEFINE INDEX tagged_tags ON tagged FIELDS tags UNIQUE NULLS NOT DISTINCT;
CREATE tagged:1 SET tags = [] RETURN NONE;
CREATE tagged:2 SET tags = [] RETURN NONE;
CREATE tagged:3 SET tags = NULL RETURN NONE;

(INFO FOR TABLE solo).indexes;
(INFO FOR TABLE solo STRUCTURE).indexes;

DEFINE TABLE plain SCHEMALESS;
DEFINE INDEX plain_v ON plain FIELDS v UNIQUE;
CREATE plain:1 SET v = NULL RETURN NONE;
CREATE plain:2 SET v = NULL RETURN NONE;
DEFINE INDEX OVERWRITE plain_v ON plain FIELDS v UNIQUE NULLS NOT DISTINCT;
(INFO FOR TABLE plain).indexes;
DELETE plain:2;
DEFINE INDEX OVERWRITE plain_v ON plain FIELDS v UNIQUE NULLS NOT DISTINCT;
CREATE plain:3 SET v = NULL RETURN NONE;
