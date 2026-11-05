/**
[env]
timeout = 5000

[test]
reason = "Adding REFERENCE to a field that already holds records writes the reference keys of the links they hold, so <~ finds them and ON DELETE applies without a record being rewritten"
issue = 7144

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
value = "[{ id: child:c1, name: 'child1' }]"

[[test.results]]
value = "[{ child_ref: child:c1, id: parent:p1, name: 'parent1' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: child:c1, refs: [parent:p1] }]"

[[test.results]]
value = "[{ child_ref: child:c1, id: parent:p1, name: 'parent1' }]"

[[test.results]]
value = "[{ id: child:c1, refs: [parent:p1] }]"

[[test.results]]
error = "Cannot delete `child:c1` as it is referenced by `parent:p1` with an ON DELETE REJECT clause"

[[test.results]]
value = "[{ id: tag:t1 }]"

[[test.results]]
value = "[{ id: tag:t2 }]"

[[test.results]]
value = "[{ id: post:p1, owner: tag:t1, tags: [tag:t1, tag:t2] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: tag:t1, refs: [post:p1, post:p1] }, { id: tag:t2, refs: [post:p1] }]"

[[test.results]]
value = "[{ id: post:p2, owner: tag:t1, tags: [tag:t1, tag:t2] }]"

[[test.results]]
value = "[{ id: tag:t1, refs: 4 }, { id: tag:t2, refs: 2 }]"

[[test.results]]
value = "[{ id: person:a }]"

[[test.results]]
value = "[{ id: robot:a }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pet:1, who: robot:a }]"

[[test.results]]
value = "[{ id: pet:2, who: person:a }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:a, refs: [pet:2] }, { id: robot:a, refs: [] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ by: person:a, id: toy:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:a, refs: [toy:1] }]"
*/


DEFINE TABLE child SCHEMAFULL;
DEFINE FIELD name ON child TYPE string;
DEFINE TABLE parent SCHEMAFULL;
DEFINE FIELD name ON parent TYPE string;
DEFINE FIELD child_ref ON parent TYPE record<child>;
CREATE child:c1 SET name = 'child1';
CREATE parent:p1 SET name = 'parent1', child_ref = child:c1;
DEFINE FIELD OVERWRITE child_ref ON parent TYPE record<child> REFERENCE ON DELETE REJECT;
SELECT id, <~parent AS refs FROM child:c1;
UPDATE parent:p1 SET child_ref = child:c1;
SELECT id, <~parent AS refs FROM child:c1;
-- A DELETE of the referenced record honours the ON DELETE REJECT the field gained.
DELETE child:c1;
-- A collection of links is backfilled link by link, and a field defined fresh over stored data too.
CREATE tag:t1; CREATE tag:t2;
CREATE post:p1 SET tags = [tag:t1, tag:t2], owner = tag:t1;
DEFINE FIELD tags ON post TYPE array<record<tag>> REFERENCE;
DEFINE FIELD owner ON post TYPE record<tag> REFERENCE;
SELECT id, <~post AS refs FROM tag ORDER BY id;
-- The keys the backfill wrote match those a write of the same links produces.
CREATE post:p2 SET tags = [tag:t1, tag:t2], owner = tag:t1;
SELECT id, array::len(<~post) AS refs FROM tag ORDER BY id;
-- Narrowing the kind purges the links it no longer admits, and the backfill
-- does not write them back from the values still stored.
CREATE person:a; CREATE robot:a;
DEFINE FIELD who ON pet TYPE record<person | robot> REFERENCE;
CREATE pet:1 SET who = robot:a; CREATE pet:2 SET who = person:a;
DEFINE FIELD OVERWRITE who ON pet TYPE record<person> REFERENCE;
SELECT id, <~pet AS refs FROM [person:a, robot:a];
-- ALTER FIELD that changes the type and adds the REFERENCE backfills too.
DEFINE FIELD by ON toy TYPE record<person>;
CREATE toy:1 SET by = person:a;
ALTER FIELD by ON toy TYPE option<record<person>> REFERENCE;
SELECT id, <~toy AS refs FROM person:a;
