/**
[env]
timeout = 5000

[test]
reason = "A COMPUTED field whose name equals a field read in a subquery over another table is not a cycle"
issue = 7468

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
value = "[{ id: parent:1, loc: NONE, name: 'p1' }]"

[[test.results]]
value = "[{ id: child:1, loc: (90.41f, 23.81f), ref: parent:1 }]"

[[test.results]]
value = "[{ id: parent:1, loc: (90.41f, 23.81f) }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among field clauses: members -> roster -> members"

*/

DEFINE TABLE parent SCHEMAFULL;
DEFINE FIELD name ON parent TYPE string;
DEFINE FIELD members ON parent TYPE any;
DEFINE TABLE child SCHEMAFULL;
DEFINE FIELD ref ON child TYPE record<parent>;
DEFINE FIELD loc ON child TYPE option<geometry<point>>;
DEFINE FIELD loc ON parent COMPUTED (SELECT VALUE loc FROM child WHERE ref = $parent.id LIMIT 1)[0];
CREATE parent:1 CONTENT { name: 'p1' };
CREATE child:1 CONTENT { ref: parent:1, loc: type::point([90.41, 23.81]) };
SELECT id, loc FROM parent;
-- A subquery's own source is still read against the document, so a real cycle through it is refused.
DEFINE FIELD roster ON parent COMPUTED (SELECT VALUE ref FROM $this.members);
DEFINE FIELD members ON parent COMPUTED roster;
