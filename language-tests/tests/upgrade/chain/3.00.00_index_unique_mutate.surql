/**
[test]
reason = "Chain mutation for index_unique.surql: every walked version inserts one row through the unique index an earlier version built, keyed by a $hop-derived email so no hop can collide with another. The record id is generated, since a literal id would already exist on the second hop."
run = false
*/
CREATE chain_uniq SET
	email = string::concat('hop', <string> $hop, '@example.com'),
	hop = $hop,
	wrote_at = $version;
