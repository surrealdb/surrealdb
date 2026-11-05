/**
[test]
reason = "Setup: a high-fanout graph whose edge table has a per-row SELECT permission, so a record-user traversal resolves every flushed edge batch through the permission check. Fanout (1500) exceeds the default `scan_batch_size` (1000) so the scan's pipelined resolve engages mid-traversal."
run = false
*/

DEFINE TABLE person SCHEMALESS PERMISSIONS FOR select FULL;

-- Edge table with a `Specific` SELECT permission: a record user's
-- traversal must resolve each edge record to evaluate it, and only
-- edges with ids up to 1200 are visible.
DEFINE TABLE knows TYPE RELATION SCHEMALESS
	PERMISSIONS
		FOR select WHERE record::id(id) <= 1200;

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE person:viewer;
CREATE person:source;

-- One source vertex with 1500 outbound edges, each to a fresh target
-- vertex. Explicit numeric IDs keep iteration order stable and
-- deterministic across backends.
FOR $i IN 1..=1500 {
	LET $target = type::record("person", $i);
	LET $edge = type::record("knows", $i);
	CREATE $target;
	RELATE person:source -> $edge -> $target;
};
