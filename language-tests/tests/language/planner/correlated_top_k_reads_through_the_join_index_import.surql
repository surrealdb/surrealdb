/**
[test]
reason = """
	Setup for correlated_top_k_reads_through_the_join_index and its
	_permissions companion: 80 parents with four children each,
	the children indexed by `owner`, their `day` values tied in pairs, their
	ids in the reverse of creation order, and readable by a record user only
	where `day` is not 1.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;

DEFINE TABLE parent SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE child SCHEMALESS PERMISSIONS FOR select WHERE day != 1, FOR create, update, delete FULL;
DEFINE INDEX child_owner ON child FIELDS owner;
FOR $p IN 1..=80 {
	CREATE type::record('parent', $p) SET n = $p RETURN NONE;
	FOR $c IN 0..4 {
		CREATE type::record('child', $p * 10 + 9 - $c) SET owner = type::record('parent', $p), day = ($p + $c) % 3, at = $c RETURN NONE;
	};
};
