/**
[test]
reason = """
	Setup for id_components_differential_permissions: a keyed table `k` and
	its unkeyed twin `u` holding the same rows, both readable by a record
	user only where the row's leading component is one of the user's sites
	and its datetime is not the user's excluded instant.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:bob SET sites = ['a', 'b'], skip = d'2024-01-01T00:00:01Z';

DEFINE TABLE k SCHEMALESS PERMISSIONS FOR select, update WHERE s IN $auth.sites AND t != $auth.skip;
DEFINE FIELD id ON k TYPE [s: string, t: datetime];
DEFINE TABLE u SCHEMALESS PERMISSIONS FOR select, update WHERE s IN $auth.sites AND t != $auth.skip;
FOR $i IN 0..60 {
	LET $s = ['a', 'b', 'c'][$i % 3];
	LET $t = time::from_nanos(1704067200000000000 + ($i % 20) * 250000000 + ($i % 2));
	CREATE k SET s = $s, t = $t, n = $i RETURN NONE;
	CREATE u SET s = $s, t = $t, n = $i RETURN NONE;
};
