/**
[env]
namespace = true
database = true
imports = ["pgsql/_qualified_wildcard.surql"]

[test]
reason = "INSERT ... SELECT inserts the columns of the one table its query reads for the * of that table, as it does for a bare *, with or without a column list"

[[test.results]]
value = "[{ k: 1, name: 'ada' }, { k: 2, name: 'bob' }]"

[[test.results]]
value = "[{ k: 3, name: 'cy' }]"

[[test.results]]
value = "[{ k: 1, name: 'ada' }, { k: 2, name: 'bob' }]"

[[test.results]]
value = "[{ k: 3, name: 'cy' }]"
*/

INSERT INTO qw_dst SELECT s.* FROM qw_src s WHERE s.k < 3 RETURNING k, name;

INSERT INTO qw_dst2 (id, k, name) SELECT qw_src.* FROM qw_src WHERE qw_src.k = 3 RETURNING k, name;

SELECT k, name FROM qw_dst ORDER BY k;

SELECT k, name FROM qw_dst2 ORDER BY k;
