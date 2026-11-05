/**
[env]
namespace = true
database = true

[test]
reason = "a CREATE over a count of ids creates that many records with record ids of the table, as the serial run does, under every morsel configuration"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 300 }]"

[[test.results]]
value = "[{ count: 300 }]"

[[test.results]]
value = "[true, true, true]"

*/

DEFINE TABLE c;
CREATE |c:300| SET n = 1 RETURN NONE;
SELECT count() FROM c GROUP ALL;
SELECT count() FROM c WHERE type::is_record(id) AND record::tb(id) = 'c' GROUP ALL;
RETURN (CREATE |c:3| RETURN VALUE type::is_record(id));
