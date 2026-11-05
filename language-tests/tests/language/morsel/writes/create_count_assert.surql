/**
[env]
namespace = true
database = true

[test]
reason = "a CREATE over a count of ids whose every record fails a field's ASSERT fails, leaves no record of the statement, and a CREATE that passes it keeps its index entries, under every morsel configuration"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = true
match = '''$error = /^Found 0 for field `n`, with record `c:[a-z0-9]{20}`, but field must conform to: .+ > 0$/'''

[[test.results]]
value = "[{ count: 300 }]"

[[test.results]]
value = "[{ count: 300 }]"
*/

DEFINE TABLE c SCHEMAFULL;
DEFINE FIELD n ON c TYPE int ASSERT $value > 0;
DEFINE INDEX by_n ON c FIELDS n;
CREATE |c:300| SET n = 1 RETURN NONE;
CREATE |c:300| SET n = 0 RETURN NONE;
SELECT count() FROM c GROUP ALL;
SELECT count() FROM c WITH INDEX by_n WHERE n = 1 GROUP ALL;
