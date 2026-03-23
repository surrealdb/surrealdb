/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Complex WHERE conditions with type::field, OR/AND, parameterized table, and closure-based field checks"

[[test.results]]
value = "'OK'"

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
value = "[{ chunking_metadata: { foo: 42 }, id: file:1 }]"
*/

{
    CREATE file:1 SET chunking_metadata = {foo: 42};
    CREATE file:2 SET flow_keywords = "abc";
    CREATE file:3 SET flow_keywords = "96743b94d47fb21423a1a37866e9b88a";
    RETURN "OK";
};

LET $table = "file";
LET $field = "flow_keywords";
LET $rerun_when_updated = true;
LET $hash = "96743b94d47fb21423a1a37866e9b88a";
LET $deps = ["chunking_metadata"];

-- file:1 has no flow_keywords (NONE) and has chunking_metadata → excluded by deps check
-- file:2 has flow_keywords = "abc" (not NONE, != hash) and no chunking_metadata → included
-- file:3 has flow_keywords = hash → excluded by != hash check
SELECT * FROM type::table($table)
WHERE ((type::field($field) == NONE) OR ($rerun_when_updated AND type::field($field) != $hash))
AND (NONE NOT IN $deps.map(|$x| type::field($x)));
