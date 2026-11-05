/**
[env]
namespace = true
database = true

[test]
reason = "A paged statement the planner proves reads no row, over a table, a record range or a list of records, runs whatever its body evaluates, a COMPUTED field's body included: it answers no row."

[[test.results]]
value = "[{ id: tag:1, target: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: doc:1, t: [] }]"
*/
CREATE tag:1 SET target = 1;
RETURN (SELECT VALUE target.foo() FROM tag WHERE false LIMIT 1);
RETURN (SELECT VALUE id FROM tag:1..5 WHERE false LIMIT 1);
LET $x = (SELECT VALUE <int>target FROM [tag:1, tag:2] WHERE false LIMIT 1);
DEFINE FIELD t ON doc COMPUTED (SELECT VALUE id FROM tag WHERE false AND target.len() > 0 LIMIT 1);
CREATE doc:1;
