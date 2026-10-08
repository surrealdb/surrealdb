/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A function body that defines a table named by its argument and then writes and reads it loads that table when each statement runs"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: y3:1 }]"

[[test.results]]
value = "[{ id: y4:1 }]"
*/
DEFINE FUNCTION fn::c($t: string) { DEFINE TABLE $t; CREATE type::record($t, 1); RETURN SELECT * FROM type::table($t) };
RETURN fn::c('y3');
RETURN fn::c('y4');
