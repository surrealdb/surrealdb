/**
[env]
namespace = true
database = true

[test]
reason = "A type a view's fields or condition name through a cast, in a plain or an aggregated view, is not removed"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the view 'v' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the view 'w' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the view 'g' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TABLE t SCHEMALESS;
DEFINE TABLE v AS SELECT * FROM t WHERE <mood>status = 'ok';
REMOVE TYPE mood;
REMOVE TABLE v;
DEFINE TABLE w AS SELECT <mood>status AS s FROM t;
REMOVE TYPE mood;
REMOVE TABLE w;
DEFINE TABLE g AS SELECT count() AS n FROM t WHERE <mood>status = 'ok' GROUP ALL;
REMOVE TYPE mood;
REMOVE TABLE g;
REMOVE TYPE mood;
