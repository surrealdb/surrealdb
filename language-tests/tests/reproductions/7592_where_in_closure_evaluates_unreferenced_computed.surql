/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A WHERE inside a closure invoked by a built-in, or inside an API handler, evaluates only the COMPUTED fields it references (an erroring one proves it is never read)"
issue = 7592

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: [1], status: 200 }"
*/
DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD n ON item TYPE int;
DEFINE FIELD boom ON item COMPUTED { THROW "boom was evaluated" } TYPE bool;
CREATE item SET n = 1 RETURN NONE;
DEFINE FUNCTION fn::filtered() { SELECT VALUE n FROM item WHERE n >= 0 };
SELECT VALUE n FROM item WHERE n >= 0;
RETURN fn::filtered();
RETURN [1].map(|$i| SELECT VALUE n FROM item);
RETURN [1].map(|$i| SELECT VALUE n FROM item WHERE n >= 0);
RETURN [1].map(|$i| fn::filtered());
DEFINE API "/where" FOR get THEN { RETURN { status: 200, body: SELECT VALUE n FROM item WHERE n >= 0 } };
api::invoke("/where").{ body, status };
