/**
[env]
timeout = 5000

[test]
reason = "A SELECT with WHERE inside a closure invoked by a builtin, or a function called from one, must not evaluate COMPUTED fields it does not reference"
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
value = "NONE"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[1]"
*/

DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD n ON item TYPE int;
DEFINE FIELD boom ON item COMPUTED { THROW "boom was evaluated" } TYPE bool;
CREATE item SET n = 1 RETURN NONE;
DEFINE FUNCTION fn::filtered() { SELECT VALUE n FROM item WHERE n >= 0 };
DEFINE API "/where" FOR get THEN { RETURN { status: 200, body: SELECT VALUE n FROM item WHERE n >= 0 } };
RETURN [1].map(|$i| SELECT VALUE n FROM item WHERE n >= 0);
RETURN [1].map(|$i| fn::filtered());
(api::invoke("/where")).body;
