/**
[env]
namespace = true
database = true

[test]
reason = """
A failing API handler leaves none of what it changed behind, and that includes
a value it drew from a sequence defined in the same transaction: the draw is
provisional, and the handler's savepoint takes it back, so the next draw returns
the same value. A handler that only reads still draws, so it runs behind a
savepoint in a transaction that defined a sequence. A sequence committed
earlier is claimed outside the transaction, and keeps its progress.
"""

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
value = "500"

[[test.results]]
value = "0"

[[test.results]]
value = "500"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "500"

[[test.results]]
value = "1"
*/

DEFINE API "/draw" FOR get THEN {
	LET $n = sequence::nextval('sq');
	THROW "boom";
};
DEFINE API "/committed" FOR get THEN {
	LET $n = sequence::nextval('committed');
	THROW "boom";
};
DEFINE SEQUENCE committed;

BEGIN;
DEFINE SEQUENCE sq;
RETURN api::invoke("/draw").status;
RETURN sequence::nextval('sq');
RETURN api::invoke("/draw").status;
RETURN sequence::nextval('sq');
COMMIT;

RETURN api::invoke("/committed").status;
RETURN sequence::nextval('committed');
