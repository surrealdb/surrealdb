/**
[env]
namespace = true
database = true

[test]
requires = ["scripting"]
reason = """
A script that catches a failing `surrealdb.query` goes on in the same
transaction, and the failed call leaves none of what it changed behind,
including a value it drew from a sequence defined in that transaction: the
draw is provisional, and the call's savepoint takes it back, so the next draw
returns it again. A call that only reads still draws, so in a transaction that
defined a sequence it runs behind a savepoint of its own. A call awaited beside
a failing one keeps its own draw.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "0f"

[[test.results]]
value = "['rejected', 1f]"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"
*/

BEGIN;
DEFINE SEQUENCE sq;
RETURN function() {
	try {
		await surrealdb.query("{ LET $n = sequence::nextval('sq'); THROW 'boom'; }");
	} catch (e) {
		return await surrealdb.functions.sequence.nextval('sq');
	}
	return 'not caught';
};
RETURN function() {
	const settled = await Promise.allSettled([
		surrealdb.query("{ LET $n = sequence::nextval('sq'); THROW 'boom'; }"),
		surrealdb.functions.sequence.nextval('sq')
	]);
	return settled.map((s) => s.status == 'fulfilled' ? s.value : s.status);
};
RETURN sequence::nextval('sq');
COMMIT;
