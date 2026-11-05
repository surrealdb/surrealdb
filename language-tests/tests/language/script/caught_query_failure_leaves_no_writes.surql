/**
[env]
namespace = true
database = true

[test]
requires = ["scripting"]
reason = """
A script can catch a rejected `surrealdb.query`, or a rejected builtin that
writes such as `eval::surql`, and go on in the same transaction, which commits.
The call that failed must leave none of what it wrote behind: each call that
may write runs behind a savepoint of its own, rolled back when the call fails.
A call that succeeds keeps its writes, before or after a caught failure, and
calls the script awaits together each keep or lose only their own.

The unique index on `h` is checked before the one on `v`, so a failing write
has already written its `h` entry when `v` conflicts. Writing those `h` values
again at the end proves no entry survived.
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
value = "[]"

[[test.results]]
value = "'caught'"

[[test.results]]
value = "'caught'"

[[test.results]]
value = "'caught'"

[[test.results]]
value = "['fulfilled', 'rejected', 'fulfilled']"

[[test.results]]
value = "[{ h: 'h1', id: t:1, v: 1 }, { h: 'h2', id: t:2, v: 2 }, { h: 'h4', id: t:4, v: 4 }, { h: 'h8', id: t:8, v: 8 }, { h: 'h10', id: t:10, v: 10 }]"

[[test.results]]
value = "[t:1, t:2, t:4, t:8, t:10]"

[[test.results]]
value = "[t:13, t:15, t:16, t:17, t:19]"

*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX a_h ON t FIELDS h UNIQUE;
DEFINE INDEX z_v ON t FIELDS v UNIQUE;
DEFINE TABLE log SCHEMALESS;
DEFINE EVENT logged ON t WHEN $event = 'CREATE' THEN (CREATE log SET of = $after.id);
CREATE t:1 SET h = 'h1', v = 1 RETURN NONE;

-- A caught CREATE that fails on its second unique index, between two that
-- succeed
function() {
	await surrealdb.query(`CREATE t:2 SET h = 'h2', v = 2`);
	try {
		await surrealdb.query(`CREATE t:3 SET h = 'h3', v = 1`);
	} catch (e) {
		await surrealdb.query(`CREATE t:4 SET h = 'h4', v = 4`);
		return 'caught';
	}
	return 'not caught';
};

-- A caught query whose block fails after a statement that succeeded
function() {
	try {
		await surrealdb.query(`{ CREATE t:5 SET h = 'h5', v = 5; CREATE t:6 SET h = 'h6', v = 1; }`);
	} catch (e) {
		return 'caught';
	}
	return 'not caught';
};

-- A caught `eval::surql` reached through the functions bridge
function() {
	try {
		await surrealdb.functions.eval.surql(`CREATE t:7 SET h = 'h7', v = 1`);
	} catch (e) {
		return 'caught';
	}
	return 'not caught';
};

-- Calls awaited together: the failing one loses its writes, the others keep
-- theirs
function() {
	const settled = await Promise.allSettled([
		surrealdb.query(`CREATE t:8 SET h = 'h8', v = 8`),
		surrealdb.functions.eval.surql(`CREATE t:9 SET h = 'h9', v = 1`),
		surrealdb.query(`CREATE t:10 SET h = 'h10', v = 10`)
	]);
	return settled.map((s) => s.status);
};

SELECT * FROM t ORDER BY id;
SELECT VALUE of FROM log ORDER BY of;

-- No unique index entry of the failed writes survived
INSERT INTO t [
	{ id: 13, h: 'h3', v: 13 },
	{ id: 15, h: 'h5', v: 15 },
	{ id: 16, h: 'h6', v: 16 },
	{ id: 17, h: 'h7', v: 17 },
	{ id: 19, h: 'h9', v: 19 }
] RETURN VALUE id;
