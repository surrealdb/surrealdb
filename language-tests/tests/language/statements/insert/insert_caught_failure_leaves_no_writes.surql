/**
[env]
namespace = true
database = true

[test]
requires = ["scripting"]
reason = """
A plain INSERT (no IGNORE, no ON DUPLICATE KEY UPDATE) does not always fail
its transaction: a `DEFINE API` handler turns the error into a 500 response,
and a script can catch a rejected `surrealdb.query`, and in both cases the
transaction goes on to commit. The failing INSERT must still leave nothing
behind: not the failing row's record, nor the unique index entries it wrote
before the conflict, nor the rows the same statement inserted before it.

The unique index on `h` is checked before the one on `v`, so a failing row has
already written its `h` entry when `v` conflicts. Inserting those `h` values
again at the end proves no entry survived.
"""

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
value = "500"

[[test.results]]
value = "[{ h: 'h1', id: t:1, v: 1 }]"

[[test.results]]
value = "'caught'"

[[test.results]]
value = "[{ h: 'h1', id: t:1, v: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "500"

[[test.results]]
value = "[{ h: 'h1', id: t:1, v: 1 }]"

[[test.results]]
value = "'caught'"

[[test.results]]
value = "[{ h: 'h1', id: t:1, v: 1 }]"

[[test.results]]
value = "[t:12, t:13, t:14, t:15, t:16, t:17]"

*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX a_h ON t FIELDS h UNIQUE;
DEFINE INDEX z_v ON t FIELDS v UNIQUE;
INSERT INTO t { id: 1, h: 'h1', v: 1 } RETURN NONE;

-- A failing row inside an API handler
DEFINE API '/one' FOR post THEN (INSERT INTO t { id: 2, h: 'h2', v: 1 });
api::invoke('/one', { method: 'post' }).status;
SELECT * FROM t ORDER BY id;

-- The same inside a script that catches the failure
function() {
	try {
		await surrealdb.query(`INSERT INTO t { id: 3, h: 'h3', v: 1 }`);
	} catch (e) {
		return 'caught';
	}
	return 'not caught';
};
SELECT * FROM t ORDER BY id;

-- A multi-row INSERT whose second row fails, inside an API handler: the first
-- row goes with it
DEFINE API '/two' FOR post THEN (
	INSERT INTO t [{ id: 4, h: 'h4', v: 4 }, { id: 5, h: 'h5', v: 1 }]
);
api::invoke('/two', { method: 'post' }).status;
SELECT * FROM t ORDER BY id;

-- The same inside a script that catches the failure
function() {
	try {
		await surrealdb.query(`INSERT INTO t [{ id: 6, h: 'h6', v: 6 }, { id: 7, h: 'h7', v: 1 }]`);
	} catch (e) {
		return 'caught';
	}
	return 'not caught';
};
SELECT * FROM t ORDER BY id;

-- No unique index entry of the failed rows survived
INSERT INTO t [
	{ id: 12, h: 'h2', v: 12 },
	{ id: 13, h: 'h3', v: 13 },
	{ id: 14, h: 'h4', v: 14 },
	{ id: 15, h: 'h5', v: 15 },
	{ id: 16, h: 'h6', v: 16 },
	{ id: 17, h: 'h7', v: 17 }
] RETURN VALUE id;
