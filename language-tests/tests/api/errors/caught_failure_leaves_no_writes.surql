/**
[env]
namespace = true
database = true

[test]
reason = """
An API stage that fails answers with the error's response, and the request's
transaction commits whatever the response. A failing handler or middleware must
still leave none of what it wrote behind: each runs behind a savepoint of its
own, rolled back when the stage answers with an error's response. A stage that
answers with a response of its own keeps its writes.

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
value = "NONE"

[[test.results]]
value = "500"

[[test.results]]
value = "NONE"

[[test.results]]
value = "500"

[[test.results]]
value = "NONE"

[[test.results]]
value = "201"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "500"

[[test.results]]
value = "NONE"

[[test.results]]
value = "500"

[[test.results]]
value = "[500, t:7]"

[[test.results]]
value = "[{ h: 'h1', id: t:1, v: 1 }, { h: 'h5', id: t:5, v: 5 }, { h: 'h7', id: t:7, v: 7 }]"

[[test.results]]
value = "[t:1, t:5, t:7]"

[[test.results]]
value = "[t:12, t:13, t:14, t:16, t:18]"

*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX a_h ON t FIELDS h UNIQUE;
DEFINE INDEX z_v ON t FIELDS v UNIQUE;
DEFINE TABLE log SCHEMALESS;
DEFINE EVENT logged ON t WHEN $event = 'CREATE' THEN (CREATE log SET of = $after.id);
CREATE t:1 SET h = 'h1', v = 1 RETURN NONE;

-- A CREATE that fails on its second unique index
DEFINE API '/create' FOR post THEN (CREATE t:2 SET h = 'h2', v = 1);
api::invoke('/create', { method: 'post' }).status;

-- A handler body whose later statement fails takes its earlier writes with it
DEFINE API '/body' FOR post THEN {
	CREATE t:3 SET h = 'h3', v = 3;
	CREATE t:4 SET h = 'h4', v = 1;
	{ status: 201 }
};
api::invoke('/body', { method: 'post' }).status;

-- A handler that answers with its own response keeps its writes
DEFINE API '/ok' FOR post THEN {
	CREATE t:5 SET h = 'h5', v = 5;
	{ status: 201 }
};
api::invoke('/ok', { method: 'post' }).status;

-- A middleware that fails after the rest of the chain answered takes the
-- handler's writes with it
DEFINE FUNCTION fn::fails_after($req: object, $next: function) -> object {
	LET $res = $next($req);
	THROW 'after the handler';
};
DEFINE API '/after' FOR post MIDDLEWARE fn::fails_after() THEN {
	CREATE t:6 SET h = 'h6', v = 6;
	{ status: 201 }
};
api::invoke('/after', { method: 'post' }).status;

-- A handler that answers with something that is not a response answers with
-- the error's response, and takes its writes with it
DEFINE API '/not_a_response' FOR post THEN {
	CREATE t:8 SET h = 'h8', v = 8;
	'not a response'
};
api::invoke('/not_a_response', { method: 'post' }).status;

-- A failing handler's writes are undone while the statement that invoked it
-- goes on and keeps its own
[api::invoke('/create', { method: 'post' }).status, (CREATE ONLY t:7 SET h = 'h7', v = 7).id];

SELECT * FROM t ORDER BY id;
SELECT VALUE of FROM log ORDER BY of;

-- No unique index entry of the failed writes survived
INSERT INTO t [
	{ id: 12, h: 'h2', v: 12 },
	{ id: 13, h: 'h3', v: 13 },
	{ id: 14, h: 'h4', v: 14 },
	{ id: 16, h: 'h6', v: 16 },
	{ id: 18, h: 'h8', v: 18 }
] RETURN VALUE id;
