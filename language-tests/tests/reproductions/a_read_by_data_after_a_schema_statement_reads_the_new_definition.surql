/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A block reads a table or a record that only the data names — `FROM` a
	call or a parameter, a `FETCH` — after a schema statement of the same
	block changed what a read of that table finds. The read finds the table as
	the schema statement left it: the computed field it defined is derived.
	A table the block defines by writing to it is read whole by a later read
	the data names.
"""

[[test.results]]
value = "[{ id: t:1, n: 1 }]"

[[test.results]]
value = "[{ c: 7, id: t:1, n: 1 }]"

[[test.results]]
value = "[{ c: 7, id: t:1, n: 1 }]"

[[test.results]]
value = "[{ id: log:1, p: { c: 7, id: t:1, n: 1 } }]"

[[test.results]]
value = "[{ id: fresh:1, n: 1 }]"

*/

{
	DEFINE TABLE t;
	DEFINE FIELD n ON t TYPE int;
	CREATE t:1 SET n = 1;
};

{
	LET $t = (SELECT VALUE 't' FROM ONLY t:1);
	DEFINE FIELD OVERWRITE c ON t COMPUTED 7;
	RETURN SELECT * FROM type::table($t);
};

{
	DEFINE FIELD OVERWRITE c ON t COMPUTED 7;
	LET $r = (SELECT VALUE id FROM ONLY t:1);
	RETURN SELECT * FROM $r;
};

{
	DEFINE FIELD OVERWRITE c ON t COMPUTED 7;
	CREATE log:1 SET p = t:1;
	RETURN SELECT * FROM log FETCH p;
};

{
	CREATE fresh:1 SET n = 1;
	LET $t = (SELECT VALUE 'fresh' FROM ONLY t:1);
	RETURN SELECT * FROM type::table($t);
};
