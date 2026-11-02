/**
[env]
namespace = true
database = true

[test]
requires = ["scripting"]
reason = """
A grouped query whose aggregate argument runs a nested statement that writes
and then fails is a failing query, as it is ungrouped, not one that answers
`NONE`. A script that catches it goes on in the same transaction, and the
failed query leaves none of what its nested statement wrote behind.
"""

[[test.results]]
value = "[{ id: s:1 }]"

[[test.results]]
value = "'caught'"

[[test.results]]
value = "'caught'"

[[test.results]]
error = "The table 't' does not exist"
*/

CREATE s:1;
function() {
	try {
		await surrealdb.query(`SELECT array::group((INSERT INTO t [{ id: t:1, n: 1 }, { id: t:2, n: 'a' }] RETURN n + 1)) AS x FROM s GROUP ALL`);
	} catch (e) {
		return 'caught';
	}
	return 'not caught';
};
function() {
	try {
		await surrealdb.query(`SELECT array::group({ CREATE t:3; 1 + 'a' }) AS x FROM s GROUP ALL`);
	} catch (e) {
		return 'caught';
	}
	return 'not caught';
};
SELECT VALUE id FROM t;
