/**
[env]
namespace = true
database = true

[test]
reason = """
A handler whose grouped query runs a nested statement that writes and then
fails fails, as it would ungrouped, rather than reading the statement as
`NONE`. The handler answers with the error's response, and its savepoint takes
back what the nested statement wrote.
"""

[[test.results]]
value = "[{ id: s:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "500"

[[test.results]]
error = "The table 't' does not exist"
*/

CREATE s:1;
DEFINE API "/grouped" FOR get THEN {
	RETURN {
		status: 200,
		body: (SELECT array::group((CREATE t:1 RETURN 1 + 'a')) AS x FROM s GROUP ALL)
	};
};
RETURN api::invoke("/grouped").status;
SELECT VALUE id FROM t;
