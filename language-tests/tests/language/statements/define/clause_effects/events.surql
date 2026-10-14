/**
[env]
namespace = true
database = true

[test]
reason = """
	An event body is no schema clause that must not have effects: it may
	write, directly and through a stored function, and draw from a sequence.
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
value = "[{ drawn: true, id: log:direct }, { drawn: false, id: log:fn }]"
*/

DEFINE SEQUENCE s;
DEFINE TABLE log SCHEMALESS;
DEFINE FUNCTION fn::writes() { CREATE log:fn; RETURN true; };
DEFINE TABLE t SCHEMALESS;
DEFINE EVENT e ON t WHEN true THEN {
	CREATE log:direct SET n = sequence::nextval('s');
	fn::writes();
};
CREATE t:1 RETURN NONE;
SELECT id, type::is_int(n) AS drawn FROM log ORDER BY id;
