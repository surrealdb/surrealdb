/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A bound that raises when evaluated is judged as an index bound is on any
	table. Where the access path reads no row, no conjunct is evaluated and
	nothing is raised; where it reads a row, the residual predicate evaluates
	the bound and raises. A key range over a named record id and a B-tree
	index over a stored field answer the same, and WITH NOINDEX, which
	evaluates the predicate on every row, raises in both.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'london', id: k:['london', 5s], span: 5s }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'london', id: p:1, span: 5s }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function duration::from_secs(). Argument 1 was the wrong type. Expected `int` but found `'x'`"

[[test.results]]
error = "Incorrect arguments for function duration::from_secs(). Argument 1 was the wrong type. Expected `int` but found `'x'`"

[[test.results]]
error = "Incorrect arguments for function duration::from_secs(). Argument 1 was the wrong type. Expected `int` but found `'x'`"

[[test.results]]
error = "Incorrect arguments for function duration::from_secs(). Argument 1 was the wrong type. Expected `int` but found `'x'`"

*/

DEFINE FIELD id ON k TYPE [city: string, span: duration];
CREATE k SET city = 'london', span = 5s;
DEFINE TABLE p SCHEMALESS;
DEFINE INDEX p_city ON p FIELDS city;
CREATE p:1 SET city = 'london', span = 5s;
LET $bad = 'x';
SELECT * FROM k WHERE span > duration::from_secs($bad) AND city = 'absent';
SELECT * FROM p WHERE span > duration::from_secs($bad) AND city = 'absent';
SELECT * FROM k WITH NOINDEX WHERE span > duration::from_secs($bad) AND city = 'absent';
SELECT * FROM p WITH NOINDEX WHERE span > duration::from_secs($bad) AND city = 'absent';
SELECT * FROM k WHERE span > duration::from_secs($bad) AND city = 'london';
SELECT * FROM p WHERE span > duration::from_secs($bad) AND city = 'london';
