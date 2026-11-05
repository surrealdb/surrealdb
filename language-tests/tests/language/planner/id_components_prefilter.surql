/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["language/planner/id_components_key_range_import.surql"]

[test]
reason = """
	A scan of a table whose record ids name their components judges a stored
	field's comparison from each record's stored bytes before decoding it
	(`pre_decode_filter: yes`), as any scan does. A component is not in the
	stored bytes: it is read from the record id, so a predicate that reads one
	is never judged from them, and the key range reads the component instead.
	Either way the rows are those the predicate admits.
"""

[[test.results]]
match = '''string::contains($result, "TableScan") AND string::contains($result, "pre_decode_filter: yes")'''
error = false

[[test.results]]
value = "[51, 52, 53, 54, 55, 56, 57, 58, 59]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND !string::contains($result, "pre_decode_filter")'''
error = false

[[test.results]]
value = "[51, 54, 57]"

[[test.results]]
value = "[56, 59]"

[[test.results]]
value = "true"
*/

EXPLAIN SELECT VALUE temp FROM reading WHERE temp > 50;
SELECT VALUE temp FROM reading WHERE temp > 50 ORDER BY temp;
EXPLAIN SELECT VALUE temp FROM reading WHERE city = 'London' AND temp > 50;
SELECT VALUE temp FROM reading WHERE city = 'London' AND temp > 50 ORDER BY temp;
SELECT VALUE temp FROM reading WHERE city = 'Rome' AND temp >= 56 ORDER BY temp;
RETURN (SELECT VALUE temp FROM reading WHERE city = 'Rome' AND temp >= 56 ORDER BY temp) == (SELECT VALUE temp FROM reading WITH NOINDEX WHERE city = 'Rome' AND temp >= 56 ORDER BY temp);
