/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["language/planner/id_components_key_range_import.surql"]

[test]
reason = """
	A key range whose bounds come from a parameter, a function's arguments, a
	closure's arguments, a statement in a block or the element a closure is
	mapped over is read with the values of that call: each call of the same
	body reads the range its own arguments select, never a range an earlier
	call evaluated. A bound whose value its component cannot hold (a string
	against a datetime, NONE, an array for an equality) widens the access and
	the rows are those a scan of the whole table returns. The readings of
	London from 20s are temps 51, 54 and 57, of Rome from 20s 50, 53, 56 and
	59, of Rome from 22s 56 and 59, and of Paris after 22.5s 58.
"""

[[test.results]]
value = "NONE"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[56, 59]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[51, 54, 57], [56, 59], []]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[51, 54, 57], [50, 53, 56, 59], []]"

[[test.results]]
value = "[[58], [59]]"

[[test.results]]
value = "[58]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"
*/

LET $city = 'Rome';
EXPLAIN SELECT temp FROM reading WHERE city = $city AND time >= d'2024-01-01T00:00:22Z';
SELECT VALUE temp FROM reading WHERE city = $city AND time >= d'2024-01-01T00:00:22Z' ORDER BY temp;

DEFINE FUNCTION fn::since($city: string, $since: datetime) {
	RETURN (SELECT VALUE temp FROM reading WHERE city = $city AND time >= $since ORDER BY temp);
};
RETURN [
	fn::since('London', d'2024-01-01T00:00:20Z'),
	fn::since('Rome', d'2024-01-01T00:00:22Z'),
	fn::since('Nowhere', d'2024-01-01T00:00:00Z'),
];

LET $from20 = |$c: string| (SELECT VALUE temp FROM reading WHERE city = $c AND time >= d'2024-01-01T00:00:20Z' ORDER BY temp);
RETURN [$from20('London'), $from20('Rome'), $from20('Nowhere')];

RETURN ['Paris', 'Rome'].map(|$c| (SELECT VALUE temp FROM reading WHERE city = $c AND time > d'2024-01-01T00:00:22.5Z' ORDER BY temp DESC LIMIT 1));

{
	LET $since = d'2024-01-01T00:00:23Z';
	RETURN (SELECT VALUE temp FROM reading WHERE city = 'Paris' AND time >= $since);
};

LET $bad = 'not a datetime';
RETURN (SELECT VALUE temp FROM reading WHERE city = 'London' AND time >= $bad ORDER BY temp)
	== (SELECT VALUE temp FROM reading WITH NOINDEX WHERE city = 'London' AND time >= $bad ORDER BY temp);
SELECT VALUE temp FROM reading WHERE city = $nothing ORDER BY temp;
RETURN (SELECT VALUE temp FROM reading WHERE city = ['London', 'Rome'] ORDER BY temp)
	== (SELECT VALUE temp FROM reading WITH NOINDEX WHERE city = ['London', 'Rome'] ORDER BY temp);
RETURN (SELECT VALUE temp FROM reading WHERE city = 'Rome' AND time < $nothing ORDER BY temp)
	== (SELECT VALUE temp FROM reading WITH NOINDEX WHERE city = 'Rome' AND time < $nothing ORDER BY temp);
