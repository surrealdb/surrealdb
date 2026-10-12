/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A record-id range over a table whose record ids name a datetime component
	returns the records the same bounds written as a WHERE return, including
	those in the bounds' own seconds, whose keys do not sort by their
	sub-second part: the range is read widened to whole seconds and judged by
	the record ids compared as values. A float equal to an int names the int's
	record. `ORDER BY id DESC LIMIT 1` returns the true latest record. An
	UPDATE or a DELETE over such a range under a WHERE touches only the
	records in the range as written, not the rest of the seconds it is read
	in.
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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[2, 1, 5]"

[[test.results]]
value = "[2, 1, 5]"

[[test.results]]
value = "[2, 1]"

[[test.results]]
value = "[1, 5, 4]"

[[test.results]]
value = "[4]"

[[test.results]]
value = "[4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: counter:['a', 5], n: 5, site: 'a', v: 1 }]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[0, 3, 4, 5]"
*/

DEFINE TABLE tick SCHEMALESS;
DEFINE FIELD id ON tick TYPE [feed: string, at: datetime];
DEFINE TABLE counter SCHEMALESS;
DEFINE FIELD id ON counter TYPE [site: string, n: int];
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:00.900Z', n = 1 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:00Z', n = 0 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:00.500Z', n = 2 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:00.050Z', n = 3 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:01.250Z', n = 4 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:00.999999999Z', n = 5 RETURN NONE;

SELECT VALUE n FROM tick:['f', d'2024-01-01T00:00:00.500Z']..=['f', d'2024-01-01T00:00:00.999999999Z'] ORDER BY at;
SELECT VALUE n FROM tick WHERE feed = 'f' AND at >= d'2024-01-01T00:00:00.500Z' AND at <= d'2024-01-01T00:00:00.999999999Z' ORDER BY at;
SELECT VALUE n FROM tick:['f', d'2024-01-01T00:00:00.500Z']..['f', d'2024-01-01T00:00:00.999999999Z'] ORDER BY at;
SELECT VALUE n FROM tick:['f', d'2024-01-01T00:00:00.500Z']>..['f', d'2024-01-01T00:00:02Z'] ORDER BY at;
SELECT VALUE n FROM tick ORDER BY id DESC LIMIT 1;
SELECT VALUE n FROM tick WHERE feed = 'f' ORDER BY id DESC LIMIT 1;

CREATE counter SET site = 'a', n = 5, v = 1 RETURN NONE;
SELECT * FROM counter:['a', 5.0];
SELECT VALUE v FROM counter WHERE id = counter:['a', 5.0];

UPDATE tick:['f', d'2024-01-01T00:00:00.500Z']..=['f', d'2024-01-01T00:00:00.900Z'] SET hit = true WHERE n >= 0 RETURN NONE;
SELECT VALUE n FROM tick WHERE hit = true ORDER BY n;
DELETE tick:['f', d'2024-01-01T00:00:00.500Z']..['f', d'2024-01-01T00:00:00.999999999Z'] WHERE n >= 0;
SELECT VALUE n FROM tick ORDER BY n;
