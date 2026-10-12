/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A bound on a datetime component of the record id is widened to the whole
	second it lies in before an index over the component is read. A bound
	with no widening (an exclusive lower bound at the first second of year
	0000, which has no earlier instant a key can hold, or a datetime outside
	the years 0000 to 9999) is not dropped from the index access, since the
	access would then read as exact and a NOT over it would subtract records
	that satisfy it. Every answer equals the one WITH NOINDEX gives.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ at: d'0000-01-01T00:00:00Z', feed: 'f', id: reading:['f', d'0000-01-01T00:00:00Z'] }]"

[[test.results]]
value = "[{ at: d'0000-01-01T00:00:00.500Z', feed: 'f', id: reading:['f', d'0000-01-01T00:00:00.500Z'] }]"

[[test.results]]
value = "[{ at: d'2025-01-01T00:00:00Z', feed: 'f', id: reading:['f', d'2025-01-01T00:00:00Z'] }]"

[[test.results]]
value = "[reading:['f', d'0000-01-01T00:00:00Z']]"

[[test.results]]
value = "[reading:['f', d'0000-01-01T00:00:00Z']]"

[[test.results]]
value = "[reading:['f', d'0000-01-01T00:00:00.500Z'], reading:['f', d'2025-01-01T00:00:00Z']]"

[[test.results]]
value = "[reading:['f', d'0000-01-01T00:00:00.500Z'], reading:['f', d'2025-01-01T00:00:00Z']]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

*/

DEFINE FIELD id ON reading TYPE [feed: string, at: datetime];
DEFINE INDEX by_at ON reading FIELDS at;
DEFINE INDEX by_feed_at ON reading FIELDS feed, at;
CREATE reading SET feed = 'f', at = d'0000-01-01T00:00:00Z';
CREATE reading SET feed = 'f', at = d'0000-01-01T00:00:00.500Z';
CREATE reading SET feed = 'f', at = d'2025-01-01T00:00:00Z';
SELECT VALUE id FROM reading WHERE feed = 'f' AND NOT (at > d'0000-01-01T00:00:00Z') ORDER BY id;
SELECT VALUE id FROM reading WITH NOINDEX WHERE feed = 'f' AND NOT (at > d'0000-01-01T00:00:00Z') ORDER BY id;
SELECT VALUE id FROM reading WHERE at > d'0000-01-01T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM reading WITH NOINDEX WHERE at > d'0000-01-01T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM reading WHERE NOT (at < d'+10000-01-01T00:00:00Z') ORDER BY id;
SELECT VALUE id FROM reading WITH NOINDEX WHERE NOT (at < d'+10000-01-01T00:00:00Z') ORDER BY id;
SELECT count() FROM reading WHERE at > d'0000-01-01T00:00:00Z' GROUP ALL;
SELECT count() FROM reading WITH NOINDEX WHERE at > d'0000-01-01T00:00:00Z' GROUP ALL;
