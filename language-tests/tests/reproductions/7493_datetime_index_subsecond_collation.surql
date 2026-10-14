/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An index over a Datetime field must order and bound its entries by the instant
they hold. Whole seconds sort before the fractions within them; one instant
written at different precisions or offsets is one key; years before 0000 and
after 9999 sort by time; and every comparison operator, on a single-field and on
a compound index, returns what the same query returns without the index."""
issue = 7493

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
value = "[t:0, t:1, t:2, t:3, t:4, t:5, t:6]"

[[test.results]]
value = "[t:0, t:1, t:2, t:3, t:4, t:5, t:6]"

[[test.results]]
value = "[t:6, t:5, t:4, t:3, t:2, t:1, t:0]"

[[test.results]]
value = "[t:6, t:5, t:4, t:3, t:2, t:1, t:0]"

[[test.results]]
value = "[t:0, t:1, t:2, t:3, t:4, t:5, t:6]"

[[test.results]]
value = "[t:0, t:1, t:2, t:3, t:4, t:5, t:6]"

[[test.results]]
value = "[t:0, t:1, t:2]"

[[test.results]]
value = "[t:0, t:1, t:2]"

[[test.results]]
value = "[t:0, t:1, t:2, t:3, t:4]"

[[test.results]]
value = "[t:0, t:1, t:2, t:3, t:4]"

[[test.results]]
value = "[t:3, t:4, t:5, t:6]"

[[test.results]]
value = "[t:3, t:4, t:5, t:6]"

[[test.results]]
value = "[t:1, t:2, t:3, t:4]"

[[test.results]]
value = "[t:1, t:2, t:3, t:4]"

[[test.results]]
value = "[t:0]"

[[test.results]]
value = "[t:0]"

[[test.results]]
value = "[t:2]"

[[test.results]]
value = "[t:2]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:2, t:3, t:4, t:5]"

[[test.results]]
value = "[t:2, t:3, t:4, t:5]"

[[test.results]]
value = "[t:0, t:1, t:3, t:4]"

[[test.results]]
value = "[t:0, t:1, t:3, t:4]"

[[test.results]]
value = "[t:0, t:1, t:3, t:4, t:5, t:6]"

[[test.results]]
value = "[t:0, t:1, t:3, t:4, t:5, t:6]"

[[test.results]]
value = "[era:0, era:1, era:2, era:3, era:4]"

[[test.results]]
value = "[era:0, era:1, era:2, era:3, era:4]"

[[test.results]]
value = "[era:0, era:1]"

[[test.results]]
value = "[era:0, era:1]"

[[test.results]]
value = "[era:3, era:4]"

[[test.results]]
value = "[era:3, era:4]"

[[test.results]]
value = "[era:1]"

[[test.results]]
value = "[era:1]"
*/

DEFINE INDEX idx_ts ON t FIELDS ts;
DEFINE INDEX idx_state_ts ON t FIELDS state, ts;
DEFINE INDEX idx_era_ts ON era FIELDS ts;
-- Precisions of 0, 1, 3, 6 and 9 fractional digits, inserted out of order.
INSERT INTO t [
	{ id: 5, state: 'PENDING', ts: d'2026-08-28T09:00:01Z' },
	{ id: 3, state: 'PENDING', ts: d'2026-08-28T09:00:00.152000001Z' },
	{ id: 0, state: 'PENDING', ts: d'2026-08-28T09:00:00Z' },
	{ id: 6, state: 'PENDING', ts: d'2026-08-28T09:00:01.07Z' },
	{ id: 2, state: 'DONE', ts: d'2026-08-28T09:00:00.152Z' },
	{ id: 4, state: 'PENDING', ts: d'2026-08-28T09:00:00.152300Z' },
	{ id: 1, state: 'PENDING', ts: d'2026-08-28T09:00:00.1Z' },
] RETURN NONE;
-- Years that need a sign, inserted out of order.
INSERT INTO era [
	{ id: 4, ts: d'+10000-01-01T00:00:00Z' },
	{ id: 1, ts: d'-0100-01-01T00:00:00Z' },
	{ id: 3, ts: d'9999-12-31T23:59:59.999999999Z' },
	{ id: 0, ts: d'-0200-01-01T00:00:00Z' },
	{ id: 2, ts: d'0000-01-01T00:00:00Z' },
] RETURN NONE;

-- Each indexed read is followed by the same read without the index.

-- Index-ordered iteration, both directions.
SELECT VALUE id FROM t WITH INDEX idx_ts ORDER BY ts ASC;
SELECT VALUE id FROM t WITH NOINDEX ORDER BY ts ASC;
SELECT VALUE id FROM t WITH INDEX idx_ts ORDER BY ts DESC;
SELECT VALUE id FROM t WITH NOINDEX ORDER BY ts DESC;

-- Range bounds through the single-field index.
SELECT VALUE id FROM t WITH INDEX idx_ts WHERE ts >= d'2026-08-28T09:00:00Z' ORDER BY ts ASC LIMIT 20;
SELECT VALUE id FROM t WITH NOINDEX WHERE ts >= d'2026-08-28T09:00:00Z' ORDER BY ts ASC LIMIT 20;
SELECT VALUE id FROM t WITH INDEX idx_ts WHERE ts <= d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM t WITH NOINDEX WHERE ts <= d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM t WITH INDEX idx_ts WHERE ts < d'2026-08-28T09:00:01Z' ORDER BY id;
SELECT VALUE id FROM t WITH NOINDEX WHERE ts < d'2026-08-28T09:00:01Z' ORDER BY id;
SELECT VALUE id FROM t WITH INDEX idx_ts WHERE ts > d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM t WITH NOINDEX WHERE ts > d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM t WITH INDEX idx_ts WHERE ts > d'2026-08-28T09:00:00Z' AND ts < d'2026-08-28T09:00:01Z' ORDER BY id;
SELECT VALUE id FROM t WITH NOINDEX WHERE ts > d'2026-08-28T09:00:00Z' AND ts < d'2026-08-28T09:00:01Z' ORDER BY id;

-- Equality seeks: a whole second, a fraction, and a fraction spelled at another precision.
SELECT VALUE id FROM t WITH INDEX idx_ts WHERE ts = d'2026-08-28T09:00:00Z';
SELECT VALUE id FROM t WITH NOINDEX WHERE ts = d'2026-08-28T09:00:00Z';
SELECT VALUE id FROM t WITH INDEX idx_ts WHERE ts = d'2026-08-28T09:00:00.152Z';
SELECT VALUE id FROM t WITH NOINDEX WHERE ts = d'2026-08-28T09:00:00.152Z';
SELECT VALUE id FROM t WITH INDEX idx_ts WHERE ts = d'2026-08-28T09:00:00.100000000Z';
SELECT VALUE id FROM t WITH NOINDEX WHERE ts = d'2026-08-28T09:00:00.100000000Z';

-- Bounds written with a UTC offset name the same instants.
SELECT VALUE id FROM t WITH INDEX idx_ts WHERE ts >= d'2026-08-28T11:00:00.152+02:00' AND ts <= d'2026-08-28T11:00:01+02:00' ORDER BY id;
SELECT VALUE id FROM t WITH NOINDEX WHERE ts >= d'2026-08-28T11:00:00.152+02:00' AND ts <= d'2026-08-28T11:00:01+02:00' ORDER BY id;

-- A compound index: equality on the leading column, a range on the Datetime.
SELECT VALUE id FROM t WITH INDEX idx_state_ts WHERE state = 'PENDING' AND ts <= d'2026-08-28T09:00:00.1523Z' ORDER BY id;
SELECT VALUE id FROM t WITH NOINDEX WHERE state = 'PENDING' AND ts <= d'2026-08-28T09:00:00.1523Z' ORDER BY id;
SELECT VALUE id FROM t WITH INDEX idx_state_ts WHERE state = 'PENDING' AND ts >= d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM t WITH NOINDEX WHERE state = 'PENDING' AND ts >= d'2026-08-28T09:00:00Z' ORDER BY id;

-- Signed years order and bound by time.
SELECT VALUE id FROM era WITH INDEX idx_era_ts ORDER BY ts ASC;
SELECT VALUE id FROM era WITH NOINDEX ORDER BY ts ASC;
SELECT VALUE id FROM era WITH INDEX idx_era_ts WHERE ts < d'0000-01-01T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM era WITH NOINDEX WHERE ts < d'0000-01-01T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM era WITH INDEX idx_era_ts WHERE ts > d'9999-01-01T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM era WITH NOINDEX WHERE ts > d'9999-01-01T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM era WITH INDEX idx_era_ts WHERE ts = d'-0100-01-01T00:00:00Z';
SELECT VALUE id FROM era WITH NOINDEX WHERE ts = d'-0100-01-01T00:00:00Z';
