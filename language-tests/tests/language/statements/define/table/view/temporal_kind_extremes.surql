/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ at: ts'2026-10-27T09:00:00', day: pd'2026-10-27', id: shift:1, starts: pt'09:00:00' }]"

[[test.results]]
value = "[{ at: ts'2026-10-25T07:30:00', day: pd'2026-10-25', id: shift:2, starts: pt'07:30:00' }]"

[[test.results]]
value = "[{ at: ts'2026-10-29T11:15:00', day: pd'2026-10-29', id: shift:3, starts: pt'11:15:00' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ earliest: pt'07:30:00', first_at: ts'2026-10-25T07:30:00', first_day: pd'2026-10-25', last_at: ts'2026-10-29T11:15:00', last_day: pd'2026-10-29', latest: pt'11:15:00' }]"

[[test.results]]
value = "[{ earliest: pt'07:30:00', first_at: ts'2026-10-25T07:30:00', first_day: pd'2026-10-25', last_at: ts'2026-10-29T11:15:00', last_day: pd'2026-10-29', latest: pt'11:15:00' }]"

[[test.results]]
value = "[{ d: 'date', t: 'time', ts: 'timestamp' }]"

[[test.results]]
value = "[{ at: ts'2026-11-01T00:00:00', day: pd'2026-10-20', id: shift:4, starts: pt'23:59:59' }]"

[[test.results]]
value = "[{ earliest: pt'07:30:00', first_at: ts'2026-10-25T07:30:00', first_day: pd'2026-10-20', last_at: ts'2026-11-01T00:00:00', last_day: pd'2026-10-29', latest: pt'23:59:59' }]"

[[test.results]]
value = "[{ at: ts'2026-10-26T00:00:00', day: pd'2026-10-28', id: shift:4, starts: pt'08:00:00' }]"

[[test.results]]
value = "[{ earliest: pt'07:30:00', first_at: ts'2026-10-25T07:30:00', first_day: pd'2026-10-25', last_at: ts'2026-10-29T11:15:00', last_day: pd'2026-10-29', latest: pt'11:15:00' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ earliest: pt'08:00:00', first_at: ts'2026-10-26T00:00:00', first_day: pd'2026-10-27', last_at: ts'2026-10-27T09:00:00', last_day: pd'2026-10-28', latest: pt'09:00:00' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ first_day: NONE, last_day: NONE }]"

[[test.results]]
value = "[{ at: ts'2027-03-01T10:00:00', day: pd'2027-03-01', id: shift:5, starts: pt'10:00:00' }]"

[[test.results]]
value = "[{ first_day: pd'2027-03-01', last_day: pd'2027-03-01' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ first_day: NONE, last_day: NONE }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: mixed:1, v: pd'2026-10-27' }]"

[[test.results]]
value = "[{ id: mixed:2, v: pt'09:00:00' }]"

[[test.results]]
value = "[{ last: pd'2026-10-27' }]"

[[test.results]]
value = "[{ last: pd'2026-10-27' }]"

[[test.results]]
value = "[{ id: mixed:2, v: pd'2026-10-30' }]"

[[test.results]]
value = "[{ last: pd'2026-10-30' }]"

[[test.results]]
value = "[{ id: mixed:2, v: pt'10:00:00' }]"

[[test.results]]
value = "[{ last: pd'2026-10-27' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ last: pd'2026-10-27' }]"

[[test.results]]
error = "Incorrect arguments for function time::max(). Argument 1 was the wrong type. Expected `date` but found `'soon'`"
*/

DEFINE TABLE shift;
DEFINE FIELD day ON shift TYPE date;
DEFINE FIELD starts ON shift TYPE time;
DEFINE FIELD at ON shift TYPE timestamp;

CREATE shift:1 SET day = pd'2026-10-27', starts = pt'09:00:00', at = ts'2026-10-27T09:00:00';
CREATE shift:2 SET day = pd'2026-10-25', starts = pt'07:30:00', at = ts'2026-10-25T07:30:00';
CREATE shift:3 SET day = pd'2026-10-29', starts = pt'11:15:00', at = ts'2026-10-29T11:15:00';

# Defined over existing rows: the backfill.
DEFINE TABLE span AS SELECT
	time::min(day) AS first_day, time::max(day) AS last_day,
	time::min(starts) AS earliest, time::max(starts) AS latest,
	time::min(at) AS first_at, time::max(at) AS last_at
	FROM shift GROUP ALL;
SELECT * OMIT id FROM span;
SELECT
	time::min(day) AS first_day, time::max(day) AS last_day,
	time::min(starts) AS earliest, time::max(starts) AS latest,
	time::min(at) AS first_at, time::max(at) AS last_at
	FROM shift GROUP ALL;
SELECT type::of(first_day) AS d, type::of(earliest) AS t, type::of(first_at) AS ts FROM span;

# A new extreme on each side.
CREATE shift:4 SET day = pd'2026-10-20', starts = pt'23:59:59', at = ts'2026-11-01T00:00:00';
SELECT * OMIT id FROM span;

# Updating the extreme rows to retreat from the extremes re-reads the group.
UPDATE shift:4 SET day = pd'2026-10-28', starts = pt'08:00:00', at = ts'2026-10-26T00:00:00';
SELECT * OMIT id FROM span;

# Deleting the extreme rows re-reads the group.
DELETE shift:2;
DELETE shift:3;
SELECT * OMIT id FROM span;

# An empty view takes the kind of its first value.
DEFINE TABLE day_span AS SELECT time::min(day) AS first_day, time::max(day) AS last_day FROM shift WHERE day > pd'2027-01-01' GROUP ALL;
SELECT * OMIT id FROM day_span;
CREATE shift:5 SET day = pd'2027-03-01', starts = pt'10:00:00', at = ts'2027-03-01T10:00:00';
SELECT * OMIT id FROM day_span;
DELETE shift:5;
SELECT * OMIT id FROM day_span;

# A group of two temporal kinds keeps the first it folds and skips the other,
# as the plain aggregate does, through inserts, updates and deletes.
DEFINE TABLE mixed;
DEFINE TABLE mixed_span AS SELECT time::max(v) AS last FROM mixed GROUP ALL;
CREATE mixed:1 SET v = pd'2026-10-27';
CREATE mixed:2 SET v = pt'09:00:00';
SELECT * OMIT id FROM mixed_span;
SELECT time::max(v) AS last FROM mixed GROUP ALL;
UPDATE mixed:2 SET v = pd'2026-10-30';
SELECT * OMIT id FROM mixed_span;
UPDATE mixed:2 SET v = pt'10:00:00';
SELECT * OMIT id FROM mixed_span;
DELETE mixed:2;
SELECT * OMIT id FROM mixed_span;
# A value of no temporal kind is refused.
CREATE mixed:3 SET v = 'soon';
