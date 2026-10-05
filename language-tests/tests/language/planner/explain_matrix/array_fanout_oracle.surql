/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
	Oracle suite for array fan-out: a b-tree index column holding an array
	contributes one key per element, so one record is reachable through several
	keys. A scan whose key range does not pin every fanning column is wrapped in
	`Distinct`, which collapses those entries back to one row per record, so
	each matching record reaches the rest of the plan exactly once. Under
	EXPLAIN ANALYZE the scan still reports the ENTRY count in its `{rows: N}`
	and the `Distinct` above it reports the RECORD count. A key range that pins
	every fanning column reaches each record through one key and carries no
	`Distinct`; a single-value containment leaf pins its column exactly as an
	equality does, and the prefix extends as far as consecutive pinned columns
	reach. Both halves of that contrast are asserted here on one index over two
	fanning columns: the presence or absence of the node is the only observable
	difference between an exact seek and a seek-then-collapse.

	Every indexed query here is paired with the same query under WITH NOINDEX —
	a table scan, and therefore the oracle for what the rows are — and every
	query touching a fanning column is also paired on `count() GROUP ALL`. The
	index-only count path answers from the key space without fetching a record,
	so it can neither collapse entries nor re-check a predicate; it therefore
	declines a scan that fans out, and equally declines one carrying a residual
	Filter. `IndexCountScan` is reached only where the key range proves the whole
	predicate with nothing left to re-check above the scan: a containment leaf
	is proved that way when the index is the single column it names, while over
	a compound index it survives as a Filter and the count runs through the row
	path. An `EXPLAIN` over each count is asserted beside the number so that the
	choice of path is visible as the presence or absence of `IndexCountScan`
	rather than only as a total that happens to agree.

	The `predicate:` attribute on IndexScan is what distinguishes an index that
	narrowed from one that also proved the predicate: a scan carrying both a
	narrow `access:` and a `predicate:` re-checks every fetched record, so its
	rows can be right while an index-only count over the same predicate is
	wrong.

	This file owns the fan-out KEY MODEL. How far a compound prefix reaches over
	a fanning column, and what a containment leaf contributes to it, is a
	different dimension and is pinned in compound_prefix.surql; the compound
	fixtures here exist for what their key sets contain — a placeholder key for
	an empty and for an absent array, one key for a repeated element, and the
	full cartesian product of two fanning columns — not for prefix construction.

	Three behaviours recorded here are wrong, and are recorded as they are so a
	fix is recognisable rather than being bent out of the suite. All three are
	the same gap: the write path expands any array VALUE, while the planner's
	fan-out guards key off a column spelling `.*`. A plain column whose field
	declares a kind admitting an array is planned as its `.*` spelling and is
	guarded (reproductions/7539_*); a plain column with no declared kind, as
	every table here is, is planned as written, so when its value happens to be
	an array it fans out unguarded. Each of the three has a `.*` counterpart
	elsewhere in the file which behaves correctly over the same shape, so the
	gap is legible as the difference between the two.

	1. Whole-array equality against a plain index over that array column
	   (`tags = ['x', 'y']`) seeks the single key `['x', 'y']`, which the
	   per-element write never made, and returns []. The oracle returns the
	   record; [] is wrong. Its count pair is 0 against an oracle of 1. The key
	   is missing the same way when the array column is the trailing column of
	   an exact compound prefix, which is asserted too: the range is exact and
	   still addresses nothing, and that count is 0 against an oracle of 1 as
	   well.
	2. `count()` over element equality on that same column (`tags = 'y'`)
	   returns 2 where the oracle returns 0: IndexCountScan counts the two index
	   entries carrying 'y' and, unlike the IndexScan that serves the row query,
	   never applies the per-record re-check that makes the row answer 0
	   correct.
	3. A compound index whose trailing column is plain and holds an array,
	   seeked on the leading column alone, emits one row per element rather than
	   one per record, and its count inflates the same way: three rows and a
	   count of 3 against an oracle of two records. The scan carries no Distinct
	   and the count reaches IndexCountScan. The `.*` spelling of the same index
	   over the same shape is the sparse case below, which gets both.

	One behaviour is correct but easy to mistake for a defect, so it is pinned
	as intended: a containment predicate on a non-flattening column with no
	declared kind is not served by the index at all — a table scan, with the
	right rows and the right count — because a containment leaf is matched to a
	column only when the column flattens, and nothing declares that this one's
	values are arrays.

	One behaviour is recorded without being judged: under UNIQUE over a fanning
	column, a record whose own array repeats an element is rejected as colliding
	with itself and is not stored, while the same repeat under a non-unique
	index collapses to one key and is stored. The two are inconsistent, and
	which is right is not settled here; the expectation is the current
	behaviour, so a change to it is visible rather than silent.

	Narrowed to the streaming strategies because the file asserts EXPLAIN plans,
	which compute-only does not render; both are pinned because they plan
	identically by contract.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: tagged:both, tags: ['x', 'y'] }]"

[[test.results]]
value = "[{ id: tagged:one, tags: ['y'] }]"

[[test.results]]
value = "[tagged:both]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: tagged_tags, access: = ['x', 'y'], direction: Forward, predicate: tags = ['x', 'y']]
"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: tagged, condition: tags = ['x', 'y']]
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 0}
    IndexScan [ctx: Db] [index: tagged_tags, access: = 'y', direction: Forward, predicate: tags = 'y'] {rows: 0}

Total rows: 0"'''

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: tagged, condition: tags = 'y']
"'''

[[test.results]]
value = "[tagged:both, tagged:one]"

[[test.results]]
value = "[tagged:both, tagged:one]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: tagged, direction: Forward, predicate: tags CONTAINS 'y', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    TableScan [ctx: Db] [table: tagged, direction: Forward, predicate: tags CONTAINS 'y', pre_decode_filter: yes]
"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ grp: 'g', id: plainfan:two, tags: ['x', 'y'] }]"

[[test.results]]
value = "[{ grp: 'g', id: plainfan:one, tags: ['z'] }]"

[[test.results]]
value = "[plainfan:one, plainfan:two]"

[[test.results]]
value = "[plainfan:two, plainfan:two, plainfan:one]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 3}
    IndexScan [ctx: Db] [index: plainfan_grp_tags, access: ['g'], direction: Forward, predicate: grp = 'g'] {rows: 3}

Total rows: 3"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: plainfan, condition: grp = 'g']
"'''

[[test.results]]
value = "[plainfan:two]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: plainfan_grp_tags, access: ['g', ['x', 'y']], direction: Forward, predicate: grp = 'g' AND tags = ['x', 'y']]
"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: plainfan, condition: grp = 'g' AND tags = ['x', 'y']]
"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: uniqf:1, labels: ['p', 'q'] }]"

[[test.results]]
error = "Database index `uniqf_labels` already contains 'q', with record `uniqf:1`"

[[test.results]]
value = "[{ id: uniqf:3, labels: ['s', 't'] }]"

[[test.results]]
error = "Database index `uniqf_labels` already contains 'u', with record `uniqf:4`"

[[test.results]]
value = "[uniqf:1, uniqf:3]"

[[test.results]]
value = "[uniqf:1]"

[[test.results]]
value = "[uniqf:1]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 1}
    IndexScan [ctx: Db] [index: uniqf_labels, access: = 'q', direction: Forward, predicate: labels.* CONTAINS 'q'] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: uniqf, condition: labels.* CONTAINS 'q']
"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: ['p', 'q'], b: ['1', '2'], id: prod:one }]"

[[test.results]]
error = "Database index `prod_ab` already contains ['p', '1'], with record `prod:one`"

[[test.results]]
error = "Database index `prod_ab` already contains ['p', '2'], with record `prod:one`"

[[test.results]]
error = "Database index `prod_ab` already contains ['q', '1'], with record `prod:one`"

[[test.results]]
error = "Database index `prod_ab` already contains ['q', '2'], with record `prod:one`"

[[test.results]]
value = "[{ a: ['z'], b: ['1'], id: prod:probe_z1 }]"

[[test.results]]
value = "[prod:one]"

[[test.results]]
value = "[prod:one]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 1}
    DistinctRecords [ctx: Db] {rows: 1}
        IndexScan [ctx: Db] [index: prod_ab, access: ['p'], direction: Forward, predicate: a.* CONTAINS 'p'] {rows: 2}

Total rows: 1"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    DistinctRecords [ctx: Db]
        IndexScan [ctx: Db] [index: prod_ab, access: ['p'], direction: Forward, predicate: a.* CONTAINS 'p']
"'''

[[test.results]]
value = "[prod:one]"

[[test.results]]
value = "[prod:one]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 1}
    IndexScan [ctx: Db] [index: prod_ab, access: ['p', '2'], direction: Forward, predicate: a.* CONTAINS 'p' AND b.* CONTAINS '2'] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: prod, condition: a.* CONTAINS 'p' AND b.* CONTAINS '2']
"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ grp: 'g', id: sparse:empty, labels: [] }]"

[[test.results]]
value = "[{ grp: 'g', id: sparse:missing }]"

[[test.results]]
value = "[{ grp: 'g', id: sparse:two, labels: ['l1', 'l2'] }]"

[[test.results]]
value = "[sparse:empty, sparse:missing, sparse:two]"

[[test.results]]
value = "[sparse:missing, sparse:empty, sparse:two]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 3}
    DistinctRecords [ctx: Db] {rows: 3}
        IndexScan [ctx: Db] [index: sparse_grp_labels, access: ['g'], direction: Forward, predicate: grp = 'g'] {rows: 4}

Total rows: 3"'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    DistinctRecords [ctx: Db]
        IndexScan [ctx: Db] [index: sparse_grp_labels, access: ['g'], direction: Forward, predicate: grp = 'g']
"'''

[[test.results]]
value = "[sparse:two]"

[[test.results]]
value = "[sparse:two]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 1}
    IndexScan [ctx: Db] [index: sparse_grp_labels, access: ['g', 'l1'], direction: Forward, predicate: grp = 'g' AND labels.* CONTAINS 'l1'] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: sparse, condition: grp = 'g' AND labels.* CONTAINS 'l1']
"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ grp: 'd', id: dup:one, labels: ['same', 'same', 'other'] }]"

[[test.results]]
value = "[dup:one]"

[[test.results]]
value = "[dup:one]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 1}
    DistinctRecords [ctx: Db] {rows: 1}
        IndexScan [ctx: Db] [index: dup_grp_labels, access: ['d'], direction: Forward, predicate: grp = 'd'] {rows: 2}

Total rows: 1"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    DistinctRecords [ctx: Db]
        IndexScan [ctx: Db] [index: dup_grp_labels, access: ['d'], direction: Forward, predicate: grp = 'd']
"'''

[[test.results]]
value = "[dup:one]"

[[test.results]]
value = "[dup:one]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 1}
    IndexScan [ctx: Db] [index: dup_grp_labels, access: ['d', 'same'], direction: Forward, predicate: grp = 'd' AND labels.* CONTAINS 'same'] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: dup, condition: grp = 'd' AND labels.* CONTAINS 'same']
"'''

*/

-- ---------------------------------------------------------------------------
-- A plain (non-flattening) index column whose value is an array. The write
-- path expands any array value, so the keys for tagged:both are 'x' and 'y' —
-- never the array itself — while the dedupe and the count guard read the column
-- idiom, which here does not spell `.*`. No scan in this section carries a
-- Distinct. tagged:both holds two distinct elements because a single-element
-- array cannot show duplicate emission, count inflation or a missing key.
-- ---------------------------------------------------------------------------
DEFINE TABLE tagged SCHEMALESS;
DEFINE INDEX tagged_tags ON tagged COLUMNS tags;
CREATE tagged:both SET tags = ['x', 'y'];
CREATE tagged:one SET tags = ['y'];

-- (a) Whole-array equality. The scan seeks the key ['x', 'y'], which the
-- per-element write never made, so the record is unreachable through the index
-- and the answer disagrees with the oracle. The plan shows the index both
-- narrowing and re-checking, which is why the wrong answer is a missing key
-- rather than a missing filter. Correct answer: [tagged:both], and a count
-- of 1.
SELECT VALUE id FROM tagged WITH NOINDEX WHERE tags = ['x', 'y'];
SELECT VALUE id FROM tagged WHERE tags = ['x', 'y'];
EXPLAIN SELECT VALUE id FROM tagged WHERE tags = ['x', 'y'];
SELECT count() FROM tagged WITH NOINDEX WHERE tags = ['x', 'y'] GROUP ALL;
SELECT count() FROM tagged WHERE tags = ['x', 'y'] GROUP ALL;
EXPLAIN SELECT count() FROM tagged WHERE tags = ['x', 'y'] GROUP ALL;

-- (b) Element equality. Both records own a 'y' key, so the scan fetches both;
-- `tags = 'y'` compares an array against a string and is false for each, so
-- the rows agree with the oracle only because the scan re-checks the
-- predicate per fetched record. The `predicate:` attribute beside a narrow
-- `access:` is that re-check, and {rows: 0} under a scan that seeked two keys
-- is it working.
SELECT VALUE id FROM tagged WITH NOINDEX WHERE tags = 'y';
SELECT VALUE id FROM tagged WHERE tags = 'y';
EXPLAIN ANALYZE SELECT VALUE id FROM tagged WHERE tags = 'y';

-- (c) The same predicate counted. IndexCountScan answers from the key space
-- alone — no record is fetched, so the re-check that makes (b) right cannot
-- run — and reports the two entries carrying 'y'. Correct answer: 0, the
-- oracle's count and the length of (b)'s row answer.
SELECT count() FROM tagged WITH NOINDEX WHERE tags = 'y' GROUP ALL;
SELECT count() FROM tagged WHERE tags = 'y' GROUP ALL;
EXPLAIN SELECT count() FROM tagged WHERE tags = 'y' GROUP ALL;

-- Containment against the same column, which is the predicate the element
-- keys actually answer, is not served by this index: a containment leaf is
-- matched to a column only when the column flattens (`.*`). The rows and the
-- count are right, by table scan; pinned so the table scan is read as the
-- intended shape for a non-flattening column rather than as a lost index. The
-- count is planned over the same table scan rather than over the key space,
-- which is why it agrees with the oracle where (c) does not.
SELECT VALUE id FROM tagged WITH NOINDEX WHERE tags CONTAINS 'y';
SELECT VALUE id FROM tagged WHERE tags CONTAINS 'y';
EXPLAIN SELECT VALUE id FROM tagged WHERE tags CONTAINS 'y';
SELECT count() FROM tagged WITH NOINDEX WHERE tags CONTAINS 'y' GROUP ALL;
SELECT count() FROM tagged WHERE tags CONTAINS 'y' GROUP ALL;
EXPLAIN SELECT count() FROM tagged WHERE tags CONTAINS 'y' GROUP ALL;

-- ---------------------------------------------------------------------------
-- (d) A compound index whose TRAILING column is plain and holds an array. The
-- write path expands the value, so the key set is the same one the `.*`
-- spelling would produce, but the guards read the idiom and find no `.*`: the
-- unpinned trailing column multiplies the rows and the count with no Distinct
-- to collapse them, and the count still reaches the key space. The sparse
-- section below is this same index and this same seek with `labels.*` in place
-- of `tags`, and is the shape both guards do cover. Correct answer here:
-- [plainfan:one, plainfan:two] once each, and a count of 2 — the oracle's.
-- ---------------------------------------------------------------------------
DEFINE TABLE plainfan SCHEMALESS;
DEFINE INDEX plainfan_grp_tags ON plainfan COLUMNS grp, tags;
CREATE plainfan:two SET grp = 'g', tags = ['x', 'y'];
CREATE plainfan:one SET grp = 'g', tags = ['z'];

SELECT VALUE id FROM plainfan WITH NOINDEX WHERE grp = 'g';
SELECT VALUE id FROM plainfan WHERE grp = 'g';
EXPLAIN ANALYZE SELECT VALUE id FROM plainfan WHERE grp = 'g';
SELECT count() FROM plainfan WITH NOINDEX WHERE grp = 'g' GROUP ALL;
SELECT count() FROM plainfan WHERE grp = 'g' GROUP ALL;
EXPLAIN SELECT count() FROM plainfan WHERE grp = 'g' GROUP ALL;

-- Pinning the trailing column makes the range exact, so the element keys
-- answer an equality on the whole array value no more than they do in (a):
-- the seek is for the key ['g', ['x', 'y']], which the per-element write never
-- made. The count over the same predicate takes the key space, where the
-- exact range addresses nothing either, so it reports 0. Correct answer:
-- [plainfan:two] and a count of 1.
SELECT VALUE id FROM plainfan WITH NOINDEX WHERE grp = 'g' AND tags = ['x', 'y'];
SELECT VALUE id FROM plainfan WHERE grp = 'g' AND tags = ['x', 'y'];
EXPLAIN SELECT VALUE id FROM plainfan WHERE grp = 'g' AND tags = ['x', 'y'];
SELECT count() FROM plainfan WITH NOINDEX WHERE grp = 'g' AND tags = ['x', 'y'] GROUP ALL;
SELECT count() FROM plainfan WHERE grp = 'g' AND tags = ['x', 'y'] GROUP ALL;
EXPLAIN SELECT count() FROM plainfan WHERE grp = 'g' AND tags = ['x', 'y'] GROUP ALL;

-- ---------------------------------------------------------------------------
-- UNIQUE over a single fanning column. Uniqueness is a property of the keys,
-- so it is enforced per element: a second record sharing one element is
-- rejected. A record whose own array repeats an element is rejected too,
-- naming itself as the holder, and is not stored — which the non-unique dup
-- case below contradicts, where the repeat collapses to one key and the
-- record is stored. The suite records that behaviour rather than deciding it
-- is right, so a change to it shows up here.
-- ---------------------------------------------------------------------------
DEFINE TABLE uniqf SCHEMALESS;
DEFINE INDEX uniqf_labels ON uniqf COLUMNS labels.* UNIQUE;
CREATE uniqf:1 SET labels = ['p', 'q'];
CREATE uniqf:2 SET labels = ['q', 'r'];
CREATE uniqf:3 SET labels = ['s', 't'];
CREATE uniqf:4 SET labels = ['u', 'u'];

-- The rejected records are absent: a per-element collision fails the whole
-- CREATE, so neither uniqf:2 nor uniqf:4 contributes any key.
SELECT VALUE id FROM uniqf WITH NOINDEX;

-- A containment leaf pins the one fanning column to a single element, so the
-- seek is exact: one entry per matching record and no Distinct above the
-- scan. The predicate is a single leaf proved by the key range, leaving no
-- Filter, so this is the one seek over a fanning column here that the
-- index-only count path accepts. The scan still carries a `predicate:` and
-- re-checks, which is what keeps the rows right if the key set ever stops
-- being unique.
SELECT VALUE id FROM uniqf WITH NOINDEX WHERE labels.* CONTAINS 'q';
SELECT VALUE id FROM uniqf WHERE labels.* CONTAINS 'q';
EXPLAIN ANALYZE SELECT VALUE id FROM uniqf WHERE labels.* CONTAINS 'q';
SELECT count() FROM uniqf WITH NOINDEX WHERE labels.* CONTAINS 'q' GROUP ALL;
SELECT count() FROM uniqf WHERE labels.* CONTAINS 'q' GROUP ALL;
EXPLAIN SELECT count() FROM uniqf WHERE labels.* CONTAINS 'q' GROUP ALL;

-- ---------------------------------------------------------------------------
-- An index over TWO fanning columns. The key set is the cartesian product of
-- the two columns' elements, so prod:one owns four keys. The product is
-- asserted through UNIQUE collisions: a probe record claiming one combination
-- is rejected exactly when that key exists, so a rejection proves the key is
-- present and a success proves it is missing. The probes cover all four
-- combinations, so a key set narrower than the full product leaves a probe
-- succeeding; prod:probe_z1 claims a combination the product cannot contain
-- and is the control that the probes can succeed at all.
-- ---------------------------------------------------------------------------
DEFINE TABLE prod SCHEMALESS;
DEFINE INDEX prod_ab ON prod COLUMNS a.*, b.* UNIQUE;
CREATE prod:one SET a = ['p', 'q'], b = ['1', '2'];
CREATE prod:probe_p1 SET a = ['p'], b = ['1'];
CREATE prod:probe_p2 SET a = ['p'], b = ['2'];
CREATE prod:probe_q1 SET a = ['q'], b = ['1'];
CREATE prod:probe_q2 SET a = ['q'], b = ['2'];
-- A combination the product does not contain: no key, so no collision.
CREATE prod:probe_z1 SET a = ['z'], b = ['1'];

-- (e) Naming one of the two fanning columns pins that column alone: the prefix
-- stops where the pinned columns stop, so column b still multiplies the keys
-- inside the range. The scan therefore visits one entry per element of b and
-- the Distinct above it yields the record once, which is the oracle's row and
-- the oracle's count. The count goes through the row path for the same reason,
-- so no IndexCountScan appears.
SELECT VALUE id FROM prod WITH NOINDEX WHERE a.* CONTAINS 'p';
SELECT VALUE id FROM prod WHERE a.* CONTAINS 'p';
EXPLAIN ANALYZE SELECT VALUE id FROM prod WHERE a.* CONTAINS 'p';
SELECT count() FROM prod WITH NOINDEX WHERE a.* CONTAINS 'p' GROUP ALL;
SELECT count() FROM prod WHERE a.* CONTAINS 'p' GROUP ALL;
EXPLAIN SELECT count() FROM prod WHERE a.* CONTAINS 'p' GROUP ALL;

-- Naming both columns pins both, so the prefix covers the whole index and the
-- range is exact: a single entry, and no Distinct — the contrast with (e) over
-- the same index and the same record. The trailing containment leaf is
-- re-checked by a Filter above the scan rather than proved by the range, so the
-- count stays on the row path.
SELECT VALUE id FROM prod WITH NOINDEX WHERE a.* CONTAINS 'p' AND b.* CONTAINS '2';
SELECT VALUE id FROM prod WHERE a.* CONTAINS 'p' AND b.* CONTAINS '2';
EXPLAIN ANALYZE SELECT VALUE id FROM prod WHERE a.* CONTAINS 'p' AND b.* CONTAINS '2';
SELECT count() FROM prod WITH NOINDEX WHERE a.* CONTAINS 'p' AND b.* CONTAINS '2' GROUP ALL;
SELECT count() FROM prod WHERE a.* CONTAINS 'p' AND b.* CONTAINS '2' GROUP ALL;
EXPLAIN SELECT count() FROM prod WHERE a.* CONTAINS 'p' AND b.* CONTAINS '2' GROUP ALL;

-- ---------------------------------------------------------------------------
-- A fanning column with nothing to fan out. An empty array and an absent
-- field both still have to put the record in the index, or a seek on the
-- leading column would lose it altogether.
-- ---------------------------------------------------------------------------
DEFINE TABLE sparse SCHEMALESS;
DEFINE INDEX sparse_grp_labels ON sparse COLUMNS grp, labels.*;
CREATE sparse:empty SET grp = 'g', labels = [];
CREATE sparse:missing SET grp = 'g';
CREATE sparse:two SET grp = 'g', labels = ['l1', 'l2'];

-- (f) An equality on the leading column alone leaves the trailing fanning
-- column unpinned. All three records are reachable, which is what the
-- placeholder keys buy, and sparse:two is reachable through two entries: the
-- scan reports the entry count and the Distinct above it the record count.
-- Both the rows and the count agree with the oracle, the count because the
-- fast path declines the fanning scan and falls back to the row path. The row
-- order is the key order, so the placeholder keys of the empty and absent
-- cases sort ahead of the elements.
SELECT VALUE id FROM sparse WITH NOINDEX WHERE grp = 'g';
SELECT VALUE id FROM sparse WHERE grp = 'g';
EXPLAIN ANALYZE SELECT VALUE id FROM sparse WHERE grp = 'g';
SELECT count() FROM sparse WITH NOINDEX WHERE grp = 'g' GROUP ALL;
SELECT count() FROM sparse WHERE grp = 'g' GROUP ALL;
EXPLAIN SELECT count() FROM sparse WHERE grp = 'g' GROUP ALL;

-- Pinning the fanning column with a containment leaf makes the prefix exact:
-- one entry per matching record, and no Distinct — the contrast with (f) over
-- the same index. The records with no element own no key under the extended
-- prefix. The containment leaf is re-checked by a Filter above the scan
-- rather than proved by the range, so the count stays on the row path:
-- a residual Filter declines the fast path independently of fan-out.
SELECT VALUE id FROM sparse WITH NOINDEX WHERE grp = 'g' AND labels.* CONTAINS 'l1';
SELECT VALUE id FROM sparse WHERE grp = 'g' AND labels.* CONTAINS 'l1';
EXPLAIN ANALYZE SELECT VALUE id FROM sparse WHERE grp = 'g' AND labels.* CONTAINS 'l1';
SELECT count() FROM sparse WITH NOINDEX WHERE grp = 'g' AND labels.* CONTAINS 'l1' GROUP ALL;
SELECT count() FROM sparse WHERE grp = 'g' AND labels.* CONTAINS 'l1' GROUP ALL;
EXPLAIN SELECT count() FROM sparse WHERE grp = 'g' AND labels.* CONTAINS 'l1' GROUP ALL;

-- ---------------------------------------------------------------------------
-- Duplicate elements in one array. Identical tuples encode to one key, so the
-- store holds one entry for the repeat: the record owns one key per DISTINCT
-- element, not one per element.
-- ---------------------------------------------------------------------------
DEFINE TABLE dup SCHEMALESS;
DEFINE INDEX dup_grp_labels ON dup COLUMNS grp, labels.*;
CREATE dup:one SET grp = 'd', labels = ['same', 'same', 'other'];

-- Two entries under the unpinned prefix, not three: the repeated element
-- collapses in the store, so the scan's own {rows: N} is the distinct-element
-- count. The Distinct above it yields the one record, matching the oracle's
-- row and count.
SELECT VALUE id FROM dup WITH NOINDEX WHERE grp = 'd';
SELECT VALUE id FROM dup WHERE grp = 'd';
EXPLAIN ANALYZE SELECT VALUE id FROM dup WHERE grp = 'd';
SELECT count() FROM dup WITH NOINDEX WHERE grp = 'd' GROUP ALL;
SELECT count() FROM dup WHERE grp = 'd' GROUP ALL;
EXPLAIN SELECT count() FROM dup WHERE grp = 'd' GROUP ALL;

-- Seeking the repeated element reaches the one collapsed key, so the prefix is
-- exact and the scan carries no Distinct. As in the sparse case, the
-- containment leaf survives as a Filter above the scan, which keeps the count
-- on the row path.
SELECT VALUE id FROM dup WITH NOINDEX WHERE grp = 'd' AND labels.* CONTAINS 'same';
SELECT VALUE id FROM dup WHERE grp = 'd' AND labels.* CONTAINS 'same';
EXPLAIN ANALYZE SELECT VALUE id FROM dup WHERE grp = 'd' AND labels.* CONTAINS 'same';
SELECT count() FROM dup WITH NOINDEX WHERE grp = 'd' AND labels.* CONTAINS 'same' GROUP ALL;
SELECT count() FROM dup WHERE grp = 'd' AND labels.* CONTAINS 'same' GROUP ALL;
EXPLAIN SELECT count() FROM dup WHERE grp = 'd' AND labels.* CONTAINS 'same' GROUP ALL;
