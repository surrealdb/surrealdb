/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "`string::starts_with(<indexed column>, '<literal>')` over a b-tree index, end to end. The strings carrying a prefix are exactly those at or after it and before its successor, and the key encoding orders strings the way the predicate compares them, so the predicate names one contiguous key range. This file pins where each bound lands, when the upper bound must be absent, and which spellings name no range at all. Every indexed query is paired with the same query `WITH NOINDEX`, which forces a table scan and is the oracle: an upper bound placed one string too low returns a subset while still rendering an IndexScan, and a silently dropped row is the failure this file exists to catch. The fixture is built so a bound in the wrong place moves a row rather than only a plan. Around `alpha` it carries the prefix itself, a longer string extending it, `alpha` followed by the last scalar value, a string extending THAT, and the successor `alphb`, which carries the prefix in neither direction and must not be returned. The last two are a pair and are what separates the correct upper bound from an upper bound of the prefix followed by the last scalar value: both admit the shorter string, only the correct one admits the longer, so a bound stopping at the largest single-character extension shows up as a missing row rather than passing unnoticed. `alp`, `ALPHA` and `beta` sit below and above the stretch. The successor is exact in both of its forms -- over string values `[prefix, successor)` and, where no successor can be formed, `[prefix, ..)` each hold precisely the strings carrying the prefix -- so the call is kept as a residual `Filter` rather than because the range needs narrowing, and that `Filter` is asserted on every indexed shape; where no index is reached the call is instead carried inline as the scan's own `predicate:` with no `Filter` above it, so the two outcomes are told apart by which node holds the call. E2 is where the residual carries something no range can: the equality on the trailing column lives there, so dropping the residual returns every row the range admits instead of the one row wanted. The last scalar value gets its own group: at the end of a longer prefix the increment carries into the character before it, so that prefix runs up to `alphb` -- the same upper bound the plain `alpha` has, reached from the other side -- while standing alone it has no successor at all and the range carries a lower bound only. An open end is the only correct answer there, since an invented upper bound would sit below the very strings the predicate selects; both cases carry a string extending the last scalar value so that an invented bound moves a row. A multibyte prefix steps by scalar value rather than by byte, so `hé` runs up to `hê` and `hêlp` is the first string above the stretch. Four spellings reach no index and pin their TableScan, because a shape silently acquiring an IndexScan is as much a regression as one silently losing it: an empty prefix, whose stretch is the whole key space; the call against an unindexed column; the arguments reversed, which asks which names are prefixes OF a literal rather than which carry one; and a second argument that is not a literal, whose bounds cannot be built at plan time. A compound index is seeked on its leading column and not on a trailing one, and an equality on the trailing column stays in the residual rather than pinning anything behind a range. A UNIQUE index constrains what may be written, not how the key space is ordered, and addresses the same stretch the same way over the same boundary strings. Every indexed column here holds a scalar string, so nothing fans out to more than one index entry per record and no count() companion is needed; the fixture keeps to strings deliberately, because `string::starts_with` errors on a non-string first argument, which the table-scan oracle would raise and a range that filtered the value away before the call would not. `EXPLAIN` is rejected by the compute-only engine, so the planner axis is narrowed to the two streaming strategies; they plan identically by contract, so both are pinned rather than one. Each plan is asserted on the query without `ORDER BY` and the rows on the same query with `ORDER BY id`, which makes the row comparison deterministic without putting a sort wrapper into every plan."

[[test.results]]
value = "NONE"

[[test.results]]
value = "['alpha', 'alphabet', 'alpha􏿿', 'alpha􏿿tail', 'alphb', 'alp', 'ALPHA', 'beta', 'hé', 'héllo', 'hêlp', '􏿿', '􏿿tail']"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: string::starts_with(...)]
        IndexScan [ctx: Db] [index: ps_name, access: >='alpha' <'alphb', direction: Forward]
"'''

[[test.results]]
value = "[ps:1, ps:2, ps:3, ps:4]"

[[test.results]]
value = "[ps:1, ps:2, ps:3, ps:4]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: string::starts_with(...)]
        IndexScan [ctx: Db] [index: ps_name, access: >='alphab' <'alphac', direction: Forward]
"'''

[[test.results]]
value = "[ps:2]"

[[test.results]]
value = "[ps:2]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: string::starts_with(...)]
        IndexScan [ctx: Db] [index: ps_name, access: >='zzz' <'zz{', direction: Forward]
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: string::starts_with(...)]
        IndexScan [ctx: Db] [index: ps_name, access: >='ALPHA' <'ALPHB', direction: Forward]
"'''

[[test.results]]
value = "[ps:7]"

[[test.results]]
value = "[ps:7]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: string::starts_with(...)]
        IndexScan [ctx: Db] [index: ps_name, access: >='hé' <'hê', direction: Forward]
"'''

[[test.results]]
value = "[ps:9, ps:10]"

[[test.results]]
value = "[ps:9, ps:10]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: string::starts_with(...)]
        IndexScan [ctx: Db] [index: ps_name, access: >='alpha􏿿' <'alphb', direction: Forward]
"'''

[[test.results]]
value = "[ps:3, ps:4]"

[[test.results]]
value = "[ps:3, ps:4]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: string::starts_with(...)]
        IndexScan [ctx: Db] [index: ps_name, access: >='􏿿', direction: Forward]
"'''

[[test.results]]
value = "[ps:12, ps:13]"

[[test.results]]
value = "[ps:12, ps:13]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: ps, direction: Forward, predicate: string::starts_with(...), pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = "[ps:1, ps:2, ps:3, ps:4, ps:5, ps:6, ps:7, ps:8, ps:9, ps:10, ps:11, ps:12, ps:13]"

[[test.results]]
value = "[ps:1, ps:2, ps:3, ps:4, ps:5, ps:6, ps:7, ps:8, ps:9, ps:10, ps:11, ps:12, ps:13]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: ps, direction: Forward, predicate: string::starts_with(...), pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = "[ps:1, ps:2, ps:3, ps:4]"

[[test.results]]
value = "[ps:1, ps:2, ps:3, ps:4]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: ps, direction: Forward, predicate: string::starts_with(...), pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = "[ps:1, ps:6]"

[[test.results]]
value = "[ps:1, ps:6]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: ps, direction: Forward, predicate: string::starts_with(...), pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = "[ps:1, ps:2, ps:3, ps:4]"

[[test.results]]
value = "[ps:1, ps:2, ps:3, ps:4]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: string::starts_with(...)]
        IndexScan [ctx: Db] [index: pc_name_tier, access: >='alpha' <'alphb', direction: Forward]
"'''

[[test.results]]
value = "[pc:1, pc:2, pc:3, pc:4]"

[[test.results]]
value = "[pc:1, pc:2, pc:3, pc:4]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: string::starts_with(...) AND tier = 'beta']
        IndexScan [ctx: Db] [index: pc_name_tier, access: >='alpha' <'alphb', direction: Forward]
"'''

[[test.results]]
value = "[pc:2]"

[[test.results]]
value = "[pc:2]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: pc, direction: Forward, predicate: string::starts_with(...), pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = "[pc:1, pc:3, pc:4, pc:5]"

[[test.results]]
value = "[pc:1, pc:3, pc:4, pc:5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: string::starts_with(...)]
        IndexScan [ctx: Db] [index: pu_name, access: >='alpha' <'alphb', direction: Forward]
"'''

[[test.results]]
value = "[pu:1, pu:2, pu:3, pu:4]"

[[test.results]]
value = "[pu:1, pu:2, pu:3, pu:4]"
*/

-- ===========================================================================
-- Group A: a plain scalar index and an ASCII prefix. The strings carrying a
-- prefix are exactly those at or after it and before its successor, so the
-- predicate names one contiguous stretch of the key space. The fixture holds
-- the strings that decide whether the two bounds are placed correctly: the
-- prefix itself, a string extending it, `alpha` followed by the last scalar
-- value, a string extending THAT, and the successor `alphb` itself, which
-- carries the prefix in neither direction and must not be returned. The last
-- two are a pair: an upper bound of `alpha` followed by the last scalar value
-- admits the shorter of them and drops the longer, so only a correct bound
-- returns both. `alp`, `ALPHA` and `beta` sit below and above the stretch.
-- ===========================================================================

{
    DEFINE TABLE OVERWRITE ps SCHEMALESS;
    DEFINE INDEX OVERWRITE ps_name ON ps FIELDS name;
    CREATE ps:1 SET name = 'alpha', alias = 'alpha', pat = 'alpha';
    CREATE ps:2 SET name = 'alphabet', alias = 'alphabet', pat = 'alpha';
    CREATE ps:3 SET name = "alpha\u{10FFFF}", alias = "alpha\u{10FFFF}", pat = 'alpha';
    CREATE ps:4 SET name = "alpha\u{10FFFF}tail", alias = "alpha\u{10FFFF}tail", pat = 'alpha';
    CREATE ps:5 SET name = 'alphb', alias = 'alphb', pat = 'alpha';
    CREATE ps:6 SET name = 'alp', alias = 'alp', pat = 'alpha';
    CREATE ps:7 SET name = 'ALPHA', alias = 'ALPHA', pat = 'alpha';
    CREATE ps:8 SET name = 'beta', alias = 'beta', pat = 'alpha';
    CREATE ps:9 SET name = "hé", alias = "hé", pat = 'alpha';
    CREATE ps:10 SET name = "héllo", alias = "héllo", pat = 'alpha';
    CREATE ps:11 SET name = "hêlp", alias = "hêlp", pat = 'alpha';
    CREATE ps:12 SET name = "\u{10FFFF}", alias = "\u{10FFFF}", pat = 'alpha';
    CREATE ps:13 SET name = "\u{10FFFF}tail", alias = "\u{10FFFF}tail", pat = 'alpha';
    RETURN NONE;
};

-- A0: the fixture as stored, so the boundary strings the rest of the group
-- turns on are readable without decoding record ids.
SELECT VALUE name FROM ps ORDER BY id;

-- A1: a plain ASCII prefix. The key range runs from the prefix inclusive to
-- its successor exclusive. Both strings built on `alpha` plus the last scalar
-- value are inside it -- an upper bound stopping at the shorter of them would
-- drop the longer -- and `alphb`, the successor itself, is outside it.
EXPLAIN SELECT VALUE id FROM ps WHERE string::starts_with(name, 'alpha');
SELECT VALUE id FROM ps WHERE string::starts_with(name, 'alpha') ORDER BY id;
SELECT VALUE id FROM ps WITH NOINDEX WHERE string::starts_with(name, 'alpha') ORDER BY id;

-- A2: a prefix one character longer, which excludes the shorter string that
-- A1 returns first.
EXPLAIN SELECT VALUE id FROM ps WHERE string::starts_with(name, 'alphab');
SELECT VALUE id FROM ps WHERE string::starts_with(name, 'alphab') ORDER BY id;
SELECT VALUE id FROM ps WITH NOINDEX WHERE string::starts_with(name, 'alphab') ORDER BY id;

-- A3: a prefix no string carries. The range is still built and still seeked;
-- what it addresses is empty.
EXPLAIN SELECT VALUE id FROM ps WHERE string::starts_with(name, 'zzz');
SELECT VALUE id FROM ps WHERE string::starts_with(name, 'zzz') ORDER BY id;
SELECT VALUE id FROM ps WITH NOINDEX WHERE string::starts_with(name, 'zzz') ORDER BY id;

-- A4: the prefix test is case-sensitive, so `ALPHA` belongs to its own
-- stretch and not to `alpha`'s.
EXPLAIN SELECT VALUE id FROM ps WHERE string::starts_with(name, 'ALPHA');
SELECT VALUE id FROM ps WHERE string::starts_with(name, 'ALPHA') ORDER BY id;
SELECT VALUE id FROM ps WITH NOINDEX WHERE string::starts_with(name, 'ALPHA') ORDER BY id;

-- ===========================================================================
-- Group B: a multibyte prefix. The successor is the next scalar VALUE after
-- the last character, not the next byte: `hé` runs up to `hê`, so `héllo` is
-- inside the stretch and `hêlp` is the first string above it.
-- ===========================================================================

-- B1: a prefix whose last character is multibyte.
EXPLAIN SELECT VALUE id FROM ps WHERE string::starts_with(name, "hé");
SELECT VALUE id FROM ps WHERE string::starts_with(name, "hé") ORDER BY id;
SELECT VALUE id FROM ps WITH NOINDEX WHERE string::starts_with(name, "hé") ORDER BY id;

-- ===========================================================================
-- Group C: the last scalar value at the end of the prefix. Incrementing it is
-- impossible, so the successor is formed by dropping it and incrementing what
-- precedes it; when nothing precedes it there is no successor at all and the
-- range must be left OPEN above. An upper bound invented for that case would
-- sit below the very strings the predicate selects, and the scan would
-- silently return nothing. Each case carries a string that extends the last
-- scalar value, so a bound placed on it moves a row rather than only a plan.
-- ===========================================================================

-- C1: the last scalar value at the end of a longer prefix. The increment
-- carries into `alpha`, so the stretch ends at `alphb` -- the same upper
-- bound A1 has, reached from the other side.
EXPLAIN SELECT VALUE id FROM ps WHERE string::starts_with(name, "alpha\u{10FFFF}");
SELECT VALUE id FROM ps WHERE string::starts_with(name, "alpha\u{10FFFF}") ORDER BY id;
SELECT VALUE id FROM ps WITH NOINDEX WHERE string::starts_with(name, "alpha\u{10FFFF}") ORDER BY id;

-- C2: a prefix that is the last scalar value and nothing else. No string
-- sorts above the stretch, so the range carries a lower bound alone.
EXPLAIN SELECT VALUE id FROM ps WHERE string::starts_with(name, "\u{10FFFF}");
SELECT VALUE id FROM ps WHERE string::starts_with(name, "\u{10FFFF}") ORDER BY id;
SELECT VALUE id FROM ps WITH NOINDEX WHERE string::starts_with(name, "\u{10FFFF}") ORDER BY id;

-- ===========================================================================
-- Group D: spellings that name no contiguous stretch, or name it over
-- something the index does not hold. Each is served by a TableScan carrying
-- the call inline as its own `predicate:`, with no `Filter` above it, and
-- each pins that TableScan: a shape silently acquiring an IndexScan is as
-- much a regression as one silently losing it.
-- ===========================================================================

-- D1: the empty prefix. Every string carries it, so the stretch is the whole
-- key space and no candidate is produced.
EXPLAIN SELECT VALUE id FROM ps WHERE string::starts_with(name, '');
SELECT VALUE id FROM ps WHERE string::starts_with(name, '') ORDER BY id;
SELECT VALUE id FROM ps WITH NOINDEX WHERE string::starts_with(name, '') ORDER BY id;

-- D2: the same call against a column no index holds.
EXPLAIN SELECT VALUE id FROM ps WHERE string::starts_with(alias, 'alpha');
SELECT VALUE id FROM ps WHERE string::starts_with(alias, 'alpha') ORDER BY id;
SELECT VALUE id FROM ps WITH NOINDEX WHERE string::starts_with(alias, 'alpha') ORDER BY id;

-- D3: the arguments reversed. The indexed column is the string being tested
-- rather than the prefix, so the predicate selects the rows whose name is a
-- prefix OF `alpha` -- a set no single key range addresses.
EXPLAIN SELECT VALUE id FROM ps WHERE string::starts_with('alpha', name);
SELECT VALUE id FROM ps WHERE string::starts_with('alpha', name) ORDER BY id;
SELECT VALUE id FROM ps WITH NOINDEX WHERE string::starts_with('alpha', name) ORDER BY id;

-- D4: a second argument that is not a literal. The bounds are built at plan
-- time, so a prefix known only per row cannot supply them.
EXPLAIN SELECT VALUE id FROM ps WHERE string::starts_with(name, pat);
SELECT VALUE id FROM ps WHERE string::starts_with(name, pat) ORDER BY id;
SELECT VALUE id FROM ps WITH NOINDEX WHERE string::starts_with(name, pat) ORDER BY id;

-- ===========================================================================
-- Group E: a compound index. A prefix test is a range, and a range bounds the
-- compound key only from the LEADING column; it pins no column, so the
-- trailing column stays unconstrained inside the stretch and a condition on
-- it stays in the residual.
-- ===========================================================================

{
    DEFINE TABLE OVERWRITE pc SCHEMALESS;
    DEFINE INDEX OVERWRITE pc_name_tier ON pc FIELDS name, tier;
    CREATE pc:1 SET name = 'alpha', tier = 'alpha1';
    CREATE pc:2 SET name = 'alphabet', tier = 'beta';
    CREATE pc:3 SET name = "alpha\u{10FFFF}", tier = 'alpha2';
    CREATE pc:4 SET name = "alpha\u{10FFFF}tail", tier = 'alpha3';
    CREATE pc:5 SET name = 'alphb', tier = 'alpha4';
    CREATE pc:6 SET name = 'beta', tier = 'beta';
    RETURN NONE;
};

-- E1: the call on the compound index's LEADING column.
EXPLAIN SELECT VALUE id FROM pc WHERE string::starts_with(name, 'alpha');
SELECT VALUE id FROM pc WHERE string::starts_with(name, 'alpha') ORDER BY id;
SELECT VALUE id FROM pc WITH NOINDEX WHERE string::starts_with(name, 'alpha') ORDER BY id;

-- E2: the same call with an equality on the trailing column. A range does not
-- pin the column it bounds, so nothing behind it can be seeked and the
-- equality stays in the residual alongside the call. This is where the
-- residual carries something the range cannot: dropping it would return every
-- row the range admits instead of the one row wanted.
EXPLAIN SELECT VALUE id FROM pc WHERE string::starts_with(name, 'alpha') AND tier = 'beta';
SELECT VALUE id FROM pc WHERE string::starts_with(name, 'alpha') AND tier = 'beta' ORDER BY id;
SELECT VALUE id FROM pc WITH NOINDEX WHERE string::starts_with(name, 'alpha') AND tier = 'beta' ORDER BY id;

-- E3: the call on the compound index's TRAILING column. A stretch of that
-- column's values is not a contiguous stretch of the compound key.
EXPLAIN SELECT VALUE id FROM pc WHERE string::starts_with(tier, 'alpha');
SELECT VALUE id FROM pc WHERE string::starts_with(tier, 'alpha') ORDER BY id;
SELECT VALUE id FROM pc WITH NOINDEX WHERE string::starts_with(tier, 'alpha') ORDER BY id;

-- ===========================================================================
-- Group F: a UNIQUE index. Uniqueness constrains what may be written, not how
-- the key space is ordered, so the same stretch is addressed the same way
-- over the same boundary strings.
-- ===========================================================================

{
    DEFINE TABLE OVERWRITE pu SCHEMALESS;
    DEFINE INDEX OVERWRITE pu_name ON pu FIELDS name UNIQUE;
    CREATE pu:1 SET name = 'alpha';
    CREATE pu:2 SET name = 'alphabet';
    CREATE pu:3 SET name = "alpha\u{10FFFF}";
    CREATE pu:4 SET name = "alpha\u{10FFFF}tail";
    CREATE pu:5 SET name = 'alphb';
    CREATE pu:6 SET name = 'beta';
    RETURN NONE;
};

-- F1: the call against a UNIQUE index.
EXPLAIN SELECT VALUE id FROM pu WHERE string::starts_with(name, 'alpha');
SELECT VALUE id FROM pu WHERE string::starts_with(name, 'alpha') ORDER BY id;
SELECT VALUE id FROM pu WITH NOINDEX WHERE string::starts_with(name, 'alpha') ORDER BY id;
