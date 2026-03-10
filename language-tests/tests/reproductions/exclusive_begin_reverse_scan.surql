/**
[test]
reason = """Regression: reverse scans with exclusive lower bound must not include the boundary value.
The first-batch boundary filter only checked the exclusive end boundary (check_exclusive_end),
but had no equivalent protection for an exclusive begin boundary when scanning backward.
Given WHERE v > 5 ORDER BY v DESC, the lower bound is exclusive (> 5).
Reverse scans keep beg fixed while moving end downward between batches,
so the lowest key in the range can eventually be emitted even when it is the excluded lower boundary."""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ v: 15 }, { v: 14 }, { v: 13 }, { v: 12 }, { v: 11 }, { v: 10 }, { v: 9 }, { v: 8 }, { v: 7 }, { v: 6 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ v: 15 }, { v: 14 }, { v: 13 }, { v: 12 }, { v: 11 }, { v: 10 }, { v: 9 }, { v: 8 }, { v: 7 }, { v: 6 }]"

[[test.results]]
value = "[{ v: 15 }, { v: 14 }, { v: 13 }, { v: 12 }, { v: 11 }, { v: 10 }, { v: 9 }, { v: 8 }, { v: 7 }, { v: 6 }, { v: 5 }]"

[[test.results]]
value = "[{ v: 15 }, { v: 14 }, { v: 13 }, { v: 12 }, { v: 11 }, { v: 10 }, { v: 9 }, { v: 8 }, { v: 7 }, { v: 6 }, { v: 5 }]"
*/

-- Setup: non-unique index
{
    DEFINE INDEX idx ON TABLE i COLUMNS v;
    FOR $i IN 1..=15 { CREATE i:[$i] SET v = $i; };
    RETURN "OK";
};

-- Non-unique index: WHERE v > 5 ORDER BY v DESC without LIMIT
-- Must NOT include v = 5
SELECT v FROM i WHERE v > 5 ORDER BY v DESC;

-- Setup: unique index
{
    DEFINE INDEX idx_u ON TABLE u COLUMNS v UNIQUE;
    FOR $i IN 1..=15 { CREATE u:[$i] SET v = $i; };
    RETURN "OK";
};

-- Unique index: WHERE v > 5 ORDER BY v DESC without LIMIT
-- Must NOT include v = 5
SELECT v FROM u WHERE v > 5 ORDER BY v DESC;

-- Non-unique index: WHERE v >= 5 ORDER BY v DESC without LIMIT (inclusive for comparison)
-- MUST include v = 5
SELECT v FROM i WHERE v >= 5 ORDER BY v DESC;

-- Unique index: WHERE v >= 5 ORDER BY v DESC without LIMIT (inclusive for comparison)
-- MUST include v = 5
SELECT v FROM u WHERE v >= 5 ORDER BY v DESC;
