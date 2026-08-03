/**
[test]
run = false
*/

-- Seed for `distinct_numeric_variants.gql`: one value per row, spelled with
-- different numeric variants. `0.1f` and `0.1dec` compare equal but hash apart,
-- so they are the pair that catches a dedup keyed on hashes rather than on
-- ordering; the integral spellings hash alike and would collapse either way.
CREATE num:1 SET v = 0.1f RETURN NONE;
CREATE num:2 SET v = 0.1dec RETURN NONE;
CREATE num:3 SET v = 1 RETURN NONE;
CREATE num:4 SET v = 1.0f RETURN NONE;
CREATE num:5 SET v = 1dec RETURN NONE;
CREATE num:6 SET v = 1.5f RETURN NONE;
CREATE num:7 SET v = 1.50dec RETURN NONE;
-- A genuinely different value, so dedup cannot pass by collapsing everything.
CREATE num:8 SET v = 0.2f RETURN NONE;
