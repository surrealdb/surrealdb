/**
[test]
run = false
*/

-- Seed for `distinct_numeric_variants.gql`: one value per row, spelled with
-- different numeric variants. `1`, `1.0f` and `1dec` are one value, as are
-- `1.5f` and `1.50dec`; `0.1f` and `0.1dec` are two, a float equalling a
-- decimal only where the decimal spells the float's exact value.
CREATE num:1 SET v = 0.1f RETURN NONE;
CREATE num:2 SET v = 0.1dec RETURN NONE;
CREATE num:3 SET v = 1 RETURN NONE;
CREATE num:4 SET v = 1.0f RETURN NONE;
CREATE num:5 SET v = 1dec RETURN NONE;
CREATE num:6 SET v = 1.5f RETURN NONE;
CREATE num:7 SET v = 1.50dec RETURN NONE;
-- A genuinely different value, so dedup cannot pass by collapsing everything.
CREATE num:8 SET v = 0.2f RETURN NONE;
