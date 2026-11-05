/**
[test]
run = false
reason = "Setup only: columns that hold numbers of every type, values that are not numbers, and floats, read by sum_avg_distinct_values"
*/

-- Numbers and the values an aggregate skips: text, a boolean, an array, NULL and a missing value.
CREATE dsv_mixed:1 SET v = 1 RETURN NONE;
CREATE dsv_mixed:2 SET v = 'a' RETURN NONE;
CREATE dsv_mixed:3 SET v = true RETURN NONE;
CREATE dsv_mixed:4 SET v = [5, 6] RETURN NONE;
CREATE dsv_mixed:5 SET v = NULL RETURN NONE;
CREATE dsv_mixed:6 SET v = 2 RETURN NONE;
CREATE dsv_mixed:7 SET v = 2 RETURN NONE;
CREATE dsv_mixed:8 SET k = 8 RETURN NONE;
CREATE dsv_text:1 SET v = 'a' RETURN NONE;
CREATE dsv_text:2 SET v = 'b' RETURN NONE;
-- The number 1 as an int, a float and a decimal, and the number 2.
CREATE dsv_same:1 SET v = 1 RETURN NONE;
CREATE dsv_same:2 SET v = 1.0f RETURN NONE;
CREATE dsv_same:3 SET v = 1dec RETURN NONE;
CREATE dsv_same:4 SET v = 2 RETURN NONE;
-- Floats whose sum depends on the order they are added in.
CREATE dsv_float:1 SET v = 0.1f RETURN NONE;
CREATE dsv_float:2 SET v = 0.2f RETURN NONE;
CREATE dsv_float:3 SET v = 0.3f RETURN NONE;
CREATE dsv_float:4 SET v = 0.1f RETURN NONE;
CREATE dsv_inf:1 SET v = math::inf RETURN NONE;
CREATE dsv_inf:2 SET v = 1 RETURN NONE;
CREATE dsv_nan:1 SET v = NaN RETURN NONE;
CREATE dsv_nan:2 SET v = 1 RETURN NONE;
CREATE dsv_nan:3 SET v = NaN RETURN NONE;
-- Numbers with no repeat, where the plain and the DISTINCT aggregates must agree.
CREATE dsv_ints:1 SET v = 5 RETURN NONE;
CREATE dsv_ints:2 SET v = 7 RETURN NONE;
CREATE dsv_ints:3 SET v = 11 RETURN NONE;
CREATE dsv_decs:1 SET v = 0.1dec RETURN NONE;
CREATE dsv_decs:2 SET v = 0.2dec RETURN NONE;
CREATE dsv_decs:3 SET v = 0.3dec RETURN NONE;
CREATE dsv_floats:1 SET v = 0.1f RETURN NONE;
CREATE dsv_floats:2 SET v = 0.2f RETURN NONE;
CREATE dsv_floats:3 SET v = 0.3f RETURN NONE;
