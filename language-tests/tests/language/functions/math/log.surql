/**
[test]

[[test.results]]
value = "0.43429738512450866f"

[[test.results]]
value = "0.30102999566398114f"

[[test.results]]
value = "0f"

[[test.results]]
value = "true"

[[test.results]]
value = "'NaN'"

*/
math::log(2.7183, 10);
math::log(2, 10);
math::log(1, 10);
math::log(0, 10) == math::neg_inf;
<string> math::log(-1, 10);
