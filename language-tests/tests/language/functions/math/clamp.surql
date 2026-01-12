/**
[test]

[[test.results]]
value = "2"

[[test.results]]
value = "2f"

[[test.results]]
value = "4"

[[test.results]]
value = "4f"

[[test.results]]
value = "2f"

[[test.results]]
error = "Incorrect arguments for function math::clamp(). Lowerbound for clamp must be smaller the the upperbound"

*/
math::clamp(1, 2, 4);
math::clamp(1f, 2, 4);
math::clamp(5, 2, 4);
math::clamp(5.0, 2, 4);
math::clamp(1, 2f, 4);
math::clamp(1, 5, 4);
