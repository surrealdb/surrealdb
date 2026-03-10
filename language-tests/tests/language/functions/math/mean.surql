/**
[test]

[[test.results]]
value = "true"

[[test.results]]
value = "172f"

[[test.results]]
value = "172.33333333333334f"

[[test.results]]
value = "172.46666666666667f"

[[test.results]]
value = "173.83333333333334f"

*/
// no good way to compare against NaN so just compare against the string representation
<string> math::mean([]) == "NaN";
math::mean([101, 213, 202]);
math::mean([101, 213, 203]);
math::mean([101, 213, 203.4]);
math::mean([101.5, 213.5, 206.5]);
