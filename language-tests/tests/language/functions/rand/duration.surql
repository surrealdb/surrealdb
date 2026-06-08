/**
[test]

[[test.results]]
value = "true"

[[test.results]]
value = "10s"

[[test.results]]
value = "true"

[[test.results]]
error = "Incorrect arguments for function rand::duration(). Lowerbound of the range must be less than or equal to the upperbound."

*/
type::is_duration(rand::duration(0ns, duration::MAX));
rand::duration(10s, 10s);
rand::duration(10s, 20s) IN 10s..20s;
rand::duration(20s, 10s);
