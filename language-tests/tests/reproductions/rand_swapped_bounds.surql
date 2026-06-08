/**
[test]
reason = "Regression for `rand::float`, `rand::int`, `rand::duration`, and `rand::time`: when called with `min > max` they would silently swap the bounds, which was inconsistent with `rand::id` and `rand::string` (which error). Per the TODO in `core/src/fnc/rand.rs`, callers should get an error rather than have invalid input silently coerced into a range."

[[test.results]]
error = "Incorrect arguments for function rand::float(). Lowerbound of the range must be less than or equal to the upperbound."

[[test.results]]
error = "Incorrect arguments for function rand::int(). Lowerbound of the range must be less than or equal to the upperbound."

[[test.results]]
error = "Incorrect arguments for function rand::duration(). Lowerbound of the range must be less than or equal to the upperbound."

[[test.results]]
error = "Incorrect arguments for function rand::time(). Lowerbound of the range must be less than or equal to the upperbound."

[[test.results]]
error = "Incorrect arguments for function rand::time(). Lowerbound of the range must be less than or equal to the upperbound."

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "10s"

[[test.results]]
value = "d'1970-01-01T00:00:00Z'"

*/

-- min > max should error rather than silently swap.
rand::float(10, 5);
rand::int(10, 5);
rand::duration(20s, 10s);
rand::time(100, 0);
rand::time(d'2025-01-01', d'2020-01-01');

-- Equal bounds remain valid.
rand::float(5, 5) = 5;
rand::int(5, 5) = 5;
rand::duration(10s, 10s);
rand::time(d'1970-01-01', d'1970-01-01');
