/**
[test]
reason = "Regression for `rand::string`: same negative-input bug as `rand::id`. A negative `lower` cast straight to `usize` wrapped to a huge length and either panicked `random_range` (`cannot sample empty range`) when paired with a non-negative `upper`, or asked `Alphanumeric.sample_string` for a multi-exabyte string when called single-arg. Mirrors the established negative-input rejection pattern from PRs #135 / #136 / #139."

[[test.results]]
error = 'Failed to compute: "rand::string(-1)", as the operation results in a negative value.'

[[test.results]]
error = 'Failed to compute: "rand::string(-1)", as the operation results in a negative value.'

[[test.results]]
error = 'Failed to compute: "rand::string(-5)", as the operation results in a negative value.'

[[test.results]]
error = "Incorrect arguments for function rand::string(). Lowerbound of number of characters must be less then the upperbound."
*/

rand::string(-1);
rand::string(-1, 5);
rand::string(-5, -3);
rand::string(5, -3);
