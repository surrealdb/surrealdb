/**
[test]
reason = "Regression for `rand::id`: a negative `lower` (with or without `upper`) was cast straight to `usize`, wrapping to a huge length. With both bounds supplied this fed `random_range((huge..=small_upper))` and panicked with `cannot sample empty range`; with one bound it tried to allocate a string of wrapped-huge length and OOMed the process. After the fix, both bounds must reject negatives before any cast and surface `ArithmeticNegativeOverflow` like the parallel `array::repeat` / `string::repeat` / `string::semver::set::*` checks (PRs #135, #136, #139)."

[[test.results]]
error = 'Failed to compute: "rand::id(-1)", as the operation results in a negative value.'

[[test.results]]
error = 'Failed to compute: "rand::id(-1)", as the operation results in a negative value.'

[[test.results]]
error = 'Failed to compute: "rand::id(-5)", as the operation results in a negative value.'

[[test.results]]
error = "Incorrect arguments for function rand::id(). Lowerbound of number of characters must be less then the upperbound."
*/

rand::id(-1);
rand::id(-1, 5);
rand::id(-5, -3);
rand::id(5, -3);
