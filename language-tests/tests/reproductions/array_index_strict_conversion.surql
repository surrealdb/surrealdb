/**
[env]
namespace = true
database = true

[test]
reason = "Indexing an array with a non-integer-representable Number (negative integer, negative float, fractional float, NaN, fractional Decimal, negative Decimal) used to go through `Number::to_usize()`, which silently produced wrong indices via saturating/truncating casts and `unwrap_or_default()`. For example `[1,2,3][-1.0]` returned `1` (because `-1.0 as usize` saturates to 0) and `[1,2,3][1.5]` returned `2` (because `1.5 as usize` truncates to 1). All such cases now return NONE."

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

-- Integer-valued float is still a valid index.
[1, 2, 3][0.0];

-- Negative integer-valued float must not address index 0.
[1, 2, 3][-1.0];

-- Fractional float must not index by its integer part.
[1, 2, 3][1.5];

-- Out-of-range float (huge magnitude) must skip.
[1, 2, 3][1e30];

-- Negative integer is rejected.
[1, 2, 3][-1];

-- Out-of-range positive integer is rejected.
[1, 2, 3][9999999999999];

-- Integer-valued Decimal is a valid index.
[1, 2, 3][1dec];

-- Fractional Decimal must not index by its integer part.
[1, 2, 3][1.5dec];

-- Negative Decimal must not address index 0.
[1, 2, 3][-1dec];

-- NaN must not address index 0.
[1, 2, 3][(0.0 / 0.0)];
