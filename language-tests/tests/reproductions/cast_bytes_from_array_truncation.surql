/**
[test]
reason = "Regression for `<bytes>` (and `type::bytes`) when cast from an array. Each element used to be cast to `i64` and then silently truncated via `as u8`, so values like 256 wrapped to 0 and -1 wrapped to 255. The cast now rejects elements that don't fit in `0..=255` so the result either round-trips losslessly or fails fast."

[[test.results]]
value = 'b"61"'

[[test.results]]
value = 'b"6263"'

[[test.results]]
value = 'b""'

[[test.results]]
error = "Could not cast into `bytes` using input `[256]`"

[[test.results]]
error = "Could not cast into `bytes` using input `[-1]`"

[[test.results]]
error = "Could not cast into `bytes` using input `[1.5f]`"

[[test.results]]
error = "Could not cast into `bytes` using input `[300]`"

[[test.results]]
error = "Could not cast into `bytes` using input `[0, 256]`"

*/

-- 1: A small in-range array still casts losslessly.
<bytes> [97];

-- 2: type::bytes accepts the same shape.
type::bytes([98, 99]);

-- 3: An empty array produces empty bytes.
<bytes> [];

-- 4: Values above 255 are rejected (used to silently truncate to 0).
<bytes> [256];

-- 5: Negative values are rejected (used to silently wrap to 255).
<bytes> [-1];

-- 6: Non-integer numerics are rejected (used to truncate the fractional part).
<bytes> [1.5];

-- 7: The function form rejects out-of-range elements as well.
type::bytes([300]);

-- 8: A single bad element in an otherwise valid array fails the whole cast.
<bytes> [0, 256];
