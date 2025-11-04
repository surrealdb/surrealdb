/**
[test]
reason = "ASSERT constraints on individual array indexes work when elements exist, but are ignored when the array is shorter than expected or empty"
issue = 5677

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$error = /Couldn't coerce value for field `some_data` of `device:.*`: Expected `number` but found `'not correct'` when coercing an element of `array<number>`/"
error = true

[[test.results]]
match = "$error = /Found 5000000000 for field `some_data\\[0\\]`, with record `device:.*`, but field must conform to: \\$value INSIDE 0\\.\\.=4294967295/"
error = true

[[test.results]]
match = "$error = /Found 99999999999999999999dec for field `some_data\\[1\\]`, with record `device:.*`, but field must conform to: \\$value INSIDE 0\\.\\.=18446744073709551615dec/"
error = true

[[test.results]]
match = "$error = /Couldn't coerce value for field `some_data` of `device:.*`: Expected `array<number,2>` but found an collection of length `0`/"
error = true
*/

-- Define array field with specific type and size constraints
DEFINE FIELD some_data ON TABLE device TYPE array<number, 2>;

-- Define constraints on individual array indexes
DEFINE FIELD some_data[0] ON TABLE device TYPE number ASSERT $value IN 0..=4294967295;
DEFINE FIELD some_data[1] ON TABLE device TYPE number ASSERT $value IN 0..=18446744073709551615dec;

-- This fails correctly due to type constraint on array elements
CREATE device SET some_data = ["not correct", "type"];

-- These fail correctly - ASSERT constraints DO work when array elements exist
CREATE device SET some_data = [5000000000, 123];  -- First value exceeds max allowed for [0]
CREATE device SET some_data = [123, 99999999999999999999dec];  -- Second value exceeds max allowed for [1]

-- This currently fails only due to array size constraint (expecting length 2, got 0)
-- but does NOT fail due to the ASSERT constraints on individual indexes [0] and [1]
-- The bug is that ASSERT constraints on array indexes are ignored when those indexes don't exist
CREATE device SET some_data = [];
