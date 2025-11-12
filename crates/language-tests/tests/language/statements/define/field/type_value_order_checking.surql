/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'test' does not exist"

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
error = "Cannot perform multiplication with 'NONE' and '2'"

[[test.results]]
value = "[{ can_be_number: 123456789, id: test:admin, must_be_number: 8, unchecked_number: 123456789, version: 123456789 }]"

[[test.results]]
value = "[{ can_be_number: 1, id: test:admin, must_be_number: 8, unchecked_number: 123456789, version: 123456789 }]"

[[test.results]]
error = "Couldn't coerce value for field `can_be_number` of `test:admin`: Expected `number` but found `'asd'`"

[[test.results]]
error = "Couldn't coerce value for field `must_be_number` of `test:admin`: Expected `number` but found `'asd'`"

[[test.results]]
error = "Couldn't coerce value for field `must_be_number` of `test:admin`: Expected `number` but found `'asd'`"

[[test.results]]
value = "[{ can_be_number: 1, id: test:admin, must_be_number: 8, unchecked_number: 222, version: 123456789 }]"

[env]
namespace = "test"
database = "test"

*/

USE NS test DB test;
REMOVE TABLE test;
DEFINE TABLE test SCHEMAFULL;

-- This should always be a number, can be checked after VALUE clause
DEFINE FIELD version ON test TYPE number VALUE 123456789;

-- This should always be a number - we want it checked after VALUE clause,
-- because it doesn't matter if the $value is a <number> or not
DEFINE FIELD can_be_number ON test TYPE number VALUE $value || 123456789;

-- This should always be a number - we want it checked before VALUE clause,
-- so that the $value is guaranteed to be a <number>
DEFINE FIELD must_be_number ON test TYPE number VALUE $value * 2 || 123456789;

-- This should always be a number - we want it checked before VALUE clause,
-- so that the $value is guaranteed to be a <number>
DEFINE FIELD unchecked_number ON test TYPE number VALUE IF $value.is_number() && $value > 100 { $value * 2 } else { 123456789 };

UPSERT test:admin CONTENT { };
UPSERT test:admin CONTENT { must_be_number: 4 };
UPSERT test:admin CONTENT { can_be_number: 1, must_be_number: 4 };
UPSERT test:admin CONTENT { can_be_number: 'asd' };
UPSERT test:admin CONTENT { must_be_number: 'asd' };
UPSERT test:admin CONTENT { can_be_number: 1, must_be_number: 'asd' };
UPSERT test:admin CONTENT { can_be_number: 1, must_be_number: 4, unchecked_number: 111 };
