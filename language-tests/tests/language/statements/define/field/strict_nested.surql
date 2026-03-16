/**

[test]

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ any: 'foo', id: test1:1 }]"

[[test.results]]
value = "[{ any: 'bar', id: test1:2 }]"

[[test.results]]
value = "[{ any: { sub: { field: 'baz' } }, id: test1:3 }]"

[[test.results]]
value = "[{ any: { sub: 'ham' }, id: test1:3 }]"

[[test.results]]
value = "[{ id: test2:1, obj_any: { a: 1, b: 'foo' } }]"

[[test.results]]
value = "[{ id: test2:2, obj_any: { a: 2, b: 'bar' } }]"

[[test.results]]
value = "[{ id: test2:3, obj_any: { c: true } }]"

[[test.results]]
value = "[{ id: test2:3, obj_any: { a: 3, b: 'baz' } }]"

[[test.results]]
error = "Couldn't coerce value for field `obj_any` of `test2:3`: Expected `object` but found `'not an object'`"

[[test.results]]
error = "Couldn't coerce value for field `obj_limited.b` of `test3:1`: Expected `number` but found `NONE`"

[[test.results]]
error = "Couldn't coerce value for field `obj_limited.a` of `test3:2`: Expected `string` but found `NONE`"

[[test.results]]
value = "[{ id: test3:3, obj_limited: { a: 'foo', b: 1 } }]"

[[test.results]]
value = "[{ id: test3:4, obj_limited: { a: 'foo', b: 1, c: true } }]"

[[test.results]]
value = "[{ id: test3:5, obj_limited: { a: 'foo', b: 1, c: true } }]"

[[test.results]]
value = "[{ id: test3:6, obj_limited: { a: 'foo', b: 1 } }]"

[[test.results]]
error = "Couldn't coerce value for field `obj_limited.b` of `test3:7`: Expected `number` but found `NONE`"

[[test.results]]
error = "Couldn't coerce value for field `obj_limited.a` of `test3:8`: Expected `string` but found `NONE`"


*/

DEFINE TABLE test1 SCHEMAFULL;
DEFINE TABLE test2 SCHEMAFULL;
DEFINE TABLE test3 SCHEMAFULL;

-- This should allow any subfields to be defined.
DEFINE FIELD any ON test1 TYPE any;

-- This should allow any subfields to be defined.
DEFINE FIELD obj_any ON test2 TYPE object FLEXIBLE;

-- This should not allow setting `a`, `b` or `c` explicitly
-- OR setting `obj_limited` to an object with (`a` and `b`) and optionally `c`.
DEFINE FIELD obj_limited.a ON test3 TYPE string;
DEFINE FIELD obj_limited.b ON test3 TYPE number;
DEFINE FIELD obj_limited.c ON test3 TYPE option<bool>;

########################################################
# `test1`: Tests for `any` type.
########################################################
CREATE test1:1 CONTENT { any: 'foo' };
CREATE test1:2 SET any = 'bar';
CREATE test1:3 SET any.sub.field = 'baz';
# This clobbers the `any.sub.field` value.
UPDATE test1:3 SET any.sub = 'ham';

########################################################
# `test2`: Tests for `object` type.
########################################################
CREATE test2:1 CONTENT { obj_any: { a: 1, b: 'foo' } };
CREATE test2:2 SET obj_any = { a: 2, b: 'bar' };
CREATE test2:3 SET obj_any.c = true;
# This clobbers the `obj_any.c` value.
UPDATE test2:3 SET obj_any = { a: 3, b: 'baz' };

# This should fail
UPDATE test2:3 SET obj_any = 'not an object';

########################################################
# `test3`: Tests for `object<a: string, b: number, c: option<bool>>` type.
########################################################

CREATE test3:1 SET obj_limited.a = 'foo'; # ❌ (missing `b`)
CREATE test3:2 SET obj_limited.b = 1; # ❌ (missing `a`)
CREATE test3:3 SET obj_limited.a = 'foo', obj_limited.b = 1;
CREATE test3:4 SET obj_limited.a = 'foo', obj_limited.b = 1, obj_limited.c = true;

CREATE test3:5 SET obj_limited = { a: 'foo', b: 1, c: true };
CREATE test3:6 SET obj_limited = { a: 'foo', b: 1 };
CREATE test3:7 SET obj_limited = { a: 'foo', c: true }; # ❌ (missing `b`)
CREATE test3:8 SET obj_limited = { b: 1, c: true }; # ❌ (missing `a`)
