/**
[env]
imports = ["reproductions/closure_call_guard_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A closure a builtin runs for the caller — `array::any`, `array::filter`,
	`array::map`, `value::chain`, the method forms — evaluates its body
	against the row, so a body reading a field the caller may not select
	reads the guarded value, however the closure reaches the call: as a
	parameter, as a field of an object held in a parameter, or written in
	the call. `secret` is denied to the record user, so every oracle over it
	selects nothing and every projection of it reads `NONE`. An ordinary
	builtin over a readable field is unaffected, and so is a closure-running
	builtin whose closure position holds no closure: the array it searches
	may be a field or a parameter without the call reading anything but it.
"""

# 0: the closure as a parameter.
[[test.results]]
value = "NONE"

# 1: the closure as a field of an object held in a parameter.
[[test.results]]
value = "NONE"

# 2: `array::any` over a parameter closure selects nothing.
[[test.results]]
value = "[]"

# 3: `array::filter`.
[[test.results]]
value = "[]"

# 4: `array::map`.
[[test.results]]
value = "[]"

# 5: `value::chain`.
[[test.results]]
value = "[]"

# 6: `array::fold`, the closure as the last of three arguments.
[[test.results]]
value = "[]"

# 7: the closure read off an object held in a parameter.
[[test.results]]
value = "[]"

# 8: the method form over a parameter closure.
[[test.results]]
value = "[]"

# 9: the closure written in the call.
[[test.results]]
value = "[]"

# 10: projected, the body reads the guarded value.
[[test.results]]
value = "[{ s: false }, { s: false }]"

# 11: an ordinary builtin over a readable field still selects.
[[test.results]]
value = "['a', 'b']"

# 12: a closure-running builtin with a literal in the closure position reads
# only the field it searches.
[[test.results]]
value = "['a']"

# 13: the same with the array as a parameter and a parameter in the closure
# position that holds a string, bound by the statement.
[[test.results]]
value = "NONE"

[[test.results]]
value = "['a']"

# 15: a parameter in the closure position that holds a closure still reads
# the guarded row.
[[test.results]]
value = "[]"
*/

LET $pred = |$x| secret = 'yes';
LET $o = { check: |$x| secret = 'yes' };

-- The closure reaches the builtin as a parameter: its body is nowhere in the
-- statement, yet it reads the row the statement is evaluating.
SELECT VALUE public FROM t WHERE array::any([1], $pred);
SELECT VALUE public FROM t WHERE array::filter([1], $pred).len() > 0;
SELECT VALUE public FROM t WHERE array::map([1], $pred)[0];
SELECT VALUE public FROM t WHERE value::chain(1, $pred);
SELECT VALUE public FROM t WHERE array::fold([1], false, |$acc, $x| $acc OR secret = 'yes');

-- Stored under a field of an object held in a parameter.
SELECT VALUE public FROM t WHERE array::any([1], $o.check);

-- As a method on the receiver.
SELECT VALUE public FROM t WHERE [1].any($pred);

-- Written in the call, the body is in the statement and reads by name.
SELECT VALUE public FROM t WHERE array::any([1], |$x| secret = 'yes');

-- Projected rather than used as an oracle.
SELECT array::any([1], $pred) AS s FROM t;

-- An ordinary builtin over a readable field.
SELECT VALUE public FROM t WHERE string::len(public) > 0;

-- The closure position holds a value that is not a closure: the call reads
-- `tags` by name and nothing else.
SELECT VALUE public FROM t WHERE array::includes(tags, 'x');
LET $tag = 'x';
SELECT VALUE public FROM t WHERE array::any(tags, $tag);

-- The same shape with a closure in the position is judged as before.
SELECT VALUE public FROM t WHERE array::any(tags, $pred);
