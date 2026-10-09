/**

[test]
reason = """
Every surface form of an array filter or map answers as the one array operation
it is: a closure-written `array::filter` or `array::map` as a call or a method,
over an array literal or a cast, and the `[WHERE]` filter. A base that is not an
array, a closure that declares a parameter kind, a return kind or a second
parameter, a body that reads the row, and a body that raises, each keep the
closure's own result or error.
"""

[[test.results]]
value = "[7, 9]"

[[test.results]]
value = "[7, 9]"

[[test.results]]
value = "[7, 9]"

[[test.results]]
value = "[5, 7, 9]"

[[test.results]]
value = "[5, 7, 9]"

[[test.results]]
value = "['a', 9, [1]]"

[[test.results]]
value = "[[1], [2], [3]]"

[[test.results]]
value = "[[1], [2], [3]]"

[[test.results]]
value = "[[[1, 2]], [[3]]]"

[[test.results]]
value = "['small', 'big']"

[[test.results]]
error = "Incorrect arguments for function array::filter(). Argument 1 was the wrong type. Expected `array` but found `'abc'`"

[[test.results]]
error = "There was a problem running the filter() function. no such method found for the string type"

[[test.results]]
error = "Incorrect arguments for function array::map(). Argument 1 was the wrong type. Expected `array` but found `{ a: 1 }`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'closure field'"

[[test.results]]
error = "Incorrect arguments for function ANONYMOUS(). Expected a value of type 'string' for argument $v"

[[test.results]]
error = "Couldn't coerce return value from function `ANONYMOUS`: Expected `string` but found `1`"

[[test.results]]
value = "[[1, 0], [2, 1]]"

[[test.results]]
value = "[[2, 3]]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[[[1, 5], [2, 5]]]"

*/
array::filter([5, 7, 9], |$v| $v > 6);
[5, 7, 9].filter(|$v| $v > 6);
(<array> [5, 7, 9]).filter(|$v| $v > 6);
array::filter([5, 7, 9], |$v| true);
[5, 7, 9][WHERE true];
array::filter([5, 'a', NONE, 9, [1]], |$v| $v > 6);
array::map([1, 2, 3], |$v| [$v]);
[1, 2, 3].map(|$v| [$v]);
array::map([[1, 2], [3]], |$v| [$v]);
array::map([1, 2], |$v| IF $v > 1 THEN 'big' ELSE 'small' END);
array::filter('abc', |$v| $v > 1);
'abc'.filter(|$v| $v > 1);
array::map({ a: 1 }, |$v| [$v]);
LET $o = { filter: |$v| 'closure field' };
$o.filter(|$v| true);
array::filter([1, 2], |$v: string| true);
array::map([1, 2], |$v| -> string { $v });
array::map([1, 2], |$v, $i| [$v, $i]);
SELECT VALUE array::filter([1, 2, 3], |$v| $v > n) FROM [{ n: 1 }];
array::filter([1, 2], |$v| <int> 'x');
[1, 2].filter(|$v| <int> 'x');
SELECT VALUE array::map([1, 2], |$v| [$v, n]) FROM [{ n: 5 }];
