/**
[env.capabilities]
deny-functions = ["math::abs", "array::any", "array::sort::asc", "value::chain", "geo::centroid"]

[test]
reason = """
Regression for GHSA-p296-7pr4-87rr: a denied function is refused whichever
method spelling reaches it, because the capability check names the registry
entry the method resolves to, not the surface spelling.

These are the method shapes the advisory called out as bypassing on the legacy
canonicalization, one per resolution kind:

- `(5).abs()` -> `math::abs` (a number receiver; the old mapping checked the
  value kind `int` against a `number` key and missed).
- `[1, 2].some(...)` -> `array::any` (a registered alias).
- `[3, 1, 2].sort_asc()` -> `array::sort::asc` (a nested function name).
- `(5).chain(...)` -> `value::chain` (the value namespace).
- `<point>.centroid()` -> `geo::centroid` (a geometry receiver; the old mapping
  checked the value kind `geometry<point>` against a `geometry` key and missed).

Each is paired with its direct call, which the deny list already refuses, so a
refusal on the method form is the capability and not the shape. The final
control calls a method whose entry (`array::len`) is not denied, so it runs.
"""

[[test.results]]
error = "Function 'math::abs' is not allowed to be executed"

[[test.results]]
error = "Function 'math::abs' is not allowed to be executed"

[[test.results]]
error = "Function 'array::any' is not allowed to be executed"

[[test.results]]
error = "Function 'array::any' is not allowed to be executed"

[[test.results]]
error = "Function 'array::sort::asc' is not allowed to be executed"

[[test.results]]
error = "Function 'array::sort::asc' is not allowed to be executed"

[[test.results]]
error = "Function 'value::chain' is not allowed to be executed"

[[test.results]]
error = "Function 'value::chain' is not allowed to be executed"

[[test.results]]
error = "Function 'geo::centroid' is not allowed to be executed"

[[test.results]]
error = "Function 'geo::centroid' is not allowed to be executed"

[[test.results]]
value = "3"
*/

-- math::abs: direct call and number-receiver method form.
RETURN math::abs(5);
RETURN (5).abs();

-- array::any reached through its alias .some().
RETURN array::any([1, 2], |$x| $x > 0);
RETURN [1, 2].some(|$x| $x > 0);

-- array::sort::asc reached through the .sort_asc() method.
RETURN array::sort::asc([3, 1, 2]);
RETURN [3, 1, 2].sort_asc();

-- value::chain reached through the .chain() method.
RETURN value::chain(5, |$x| $x);
RETURN (5).chain(|$x| $x);

-- geo::centroid reached through a method on a geometry receiver.
RETURN geo::centroid(<geometry<point>>{ type: 'Point', coordinates: [1, 2] });
RETURN (<geometry<point>>{ type: 'Point', coordinates: [1, 2] }).centroid();

-- Control: a method whose entry is not denied still runs.
RETURN [3, 1, 2].len();
