/**
[test]
reason = "Repeated record results count toward the minimum depth, preserving invalid-target behavior"
issue = 7649
results = [
    "[]",
    "s:x", "s:x", "s:x", "s:x", "s:x", "s:x", "s:x", "s:x", "s:x", "s:x",
    { error = "Exceeded the idiom recursion limit of 256." },
    "NONE", "NONE", "NONE", "[]", "NONE", "[s:x, 1]",
    { error = "Expected a record ID during recursive traversal, but found `1`" },
    "NONE", "true", "false"
]
*/

CREATE s:x SET e = s:x, k = 1 RETURN NONE;
RETURN s:x.e;
RETURN s:x.e.e;
RETURN s:x.{1}(.e);
RETURN s:x.{1..3}(.e);
RETURN s:x.{3}(.e);
RETURN s:x.{3..5}(.e);
RETURN s:x.{..}(.e);
RETURN s:x.{3};
RETURN s:x.{256}(.e);
RETURN s:x.{256..}(.e);
RETURN s:x.{257..}(.e);

-- An invalid identity target is terminal rather than a successful traversal.
LET $number = 1;
RETURN $number.{2};
LET $numbers = [1];
RETURN $numbers.{2};
LET $mixed = [s:x, 1];
RETURN $mixed.{2};
RETURN s:x.{2}(.k);

-- Equal numeric values can have different representations in a mixed target.
LET $result = s:x.{1..3}(.chain(|$value| IF type::is_array($value) {
    [s:x, 1f]
} ELSE {
    [s:x, 1]
}));
RETURN type::is_int($result[1]);
RETURN type::is_float($result[1]);
