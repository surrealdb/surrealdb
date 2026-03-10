/**
[test]

[[test.results]]
value = "{{ foo: 1 }, { foo: 2 }}"

[[test.results]]
value = "{ person: NONE }"

[[test.results]]
value = "{{,}, {1,}, {1, 2, 3}, {2,}, {{5,}, {{3, 4},}}}"

[[test.results]]
value = "{{ foo: 1 }, { foo: 2 }, { foo: 3 }}"

[[test.results]]
value = "{{ foo: 2 },}"

[[test.results]]
error = "Incorrect arguments for function set::contains(). Argument 1 was the wrong type. Expected `set` but found `{ person: NONE }`"

[[test.results]]
error = "Incorrect arguments for function set::contains(). Argument 1 was the wrong type. Expected `set` but found `{ person: NONE }`"

[[test.results]]
value = "{1f, 1h, 1d}"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{'param value',}"

[[test.results]]
value = "{'param value', 55}"

[[test.results]]
value = "{[1, 2, 3],}"

[[test.results]]
value = "{[1, 2], [3, 4]}"

*/

-- Sets with objects
RETURN {{ foo: 1 }, { foo: 2 }, { foo: 1 }};

-- Sets with record IDs (they're interpreted as objects as there's no way to tell them apart from objects)
RETURN {person:john, person:jane, person:john};

-- Sets with sets
RETURN {{,}, {1,}, {2,}, {1,2,3}, {{{3,4},},{5,}}};

-- Union with objects
RETURN set::union({{ foo: 1 }, { foo: 2 }}, {{ foo: 2 }, { foo: 3 }});

-- Intersection with objects
RETURN set::intersect({{ foo: 1 }, { foo: 2 }}, {{ foo: 2 }, { foo: 3 }});

-- Contains with record ID
RETURN set::contains({person:john, person:jane}, person:john);
RETURN set::contains({person:john, person:jane}, person:alice);

-- Sets with different number types (should deduplicate based on value equality)
RETURN {1d, 1f, 1h, 1d};

-- Sets with params
DEFINE PARAM $a VALUE "param value";
DEFINE PARAM $b VALUE 55;
RETURN { $a, };
RETURN {$a,$b};

-- Sets with arrays
RETURN { [1, 2, 3], };
RETURN { [1, 2], [3, 4] };
