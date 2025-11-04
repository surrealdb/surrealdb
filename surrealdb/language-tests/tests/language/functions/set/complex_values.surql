/**
[test]
wip = true
reason = "Set literals are not yet supported"
*/

-- Sets with objects
RETURN {{ foo: 1 }, { foo: 2 }, { foo: 1 }};

-- Sets with record IDs
RETURN {person:john, person:jane, person:john};

-- Union with objects
RETURN set::union({{ foo: 1 }, { foo: 2 }}, {{ foo: 2 }, { foo: 3 }});

-- Intersection with objects
RETURN set::intersect({{ foo: 1 }, { foo: 2 }}, {{ foo: 2 }, { foo: 3 }});

-- Contains with record ID
RETURN set::contains({person:john, person:jane}, person:john);
RETURN set::contains({person:john, person:jane}, person:alice);

-- Sets with different number types (should deduplicate based on value equality)
RETURN type::set([1d, 1f, 1h, 1d]);

