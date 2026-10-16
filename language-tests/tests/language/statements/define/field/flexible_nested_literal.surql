/**
[test]
reason = """
Field-wide FLEXIBLE relaxes nested objects inside literal types, arrays, and options. Without it,
the kind implies no definition for its elements, because its literal member's values are not the
elements of its `array<object>` member, and the literal's interior is judged by the kinds it names:
`various.data.objects` is an `array<object>`, whose objects refuse a key no definition names."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: is_flexible:1, various: { data: { objects: [{ random: 'data' }] }, more_data: { maybe_object: { more: 'data' } } } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'various.data.objects[0].random', but no such field exists for table 'not_flexible'"

*/

DEFINE TABLE is_flexible SCHEMAFULL;

DEFINE FIELD various ON is_flexible TYPE string | array<object> | {
    data: {
        objects: array<object>
    },
    more_data: {
        maybe_object: option<object>
    }
} FLEXIBLE;

CREATE is_flexible:1 SET various = {
    data: {
        objects: [
            { random: "data" }
        ]
    },
    more_data: {
        maybe_object: {
            more: "data"
        }
    }
};

DEFINE TABLE not_flexible SCHEMAFULL;

DEFINE FIELD various ON not_flexible TYPE string | array<object> | {
    data: {
        objects: array<object>
    },
    more_data: {
        maybe_object: option<object>
    }
};

CREATE not_flexible SET various = {
    data: {
        objects: [
            { random: "data" }
        ]
    },
    more_data: {
        maybe_object: {
            more: "data"
        }
    }
};
