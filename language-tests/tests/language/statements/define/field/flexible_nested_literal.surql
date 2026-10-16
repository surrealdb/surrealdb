/**
[test]
reason = """
Field-wide FLEXIBLE relaxes nested objects inside literal types, arrays, and options. Without it,
the kind implies no definition for its elements, because its literal member's values are not the
elements of its `array<object>` member, so the literal's interior is judged by coercion alone, as
a literal kind's is, and the same value is admitted."""

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
match = '''$result[0].various == { data: { objects: [{ random: 'data' }] }, more_data: { maybe_object: { more: 'data' } } }'''

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
