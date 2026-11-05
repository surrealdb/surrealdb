/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
requires = ["scripting"]
reason = """
A range passed into a scripting function arrives as a `Range` instance, not `undefined`: `start` and `end` hold the bounds (`undefined` when a side is unbounded), `startInclusive` and `endInclusive` say whether each is part of the range, and `toString` renders the SurrealQL range. Returning the instance, or one built with `new Range(start, end)`, yields the range again.
"""
issue = 5361

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ name: 'Range', ret: d'2025-01-25T00:00:00Z'..d'2025-02-02T00:00:00Z', string: "d'2025-01-25T00:00:00Z'..d'2025-02-02T00:00:00Z'", type: 'object' }"""

[[test.results]]
value = "{ name: 'Range', ret: 1..3, string: '1..3', type: 'object' }"

[[test.results]]
value = "{ name: 'Range', ret: 1..=3, string: '1..=3', type: 'object' }"

[[test.results]]
value = "{ name: 'Range', ret: ..3, string: '..3', type: 'object' }"

[[test.results]]
value = "1f..5f"

[[test.results]]
value = "1f>..=5f"

[[test.results]]
value = "[1f, 4f, false, true]"

*/
DEFINE FUNCTION OVERWRITE fn::test($aa: any) {
    RETURN function($aa) {
        let [aa] = arguments;
        return { type: typeof aa, name: aa?.constructor.name, string: `${aa}`, ret: aa };
    };
};
fn::test(d'2025-01-25'..d'2025-02-02');
fn::test(1..3);
fn::test(1..=3);
fn::test(..3);
RETURN function() { return new Range(1, 5); };
RETURN function() { return new Range(1, 5, false, true); };
RETURN function(1>..=4) { let [r] = arguments; return [r.start, r.end, r.startInclusive, r.endInclusive]; };
