/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A range passed into a scripting function.
"""
issue = 5361

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ name: NONE, ret: NONE, string: 'undefined', type: 'undefined' }"

[[test.results]]
value = "{ name: NONE, ret: NONE, string: 'undefined', type: 'undefined' }"
*/
DEFINE FUNCTION OVERWRITE fn::test($aa: any) {
    RETURN function($aa) {
        let [aa] = arguments;
        return { type: typeof aa, name: aa?.constructor.name, string: `${aa}`, ret: aa };
    };
};
fn::test(d'2025-01-25'..d'2025-02-02');
fn::test(1..3);
