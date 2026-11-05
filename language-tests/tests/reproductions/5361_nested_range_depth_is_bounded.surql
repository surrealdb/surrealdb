/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
requires = ["scripting"]
reason = "A script that nests ranges inside ranges, with no array or object between the levels, is refused at the value depth limit like any other nested value, rather than overflowing the host stack."
issue = 5361

[[test.results]]
error = """
Problem with embedded script function. An exception occurred: Error converting from js 'Range' into type 'value': The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records
"""

[[test.results]]
error = "Problem with embedded script function. Error converting from js 'Range' into type 'value': The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

*/
RETURN function() { let r = 1; for (let i = 0; i < 100000; i++) { r = new Range(r, 1); } return r; };
-- Levels around the range count toward the limit too.
RETURN function() { let r = 1; for (let i = 0; i < 200; i++) { r = new Range(r, 1); } for (let i = 0; i < 100; i++) { r = [r]; } return r; };
