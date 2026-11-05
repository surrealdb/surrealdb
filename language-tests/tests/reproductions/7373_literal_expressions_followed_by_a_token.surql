/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
The literals the report found with spans running into the next token, each
followed by one. Their spans are checked by `literal_spans_end_at_the_literal`
in surrealdb-parser-next, since no statement result shows a span; this file
keeps the inputs in the parser corpus.
"""
issue = 7373

[[test.results]]
value = "NONE"

[[test.results]]
value = "3.5f"

[[test.results]]
value = "true"

[[test.results]]
value = "['x', 15h]"

[[test.results]]
value = "true"

[[test.results]]
value = "false"
*/
LET $v = d"2024-01-01T00:00:00Z";
RETURN 1.5 + 2;
RETURN 98dec = 98dec;
RETURN ["x", 15h];
RETURN d"2024-01-01T00:00:00Z" = $v;
RETURN u"a8f30d8b-db67-47ec-8b38-ef703e05ad1b" = $v;
