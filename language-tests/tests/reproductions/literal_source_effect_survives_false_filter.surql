/**
[env]
namespace = true
database = true

[test]
reason = "a WHERE false over a literal source collapses the source, which must not drop the evaluation of a literal that writes"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"
*/

-- The rows of `[fn::writer()]` are a literal's, so `WHERE false` may replace
-- the whole source with the empty literal. Evaluating that literal is the
-- write, though, and a statement that writes is observed whether or not a
-- row survives the filter, so the collapse is only for an inert literal.

DEFINE FUNCTION fn::writer() { CREATE counter SET n = 1; RETURN 1; };

SELECT * FROM [fn::writer()] WHERE false;

SELECT count() FROM counter GROUP ALL;
