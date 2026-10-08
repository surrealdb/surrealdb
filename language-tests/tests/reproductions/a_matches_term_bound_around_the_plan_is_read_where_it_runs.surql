/**
[env]
namespace = true
database = true

[test]
reason = """
A MATCHES whose query is a parameter bound around the statement holding it —
a closure's argument, a FOR variable — searches for the value the binding holds
when the statement runs: the statement is planned each time it runs, with the
binding in place. An outer variable of the same
name is not the query, and a closure argument no outer variable names still
finds the index. A query parameter bound by the statement's context is resolved
as before.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: d:1, t: 'hello world' }]"

[[test.results]]
value = "[{ id: d:2, t: 'other thing' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[d:1]"

[[test.results]]
value = "[d:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[d:1], [d:2]]"

[[test.results]]
value = "[d:2]"

[[test.results]]
value = "[[d:1], [d:2]]"

[[test.results]]
value = "[d:2]"

[[test.results]]
value = "[d:2]"

*/

DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft ON d FIELDS t FULLTEXT ANALYZER simple BM25;
CREATE d:1 SET t = 'hello world';
CREATE d:2 SET t = 'other thing';
LET $s = 'hello';
SELECT VALUE id FROM d WHERE t @@ $s;
RETURN (|$s| (SELECT VALUE id FROM d WHERE t @@ $s))('other');
LET $f = |$s| (SELECT VALUE id FROM d WHERE t @@ $s);
RETURN array::map(['hello', 'other'], $f);
RETURN (|$q| (SELECT VALUE id FROM d WHERE t @@ $q))('other');
RETURN array::map(['hello', 'other'], |$q| (SELECT VALUE id FROM d WHERE t @@ $q));
FOR $s IN ['other'] { RETURN (SELECT VALUE id FROM d WHERE t @@ $s) };
RETURN (FOR $s IN ['other'] { RETURN (SELECT VALUE id FROM d WHERE t @@ $s) });
