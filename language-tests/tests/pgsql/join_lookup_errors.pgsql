/**
[env]
namespace = true
database = true
imports = ["pgsql/_join_lookup_errors.surql"]

[test]
reason = "A right input of an indexed join whose computed field can fail is not read through the index: the join reads it whole, so a row no left key reaches raises for every left row count above zero, whether the left input is a statement in FROM or a table read in full, and a join of no left row reads nothing. A right input that is a statement in FROM is read whole too, so its predicate raises on a row no key reaches at 50 left rows as well as at 120."

[[test.results]]
value = "[{ c: 0 }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[{ c: 0, total: NULL }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"
*/

SELECT count(*) AS c FROM (SELECT n, k FROM a ORDER BY n LIMIT 0) l JOIN e r ON l.k = r.k;

SELECT count(*) AS c, sum(r.t) AS total FROM (SELECT n, k FROM a ORDER BY n LIMIT 50) l JOIN e r ON l.k = r.k;

SELECT count(*) AS c, sum(r.t) AS total FROM (SELECT n, k FROM a ORDER BY n LIMIT 120) l JOIN e r ON l.k = r.k;

SELECT count(*) AS c, sum(r.t) AS total FROM f l JOIN e r ON l.k = r.k;

SELECT count(*) AS c, sum(r.t) AS total FROM a l JOIN e r ON l.k = r.k;

SELECT count(*) AS c, sum(r.s) AS total FROM (SELECT n, k FROM a ORDER BY n LIMIT 0) l JOIN (SELECT * FROM h WHERE CAST(s AS INTEGER) > 0) r ON l.k = r.k;

SELECT count(*) AS c, sum(r.s) AS total FROM (SELECT n, k FROM a ORDER BY n LIMIT 50) l JOIN (SELECT * FROM h WHERE CAST(s AS INTEGER) > 0) r ON l.k = r.k;

SELECT count(*) AS c, sum(r.s) AS total FROM (SELECT n, k FROM a ORDER BY n LIMIT 120) l JOIN (SELECT * FROM h WHERE CAST(s AS INTEGER) > 0) r ON l.k = r.k;
