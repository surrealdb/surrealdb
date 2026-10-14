/**
[env]
namespace = true
database = true
imports = ["pgsql/_join_lookup_errors.surql"]

[test]
reason = "A fallible computed field of the right input of an indexed join is derived for the rows a left key reaches while the lookup serves, and for every right row once the build table is read: a row no key reaches raises only after the lookup has switched to the build table, and a join of no left row reads nothing. The left input is a table read in full as well as a statement in FROM. A right input that is a statement in FROM is read whole for every left row count above zero, so its predicate raises on a row no key reaches at 50 left rows as well as at 120."

[[test.results]]
value = "[{ c: 0 }]"

[[test.results]]
value = "[{ c: 50, total: 1275 }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[{ c: 50, total: 1275 }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[{ c: 0, total: 0 }]"

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
