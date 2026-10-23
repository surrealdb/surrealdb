/**
[env]
namespace = true
database = true
# Extra timeout: every scan below reads 3000 records, which splits into one morsel per record when
# the suite forces the smallest morsels, and SurrealKV does not finish that in the default timeout
# while the suite runs in parallel.
timeout = 30000

[test]
reason = "the rows a LET binds are read by the statements after it in the order they were bound: a comparison with an ordered result and a loop that runs an effect for each row both see the order of the scan, whatever width the scan runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"
*/

FOR $i IN 0..3000 { CREATE type::record('t', $i) SET k = $i % 7, n = $i };
DEFINE SEQUENCE seq;
LET $s = SELECT VALUE id FROM t;
RETURN $s == (SELECT VALUE id FROM t ORDER BY id);
LET $o = SELECT VALUE n FROM t WHERE k = 3;
RETURN $o == (SELECT VALUE n FROM t WHERE k = 3 ORDER BY id);
FOR $r IN $s { CREATE type::record('log', sequence::nextval('seq')) SET src = $r };
RETURN (SELECT VALUE src FROM log ORDER BY id) == $s;
