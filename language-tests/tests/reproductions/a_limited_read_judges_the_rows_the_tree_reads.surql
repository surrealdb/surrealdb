/**
[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 120000

[test]
reason = """
	A LIMITed read of a record range, a union of records or a table reads up to
	its limit and its START and stops, in the read's own batching: a row whose
	computed field fails is judged only where it is read, so one past the limit
	never fails the statement, and LIMIT 0 reads nothing. Failing rows sit
	either side of the storage batch of 500 records.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "1"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "1"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

[[test.results]]
value = "499"

[[test.results]]
value = "500"

[[test.results]]
value = "1"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

[[test.results]]
value = "499"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

[[test.results]]
value = "499"

[[test.results]]
value = "500"

[[test.results]]
value = "1"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

[[test.results]]
value = "499"
























































































*/
DEFINE TABLE f2; DEFINE FIELD c ON f2 COMPUTED IF a = 2 { <int> 'x' } ELSE { a };
FOR $i IN 1..=1100 { CREATE type::record('f2', $i) SET a = $i RETURN NONE; };
DEFINE TABLE f500; DEFINE FIELD c ON f500 COMPUTED IF a = 500 { <int> 'x' } ELSE { a };
FOR $i IN 1..=1100 { CREATE type::record('f500', $i) SET a = $i RETURN NONE; };
DEFINE TABLE f501; DEFINE FIELD c ON f501 COMPUTED IF a = 501 { <int> 'x' } ELSE { a };
FOR $i IN 1..=1100 { CREATE type::record('f501', $i) SET a = $i RETURN NONE; };
DEFINE TABLE f1001; DEFINE FIELD c ON f1001 COMPUTED IF a = 1001 { <int> 'x' } ELSE { a };
FOR $i IN 1..=1100 { CREATE type::record('f1001', $i) SET a = $i RETURN NONE; };
RETURN (SELECT * FROM f2:1.. LIMIT 0 START 0).len();
RETURN (SELECT * FROM f2:1.. LIMIT 1 START 0).len();
RETURN (SELECT * FROM f2:1.. LIMIT 499 START 1).len();
RETURN (SELECT * FROM f2:1.. LIMIT 500 START 0).len();
RETURN (SELECT * FROM f2:1, f2:2, f2:500, f2:501, f2:1001 LIMIT 1).len();
RETURN (SELECT * FROM f2:1, f2:2, f2:500, f2:501, f2:1001 LIMIT 0).len();
RETURN (SELECT * FROM f2 LIMIT 1).len();
RETURN (SELECT * FROM f2 LIMIT 499 START 1).len();
RETURN (SELECT * FROM f500:1.. LIMIT 0 START 0).len();
RETURN (SELECT * FROM f500:1.. LIMIT 1 START 0).len();
RETURN (SELECT * FROM f500:1.. LIMIT 499 START 1).len();
RETURN (SELECT * FROM f500:1.. LIMIT 500 START 0).len();
RETURN (SELECT * FROM f500:1, f500:2, f500:500, f500:501, f500:1001 LIMIT 1).len();
RETURN (SELECT * FROM f500:1, f500:2, f500:500, f500:501, f500:1001 LIMIT 0).len();
RETURN (SELECT * FROM f500 LIMIT 1).len();
RETURN (SELECT * FROM f500 LIMIT 499 START 1).len();
RETURN (SELECT * FROM f501:1.. LIMIT 0 START 0).len();
RETURN (SELECT * FROM f501:1.. LIMIT 1 START 0).len();
RETURN (SELECT * FROM f501:1.. LIMIT 499 START 1).len();
RETURN (SELECT * FROM f501:1.. LIMIT 500 START 0).len();
RETURN (SELECT * FROM f501:1, f501:2, f501:500, f501:501, f501:1001 LIMIT 1).len();
RETURN (SELECT * FROM f501:1, f501:2, f501:500, f501:501, f501:1001 LIMIT 0).len();
RETURN (SELECT * FROM f501 LIMIT 1).len();
RETURN (SELECT * FROM f501 LIMIT 499 START 1).len();
RETURN (SELECT * FROM f1001:1.. LIMIT 0 START 0).len();
RETURN (SELECT * FROM f1001:1.. LIMIT 1 START 0).len();
RETURN (SELECT * FROM f1001:1.. LIMIT 499 START 1).len();
RETURN (SELECT * FROM f1001:1.. LIMIT 500 START 0).len();
RETURN (SELECT * FROM f1001:1, f1001:2, f1001:500, f1001:501, f1001:1001 LIMIT 1).len();
RETURN (SELECT * FROM f1001:1, f1001:2, f1001:500, f1001:501, f1001:1001 LIMIT 0).len();
RETURN (SELECT * FROM f1001 LIMIT 1).len();
RETURN (SELECT * FROM f1001 LIMIT 499 START 1).len();
