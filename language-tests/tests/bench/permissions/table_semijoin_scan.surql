/**
[env]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bench" }

[bench]
reason = "Table SELECT permission written as a membership test against a subquery, versus the same policy as a scalar predicate — the subquery is evaluated once per record, so the allow-list is scanned 10,000 times to answer a question whose result cannot vary: it reads no field of the row it guards. The delta is what an executor that hoisted it out of the row loop (or planned it as a semi-join) would recover. Both arms return the same 10k rows"
run = true
rebuild = false
datasets = { scalar = "bench/util/records-10000-perms-where.surql", semijoin = "bench/util/records-10000-perms-semijoin.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id FROM record;
