/**
[bench]
reason = "PostgreSQL join of an aggregate's one row to an unindexed right key. Over `held` the join holds the row and streams the right table past it; over `hashed` a field permission predicate gives the right table's enforcement something to evaluate, so the join builds a hash table of the right table whole."
run = true
rebuild = false
datasets = { held = "bench/util/records-10000.surql", hashed = "bench/util/records-10000-hashed-join.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

SELECT s.m, r.id FROM (SELECT max(number) AS m FROM record) s JOIN record r ON s.m = r.number;
