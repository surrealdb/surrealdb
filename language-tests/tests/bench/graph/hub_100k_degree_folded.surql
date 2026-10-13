/**
[bench]
reason = "The degree of a single 100000-degree vertex after folding: with a clean delta tail the answer is the sum of the chunk headers' entry counts — no entries decoded, no ids resolved, no keys iterated. The O(1)-ish degree #564 asks for."
run = true
rebuild = false
graph-fold = true
datasets = { hub = "bench/util/graph-hub-100k.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count(->knows) AS degree FROM ONLY person:1;
