/**
[bench]
reason = "A ~1%-selectivity filtered traversal over a 20000-edge hub: the inline-property target case, where the `inline` dataset evaluates `score > 989` against the adjacency payloads and fetches only the ~200 matching edge records, while the `plain` twin fetches all 20000 to evaluate the same predicate."
run = true
rebuild = false
datasets = { inline = "bench/util/graph-filtered-inline.surql", plain = "bench/util/graph-filtered-plain.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE ->(knows WHERE score > 989) FROM person:1;
