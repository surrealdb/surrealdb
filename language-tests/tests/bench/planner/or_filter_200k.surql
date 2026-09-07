/**
[bench]
reason = "A dedupe-heavy top-level OR over two indexed predicates (~133k surviving rows, ~33k satisfying both branches). With bitmap union fusion the two branches drain to doc-ID bitmaps and union structurally; without it the UnionIndexScan k-way merge decodes every branch row and dedupes through a HashSet that grows with the result. Compare medians against main."
run = true
rebuild = false
datasets = { items = "bench/util/planner-items-200k.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count() FROM items WHERE category = 'red' OR flag = true GROUP ALL;
