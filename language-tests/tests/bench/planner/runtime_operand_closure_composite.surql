/**
[bench]
reason = "One read per key of a closure over a three-column index, a captured value in the first column and two conjuncts beside it, as a RAG service reads field values."
run = true
rebuild = false
datasets = { rows_5000 = "bench/util/runtime-operand-5000.surql", rows_50000 = "bench/util/runtime-operand-50000.surql" }
warmup = "2s"
sample-size = 30
measurement-time = "10s"

[test]
run = false
*/

LET $tenant = 'acme';
LET $pack = 'p1';
array::map([{ scope: 's21', generation: 4321, doc: 'd4321' }], |$key| (SELECT VALUE id FROM field_value WHERE tenant = $tenant AND scope = $key.scope AND generation = $key.generation AND doc = $key.doc AND pack = $pack));
