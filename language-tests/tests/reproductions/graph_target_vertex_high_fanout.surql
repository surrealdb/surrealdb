/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "High-fanout `->edge->vertex` traversal: stress the fast-path scan loop past the default `scan_batch_size` (1000) so the inside-cursor `rid_batch` flush threshold fires at least once. Smoke-test only; it does not exercise the legacy-format chunk-resume branch because `store_edges_data` only writes pointer-format keys today (any pre-existing legacy variants are deleted on write). A targeted test for the chunked-legacy drain would need a Rust harness that seeds legacy keys via direct KV writes."

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
value = "1500"

# 2
[[test.results]]
value = "1500"
*/

-- 0: Setup. One source vertex with 1500 outbound edges, each to a fresh
--    target vertex. Explicit numeric IDs keep iteration order stable
--    and deterministic across backends.
{
    CREATE person:source;
    FOR $i IN 1..=1500 {
        LET $target = type::record("person", $i);
        LET $edge = type::record("knows", $i);
        CREATE $target;
        RELATE person:source -> $edge -> $target;
    };
    NONE
};

-- 1: Fast-path `->edge->vertex` collapsed scan crosses the
--    `scan_batch_size` (= 1000) threshold mid-traversal and must yield
--    every target without dropping any.
array::len((SELECT VALUE ->knows->person FROM ONLY person:source));

-- 2: Same query via the edge-only `->edge` mode (`TargetId`) followed
--    by a separate vertex hop, exercising the slow-path chain. Result
--    count must match (1) to confirm both paths agree at scale.
array::len((SELECT VALUE ->knows.out FROM ONLY person:source));
