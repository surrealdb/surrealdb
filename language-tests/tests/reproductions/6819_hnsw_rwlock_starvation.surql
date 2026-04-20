/**
[test]
run = false
issue = 6819
reason = "HNSW vector search starvation (Tokio RwLock read guard held across await) is reproduced with concurrent async Rust tests — see GitHub issue #6819 and idx/planner/executor. Not expressible as a deterministic single-threaded SurrealQL script."
*/

RETURN "6819";
