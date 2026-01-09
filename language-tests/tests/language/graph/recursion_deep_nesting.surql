/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test deep nesting with 8-level category hierarchy"

[[test.results]]
value = "NONE"

[[test.results]]
value = "category:tech"

[[test.results]]
value = "category:tech"

[[test.results]]
value = "'Technology'"

[[test.results]]
value = "{ id: category:tech, name: 'Technology' }"
*/

-- Traverse exactly 8 levels up from btree to tech
category:btree.{8}.parent;

-- Traverse 1-8 levels
category:btree.{1..8}.parent;

-- Unbounded traversal from deepest node
category:btree.{..}.parent;

-- Get names at each level
category:btree.{..}(.parent).name;

-- Get id and name at each level
category:btree.{..}(.parent).{id, name};

