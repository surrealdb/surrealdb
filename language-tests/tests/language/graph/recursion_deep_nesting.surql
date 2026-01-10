/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test deep nesting with 8-level category hierarchy"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
value = "category:tech"

# 2
[[test.results]]
value = "category:tech"

# 3
[[test.results]]
value = "'Technology'"

# 4
[[test.results]]
value = "{ id: category:tech, name: 'Technology' }"
*/

-- 0: Traverse exactly 8 levels up from btree to tech
category:btree.{8}.parent;

-- 1: Traverse 1-8 levels
category:btree.{1..8}.parent;

-- 2: Unbounded traversal from deepest node
category:btree.{..}.parent;

-- 3: Get names at each level
category:btree.{..}(.parent).name;

-- 4: Get id and name at each level
category:btree.{..}(.parent).{id, name};

