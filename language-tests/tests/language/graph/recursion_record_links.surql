/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test record link recursion (non-graph edges) via parent/children fields"

# 0
[[test.results]]
value = "org:platform"

# 1
[[test.results]]
value = "org:company"

# 2
[[test.results]]
value = "location:usa"

# 3
[[test.results]]
value = "[org:engineering]"

# 4
[[test.results]]
value = "[org:infrastructure]"

# 5
[[test.results]]
value = "category:tech"

# 6
[[test.results]]
value = "[category:btree]"

# 7
[[test.results]]
value = "'Acme Corp'"

# 8
[[test.results]]
value = "{ name: 'United States', type: 'country' }"
*/

-- 0: Traverse org hierarchy via parent field (single hop)
org:infrastructure.parent;

-- 1: Traverse org hierarchy via parent field (all ancestors)
org:infrastructure.{..}.parent;

-- 2: Traverse location hierarchy (all ancestors)
location:sf_hq.{..}.parent;

-- 3: Children traversal (single level)
org:company.children;

-- 4: Children traversal (all descendants)
org:company.{..}.children;

-- 5: Category tree parent traversal
category:btree.{..}.parent;

-- 6: Category tree children traversal
category:tech.{..}.children;

-- 7: Parentheses for path isolation then get names
org:infrastructure.{..}(.parent).name;

-- 8: Multiple field access after recursion
location:sf_hq.{..}(.parent).{name, type};

