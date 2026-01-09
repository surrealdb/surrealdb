/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test record link recursion (non-graph edges) via parent/children fields"

[[test.results]]
value = "org:platform"

[[test.results]]
value = "org:company"

[[test.results]]
value = "location:usa"

[[test.results]]
value = "[org:engineering]"

[[test.results]]
value = "[org:infrastructure]"

[[test.results]]
value = "category:tech"

[[test.results]]
value = "[category:btree]"

[[test.results]]
value = "'Acme Corp'"

[[test.results]]
value = "{ name: 'United States', type: 'country' }"
*/

-- Traverse org hierarchy via parent field (single hop)
org:infrastructure.parent;

-- Traverse org hierarchy via parent field (all ancestors)
org:infrastructure.{..}.parent;

-- Traverse location hierarchy (all ancestors)
location:sf_hq.{..}.parent;

-- Children traversal (single level)
org:company.children;

-- Children traversal (all descendants)
org:company.{..}.children;

-- Category tree parent traversal
category:btree.{..}.parent;

-- Category tree children traversal
category:tech.{..}.children;

-- Parentheses for path isolation then get names
org:infrastructure.{..}(.parent).name;

-- Multiple field access after recursion
location:sf_hq.{..}(.parent).{name, type};

