/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test +collect modifier respects min_depth bounds"

[[test.results]]
value = "[person:lead_infra, person:dir_platform, person:vp_eng, person:ceo]"

[[test.results]]
value = "[person:vp_eng, person:ceo]"

[[test.results]]
value = "[person:dir_platform, person:vp_eng]"

[[test.results]]
value = "[person:alice, person:dir_platform, person:vp_eng, person:ceo]"

[[test.results]]
value = "[person:dir_platform]"
*/

-- Management chain from alice:
--   depth 1: person:lead_infra
--   depth 2: person:dir_platform
--   depth 3: person:vp_eng
--   depth 4: person:ceo

-- 0: Baseline - all reachable nodes (default min_depth=1)
person:alice.{..+collect}->reports_to->person;

-- 1: Only nodes at depth 3 and beyond
person:alice.{3..+collect}->reports_to->person;

-- 2: Only nodes at depth 2 through 3
person:alice.{2..3+collect}->reports_to->person;

-- 3: Depth 2+ with inclusive (alice at depth 0 included by +inclusive flag)
person:alice.{2..+collect+inclusive}->reports_to->person;

-- 4: Exactly depth 2 only
person:alice.{2..2+collect}->reports_to->person;
