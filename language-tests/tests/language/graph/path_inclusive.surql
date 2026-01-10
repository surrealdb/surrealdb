/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test +inclusive modifier to include starting node in results"

# 0
[[test.results]]
value = "[person:alice, person:lead_infra, person:dir_platform, person:vp_eng, person:ceo]"

# 1
[[test.results]]
value = "['Alice Zhang', 'Priya Patel', 'Sarah Kim', 'Marcus Chen', 'Elena Rodriguez']"

# 2
[[test.results]]
value = "[[person:alice, person:lead_infra, person:dir_platform, person:vp_eng, person:ceo]]"

# 3
[[test.results]]
value = "[person:alice, person:lead_infra, person:dir_platform, person:vp_eng, person:ceo]"

# 4
[[test.results]]
value = "[person:alice, person:lead_infra, person:dir_platform, person:vp_eng]"

# 5
[[test.results]]
value = "[person:alice, person:bob]"
*/

-- 0: Include alice in the collect results
person:alice.{..+collect+inclusive}->reports_to->person;

-- 1: Get names including starting node
person:alice.{..+collect+inclusive}(->reports_to->person).name;

-- 2: Include starting node in path results
person:alice.{..+path+inclusive}->reports_to->person;

-- 3: Shortest path including starting node
person:alice.{..+shortest=person:ceo+inclusive}->reports_to->person;

-- 4: VP shortest path with inclusive
person:alice.{..+shortest=person:vp_eng+inclusive}->reports_to->person;

-- 5: Knows shortest path with inclusive
person:alice.{..+shortest=person:bob+inclusive}->knows->person;

