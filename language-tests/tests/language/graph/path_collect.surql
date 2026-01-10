/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test +collect modifier for deduplicated node collection"

# 0
[[test.results]]
value = "[person:lead_infra, person:dir_platform, person:vp_eng, person:ceo]"

# 1
[[test.results]]
value = "[person:lead_infra, person:dir_platform, person:vp_eng, person:ceo]"

# 2
[[test.results]]
value = "['Priya Patel', 'Sarah Kim', 'Marcus Chen', 'Elena Rodriguez']"

# 3
[[test.results]]
value = "[person:lead_infra, person:dir_platform, person:vp_eng]"

# 4
[[test.results]]
value = "[{ managers: ['Priya Patel', 'Sarah Kim', 'Marcus Chen', 'Elena Rodriguez'], name: 'Alice Zhang' }, { managers: ['Priya Patel', 'Sarah Kim', 'Marcus Chen', 'Elena Rodriguez'], name: 'Bob Martinez' }, { managers: ['Alex Thompson', 'James Wilson', 'Marcus Chen', 'Elena Rodriguez'], name: 'Charlie Brown' }, { managers: ['Alex Thompson', 'James Wilson', 'Marcus Chen', 'Elena Rodriguez'], name: 'Dana Lee' }]"
*/

-- 0: All unique people in alice's reporting chain (deduplicated, ordered by proximity)
person:alice.{..+collect}->reports_to->person;

-- 1: Give me all of the people in alice's reporting chain, not including alice
person:alice.{1..+collect}->reports_to->person;

-- 2: All managers above alice with names
person:alice.{..+collect}(->reports_to->person).name;

-- 3: Collect with bounded depth
person:alice.{1..3+collect}->reports_to->person;

-- 4: Collect all managers for level-5 employees
SELECT name, @.{..+collect}(->reports_to->person).name AS managers FROM person WHERE level = 5;

