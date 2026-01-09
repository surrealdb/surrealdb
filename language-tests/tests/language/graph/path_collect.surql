/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test +collect modifier for deduplicated node collection"

[[test.results]]
value = "[person:lead_infra, person:dir_platform, person:vp_eng, person:ceo]"

[[test.results]]
value = "[person:lead_infra, person:dir_platform, person:vp_eng, person:ceo]"

[[test.results]]
value = "['Priya Patel', 'Sarah Kim', 'Marcus Chen', 'Elena Rodriguez']"

[[test.results]]
value = "[person:lead_infra, person:dir_platform, person:vp_eng]"

[[test.results]]
value = "[{ managers: ['Priya Patel', 'Sarah Kim', 'Marcus Chen', 'Elena Rodriguez'], name: 'Alice Zhang' }, { managers: ['Priya Patel', 'Sarah Kim', 'Marcus Chen', 'Elena Rodriguez'], name: 'Bob Martinez' }, { managers: ['Alex Thompson', 'James Wilson', 'Marcus Chen', 'Elena Rodriguez'], name: 'Charlie Brown' }, { managers: ['Alex Thompson', 'James Wilson', 'Marcus Chen', 'Elena Rodriguez'], name: 'Dana Lee' }]"
*/

-- All unique people in alice's reporting chain (deduplicated, ordered by proximity)
person:alice.{..+collect}->reports_to->person;

-- Give me all of the people in alice's reporting chain, not including alice
person:alice.{1..+collect}->reports_to->person;

-- All managers above alice with names
person:alice.{..+collect}(->reports_to->person).name;

-- Collect with bounded depth
person:alice.{1..3+collect}->reports_to->person;

-- Collect all managers for level-5 employees
SELECT name, @.{..+collect}(->reports_to->person).name AS managers FROM person WHERE level = 5;

