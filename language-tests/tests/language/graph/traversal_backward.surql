/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test backward edge traversal (<-) patterns"

# 0
[[test.results]]
value = "[person:alice, person:bob]"

# 1
[[test.results]]
value = "[person:bob, person:charlie]"

# 2
[[test.results]]
value = "['Alice Zhang', 'Bob Martinez']"

# 3
[[test.results]]
value = "[person:lead_infra]"

# 4
[[test.results]]
value = "[{ direct_reports: ['Alice Zhang', 'Bob Martinez'], id: person:lead_infra, name: 'Priya Patel' }]"
*/

-- 0: Single hop backward: who reports to lead_infra?
person:lead_infra<-reports_to<-person;

-- 1: Single hop backward: who knows alice? (incoming edges)
person:alice<-knows<-person;

-- 2: Backward traversal with field access
person:lead_infra<-reports_to<-person.name;

-- 3: Find all direct reports (people who report to someone)
person:dir_platform<-reports_to<-person;

-- 4: Backward traversal in SELECT
SELECT id, name, <-reports_to<-person.name AS direct_reports FROM person:lead_infra;

